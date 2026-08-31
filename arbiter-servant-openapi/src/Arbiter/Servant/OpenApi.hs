{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
-- A type named only in an instance head does not count as a use, so the imports the
-- schema instances below need read as redundant.
{-# OPTIONS_GHC -Wno-orphans -Wno-unused-imports #-}

-- | An OpenAPI 3 description of 'Arbiter.Servant.API.ArbiterAPI', derived from
-- the route types alone. @RegistryToAPI@ expands to the same route tree the
-- server is built from, so walking it yields every queue under its own
-- 'SpecName', carrying its own payload and result schemas. Paths, methods,
-- parameters, request and response bodies and status codes all come from the
-- API, and the document cannot describe a route the server does not serve.
--
-- A payload therefore needs a 'ToSchema' instance, the same bargain any
-- servant-openapi3 user makes. @deriving anyclass (ToSchema)@ covers a payload
-- whose JSON is generic; 'Data.Aeson.Value' has one here already, for a queue
-- whose payload is free-form.
--
-- The schemas below are the one hand-written part, and only where the JSON
-- itself is hand-written. Each applies its record's own constructor, so a
-- field that is missing, misordered or of the wrong type stops compiling
-- rather than shipping a wrong document. A type whose encoding is generic
-- takes the generic schema, which follows the same field names.
module Arbiter.Servant.OpenApi
  ( -- * The document
    openApiSpec

    -- * Serving it
  , OpenApiAPI
  , openApiServer
  ) where

import Arbiter.Core.Concurrency.Spec (ConcurrencyKey (ConcurrencyKey))
import Arbiter.Core.Concurrency.Stats
  ( ConcurrencyKeyView
  , ConcurrencyPolicyUpdate
  , ConcurrencyPolicyView
  )
import Arbiter.Core.CronSchedule (CronScheduleRow, CronScheduleUpdate)
import Arbiter.Core.Health (PgDbHealth, PgTableHealth)
import Arbiter.Core.Job.Archive qualified as Archive
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Dedup (DedupKey (IgnoreDuplicate))
import Arbiter.Core.Job.TraceContext (toTraceContext)
import Arbiter.Core.Job.Types
  ( AdmissionKeys (AdmissionKeys)
  , JobRead
  , JobStatus
  , defaultJob
  , jobStatusToText
  , setArchiveFor
  , setDedupKey
  , setGroupKey
  , setMaxAttempts
  , setNotVisibleUntil
  , setPriority
  )
import Arbiter.Core.Job.Types.Internal (JobRecord (Job))
import Arbiter.Core.Operations (QueueOverview, QueueStats)
import Arbiter.Core.Queues (QueueRow)
import Arbiter.Core.RateLimit.Spec (RateLimitKey (RateLimitKey))
import Arbiter.Core.RateLimit.Stats
  ( RateLimitBucketView
  , RateLimitPolicyUpdate
  , RateLimitPolicyView
  )
import Arbiter.Core.Sql.Jobs
  ( ArchiveSortColumn
  , DLQSortColumn
  , JobSortColumn
  , SortDir
  , archiveSortColumnName
  , dlqSortColumnName
  , jobSortColumnName
  , sortDirSql
  )
import Arbiter.Core.Worker (WorkerHealth, WorkerRow)
import Arbiter.Servant.API (ArbiterAPI)
import Arbiter.Servant.Types
import Control.Applicative (liftA2)
import Data.Aeson (ToJSON (..), Value)
import Data.HashMap.Strict.InsOrd qualified as InsOrd
import Data.HashSet.InsOrd qualified as InsOrdSet
import Data.Int (Int32, Int64)
import Data.Map.Strict (Map)
import Data.Maybe (fromMaybe, isJust)
import Data.OpenApi
  ( Definitions
  , Info (..)
  , MediaTypeObject
  , NamedSchema (..)
  , OpenApi (..)
  , OpenApiType (..)
  , Operation (..)
  , PathItem (..)
  , Referenced (Inline)
  , Response (..)
  , Responses (..)
  , Schema (..)
  , Tag (..)
  , TagName
  , ToParamSchema (..)
  , ToSchema (..)
  , declareSchemaRef
  , defaultSchemaOptions
  , genericDeclareNamedSchema
  )
import Data.OpenApi qualified as OpenApi
import Data.OpenApi.Declare (Declare)
import Data.OpenApi.Internal.Schema (GToSchema)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)
import Data.Typeable (Typeable)
import Data.UUID.Types (UUID)
import GHC.Generics (Generic, Rep)
import Servant (Get, JSON, Server, (:>))
import Servant.OpenApi (HasOpenApi, toOpenApi)

-- | The document's own route, for mounting beside 'Arbiter.Servant.API.ArbiterAPI'.
type OpenApiAPI = "openapi.json" :> Get '[JSON] Value

-- | Serve the description of a registry's API.
openApiServer :: forall registry. (HasOpenApi (ArbiterAPI registry)) => Server OpenApiAPI
openApiServer = pure (openApiSpec @registry)

-- | The description of a registry's API, from its route types.
openApiSpec :: forall registry. (HasOpenApi (ArbiterAPI registry)) => Value
openApiSpec = toJSON (sectioned described)
  where
    described =
      (toOpenApi (Proxy @(ArbiterAPI registry)))
        { _openApiInfo =
            mempty
              { _infoTitle = "Arbiter"
              , _infoVersion = "v1"
              , _infoDescription = Just apiDescription
              }
        }

-- | Group the operations into sections, so a reader meets one queue's routes at a
-- time rather than one flat list. The section is the segment under the mount point,
-- which is a queue's name for its own routes and the feature's name for the rest.
sectioned :: OpenApi -> OpenApi
sectioned spec =
  spec
    { _openApiPaths = InsOrd.mapWithKey (\path -> tagPath (section path) . describeRaw) (_openApiPaths spec)
    , _openApiTags = InsOrdSet.fromList (map describeSection sections)
    }
  where
    sections = ordNub (map section (InsOrd.keys (_openApiPaths spec)))
    ordNub = foldr (\x acc -> x : filter (/= x) acc) []

-- | The path's section: the segment after the @\/api\/v1@ mount point.
section :: FilePath -> TagName
section path = case drop (length mountSegments) (segmentsOf path) of
  name : _ -> T.pack name
  [] -> T.pack "api"
  where
    segmentsOf = filter (not . null) . foldr split [[]]
    split c acc@(current : rest)
      | c == '/' = [] : acc
      | otherwise = (c : current) : rest
    split _ [] = []

-- | The segments the API is mounted under, which name no section of their own.
mountSegments :: [FilePath]
mountSegments = ["api", "v1"]

-- | A @Raw@ route has no type to describe it, so 'toOpenApi' leaves its path with no
-- operations at all. The event stream is the only one, and it is a real endpoint, so
-- it gets the one hand-written operation in this document.
describeRaw :: PathItem -> PathItem
describeRaw item
  | any ($ item) operationFields = item
  | otherwise = item {_pathItemGet = Just streamOperation}
  where
    operationFields =
      map
        (isJust .)
        [ _pathItemGet
        , _pathItemPut
        , _pathItemPost
        , _pathItemDelete
        , _pathItemOptions
        , _pathItemHead
        , _pathItemPatch
        , _pathItemTrace
        ]

-- | The event stream, which answers with an endless @text/event-stream@ rather than a
-- body a schema could describe.
streamOperation :: Operation
streamOperation =
  mempty
    { _operationSummary = Just "Server-sent stream of job events"
    , _operationDescription =
        Just
          "Streams an event per insert, update, delete and dead-letter, as they happen. \
          \Sends a keepalive comment every 15 seconds. A server with streaming switched \
          \off answers one \"disabled\" event and closes."
    , _operationResponses =
        mempty
          { _responsesResponses =
              InsOrd.singleton
                200
                ( Inline
                    mempty
                      { _responseDescription = "An event stream."
                      , _responseContent = InsOrd.singleton "text/event-stream" mempty
                      }
                )
          }
    }

-- | Put every operation on a path into that path's section.
tagPath :: TagName -> PathItem -> PathItem
tagPath name item =
  item
    { _pathItemGet = tagged (_pathItemGet item)
    , _pathItemPut = tagged (_pathItemPut item)
    , _pathItemPost = tagged (_pathItemPost item)
    , _pathItemDelete = tagged (_pathItemDelete item)
    , _pathItemOptions = tagged (_pathItemOptions item)
    , _pathItemHead = tagged (_pathItemHead item)
    , _pathItemPatch = tagged (_pathItemPatch item)
    , _pathItemTrace = tagged (_pathItemTrace item)
    }
  where
    tagged = fmap (\op -> op {_operationTags = InsOrdSet.insert name (_operationTags op)})

-- | A section's own entry, so the reader gets a heading with a sentence under it.
describeSection :: TagName -> Tag
describeSection name =
  Tag
    { _tagName = name
    , _tagDescription = Just (fromMaybe (queueDescription name) (lookup name sectionDescriptions))
    , _tagExternalDocs = Nothing
    }
  where
    queueDescription q = "Jobs, dead letters, archive and stats for the " <> q <> " queue."

-- | What each schema-wide section is for. A section not named here is a queue.
sectionDescriptions :: [(TagName, Text)]
sectionDescriptions =
  [ ("queues", "The registered queues, their counters, and pausing them.")
  , ("cron", "Cron schedules, their overrides, and out-of-band runs.")
  , ("workers", "The worker registry, and pausing a pool.")
  , ("rate-limits", "Token-bucket policies, their live buckets, and overrides.")
  , ("concurrency", "Concurrency pools, their live keys, and overrides.")
  , ("maintenance", "The sweep a worker pool's reaper runs, on demand.")
  , ("events", "A server-sent stream of job events.")
  , ("health", "Liveness and readiness.")
  ]

apiDescription :: Text
apiDescription =
  "The Arbiter job queue over HTTP. Each registered queue has its own section, and a \
  \service in any language can use all three sides of it: enqueue jobs, run them by \
  \claiming a lease and acking, nacking or extending it, and operate the queue itself. \
  \The schema-wide sections cover queues, cron, workers, rate limits, concurrency, \
  \maintenance and health.\n\nThe document is derived from the server's own route \
  \types, so every payload and result below is the queue's real schema rather than an \
  \opaque object. The server ships no authentication: put it behind your own."

-- ---------------------------------------------------------------------------
-- Schema builders
--
-- A property names a field and pulls in the schema of the type behind it, so the two
-- travel together. Under 'Ap' that pair is a monoid, which makes a run of fields one
-- expression and a schema one line per field.
-- ---------------------------------------------------------------------------

-- | A schema's fields, carrying the type of the value they describe. That value is
-- phantom at run time; its type is not. Applying a record's own constructor to a run of
-- fields is what holds the schema to the shape the encoder writes: a field that is
-- missing, misordered or of the wrong type stops compiling here.
newtype Fields a = Fields (Declare (Definitions Schema) [(Text, Referenced Schema)])

instance Functor Fields where
  fmap _ (Fields d) = Fields d

instance Applicative Fields where
  pure _ = Fields (pure [])
  Fields f <*> Fields x = Fields (liftA2 (<>) f x)

-- | One field, named here and typed by the schema of @a@.
prop :: forall a. (ToSchema a) => Text -> Fields a
prop name = Fields (pure . (,) name <$> declareSchemaRef (Proxy @a))

-- | One patch field, whose value distinguishes an absent field from an explicit null.
patch :: forall a. (ToSchema a) => Text -> Fields (Maybe (Maybe a))
patch name = Just <$> prop @(Maybe a) name

-- | One field whose schema is written out rather than referenced, for a shape that
-- appears in a single place.
inlineProp :: forall a. Text -> Schema -> Fields a
inlineProp name schema = Fields (pure [(name, Inline schema)])

-- | An object schema over some fields, naming which of them a value must carry.
objectSchema :: Text -> [Text] -> Fields a -> Declare (Definitions Schema) NamedSchema
objectSchema name required (Fields declared) =
  NamedSchema (Just name) . schemaOver required <$> declared

-- | 'objectSchema' where every field is required.
closedSchema :: Text -> Fields a -> Declare (Definitions Schema) NamedSchema
closedSchema name (Fields declared) = named <$> declared
  where
    named ps = NamedSchema (Just name) (schemaOver (map fst ps) ps)

schemaOver :: [Text] -> [(Text, Referenced Schema)] -> Schema
schemaOver required props =
  mempty
    { _schemaType = Just OpenApiObject
    , _schemaProperties = InsOrd.fromList props
    , _schemaRequired = required
    }

-- | A schema name qualified by the payload it carries. Two queues with different
-- payload types describe different jobs, so they cannot share one definition.
carrying :: forall payload. (ToSchema payload) => Text -> Text
carrying base = maybe base ((base <> "_") <>) (OpenApi.schemaName (Proxy @payload))

-- | A string schema accepting exactly the names an enum round-trips through.
enumSchema :: forall a p. (Bounded a, Enum a) => (a -> Text) -> p a -> Schema
enumSchema name _ = stringEnum [name x | x <- [minBound .. maxBound :: a]]

-- | A string schema accepting exactly the given values.
stringEnum :: [Text] -> Schema
stringEnum values =
  mempty {_schemaType = Just OpenApiString, _schemaEnum = Just (map toJSON values)}

-- ---------------------------------------------------------------------------
-- Parameter schemas
-- ---------------------------------------------------------------------------

instance ToParamSchema JobStatus where
  toParamSchema = enumSchema jobStatusToText

instance ToParamSchema JobSortColumn where
  toParamSchema = enumSchema jobSortColumnName

instance ToParamSchema DLQSortColumn where
  toParamSchema = enumSchema dlqSortColumnName

instance ToParamSchema ArchiveSortColumn where
  toParamSchema = enumSchema archiveSortColumnName

instance ToParamSchema SortDir where
  toParamSchema = enumSchema sortDirSql

-- ---------------------------------------------------------------------------
-- Hand-encoded types
-- ---------------------------------------------------------------------------

-- | A caller-defined payload or result.
instance ToSchema Value where
  declareNamedSchema _ =
    pure . NamedSchema (Just "AnyJson") $
      mempty
        { _schemaType = Just OpenApiObject
        , _schemaDescription = Just "Caller-defined JSON."
        }

instance ToSchema JobStatus where
  declareNamedSchema = pure . NamedSchema (Just "JobStatus") . toParamSchema

instance ToSchema WorkerHealth where
  declareNamedSchema _ =
    pure (NamedSchema (Just "WorkerHealth") (stringEnum ["live", "stale", "draining"]))

instance ToSchema HealthStatus where
  declareNamedSchema _ = pure (NamedSchema (Just "HealthStatus") (stringEnum ["ok", "down"]))

instance ToSchema DedupKey where
  declareNamedSchema _ =
    closedSchema "DedupKey" $
      -- A tagged pair rather than a record, so the strategy picks the constructor.
      (\key _strategy -> IgnoreDuplicate key)
        <$> prop @Text "key"
        <*> inlineProp @Text "strategy" (stringEnum ["ignore", "replace"])

instance ToSchema RateLimitKey where
  declareNamedSchema _ = admissionKeySchema RateLimitKey "RateLimitKey"

instance ToSchema ConcurrencyKey where
  declareNamedSchema _ = admissionKeySchema ConcurrencyKey "ConcurrencyKey"

-- | A gate key, split into the policy prefix and the per-job suffix.
admissionKeySchema
  :: (Text -> Text -> a) -> Text -> Declare (Definitions Schema) NamedSchema
admissionKeySchema mk name =
  closedSchema name (mk <$> prop @Text "prefix" <*> prop @Text "suffix")

-- | The fields 'Arbiter.Servant.Types.apiJobPairs' writes. The record's own constructor
-- checks them: the encoder is flatter than the record, so the trace context and the
-- admission keys are each rebuilt from the two fields they are spread across, and
-- @isRollup@ rides along with no argument of its own because the encoder derives it.
jobFields :: forall payload. (ToSchema payload) => Fields (JobRead payload)
jobFields =
  Job
    <$> prop @Int64 "primaryKey"
    <*> prop @payload "payload"
    <*> prop @Text "queueName"
    <*> prop @(Maybe Text) "groupKey"
    <*> prop @UTCTime "insertedAt"
    <*> prop @(Maybe UTCTime) "updatedAt"
    <*> prop @Int32 "attempts"
    <*> prop @(Maybe Text) "lastError"
    <*> prop @Int32 "priority"
    <*> prop @(Maybe UTCTime) "lastAttemptedAt"
    <*> prop @(Maybe UTCTime) "notVisibleUntil"
    <*> prop @(Maybe DedupKey) "dedupKey"
    <*> prop @(Maybe Int32) "maxAttempts"
    <*> prop @(Maybe Int64) "parentId"
    <*> prop @(Maybe Value) "parentState"
    <*> (toTraceContext <$> prop @(Maybe Text) "traceparent" <*> prop @(Maybe Text) "tracestate")
    <*> prop @Bool "suspended"
    <*> prop @(Maybe UUID) "claimedBy"
    <*> prop @Int64 "claimSeq"
    <*> prop @(Maybe Int32) "archiveFor"
    <*> (AdmissionKeys <$> prop @(Maybe RateLimitKey) "rateLimit" <*> prop @(Maybe ConcurrencyKey) "concurrency")
    <* prop @Bool "isRollup"

instance (ToSchema payload) => ToSchema (ApiJob payload) where
  declareNamedSchema _ = objectSchema (carrying @payload "Job") [] (jobFields @payload)

instance (ToSchema payload) => ToSchema (ApiJobWithStatus payload) where
  declareNamedSchema _ =
    objectSchema
      (carrying @payload "JobWithStatus")
      []
      (jobFields @payload <* prop @JobStatus "status")

instance (ToSchema payload) => ToSchema (ApiJobWrite payload) where
  declareNamedSchema _ =
    objectSchema (carrying @payload "JobWrite") ["payload"] $
      ( \value group priority visibleAt dedup attempts retention ->
          setArchiveFor retention
            . setMaxAttempts attempts
            . setDedupKey dedup
            . setNotVisibleUntil visibleAt
            . setPriority priority
            . setGroupKey group
            $ defaultJob value
      )
        <$> prop @payload "payload"
        <*> prop @(Maybe Text) "groupKey"
        <*> prop @Int32 "priority"
        <*> prop @(Maybe UTCTime) "notVisibleUntil"
        <*> prop @(Maybe DedupKey) "dedupKey"
        <*> prop @(Maybe Int32) "maxAttempts"
        <*> prop @(Maybe Int32) "archiveFor"

instance (ToSchema payload) => ToSchema (ApiDLQJob payload) where
  declareNamedSchema _ =
    closedSchema (carrying @payload "DLQEntry") $
      DLQ.DLQJob
        <$> prop @Int64 "dlqPrimaryKey"
        <*> prop @UTCTime "failedAt"
        <*> (unApiJob <$> prop @(ApiJob payload) "jobSnapshot")

instance (ToSchema payload) => ToSchema (ApiArchiveJob payload) where
  declareNamedSchema _ =
    objectSchema
      (carrying @payload "ArchiveEntry")
      ["archivePrimaryKey", "completedAt", "jobSnapshot"]
      $ Archive.ArchiveJob
        <$> prop @Int64 "archivePrimaryKey"
        <*> prop @UTCTime "completedAt"
        <*> (unApiJob <$> prop @(ApiJob payload) "jobSnapshot")
        <*> prop @(Maybe Value) "result"

-- | The claimed jobs arrive under @jobs@, not under the field name behind it.
instance (ToSchema payload) => ToSchema (ClaimResponse payload) where
  declareNamedSchema _ =
    closedSchema (carrying @payload "ClaimResponse") $
      ClaimResponse <$> prop @[ApiJob payload] "jobs"

instance ToSchema ClaimRequest where
  declareNamedSchema _ =
    objectSchema "ClaimRequest" [] $
      ClaimRequest <$> prop @(Maybe Int) "maxJobs" <*> prop @(Maybe Double) "leaseSeconds"

instance ToSchema JobLease where
  declareNamedSchema _ = closedSchema "JobLease" leaseFields

-- | The result a caller may hand back with the ack, of the queue's own result type.
instance (ToSchema result) => ToSchema (AckRequest result) where
  declareNamedSchema _ =
    objectSchema
      (carrying @result "AckRequest")
      leaseRequired
      (AckRequest <$> leaseFields <*> prop @(Maybe result) "result")

instance ToSchema ExtendRequest where
  declareNamedSchema _ =
    closedSchema "ExtendRequest" (ExtendRequest <$> leaseFields <*> prop @Double "seconds")

-- | The lease a caller proves it holds a job with, spread into the request body rather
-- than nested under a field of its own.
leaseFields :: Fields JobLease
leaseFields = JobLease <$> prop @Int64 "claimSeq" <*> prop @UUID "claimedBy"

leaseRequired :: [Text]
leaseRequired = ["claimSeq", "claimedBy"]

instance ToSchema MaintenanceResponse where
  declareNamedSchema _ =
    closedSchema "MaintenanceResponse" $
      MaintenanceResponse <$> prop @(Map Text Int64) "ops" <*> prop @[Text] "failed"

instance ToSchema CronScheduleView where
  declareNamedSchema _ = do
    row <- declareSchemaRef (Proxy @CronScheduleRow)
    NamedSchema _ added <- objectSchema "" [] (prop @(Maybe UTCTime) "nextRunAt")
    pure . NamedSchema (Just "CronScheduleView") $
      mempty
        { _schemaAllOf = Just [row, Inline added]
        , _schemaDescription =
            Just "A schedule row plus the next tick it fires at, absent when it is disabled."
        }

instance ToSchema QueueOverview where
  declareNamedSchema _ =
    closedSchema "QueueOverview" $
      QueueOverview
        <$> prop @Text "queue"
        <*> prop @QueueStats "stats"
        <*> prop @Bool "paused"
        <*> prop @Int64 "workersLive"
        <*> prop @Int64 "workersPaused"

instance ToSchema RateLimitBucketView where
  declareNamedSchema _ =
    closedSchema "RateLimitBucketView" $
      RateLimitBucketView
        <$> prop @Text "key"
        <*> prop @Text "prefix"
        <*> prop @Double "tokens"
        <*> prop @Double "maxTokens"
        <*> prop @(Maybe Double) "fillFraction"
        <*> prop @UTCTime "lastRefill"

instance ToSchema RateLimitPolicyUpdate where
  declareNamedSchema _ =
    -- A patch field is Maybe (Maybe a): absent leaves the override alone, null clears
    -- it. Only the inner value has a shape, so the schema describes that and the outer
    -- Maybe is what a present field means.
    objectSchema "RateLimitPolicyUpdate" [] $
      RateLimitPolicyUpdate
        <$> patch @Double "overrideMaxTokens"
        <*> patch @Double "overrideRefillAmount"
        <*> patch @Double "overrideRefillIntervalSecs"

instance ToSchema ConcurrencyKeyView where
  declareNamedSchema _ =
    closedSchema "ConcurrencyKeyView" $
      ConcurrencyKeyView
        <$> prop @Text "key"
        <*> prop @Text "prefix"
        <*> prop @Int32 "inFlight"
        <*> prop @Int32 "effectiveLimit"
        <*> prop @(Maybe Double) "fillFraction"

instance ToSchema ConcurrencyPolicyUpdate where
  declareNamedSchema _ =
    objectSchema "ConcurrencyPolicyUpdate" [] $
      ConcurrencyPolicyUpdate <$> patch @Int32 "overrideLimit"

-- ---------------------------------------------------------------------------
-- Generic schemas, matching the generic JSON these types derive
-- ---------------------------------------------------------------------------

-- | A generic schema under a name of its own. A type applied to its payload is named
-- after its constructor alone, with 'carrying' adding the payload back where two
-- queues would otherwise collide on one definition.
renamed
  :: forall a
   . (GToSchema (Rep a), Generic a, Typeable a)
  => Text
  -> Proxy a
  -> Declare (Definitions Schema) NamedSchema
renamed name proxy = rename <$> genericDeclareNamedSchema defaultSchemaOptions proxy
  where
    rename (NamedSchema _ schema) = NamedSchema (Just name) schema

instance ToSchema QueueStats
instance ToSchema QueueRow
instance ToSchema WorkerRow
instance ToSchema CronScheduleRow
instance ToSchema CronScheduleUpdate
instance ToSchema PgDbHealth
instance ToSchema PgTableHealth
instance ToSchema RateLimitPolicyView
instance ToSchema ConcurrencyPolicyView

instance ToSchema BatchDeleteRequest
instance ToSchema BatchDeleteResponse
instance ToSchema StatsResponse
instance ToSchema AllStatsResponse
instance ToSchema QueuesResponse
instance ToSchema CronSchedulesResponse
instance ToSchema WorkersResponse
instance ToSchema RateLimitPoliciesResponse
instance ToSchema RateLimitBucketsResponse
instance ToSchema RateLimitResetResponse
instance ToSchema ConcurrencyPoliciesResponse
instance ToSchema ConcurrencyKeysResponse
instance ToSchema ConcurrencyReconcileResponse
instance ToSchema HealthResponse
instance ToSchema LivenessResponse

instance (ToSchema payload) => ToSchema (JobsResponse payload) where
  declareNamedSchema = renamed (carrying @payload "JobsResponse")

instance (ToSchema payload) => ToSchema (JobResponse (ApiJob payload)) where
  declareNamedSchema = renamed (carrying @payload "JobResponse")

instance (ToSchema payload) => ToSchema (JobResponse (ApiJobWithStatus payload)) where
  declareNamedSchema = renamed (carrying @payload "JobWithStatusResponse")

instance (ToSchema payload) => ToSchema (BatchInsertRequest payload) where
  declareNamedSchema = renamed (carrying @payload "BatchInsertRequest")

instance (ToSchema payload) => ToSchema (BatchInsertResponse payload) where
  declareNamedSchema = renamed (carrying @payload "BatchInsertResponse")

instance (ToSchema payload) => ToSchema (DLQResponse payload) where
  declareNamedSchema = renamed (carrying @payload "DLQResponse")

instance (ToSchema payload) => ToSchema (ArchiveResponse payload) where
  declareNamedSchema = renamed (carrying @payload "ArchiveResponse")
