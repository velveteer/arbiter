{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- | OpenAPI 3 description of 'Arbiter.Servant.API.ArbiterAPI'. The route types
-- define paths, methods, parameters, bodies, responses, and status codes.
-- @RegistryToAPI@ expands to the server route tree and includes each queue by
-- its registry name with its payload and result schemas.
--
-- Each payload requires a 'ToSchema' instance. For generic JSON, use
-- @deriving anyclass (ToSchema)@. This module defines a 'Data.Aeson.Value'
-- instance for free-form payloads.
--
-- Handwritten schemas are present for handwritten JSON encodings. Each schema
-- applies the applicable record constructor. Missing, misordered, or incorrect
-- field types cause a compile error. Generic encodings use generic schemas.
module Arbiter.Servant.OpenApi
  ( -- * The document
    openApiSpec

    -- * Serving it
  , OpenApiAPI
  , openApiServer
  ) where

import Arbiter.Core.Concurrency.Spec (ConcurrencyKey (ConcurrencyKey))
import Arbiter.Core.Health (PgTableHealth)
import Arbiter.Core.Job.Archive qualified as Archive
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Dedup (DedupKey (IgnoreDuplicate))
import Arbiter.Core.Job.TraceContext (toTraceContext)
import Arbiter.Core.Job.Types
  ( JobRead
  , JobStatus
  , PayloadKeys (PayloadKeys)
  , Stored
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
import Arbiter.Core.Operations (QueueStats (QueueStats))
import Arbiter.Core.RateLimit.Spec (RateLimitKey (RateLimitKey))
import Arbiter.Core.Sql.Jobs
  ( ArchiveSortColumn
  , DLQSortColumn
  , JobSortColumn
  , SortDir
  , archiveSortColumnName
  , dlqSortColumnName
  , jobSortColumnName
  , sortDirName
  )
import Arbiter.Core.Worker (WorkerHealth, workerHealthToText)
import Arbiter.Servant.API (ArbiterAPI)
import Arbiter.Servant.Types
import Data.Aeson (ToJSON (..), Value (Null))
import Data.HashMap.Strict.InsOrd qualified as InsOrd
import Data.HashSet.InsOrd qualified as InsOrdSet
import Data.Int (Int32, Int64)
import Data.Map.Strict (Map)
import Data.Maybe (fromMaybe, isJust)
import Data.OpenApi
  ( Definitions
  , Info (..)
  , MediaTypeObject (..)
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
import GHC.TypeLits (ErrorMessage (Text), TypeError)
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

-- | Group operations by the first path segment below the mount point. Queue
-- routes use the queue name. Other routes use the feature name.
sectioned :: OpenApi -> OpenApi
sectioned spec =
  spec
    { _openApiPaths =
        InsOrd.mapWithKey (tagPath . section) (InsOrd.adjust describeStream eventStreamPath (_openApiPaths spec))
    , _openApiTags = InsOrdSet.fromList (map describeSection sections)
    }
  where
    sections = map section (InsOrd.keys (_openApiPaths spec))

-- | The path's section, the segment after the @\/api\/v1@ mount point.
section :: FilePath -> TagName
section path =
  case drop (length mountSegments) (filter (not . T.null) (T.splitOn "/" (T.pack path))) of
    name : _ -> name
    [] -> "api"

-- | The segments the API is mounted under.
mountSegments :: [Text]
mountSegments = ["api", "v1"]

-- | The event stream's @Raw@ route, which 'toOpenApi' gives no operation.
eventStreamPath :: FilePath
eventStreamPath = T.unpack (T.concat (map ("/" <>) (mountSegments <> ["events", "stream"])))

describeStream :: PathItem -> PathItem
describeStream item = item {_pathItemGet = Just streamOperation}

-- | Description of the continuous @text/event-stream@ response.
streamOperation :: Operation
streamOperation =
  mempty
    { _operationSummary = Just "Server-sent stream of job events"
    , _operationDescription =
        Just
          "Streams an event per insert, update, delete and dead-letter, as they happen. \
          \Each event names its queue and the job id. A dead-letter event carries the id the \
          \job had in its queue and sets dlq. The stream starts with one \"connected\" event. \
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
                      , _responseContent =
                          InsOrd.singleton "text/event-stream" mempty {_mediaTypeObjectSchema = Just (Inline jobEventSchema)}
                      }
                )
          }
    }

-- | The JSON in one job event's data line.
jobEventSchema :: Schema
jobEventSchema =
  (schemaOver ["event"] props)
    { _schemaDescription = Just "One job event. The connected and disabled events carry no job fields."
    }
  where
    props =
      [ ("event", Inline (stringEnum ["job_inserted", "job_updated", "job_deleted", "job_dlq", "connected", "disabled"]))
      , ("table", Inline mempty {_schemaType = Just OpenApiString, _schemaDescription = Just "The queue name."})
      ,
        ( "job_id"
        , Inline
            mempty
              { _schemaType = Just OpenApiInteger
              , _schemaDescription = Just "The job id. A DLQ row gives the id of the job it holds."
              }
        )
      ,
        ( "dlq"
        , Inline mempty {_schemaType = Just OpenApiBoolean, _schemaDescription = Just "The event comes from the DLQ table."}
        )
      , ("message", Inline mempty {_schemaType = Just OpenApiString})
      ]

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
    tagged = fmap (\operation -> operation {_operationTags = InsOrdSet.insert name (_operationTags operation)})

-- | A section's tag entry, a heading with a sentence under it.
describeSection :: TagName -> Tag
describeSection name =
  Tag
    { _tagName = name
    , _tagDescription = Just (fromMaybe (queueDescription name) (lookup name sectionDescriptions))
    , _tagExternalDocs = Nothing
    }
  where
    queueDescription queueName = "Jobs, dead letters, archive, groups and stats for the " <> queueName <> " queue."

-- | What each schema-wide section is for. A section not named here is a queue.
sectionDescriptions :: [(TagName, Text)]
sectionDescriptions =
  [ ("queues", "The registered queues, their counters, and pausing them.")
  , ("cron", "Cron schedules, their overrides, and out-of-band runs.")
  , ("workers", "The worker registry, and pausing a pool.")
  , ("rate-limits", "Token-bucket policies, their live buckets, overrides, token grants and pruning.")
  , ("concurrency", "Concurrency pools, their live keys, overrides and pruning.")
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
  \types. Every payload and result below is the queue's real schema. The server ships \
  \no authentication. Put it behind your own."

-- ---------------------------------------------------------------------------
-- Schema builders
--
-- A property combines a field name with the schema of its type. The applicative
-- instance combines properties into one field list.
-- ---------------------------------------------------------------------------

-- | Schema fields indexed by the described value type. The value is a runtime
-- phantom. Applying the record constructor checks the field count, order, and
-- types at compile time.
newtype Fields a = Fields (Declare (Definitions Schema) [(Text, Referenced Schema)])

instance Functor Fields where
  fmap _ (Fields declared) = Fields declared

instance Applicative Fields where
  pure _ = Fields (pure [])
  Fields left <*> Fields right = Fields (liftA2 (<>) left right)

-- | One field, named here and typed by the schema of @a@. A 'Maybe' field takes 'opt'.
prop :: forall a. (ToSchema a) => Text -> Fields (NotMaybe a)
prop name = Fields declared
  where
    Fields declared = payloadProp @a name

-- | Refuse a 'Maybe' field in 'prop'.
type family NotMaybe a where
  NotMaybe (Maybe _a) = TypeError ('Text "A Maybe field takes opt, not prop.")
  NotMaybe a = a

-- | One 'Maybe' field, which encodes 'Nothing' as null.
opt :: forall a. (ToSchema a) => Text -> Fields (Maybe a)
opt name = Fields (pure . (,) name . nullable <$> declareSchemaRef (Proxy @a))

-- | One payload field, typed by the payload's own schema.
payloadProp :: forall payload. (ToSchema payload) => Text -> Fields payload
payloadProp name = Fields (pure . (,) name <$> declareSchemaRef (Proxy @payload))

-- | Admit null. A reference or untyped schema takes a null-only branch.
nullable :: Referenced Schema -> Referenced Schema
nullable (Inline schema)
  | isJust (_schemaType schema) =
      Inline schema {_schemaNullable = Just True, _schemaEnum = (<> [Null]) <$> _schemaEnum schema}
nullable ref = Inline mempty {_schemaAnyOf = Just [ref, Inline nullOnly]}

nullOnly :: Schema
nullOnly =
  mempty {_schemaType = Just OpenApiObject, _schemaNullable = Just True, _schemaEnum = Just [Null]}

-- | One patch field, whose value distinguishes an absent field from an explicit null.
patch :: forall a. (ToSchema a) => Text -> Fields (Maybe (Maybe a))
patch name = Just <$> opt @a name

-- | One field with an inline schema for a shape used in one location.
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
    named props = NamedSchema (Just name) (schemaOver (map fst props) props)

schemaOver :: [Text] -> [(Text, Referenced Schema)] -> Schema
schemaOver required props =
  mempty
    { _schemaType = Just OpenApiObject
    , _schemaProperties = InsOrd.fromList props
    , _schemaRequired = required
    }

-- | Qualify a schema name with its payload type. Different payload types use
-- different job definitions. A payload with no name leaves the schema inline.
carrying
  :: forall payload
   . (ToSchema payload)
  => Declare (Definitions Schema) NamedSchema
  -> Declare (Definitions Schema) NamedSchema
carrying = fmap qualify
  where
    qualify (NamedSchema base schema) =
      NamedSchema (liftA2 (\b p -> b <> "_" <> p) base (OpenApi.schemaName (Proxy @payload))) schema

-- | A string schema accepting exactly the names an enum round-trips through.
enumSchema :: forall a p. (Bounded a, Enum a) => (a -> Text) -> p a -> Schema
enumSchema name _ = stringEnum [name value | value <- [minBound .. maxBound :: a]]

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
  toParamSchema = enumSchema sortDirName

-- ---------------------------------------------------------------------------
-- Hand-encoded types
-- ---------------------------------------------------------------------------

-- | A caller-defined payload or result.
instance ToSchema Value where
  declareNamedSchema _ =
    pure . NamedSchema (Just "AnyJson") $
      mempty {_schemaDescription = Just "Caller-defined JSON."}

instance ToSchema JobStatus where
  declareNamedSchema = pure . NamedSchema (Just "JobStatus") . toParamSchema

instance ToSchema WorkerHealth where
  declareNamedSchema = pure . NamedSchema (Just "WorkerHealth") . enumSchema workerHealthToText

instance ToSchema HealthStatus where
  declareNamedSchema = pure . NamedSchema (Just "HealthStatus") . enumSchema healthStatusToText

instance ToSchema DedupKey where
  declareNamedSchema _ =
    closedSchema "DedupKey" $
      -- Use the strategy field to select the constructor for this tagged pair.
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
admissionKeySchema mkKey name =
  closedSchema name (mkKey <$> prop @Text "prefix" <*> prop @Text "suffix")

-- | Fields written by the 'JobRead' encoder. The record
-- constructor checks their types and order. Reconstruct the trace context and
-- payload keys from their flattened fields. The encoder derives @isRollup@.
jobFields :: forall payload. (ToSchema payload) => Fields (JobRead payload)
jobFields =
  Job
    <$> prop @Int64 "primaryKey"
    <*> payloadProp @payload "payload"
    <*> prop @Text "queueName"
    <*> opt @Text "groupKey"
    <*> prop @UTCTime "insertedAt"
    <*> opt @UTCTime "updatedAt"
    <*> prop @Int32 "attempts"
    <*> opt @Text "lastError"
    <*> prop @Int32 "priority"
    <*> opt @UTCTime "lastAttemptedAt"
    <*> opt @UTCTime "notVisibleUntil"
    <*> opt @DedupKey "dedupKey"
    <*> opt @Int32 "maxAttempts"
    <*> opt @Int64 "parentId"
    <*> opt @Value "parentState"
    <*> (toTraceContext <$> opt @Text "traceparent" <*> opt @Text "tracestate")
    <*> prop @Bool "suspended"
    <*> opt @UUID "claimedBy"
    <*> prop @Int64 "claimSeq"
    <*> opt @Int32 "archiveFor"
    <*> ( PayloadKeys
            <$> opt @Text "kind"
            <*> opt @RateLimitKey "rateLimit"
            <*> opt @ConcurrencyKey "concurrency"
        )
    <* prop @Bool "isRollup"

-- | A stored payload documents as the payload it decodes to.
instance (ToSchema payload) => ToSchema (Stored payload) where
  declareNamedSchema _ = declareNamedSchema (Proxy @payload)

instance (ToSchema payload) => ToSchema (JobRead payload) where
  declareNamedSchema _ = carrying @payload $ closedSchema "Job" (jobFields @payload)

instance (ToSchema payload) => ToSchema (ApiJobWithStatus payload) where
  declareNamedSchema _ =
    carrying @payload $
      closedSchema
        "JobWithStatus"
        (jobFields @payload <* prop @JobStatus "status")

instance (ToSchema payload) => ToSchema (ApiJobWrite payload) where
  declareNamedSchema _ =
    carrying @payload
      $ objectSchema "JobWrite" ["payload"]
      $ ( \value group priority visibleAt dedup attempts retention ->
            setArchiveFor retention
              . setMaxAttempts attempts
              . setDedupKey dedup
              . setNotVisibleUntil visibleAt
              . setPriority priority
              . setGroupKey group
              $ defaultJob value
        )
        <$> payloadProp @payload "payload"
        <*> opt @Text "groupKey"
        <*> prop @Int32 "priority"
        <*> opt @UTCTime "notVisibleUntil"
        <*> opt @DedupKey "dedupKey"
        <*> opt @Int32 "maxAttempts"
        <*> opt @Int32 "archiveFor"

instance (ToSchema payload) => ToSchema (DLQ.DLQJob payload) where
  declareNamedSchema _ =
    carrying @payload
      $ closedSchema "DLQEntry"
      $ DLQ.DLQJob
        <$> prop @Int64 "dlqPrimaryKey"
        <*> prop @UTCTime "failedAt"
        <*> prop @(JobRead payload) "jobSnapshot"

instance (ToSchema payload) => ToSchema (Archive.ArchiveJob payload) where
  declareNamedSchema _ =
    carrying @payload
      $ closedSchema "ArchiveEntry"
      $ Archive.ArchiveJob
        <$> prop @Int64 "archivePrimaryKey"
        <*> prop @UTCTime "completedAt"
        <*> prop @(JobRead payload) "jobSnapshot"
        <*> opt @Value "result"

instance ToSchema JobLease where
  declareNamedSchema _ = closedSchema "JobLease" leaseFields

-- | Ack request with an optional queue result.
instance (ToSchema result) => ToSchema (AckRequest result) where
  declareNamedSchema _ =
    carrying @result $
      objectSchema
        "AckRequest"
        leaseRequired
        (AckRequest <$> leaseFields <*> opt @result "result")

instance ToSchema ExtendRequest where
  declareNamedSchema _ =
    closedSchema "ExtendRequest" (ExtendRequest <$> leaseFields <*> prop @Double "seconds")

-- | Lease fields at the top level of the request body.
leaseFields :: Fields JobLease
leaseFields = JobLease <$> prop @Int64 "claimSeq" <*> prop @UUID "claimedBy"

leaseRequired :: [Text]
leaseRequired = ["claimSeq", "claimedBy"]

instance (ToSchema payload) => ToSchema (PayloadEdit payload) where
  declareNamedSchema _ =
    carrying @payload $ closedSchema "PayloadEdit" (PayloadEdit <$> payloadProp @payload "payload")

instance ToSchema AddTokensRequest where
  declareNamedSchema _ = closedSchema "AddTokensRequest" (AddTokensRequest <$> prop @Double "tokens")

instance ToSchema GroupSummary where
  declareNamedSchema _ =
    closedSchema "GroupSummary" $
      GroupSummary
        <$> prop @Text "groupKey"
        <*> prop @Int64 "jobCount"
        <*> prop @Int64 "readyCount"
        <*> opt @UTCTime "nextDue"
        <*> opt @UTCTime "inFlightUntil"
        <*> prop @Bool "inFlight"
        <*> opt @Int64 "headJobId"
        <*> opt @JobStatus "headStatus"
        <*> prop @Bool "headBlocked"

instance ToSchema MaintenanceResponse where
  declareNamedSchema _ =
    closedSchema "MaintenanceResponse" $
      MaintenanceResponse <$> prop @(Map Text Int64) "ops" <*> prop @[Text] "failed"

instance ToSchema CronScheduleView where
  declareNamedSchema _ = do
    row <- declareSchemaRef (Proxy @CronScheduleRow)
    NamedSchema _ added <- closedSchema "" (opt @UTCTime "nextRunAt")
    pure . NamedSchema (Just "CronScheduleView") $
      mempty
        { _schemaAllOf = Just [row, Inline added]
        , _schemaDescription =
            Just "A schedule row plus the next tick it fires at. It is null when the schedule is disabled or has no next tick."
        }

instance ToSchema QueueStats where
  declareNamedSchema _ =
    closedSchema "QueueStats" $
      QueueStats
        <$> prop @Int64 "totalJobs"
        <*> prop @Int64 "readyJobs"
        <*> prop @Int64 "inFlightJobs"
        <*> prop @Int64 "scheduledJobs"
        <*> prop @Int64 "backoffJobs"
        <*> prop @Int64 "throttledJobs"
        <*> prop @Int64 "suspendedJobs"
        <*> prop @Int64 "cancelledJobs"
        <*> prop @Int64 "exhaustedJobs"
        <*> prop @Int64 "blockedJobs"
        <*> opt @Double "oldestReadyAgeSeconds"
        <*> opt @Double "oldestInFlightAgeSeconds"
        <*> prop @Int64 "dlqJobs"
        <*> prop @(Map Text Int64) "kindCounts"
        <*> prop @(Map Text Int64) "dlqKindCounts"

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
        <*> opt @Double "fillFraction"
        <*> prop @UTCTime "lastRefill"

instance ToSchema RateLimitPolicyUpdate where
  declareNamedSchema _ =
    -- A patch field is Maybe (Maybe a). An absent field leaves the override alone.
    -- A null clears it. The schema describes the inner value.
    objectSchema "RateLimitPolicyUpdate" [] $
      RateLimitPolicyUpdate
        <$> patch @Double "overrideMaxTokens"
        <*> patch @Double "overrideRefillAmount"
        <*> patch @Double "overrideInterval"

instance ToSchema ConcurrencyKeyView where
  declareNamedSchema _ =
    closedSchema "ConcurrencyKeyView" $
      ConcurrencyKeyView
        <$> prop @Text "key"
        <*> prop @Text "prefix"
        <*> prop @Int32 "inFlight"
        <*> prop @Int32 "effectiveLimit"
        <*> opt @Double "fillFraction"

instance ToSchema ConcurrencyPolicyUpdate where
  declareNamedSchema _ =
    objectSchema "ConcurrencyPolicyUpdate" [] $
      ConcurrencyPolicyUpdate <$> patch @Int32 "overrideLimit"

-- ---------------------------------------------------------------------------
-- Generic schemas, matching the generic JSON these types derive
-- ---------------------------------------------------------------------------

-- | A generic schema under a name of its own. A type applied to its payload is named
-- after its constructor alone. 'carrying' adds the payload back.
renamed
  :: forall a
   . (GToSchema (Rep a), Generic a, Typeable a)
  => Text
  -> Proxy a
  -> Declare (Definitions Schema) NamedSchema
renamed name proxy = rename <$> generic proxy
  where
    rename (NamedSchema _ schema) = NamedSchema (Just name) schema

-- | A generic schema whose optional fields accept null, as generic JSON encodes 'Nothing'.
generic
  :: forall a
   . (GToSchema (Rep a), Generic a, Typeable a)
  => Proxy a
  -> Declare (Definitions Schema) NamedSchema
generic proxy = orNull <$> genericDeclareNamedSchema defaultSchemaOptions proxy
  where
    orNull (NamedSchema name schema) =
      NamedSchema name schema {_schemaProperties = InsOrd.mapWithKey (optional schema) (_schemaProperties schema)}
    optional schema field ref
      | field `elem` _schemaRequired schema = ref
      | otherwise = nullable ref

instance ToSchema QueueRow where declareNamedSchema = generic
instance ToSchema WorkerRow where declareNamedSchema = generic
instance ToSchema CronScheduleRow where declareNamedSchema = generic
instance ToSchema CronScheduleUpdate where declareNamedSchema = generic
instance ToSchema PgDbHealth where declareNamedSchema = generic
instance ToSchema PgTableHealth where declareNamedSchema = generic
instance ToSchema RateLimitPolicyView where declareNamedSchema = generic
instance ToSchema ConcurrencyPolicyView where declareNamedSchema = generic

instance ToSchema ClaimRequest where declareNamedSchema = generic
instance ToSchema BatchDeleteRequest where declareNamedSchema = generic
instance ToSchema BatchDeleteResponse where declareNamedSchema = generic
instance ToSchema StatsResponse where declareNamedSchema = generic
instance ToSchema AllStatsResponse where declareNamedSchema = generic
instance ToSchema QueuesResponse where declareNamedSchema = generic
instance ToSchema CronSchedulesResponse where declareNamedSchema = generic
instance ToSchema WorkersResponse where declareNamedSchema = generic
instance ToSchema RateLimitPoliciesResponse where declareNamedSchema = generic
instance ToSchema RateLimitBucketsResponse where declareNamedSchema = renamed "RateLimitBucketsResponse"
instance ToSchema RateLimitResetResponse where declareNamedSchema = generic
instance ToSchema ConcurrencyPoliciesResponse where declareNamedSchema = generic
instance ToSchema ConcurrencyKeysResponse where declareNamedSchema = renamed "ConcurrencyKeysResponse"
instance ToSchema ConcurrencyReconcileResponse where declareNamedSchema = generic
instance ToSchema HealthResponse where declareNamedSchema = generic
instance ToSchema LivenessResponse where declareNamedSchema = generic
instance ToSchema RescheduleRequest where declareNamedSchema = generic
instance ToSchema AddTokensResponse where declareNamedSchema = generic
instance ToSchema PruneResponse where declareNamedSchema = generic

instance ToSchema GroupsResponse where
  declareNamedSchema _ = closedSchema "GroupsResponse" (pageFields @GroupSummary)

-- | The keys every paged response shares.
pageFields :: forall a. (ToSchema [a]) => Fields (Page a)
pageFields = Page <$> prop @[a] "items" <*> prop @Int "total" <*> prop @Int "offset" <*> prop @Int "limit"

instance (ToSchema payload) => ToSchema (JobsResponse payload) where
  declareNamedSchema _ =
    carrying @payload
      $ closedSchema "JobsResponse"
      $ JobsResponse @payload
        <$> pageFields
        <*> prop @(Map Int64 Int64) "childCounts"
        <*> prop @[Int64] "pausedParents"
        <*> prop @(Map Int64 Int64) "dlqChildCounts"

instance (ToSchema payload) => ToSchema (JobResponse (JobRead payload)) where
  declareNamedSchema = carrying @payload . renamed "JobResponse"

instance (ToSchema payload) => ToSchema (ClaimResponse payload) where
  declareNamedSchema = carrying @payload . renamed "ClaimResponse"

instance (ToSchema payload) => ToSchema (JobResponse (ApiJobWithStatus payload)) where
  declareNamedSchema = carrying @payload . renamed "JobWithStatusResponse"

instance (ToSchema payload) => ToSchema (BatchInsertRequest payload) where
  declareNamedSchema = carrying @payload . renamed "BatchInsertRequest"

instance (ToSchema payload) => ToSchema (BatchInsertResponse payload) where
  declareNamedSchema = carrying @payload . renamed "BatchInsertResponse"

instance (ToSchema payload) => ToSchema (DLQResponse payload) where
  declareNamedSchema _ = carrying @payload $ closedSchema "DLQResponse" (pageFields @(DLQ.DLQJob (Stored payload)))

instance (ToSchema payload) => ToSchema (ArchiveResponse payload) where
  declareNamedSchema _ = carrying @payload $ closedSchema "ArchiveResponse" (pageFields @(Archive.ArchiveJob (Stored payload)))
