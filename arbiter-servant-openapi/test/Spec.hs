{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

import Arbiter.Core.Concurrency.Stats (ConcurrencyPolicyUpdate (..))
import Arbiter.Core.CronSchedule (CronScheduleUpdate (..))
import Arbiter.Core.Job.Archive (ArchiveJob (..))
import Arbiter.Core.Job.DLQ (DLQJob (..))
import Arbiter.Core.Job.Status (JobStatus (InFlight))
import Arbiter.Core.Job.Types (JobRead, PayloadKeys (..), defaultJob)
import Arbiter.Core.Job.Types.Internal (JobRecord (..))
import Arbiter.Core.Operations (QueueStats (..))
import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Core.RateLimit.Stats (RateLimitPolicyUpdate (..))
import Arbiter.Servant.Types
  ( ApiJobWithStatus (..)
  , ArchiveResponse
  , DLQResponse
  , GroupsResponse
  , JobsResponse (..)
  , Page (..)
  )
import Data.Aeson (Object, ToJSON, Value (Array, Object, String), toJSON)
import Data.Aeson.Key (fromText, toText)
import Data.Aeson.KeyMap (elems, keys)
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Char (isAlphaNum, isAsciiLower, isAsciiUpper, isDigit)
import Data.HashMap.Strict.InsOrd qualified as InsOrd
import Data.List ((\\))
import Data.OpenApi (Schema (..), ToSchema, toSchema)
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime (..), fromGregorian)
import Test.Tasty (defaultMain, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

import Arbiter.Servant.OpenApi (openApiSpec)

main :: IO ()
main =
  defaultMain $
    testGroup
      "schemas describe the encodings"
      [ testCase "queues with unnamed payload types keep their own job schemas" $
          payloadTypes (openApiSpec @'[Queue "a" Int, Queue "b" Text]) @?= ["integer", "string"]
      , testCase "every path has an operation" $
          pathsWithoutOperation (openApiSpec @'[Queue "a" Int]) @?= []
      , testCase "every operation has a summary" $
          [name | (name, op) <- operations twoQueues, not (KeyMap.member "summary" op)] @?= []
      , testCase "operation ids are present and unique across queues" $
          let ids = [opId | (_, op) <- operations twoQueues, Just (String opId) <- [KeyMap.lookup "operationId" op]]
           in (length ids, length (distinct ids)) @?= (length (operations twoQueues), length (operations twoQueues))
      , testCase "operation ids keep queue names that differ only in case or separator apart" $
          let doc = openApiSpec @'[Queue "fooBar" Int, Queue "foobar" Text, Queue "foo-bar" Bool, Queue "foo_bar" Double]
              ids = [opId | (_, op) <- operations doc, Just (String opId) <- [KeyMap.lookup "operationId" op]]
              folded = map (T.filter isAlphaNum) ids
           in (length (distinct ids), length (distinct folded)) @?= (length (operations doc), length (operations doc))
      , testCase "operation ids are identifiers" $
          let doc = openApiSpec @'[Queue "foo-bar" Int, Queue "foo.bar" Text]
           in [opId | (_, op) <- operations doc, Just (String opId) <- [KeyMap.lookup "operationId" op], not (T.all identChar opId)]
                @?= []
      , testCase "a down readiness answer declares its body" $
          responseContent "get /api/v1/health" "503" twoQueues @?= responseContent "get /api/v1/health" "200" twoQueues
      , testCase "a lease route declares its refusal" $
          responseCodes "post /api/v1/a/jobs/{id}/ack" twoQueues @?= ["204", "400", "404", "409"]
      , testGroup
          "handwritten schemas name the keys the encoding sends"
          [ testCase "JobWithStatus" $ keyDrift ApiJobWithStatus {ajwsJob = job, ajwsStatus = InFlight} @?= ([], [])
          , testCase "GroupsResponse" $ keyDrift @GroupsResponse emptyPage @?= ([], [])
          , testCase "DLQResponse" $ keyDrift @(DLQResponse Int) emptyPage @?= ([], [])
          , testCase "ArchiveResponse" $ keyDrift @(ArchiveResponse Int) emptyPage @?= ([], [])
          , testCase "DLQEntry" $
              keyDrift DLQJob {dlqPrimaryKey = 1, failedAt = epoch, jobSnapshot = job} @?= ([], [])
          , testCase "ArchiveEntry" $ keyDrift archived @?= ([], [])
          , testCase "JobsResponse" $
              keyDrift @(JobsResponse Int)
                JobsResponse
                  { jobsPage = emptyPage
                  , childCounts = mempty
                  , pausedParents = []
                  , dlqChildCounts = mempty
                  }
                @?= ([], [])
          , testCase "RateLimitPolicyUpdate" $
              keyDrift
                RateLimitPolicyUpdate
                  { overrideMaxTokens = Just (Just 1)
                  , overrideRefillAmount = Just (Just 1)
                  , overrideInterval = Just (Just 1)
                  }
                @?= ([], [])
          , testCase "ConcurrencyPolicyUpdate" $ keyDrift ConcurrencyPolicyUpdate {overrideLimit = Just (Just 1)} @?= ([], [])
          , testCase "QueueStats" $
              keyDrift
                QueueStats
                  { totalJobs = 0
                  , readyJobs = 0
                  , inFlightJobs = 0
                  , scheduledJobs = 0
                  , backoffJobs = 0
                  , throttledJobs = 0
                  , suspendedJobs = 0
                  , cancelledJobs = 0
                  , exhaustedJobs = 0
                  , blockedJobs = 0
                  , oldestReadyAgeSeconds = Nothing
                  , oldestInFlightAgeSeconds = Nothing
                  , dlqJobs = 0
                  , kindCounts = mempty
                  , dlqKindCounts = mempty
                  }
                @?= ([], [])
          , testCase "CronScheduleUpdate" $
              keyDrift
                CronScheduleUpdate
                  { overrideExpression = Just (Just "x")
                  , overrideOverlap = Just (Just "x")
                  , overrideTimezone = Just (Just "x")
                  , enabled = Just True
                  }
                @?= ([], [])
          ]
      ]

-- | Keys the schema declares that the encoding omits, then keys the encoding sends that the schema omits.
keyDrift :: forall a. (ToJSON a, ToSchema a) => a -> ([Text], [Text])
keyDrift sample = (declared \\ sent, sent \\ declared)
  where
    declared = InsOrd.keys (_schemaProperties (toSchema (Proxy @a)))
    sent = case toJSON sample of
      Object o -> map toText (keys o)
      _ -> []

-- | The distinct types of every payload property in a document.
payloadTypes :: Value -> [Text]
payloadTypes doc = distinct [ty | p <- payloadProps doc, Just (String ty) <- [KeyMap.lookup "type" p]]

-- | Every payload property in a document, inline or defined.
payloadProps :: Value -> [Object]
payloadProps (Object o) =
  [p | Just (Object ps) <- [KeyMap.lookup "properties" o], Just (Object p) <- [KeyMap.lookup "payload" ps]]
    <> concatMap payloadProps (elems o)
payloadProps (Array vs) = concatMap payloadProps vs
payloadProps _ = []

-- | A document with two queues, so per-queue routes appear twice.
twoQueues :: Value
twoQueues = openApiSpec @'[Queue "a" Int, Queue "b" Text]

-- | Every operation in a document, named by its method and path.
operations :: Value -> [(Text, Object)]
operations (Object doc)
  | Just (Object paths) <- KeyMap.lookup "paths" doc =
      [ (toText method <> " " <> toText path, op)
      | (path, Object item) <- KeyMap.toList paths
      , (method, Object op) <- KeyMap.toList item
      ]
operations _ = []

-- | The response codes one operation declares.
responseCodes :: Text -> Value -> [Text]
responseCodes name doc =
  distinct
    [ toText code | (n, op) <- operations doc, n == name, Just (Object rs) <- [KeyMap.lookup "responses" op], code <- keys rs
    ]

-- | The content types one response of one operation declares.
responseContent :: Text -> Text -> Value -> [Text]
responseContent name code doc =
  [ toText contentType
  | (n, op) <- operations doc
  , n == name
  , Just (Object rs) <- [KeyMap.lookup "responses" op]
  , Just (Object r) <- [KeyMap.lookup (fromText code) rs]
  , Just (Object content) <- [KeyMap.lookup "content" r]
  , contentType <- keys content
  ]

-- | The paths in a document that declare no operation.
pathsWithoutOperation :: Value -> [Text]
pathsWithoutOperation (Object doc)
  | Just (Object paths) <- KeyMap.lookup "paths" doc =
      [toText path | (path, Object item) <- KeyMap.toList paths, not (any (`KeyMap.member` item) operationKeys)]
  where
    operationKeys = ["get", "put", "post", "delete", "options", "head", "patch", "trace"]
pathsWithoutOperation _ = []

identChar :: Char -> Bool
identChar c = isAsciiUpper c || isAsciiLower c || isDigit c || c == '_'

distinct :: (Ord a) => [a] -> [a]
distinct = Set.toList . Set.fromList

-- | A page with no rows.
emptyPage :: Page a
emptyPage = Page {pageItems = [], pageTotal = 0, pageOffset = 0, pageLimit = 0}

-- | An archived job without a stored result.
archived :: ArchiveJob Value
archived = ArchiveJob {archivePrimaryKey = 1, completedAt = epoch, jobSnapshot = job, archivedResult = Nothing}

epoch :: UTCTime
epoch = UTCTime (fromGregorian 2026 1 1) 0

job :: JobRead Value
job =
  (defaultJob (String "x"))
    { primaryKey = 1
    , queueName = "q"
    , insertedAt = epoch
    , payloadKeys = PayloadKeys {jobKind = Nothing, jobRateLimitKey = Nothing, jobConcurrencyKey = Nothing}
    }
