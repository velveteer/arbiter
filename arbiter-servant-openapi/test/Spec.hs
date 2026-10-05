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
import Data.Aeson.Key (toText)
import Data.Aeson.KeyMap (elems, keys)
import Data.Aeson.KeyMap qualified as KeyMap
import Data.HashMap.Strict.InsOrd qualified as InsOrd
import Data.List ((\\))
import Data.OpenApi (Schema (..), ToSchema, toSchema)
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.Text (Text)
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

-- | The paths in a document that declare no operation.
pathsWithoutOperation :: Value -> [Text]
pathsWithoutOperation (Object doc)
  | Just (Object paths) <- KeyMap.lookup "paths" doc =
      [toText path | (path, Object item) <- KeyMap.toList paths, not (any (`KeyMap.member` item) operationKeys)]
  where
    operationKeys = ["get", "put", "post", "delete", "options", "head", "patch", "trace"]
pathsWithoutOperation _ = []

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
