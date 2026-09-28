{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Servant.API (spec) where

import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyPoliciesTable)
import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.Exceptions (ParsingException (..))
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Archive (ArchiveJob, archivePrimaryKey)
import Arbiter.Core.Job.DLQ (DLQJob (..), dlqPrimaryKey)
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types
  ( DedupKey (..)
  , HasKind
  , JobRead
  , JobStatus (..)
  , Stored
  , attempts
  , claimSeq
  , claimedBy
  , decodeStored
  , dedupKey
  , defaultGroupedJob
  , defaultJob
  , groupKey
  , jobKind
  , notVisibleUntil
  , payload
  , payloadKeys
  , primaryKey
  , setArchiveFor
  , setDedupKey
  , setMaxAttempts
  , setNotVisibleUntil
  , suspended
  )
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (QueueSpec (QueueWithResult))
import Arbiter.Core.Queues qualified as Q
import Arbiter.Core.Worker qualified as W
import Arbiter.Simple (createSimpleEnvWithPool, runSimpleDb)
import Arbiter.Test.RateLimit (RLPayload (..), RLReg, rateLimitTable, setupRateLimitPolicy)
import Arbiter.Test.Setup (cleanupData, createSharedPool, setupOnce, truncateToMicros)
import Arbiter.Worker.Logger (LogConfig (..), LogDestination (..), defaultLogConfig)
import Control.Concurrent (forkIO, killThread)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, tryPutMVar)
import Control.Exception (finally)
import Control.Monad (forM_, void)
import Data.Aeson (FromJSON, ToJSON, Value (..), decode, encode, object, toJSON, (.=))
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.QQ.Simple (aesonQQ)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Builder qualified as Builder
import Data.ByteString.Lazy qualified as LB
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Foldable (toList)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, isJust)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time (addUTCTime, getCurrentTime)
import Data.UUID.Types qualified as UUID
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Network.HTTP.Types (status200, status204, status400, status404, status409)
import Network.Wai (defaultRequest, pathInfo, requestMethod, responseToStream)
import Network.Wai.Internal (ResponseReceived (..))
import System.Timeout (timeout)
import Test.Hspec
import Test.Hspec.Wai
import Test.Hspec.Wai.Internal (runWaiSession)

import Arbiter.Servant (ArbiterServerConfig (..), arbiterApp, initArbiterServer)
import Arbiter.Servant.Types
  ( AckRequest (..)
  , ApiJobWithStatus (..)
  , ApiJobWrite (..)
  , ArchiveResponse (..)
  , BatchDeleteResponse (..)
  , BatchInsertRequest (..)
  , BatchInsertResponse (..)
  , ClaimResponse (ClaimResponse)
  , DLQResponse (..)
  , GroupSummary (..)
  , GroupsResponse (..)
  , JobLease (..)
  , JobResponse (..)
  , JobsResponse (..)
  , MaintenanceResponse (..)
  , StatsResponse (..)
  , WorkersResponse (..)
  )

-- | A JSON POST. Servant answers a typed body with 415 when the header is absent.
postJson :: ByteString -> LB.ByteString -> WaiSession st SResponse
postJson path = request "POST" path [("Content-Type", "application/json")]

-- | The jobs a claim response leased.
decodeClaim :: SResponse -> [JobRead ServantTestPayload]
decodeClaim response = case decode (simpleBody response) of
  Just (ClaimResponse claimed) -> claimed
  Nothing -> error "claim response did not decode"

-- | The lease a finalize has to present for this job.
leaseBody :: JobRead ServantTestPayload -> LB.ByteString
leaseBody job = encode (JobLease (claimSeq job) (fromMaybe UUID.nil (claimedBy job)))

-- | A worker-pool identity. The finalize routes refuse to act for it.
poolWorkerId :: UUID.UUID
poolWorkerId = UUID.fromWords 0xa1b2c3d4 0xe5f60718 0x293a4b5c 0x6d7e8f90

ackPath :: JobRead ServantTestPayload -> ByteString
ackPath = jobVerbPath "ack"

nackPath :: JobRead ServantTestPayload -> ByteString
nackPath = jobVerbPath "nack"

jobVerbPath :: Text -> JobRead ServantTestPayload -> ByteString
jobVerbPath verb job =
  TE.encodeUtf8 $
    "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show (primaryKey job)) <> "/" <> verb

-- | A path under the test queue.
queuePath :: Text -> ByteString
queuePath rest = TE.encodeUtf8 ("/api/v1/arbiter_servant_test/" <> rest)

-- | A route on one row, e.g. @rowPath "dlq" 4 "retry"@.
rowPath :: Text -> Int64 -> Text -> ByteString
rowPath collection rowId verb = queuePath (collection <> "/" <> T.pack (show rowId) <> "/" <> verb)

-- | A response with this status and exactly this body.
statusWithBody :: Int -> LB.ByteString -> ResponseMatcher
statusWithBody code expected =
  ResponseMatcher
    code
    []
    (MatchBody (\_ body -> if body == expected then Nothing else Just ("unexpected body: " <> show body)))

jsonMatch :: Value -> ResponseMatcher
jsonMatch expected = ResponseMatcher 200 [] (MatchBody matcher)
  where
    matcher _ body = case decode body of
      Just actual | actual == expected -> Nothing
      Just actual -> Just $ "JSON mismatch:\n  expected: " <> show expected <> "\n  actual: " <> show actual
      Nothing -> Just "Response body is not valid JSON"

-- Test schema
testSchema :: Text
testSchema = "arbiter_servant_test"

-- | A schema of its own. Its maintenance gates are separate from the other tests.
pacedSchema :: Text
pacedSchema = "arbiter_servant_paced_test"

-- | A schema nothing created. Every maintenance operation raises in it.
missingSchema :: Text
missingSchema = "arbiter_servant_missing"

-- | Test payload type
data ServantTestPayload
  = TestMessage Text
  | TestCalculation Int Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

instance HasKind ServantTestPayload

-- | Test registry
type ServantTestRegistry = '[QueueWithResult "arbiter_servant_test" ServantTestPayload [Text]]

-- Table name for tests
testTable :: Text
testTable = "arbiter_servant_test"

-- | Decode a JSON response body or fail the test
decodeBody :: (FromJSON a) => SResponse -> IO a
decodeBody resp = case decode (simpleBody resp) of
  Just decoded -> pure decoded
  Nothing -> fail $ "Failed to decode JSON response: " <> show (simpleBody resp)

spec :: ByteString -> Spec
spec connStr = do
  runIO (setupOnce connStr testSchema testTable False)
  sharedPool <- runIO (createSharedPool connStr)
  mkEnv <- runIO (createSimpleEnvWithPool (Proxy @ServantTestRegistry) sharedPool testSchema)
  serverConfig <- runIO (initArbiterServer (runSimpleDb mkEnv))
  let app = arbiterApp @ServantTestRegistry serverConfig {queueStatsCacheTtl = 0}

  let cleanupDb :: IO ()
      cleanupDb = withResource sharedPool $ \conn -> cleanupData testSchema testTable conn

      -- Set columns on one test-queue row.
      setColumns :: Text -> Int64 -> IO ()
      setColumns assignments jobId =
        void . withResource sharedPool $ \conn ->
          PG.execute
            conn
            ( fromString . T.unpack $
                "UPDATE " <> Schema.jobQueueTable testSchema testTable <> " SET " <> assignments <> " WHERE id = ?"
            )
            (PG.Only jobId)

      throttleMarked :: Int64 -> IO Bool
      throttleMarked jobId =
        withResource sharedPool $ \conn -> do
          [PG.Only marked] <-
            PG.query
              conn
              ( fromString . T.unpack $
                  "SELECT throttled_until IS NOT NULL FROM " <> Schema.jobQueueTable testSchema testTable <> " WHERE id = ?"
              )
              (PG.Only jobId)
          pure marked

      corruptPayload :: Text -> Text -> Int64 -> IO ()
      corruptPayload tbl idColumn jobId =
        void $
          withResource sharedPool $ \conn ->
            PG.execute
              conn
              (fromString . T.unpack $ "UPDATE " <> tbl <> " SET payload = '{\"bogus\": 1}' WHERE " <> idColumn <> " = ?")
              (PG.Only jobId)

  describe "Jobs API" $ with (cleanupDb >> pure app) $ do
    it "GET /api/v1/arbiter_servant_test/jobs returns empty list initially" $ do
      get "/api/v1/arbiter_servant_test/jobs"
        `shouldRespondWith` jsonMatch
          [aesonQQ|{
              "jobs": [],
              "jobsTotal": 0,
              "jobsOffset": 0,
              "jobsLimit": 50,
              "childCounts": {},
              "pausedParents": [],
              "dlqChildCounts": {}
            }|]

    it "POST /api/v1/arbiter_servant_test/jobs inserts a new job" $ do
      postResp <-
        request
          "POST"
          "/api/v1/arbiter_servant_test/jobs"
          [("Content-Type", "application/json")]
          ( encode
              [aesonQQ|{
                "payload": {"tag": "TestMessage", "contents": "test message"},
                "queueName": "arbiter_servant_test",
                "dedupKey": {"key": "test-dedup-1", "strategy": "ignore"},
                "groupKey": "group1",
                "priority": 0,
                "maxAttempts": 3
              }|]
          )

      -- Verify POST response contains the inserted job
      liftIO $ do
        body :: JobResponse (JobRead ServantTestPayload) <- decodeBody postResp
        let returnedJob = job body
        payload returnedJob `shouldBe` TestMessage "test message"
        groupKey returnedJob `shouldBe` Just "group1"
        dedupKey returnedJob `shouldBe` Just (IgnoreDuplicate "test-dedup-1")

      -- Verify job was inserted by checking job count
      resp <- get "/api/v1/arbiter_servant_test/jobs"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsTotal body `shouldBe` 1
        length (jobs body) `shouldBe` 1

    it "POST /api/v1/arbiter_servant_test/jobs with notVisibleUntil creates a scheduled job" $ do
      futureTime <- liftIO $ truncateToMicros . addUTCTime 3600 <$> getCurrentTime
      postResp <-
        request
          "POST"
          "/api/v1/arbiter_servant_test/jobs"
          [("Content-Type", "application/json")]
          ( encode $
              object
                [ "payload"
                    .= object
                      [ "tag" .= ("TestMessage" :: Text)
                      , "contents" .= ("delayed" :: Text)
                      ]
                , "notVisibleUntil" .= futureTime
                ]
          )

      liftIO $ do
        body :: JobResponse (JobRead ServantTestPayload) <- decodeBody postResp
        let returnedJob = job body
        payload returnedJob `shouldBe` TestMessage "delayed"
        notVisibleUntil returnedJob `shouldBe` Just futureTime

    it "POST /api/v1/arbiter_servant_test/jobs returns existing job on IgnoreDuplicate hit" $ do
      firstResp <-
        request
          "POST"
          "/api/v1/arbiter_servant_test/jobs"
          [("Content-Type", "application/json")]
          ( encode
              [aesonQQ|{
                "payload": {"tag": "TestMessage", "contents": "first"},
                "queueName": "arbiter_servant_test",
                "dedupKey": {"key": "duplicate-key", "strategy": "ignore"},
                "priority": 0
              }|]
          )
      firstId <- liftIO $ do
        body :: JobResponse (JobRead ServantTestPayload) <- decodeBody firstResp
        let returnedJob = job body
        payload returnedJob `shouldBe` TestMessage "first"
        pure (primaryKey returnedJob)

      dupResp <-
        request
          "POST"
          "/api/v1/arbiter_servant_test/jobs"
          [("Content-Type", "application/json")]
          ( encode
              [aesonQQ|{
                "payload": {"tag": "TestMessage", "contents": "second"},
                "queueName": "arbiter_servant_test",
                "dedupKey": {"key": "duplicate-key", "strategy": "ignore"},
                "priority": 0
              }|]
          )
      liftIO $ do
        body :: JobResponse (JobRead ServantTestPayload) <- decodeBody dupResp
        let returnedJob = job body
        primaryKey returnedJob `shouldBe` firstId
        payload returnedJob `shouldBe` TestMessage "first"

    it "POST /api/v1/arbiter_servant_test/jobs/batch inserts multiple jobs" $ do
      postResp <-
        request
          "POST"
          "/api/v1/arbiter_servant_test/jobs/batch"
          [("Content-Type", "application/json")]
          ( encode $
              BatchInsertRequest
                [ ApiJobWrite (defaultGroupedJob "batch-g1" (TestMessage "batch 1"))
                , ApiJobWrite (defaultGroupedJob "batch-g2" (TestMessage "batch 2"))
                , ApiJobWrite (defaultGroupedJob "batch-g3" (TestMessage "batch 3"))
                ]
          )

      liftIO $ do
        body :: BatchInsertResponse ServantTestPayload <- decodeBody postResp
        insertedCount body `shouldBe` 3
        length (inserted body) `shouldBe` 3

      -- Verify jobs exist in the queue
      resp <- get "/api/v1/arbiter_servant_test/jobs"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsTotal body `shouldBe` 3

    it "POST /api/v1/arbiter_servant_test/jobs/batch with empty list returns empty result" $ do
      postResp <-
        request
          "POST"
          "/api/v1/arbiter_servant_test/jobs/batch"
          [("Content-Type", "application/json")]
          (encode $ BatchInsertRequest ([] :: [ApiJobWrite ServantTestPayload]))

      liftIO $ do
        body :: BatchInsertResponse ServantTestPayload <- decodeBody postResp
        insertedCount body `shouldBe` 0
        inserted body `shouldBe` []

    it "POST /api/v1/arbiter_servant_test/jobs/batch skips duplicates with ignore strategy" $ do
      -- Insert a job with dedup key
      _ <-
        request
          "POST"
          "/api/v1/arbiter_servant_test/jobs"
          [("Content-Type", "application/json")]
          ( encode
              [aesonQQ|{
                "payload": {"tag": "TestMessage", "contents": "existing"},
                "dedupKey": {"key": "batch-dedup", "strategy": "ignore"}
              }|]
          )

      -- Batch insert with the same dedup key skips the duplicate
      postResp <-
        request
          "POST"
          "/api/v1/arbiter_servant_test/jobs/batch"
          [("Content-Type", "application/json")]
          ( encode $
              BatchInsertRequest
                [ ApiJobWrite
                    (defaultJob (TestMessage "new job"))
                , ApiJobWrite
                    (setDedupKey (Just (IgnoreDuplicate "batch-dedup")) $ defaultJob (TestMessage "duplicate"))
                ]
          )

      liftIO $ do
        body :: BatchInsertResponse ServantTestPayload <- decodeBody postResp
        insertedCount body `shouldBe` 1

    it "GET /api/v1/arbiter_servant_test/jobs/:id returns job details" $ do
      -- Insert a job
      jobId <- liftIO $ do
        let jobWrite = defaultGroupedJob "group1" (TestMessage "get me")
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob jobWrite
        pure $ primaryKey jobRead

      resp <- get (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId))
      liftIO $ do
        body :: JobResponse (JobRead ServantTestPayload) <- decodeBody resp
        let returnedJob = job body
        payload returnedJob `shouldBe` TestMessage "get me"
        groupKey returnedJob `shouldBe` Just "group1"
        primaryKey returnedJob `shouldBe` jobId

    it "GET /api/v1/arbiter_servant_test/jobs/:id returns 404 for non-existent job" $ do
      get "/api/v1/arbiter_servant_test/jobs/99999" `shouldRespondWith` 404

    it "GET /api/v1/arbiter_servant_test/jobs supports limit parameter" $ do
      -- Insert 3 jobs
      liftIO $ do
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "g1" (TestMessage "msg1"))
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "g2" (TestMessage "msg2"))
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "g3" (TestMessage "msg3"))
        pure ()

      -- Request with limit=2 returns 2 jobs and reports a total of 3
      resp <- get "/api/v1/arbiter_servant_test/jobs?limit=2"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsLimit body `shouldBe` 2
        jobsTotal body `shouldBe` 3
        length (jobs body) `shouldBe` 2

    it "GET /api/v1/arbiter_servant_test/jobs supports group_key filter" $ do
      -- Insert jobs with different group keys
      liftIO $ do
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "groupA" (TestMessage "msg1"))
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "groupB" (TestMessage "msg2"))
        pure ()

      -- Filter by group key returns the groupA job alone
      resp <- get "/api/v1/arbiter_servant_test/jobs?group_key=groupA"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsTotal body `shouldBe` 1
        length (jobs body) `shouldBe` 1
        -- Verify only groupA jobs returned
        forM_ (jobs body) $ \listed -> groupKey (ajwsJob listed) `shouldBe` Just "groupA"

    it "GET /api/v1/arbiter_servant_test/jobs supports kind filter" $ do
      liftIO $ do
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "kinded"))
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestCalculation 1 2))
        pure ()

      resp <- get "/api/v1/arbiter_servant_test/jobs?kind=TestCalculation"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsTotal body `shouldBe` 1
        map (decodeStored . payload . ajwsJob) (jobs body) `shouldBe` [Right (TestCalculation 1 2)]

    it "GET /api/v1/arbiter_servant_test/kinds lists every label the payload carries" $ do
      resp <- get "/api/v1/arbiter_servant_test/kinds"
      liftIO $ do
        body :: [Text] <- decodeBody resp
        body `shouldBe` ["TestMessage", "TestCalculation"]

    it "GET /api/v1/arbiter_servant_test/jobs sort_by/sort_dir changes ordering" $ do
      ids <- liftIO $ do
        Just job1 <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "sort1"))
        Just job2 <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "sort2"))
        Just job3 <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "sort3"))
        pure $ map primaryKey [job1, job2, job3]
      let sorted = [minimum ids, maximum ids]

      ascResp <- get "/api/v1/arbiter_servant_test/jobs?sort_by=id&sort_dir=ASC"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody ascResp
        let returned = map (primaryKey . ajwsJob) (jobs body)
        [head returned, last returned] `shouldBe` sorted

      descResp <- get "/api/v1/arbiter_servant_test/jobs?sort_by=id&sort_dir=DESC"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody descResp
        let returned = map (primaryKey . ajwsJob) (jobs body)
        [head returned, last returned] `shouldBe` reverse sorted

    it "GET /api/v1/arbiter_servant_test/jobs roots_only and parent_id filter the tree" $ do
      (parentId, childIds) <- liftIO $ do
        Right (parent :| children) <-
          runSimpleDb mkEnv
            $ HL.insertJobTree
            $ JT.rollup
              (defaultGroupedJob "tree-parent" (TestMessage "parent"))
              ( JT.leaf (defaultJob (TestMessage "child-a"))
                  :| [JT.leaf (defaultJob (TestMessage "child-b"))]
              )
        pure (primaryKey parent, map primaryKey children)

      -- roots_only excludes children
      rootsResp <- get "/api/v1/arbiter_servant_test/jobs?roots_only"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody rootsResp
        map (primaryKey . ajwsJob) (jobs body) `shouldBe` [parentId]

      -- parent_id returns exactly the children of that parent
      childResp <- get (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs?parent_id=" <> T.pack (show parentId))
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody childResp
        jobsTotal body `shouldBe` 2
        let returned = map (primaryKey . ajwsJob) (jobs body)
        forM_ childIds $ \childId -> (childId `elem` returned) `shouldBe` True

    it "GET /api/v1/arbiter_servant_test/jobs clamps out-of-range limit and offset" $ do
      liftIO $ do
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "clamp"))
        pure ()

      -- limit above 1000 clamps to 1000, negative offset clamps to 0
      highResp <- get "/api/v1/arbiter_servant_test/jobs?limit=5000&offset=-10"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody highResp
        jobsLimit body `shouldBe` 1000
        jobsOffset body `shouldBe` 0

      -- limit below 1 clamps to 1
      lowResp <- get "/api/v1/arbiter_servant_test/jobs?limit=0"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody lowResp
        jobsLimit body `shouldBe` 1

    it "GET /api/v1/arbiter_servant_test/jobs returns dlqChildCounts for parent with DLQ'd children" $ do
      -- Insert parent + child
      parentId <- liftIO $ do
        Right (parent :| _children) <-
          runSimpleDb mkEnv
            $ HL.insertJobTree
            $ JT.rollup
              (defaultGroupedJob "dlq-count-parent" (TestMessage "parent"))
              (JT.leaf (defaultJob (TestMessage "dlq-count-child")) :| [])
        -- Claim and DLQ the child
        claimed <- runSimpleDb mkEnv $ HL.claimNextVisibleJobs 1 60 :: IO [JobRead ServantTestPayload]
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "child failed" (head claimed)
        pure $ primaryKey parent

      -- List jobs - the parent should appear with dlqChildCounts showing 1
      resp <- get "/api/v1/arbiter_servant_test/jobs"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        Map.lookup parentId (dlqChildCounts body) `shouldBe` Just 1

    it "GET /api/v1/arbiter_servant_test/jobs?status=in_flight returns empty list when no jobs are claimed" $ do
      resp <- get "/api/v1/arbiter_servant_test/jobs?status=in_flight"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsTotal body `shouldBe` 0
        jobs body `shouldBe` []

    it "GET /api/v1/arbiter_servant_test/jobs?status=in_flight returns claimed jobs" $ do
      liftIO $ do
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "in-flight test"))
        _ <- runSimpleDb mkEnv $ Ops.claimNextVisibleJobs @_ @ServantTestPayload testSchema testTable 1 60
        pure ()

      resp <- get "/api/v1/arbiter_servant_test/jobs?status=in_flight"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsTotal body `shouldBe` 1
        length (jobs body) `shouldBe` 1

    it "GET /api/v1/arbiter_servant_test/jobs?status filters across all derived states" $ do
      future <- liftIO $ truncateToMicros . addUTCTime 3600 <$> getCurrentTime
      backoffId <- liftIO $ do
        -- in_flight: insert then claim (the only visible job)
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "inflight-job"))
        _ <- runSimpleDb mkEnv $ Ops.claimNextVisibleJobs @_ @ServantTestPayload testSchema testTable 1 60
        -- backoff: insert, claim, then fail into retry backoff
        Just backoffJob <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "backoff-job"))
        claimedB <- runSimpleDb mkEnv $ HL.claimNextVisibleJobs 1 60 :: IO [JobRead ServantTestPayload]
        _ <- runSimpleDb mkEnv $ HL.updateJobForRetry 60 "boom" (head claimedB)
        -- ready
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "ready-job"))
        -- scheduled: future visibility with zero attempts
        _ <- runSimpleDb mkEnv $ HL.insertJob (setNotVisibleUntil (Just future) $ defaultJob (TestMessage "scheduled-job"))
        -- suspended
        Just suspendedJob <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "suspended-job"))
        _ <- runSimpleDb mkEnv $ Ops.suspendJob testSchema testTable (primaryKey suspendedJob)
        pure (primaryKey backoffJob)

      let expectOne status pay = do
            resp <- get (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs?status=" <> status)
            liftIO $ do
              body :: JobsResponse ServantTestPayload <- decodeBody resp
              jobsTotal body `shouldBe` 1
              map (decodeStored . payload . ajwsJob) (jobs body) `shouldBe` [Right pay]
      expectOne "ready" (TestMessage "ready-job")
      expectOne "in_flight" (TestMessage "inflight-job")
      expectOne "backoff" (TestMessage "backoff-job")
      expectOne "scheduled" (TestMessage "scheduled-job")
      expectOne "suspended" (TestMessage "suspended-job")

      -- getJob returns the derived status
      detailResp <- get (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show backoffId))
      liftIO $ do
        body :: JobResponse (ApiJobWithStatus ServantTestPayload) <- decodeBody detailResp
        ajwsStatus (job body) `shouldBe` Backoff

    it "DELETE /api/v1/arbiter_servant_test/jobs/:id cancels a job" $ do
      -- Insert a job
      jobId <- liftIO $ do
        let jobWrite = defaultGroupedJob "cancel-group" (TestMessage "cancel me")
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob jobWrite
        pure $ primaryKey jobRead

      -- Cancel the job
      delete (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId))
        `shouldRespondWith` 204

      -- Verify job is gone
      get (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId))
        `shouldRespondWith` 404

    it "DELETE /api/v1/arbiter_servant_test/jobs/:id returns 404 for non-existent job" $ do
      delete "/api/v1/arbiter_servant_test/jobs/99999" `shouldRespondWith` 404

    it "POST /api/v1/arbiter_servant_test/jobs/:id/force-cancel cancels a job" $ do
      jobId <- liftIO $ do
        let jobWrite = defaultGroupedJob "force-cancel-group" (TestMessage "force cancel me")
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob jobWrite
        pure $ primaryKey jobRead

      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/force-cancel") ""
        `shouldRespondWith` 204

      get (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId))
        `shouldRespondWith` 404

    it "POST /api/v1/arbiter_servant_test/jobs/:id/force-cancel returns 404 for non-existent job" $ do
      post "/api/v1/arbiter_servant_test/jobs/99999/force-cancel" "" `shouldRespondWith` 404

    it "POST /api/v1/arbiter_servant_test/jobs/:id/force-cancel removes a parent and its children" $ do
      -- Plain cancel refuses a parent with children. force-cancel cascade-deletes the tree.
      (parentId, childId) <- liftIO $ do
        Right (parent :| (child1 : _)) <-
          runSimpleDb mkEnv
            $ HL.insertJobTree
            $ JT.rollup
              (defaultGroupedJob "force-cancel-tree" (TestMessage "parent"))
              ( JT.leaf (defaultJob (TestMessage "fc-child-a"))
                  :| [JT.leaf (defaultJob (TestMessage "fc-child-b"))]
              )
        pure (primaryKey parent, primaryKey child1)

      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show parentId) <> "/force-cancel") ""
        `shouldRespondWith` 204

      get (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show parentId))
        `shouldRespondWith` 404
      get (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show childId))
        `shouldRespondWith` 404

    it "POST /api/v1/arbiter_servant_test/jobs/:id/promote returns 404 for non-existent job" $ do
      post "/api/v1/arbiter_servant_test/jobs/99999/promote" "" `shouldRespondWith` 404

    it "POST /api/v1/arbiter_servant_test/jobs/:id/move-to-dlq moves job to DLQ" $ do
      -- Insert a job
      jobId <- liftIO $ do
        let jobWrite = defaultGroupedJob "dlq-group" (TestMessage "move me to dlq")
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob jobWrite
        pure $ primaryKey jobRead

      -- Move to DLQ
      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/move-to-dlq") ""
        `shouldRespondWith` 204

      -- Verify job is not in main queue
      get (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId))
        `shouldRespondWith` 404

      -- Verify job is in DLQ
      dlqResp <- get "/api/v1/arbiter_servant_test/dlq"
      liftIO $ do
        body :: DLQResponse ServantTestPayload <- decodeBody dlqResp
        dlqTotal body `shouldBe` 1
        length (dlqJobs body) `shouldBe` 1

    it "GET jobs, job detail and dlq splice the stored payload bytes into the response" $ do
      -- JSONB spaces its output. A payload aeson re-encoded would carry no spaces.
      let stored = "\"payload\":{\"tag\": \"TestMessage\", \"contents\": \"as stored\"}"
          splices resp = LB.toStrict (simpleBody resp) `shouldSatisfy` BS.isInfixOf stored
      jobId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "as stored"))
        pure $ primaryKey jobRead

      get "/api/v1/arbiter_servant_test/jobs" >>= liftIO . splices
      get (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId)) >>= liftIO . splices
      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/move-to-dlq") ""
        `shouldRespondWith` 204
      get "/api/v1/arbiter_servant_test/dlq" >>= liftIO . splices

    it "POST /api/v1/arbiter_servant_test/jobs/:id/move-to-dlq returns 404 for non-existent job" $ do
      post "/api/v1/arbiter_servant_test/jobs/99999/move-to-dlq" "" `shouldRespondWith` 404

    it "POST /api/v1/arbiter_servant_test/jobs/:id/pause-children pauses children" $ do
      -- Insert a finalizer tree - parent suspended, children unsuspended
      parentId <- liftIO $ do
        Right (parent :| _children) <-
          runSimpleDb mkEnv
            $ JT.insertJobTree testSchema testTable
            $ JT.rollup
              (defaultGroupedJob "pause-parent" (TestMessage "parent"))
              (JT.leaf (defaultGroupedJob "pause-child" (TestMessage "child")) :| [])
        pure $ primaryKey parent

      -- Pause children (they start unsuspended in finalizer pattern)
      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show parentId) <> "/pause-children") ""
        `shouldRespondWith` 204

      -- Verify children are suspended
      liftIO $ do
        allJobs :: [JobRead (Stored ServantTestPayload)] <- runSimpleDb mkEnv $ Ops.listJobs testSchema testTable 10 0
        let childJobs = filter (\listed -> decodeStored (payload listed) == Right (TestMessage "child")) allJobs
        length childJobs `shouldBe` 1
        forM_ childJobs $ \listed -> suspended listed `shouldBe` True

    it "POST /api/v1/arbiter_servant_test/jobs/:id/pause-children returns 204 for job with no children" $ do
      jobId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "no children"))
        pure $ primaryKey jobRead
      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/pause-children") ""
        `shouldRespondWith` 204

    it "POST /api/v1/arbiter_servant_test/jobs/:id/resume-children resumes children" $ do
      -- Insert a finalizer tree, then pause the children
      parentId <- liftIO $ do
        Right (parent :| _) <-
          runSimpleDb mkEnv
            $ JT.insertJobTree testSchema testTable
            $ JT.rollup
              (defaultGroupedJob "resume-parent" (TestMessage "parent"))
              (JT.leaf (defaultGroupedJob "resume-child" (TestMessage "child")) :| [])
        _ <- runSimpleDb mkEnv $ Ops.pauseChildren testSchema testTable (primaryKey parent)
        pure $ primaryKey parent

      -- Resume children
      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show parentId) <> "/resume-children") ""
        `shouldRespondWith` 204

      -- Verify children are no longer suspended
      liftIO $ do
        allJobs :: [JobRead (Stored ServantTestPayload)] <- runSimpleDb mkEnv $ Ops.listJobs testSchema testTable 10 0
        let childJobs = filter (\listed -> decodeStored (payload listed) == Right (TestMessage "child")) allJobs
        length childJobs `shouldBe` 1
        suspended (head childJobs) `shouldBe` False

    it "POST /api/v1/arbiter_servant_test/jobs/:id/resume-children returns 204 for job with no children" $ do
      jobId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "no children"))
        pure $ primaryKey jobRead
      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/resume-children") ""
        `shouldRespondWith` 204

  describe "DLQ API" $ with (cleanupDb >> pure app) $ do
    it "GET /api/v1/arbiter_servant_test/dlq returns empty list initially" $ do
      get "/api/v1/arbiter_servant_test/dlq"
        `shouldRespondWith` jsonMatch [aesonQQ|{ "dlqJobs": [], "dlqTotal": 0, "dlqOffset": 0, "dlqLimit": 50 }|]

    it "GET /api/v1/arbiter_servant_test/dlq supports pagination" $ do
      -- Insert multiple jobs and move to DLQ
      liftIO $ do
        Just job1 <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "dlq1" (TestMessage "dlq msg 1"))
        Just job2 <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "dlq2" (TestMessage "dlq msg 2"))
        Just job3 <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "dlq3" (TestMessage "dlq msg 3"))
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "Test error" job1
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "Test error" job2
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "Test error" job3
        pure ()

      -- Get with limit returns 2 of 3 jobs
      limitResp <- get "/api/v1/arbiter_servant_test/dlq?limit=2"
      liftIO $ do
        body :: DLQResponse ServantTestPayload <- decodeBody limitResp
        dlqLimit body `shouldBe` 2
        dlqTotal body `shouldBe` 3
        length (dlqJobs body) `shouldBe` 2

      -- Get with offset returns the 2 remaining jobs
      offsetResp <- get "/api/v1/arbiter_servant_test/dlq?offset=1"
      liftIO $ do
        body :: DLQResponse ServantTestPayload <- decodeBody offsetResp
        dlqOffset body `shouldBe` 1
        dlqTotal body `shouldBe` 3
        length (dlqJobs body) `shouldBe` 2

    it "POST /api/v1/arbiter_servant_test/dlq/batch-delete deletes multiple DLQ jobs" $ do
      -- Insert 3 jobs and move to DLQ
      dlqIds <- liftIO $ do
        Just job1 <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "bd1" (TestMessage "batch del 1"))
        Just job2 <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "bd2" (TestMessage "batch del 2"))
        Just job3 <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "bd3" (TestMessage "batch del 3"))
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "err" job1
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "err" job2
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "err" job3
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 10 0
        pure $ map dlqPrimaryKey dlqs

      -- Batch delete
      resp <-
        request
          "POST"
          "/api/v1/arbiter_servant_test/dlq/batch-delete"
          [("Content-Type", "application/json")]
          (encode $ object ["ids" .= (dlqIds :: [Int64])])
      liftIO $ do
        body :: BatchDeleteResponse <- decodeBody resp
        deleted body `shouldBe` 3

      -- Verify DLQ is empty
      get "/api/v1/arbiter_servant_test/dlq"
        `shouldRespondWith` jsonMatch [aesonQQ|{ "dlqJobs": [], "dlqTotal": 0, "dlqOffset": 0, "dlqLimit": 50 }|]

    it "POST /api/v1/arbiter_servant_test/dlq/:id/retry moves job back to main queue" $ do
      -- Insert a job, then move it to DLQ
      dlqId <- liftIO $ do
        let jobWrite = defaultGroupedJob "group1" (TestMessage "retry me")
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob jobWrite
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "Test error" jobRead
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 1 0
        pure $ dlqPrimaryKey (head dlqs)

      -- Retry from DLQ
      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/dlq/" <> T.pack (show dlqId) <> "/retry") ""
        `shouldRespondWith` 204

      -- Verify DLQ is now empty
      get "/api/v1/arbiter_servant_test/dlq"
        `shouldRespondWith` jsonMatch [aesonQQ|{ "dlqJobs": [], "dlqTotal": 0, "dlqOffset": 0, "dlqLimit": 50 }|]

      -- Verify job is back in main queue
      liftIO $ do
        allJobs :: [JobRead (Stored ServantTestPayload)] <-
          runSimpleDb mkEnv $ Ops.listJobs testSchema testTable 10 0
        length allJobs `shouldBe` 1

    it "POST /api/v1/arbiter_servant_test/dlq/:id/retry leaves a row its payload type rejects in the DLQ" $ do
      dlqId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "bogus"))
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "Test error" jobRead
        corruptPayload (Schema.jobQueueDLQTable testSchema testTable) "job_id" (primaryKey jobRead)
        dlqs :: [DLQJob (Stored ServantTestPayload)] <- runSimpleDb mkEnv $ Ops.listDLQJobs testSchema testTable 1 0
        pure $ dlqPrimaryKey (head dlqs)

      liftIO $
        runWaiSession (post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/dlq/" <> T.pack (show dlqId) <> "/retry") "") app
          `shouldThrow` \ParsingException {} -> True

      resp <- get "/api/v1/arbiter_servant_test/dlq"
      liftIO $ do
        body :: DLQResponse ServantTestPayload <- decodeBody resp
        dlqTotal body `shouldBe` 1
      jobsResp <- get "/api/v1/arbiter_servant_test/jobs"
      liftIO $ do
        queued :: JobsResponse ServantTestPayload <- decodeBody jobsResp
        jobsTotal queued `shouldBe` 0

    it "POST /api/v1/arbiter_servant_test/archive/:id/reenqueue enqueues nothing for a row its payload type rejects" $ do
      archiveId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (setArchiveFor (Just 86400) (defaultJob (TestMessage "bogus")))
        [claimed] <- runSimpleDb mkEnv $ HL.claimNextVisibleJobsAs @ServantTestPayload 1 60 UUID.nil
        _ <- runSimpleDb mkEnv $ HL.ackJob claimed
        corruptPayload (Schema.jobQueueArchiveTable testSchema testTable) "job_id" (primaryKey jobRead)
        Just archived :: Maybe (ArchiveJob (Stored ServantTestPayload)) <-
          runSimpleDb mkEnv $ Ops.getArchivedJobById testSchema testTable (primaryKey jobRead)
        pure $ archivePrimaryKey archived

      liftIO $
        runWaiSession
          (post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/archive/" <> T.pack (show archiveId) <> "/reenqueue") "")
          app
          `shouldThrow` \ParsingException {} -> True

      resp <- get "/api/v1/arbiter_servant_test/jobs"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsTotal body `shouldBe` 0

    it "DELETE /api/v1/arbiter_servant_test/dlq/:id permanently deletes job" $ do
      -- Insert a job, then move it to DLQ
      dlqId <- liftIO $ do
        let jobWrite = defaultGroupedJob "group2" (TestMessage "delete me")
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob jobWrite
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "Test error" jobRead
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 1 0
        pure $ dlqPrimaryKey (head dlqs)

      -- Delete from DLQ
      delete (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/dlq/" <> T.pack (show dlqId))
        `shouldRespondWith` 204

      -- Verify DLQ is empty
      get "/api/v1/arbiter_servant_test/dlq"
        `shouldRespondWith` jsonMatch [aesonQQ|{ "dlqJobs": [], "dlqTotal": 0, "dlqOffset": 0, "dlqLimit": 50 }|]

      -- Verify job is absent from the main queue
      liftIO $ do
        allJobs :: [JobRead (Stored ServantTestPayload)] <-
          runSimpleDb mkEnv $ Ops.listJobs testSchema testTable 10 0
        length allJobs `shouldBe` 0

    it "POST /api/v1/arbiter_servant_test/dlq/:id/retry returns 404 for non-existent DLQ job" $ do
      post "/api/v1/arbiter_servant_test/dlq/99999/retry" "" `shouldRespondWith` 404

    it "POST /api/v1/arbiter_servant_test/dlq/:id/retry returns 409 when parent no longer exists" $ do
      -- Insert parent + child via insertJobTree, ack parent (resumes children),
      -- claim + DLQ the child, then cascade-cancel the parent, then try retry
      dlqId <- liftIO $ do
        -- Insert parent with one child
        Right (parent :| _children) <-
          runSimpleDb mkEnv
            $ HL.insertJobTree
            $ JT.rollup
              (defaultGroupedJob "orphan-parent" (TestMessage "parent"))
              (JT.leaf (defaultJob (TestMessage "orphan-child")) :| [])
        -- Claim and DLQ the child
        claimed <- runSimpleDb mkEnv $ HL.claimNextVisibleJobs 1 60 :: IO [JobRead ServantTestPayload]
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "child failed" (head claimed)
        -- Cancel parent (cascade). This removes the suspended parent
        _ <- runSimpleDb mkEnv $ HL.cancelJobCascade @ServantTestPayload (primaryKey parent)
        -- Get DLQ job ID
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 1 0
        pure $ dlqPrimaryKey (head dlqs)

      -- Retry returns 409. The parent is gone
      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/dlq/" <> T.pack (show dlqId) <> "/retry") ""
        `shouldRespondWith` 409

    it "DELETE /api/v1/arbiter_servant_test/dlq/:id returns 404 for non-existent DLQ job" $ do
      delete "/api/v1/arbiter_servant_test/dlq/99999" `shouldRespondWith` 404

    it "GET /api/v1/arbiter_servant_test/dlq searches the payload and the last error" $ do
      liftIO $ do
        Just first <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "invoice 50%_off"))
        Just second <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "receipt"))
        void $ runSimpleDb mkEnv $ HL.moveToDLQ "SMTP timeout" first
        void $ runSimpleDb mkEnv $ HL.moveToDLQ "template missing" second
      let totalFor query = do
            resp <- get (queuePath ("dlq?" <> query))
            liftIO (dlqTotal <$> (decodeBody resp :: IO (DLQResponse ServantTestPayload)))
      totalFor "payload=50%25_off" >>= liftIO . (`shouldBe` 1)
      totalFor "payload=5_%25" >>= liftIO . (`shouldBe` 0)
      totalFor "error=smtp" >>= liftIO . (`shouldBe` 1)
      totalFor "error=missing&payload=receipt" >>= liftIO . (`shouldBe` 1)
      totalFor "error=missing&payload=invoice" >>= liftIO . (`shouldBe` 0)
      totalFor "error=" >>= liftIO . (`shouldBe` 2)

    it "POST /api/v1/arbiter_servant_test/dlq/:id/retry with a payload replaces it and its kind" $ do
      (jobId, dlqId) <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "edit-group" (TestMessage "wrong"))
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "Test error" jobRead
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 1 0
        pure (primaryKey jobRead, dlqPrimaryKey (head dlqs))

      postJson (rowPath "dlq" dlqId "retry") (encode [aesonQQ|{"payload": {"tag": "TestCalculation", "contents": [2, 3]}}|])
        `shouldRespondWith` 204

      liftIO $ do
        Just retried :: Maybe (JobRead (Stored ServantTestPayload)) <-
          runSimpleDb mkEnv $ Ops.getJobById testSchema testTable jobId
        decodeStored (payload retried) `shouldBe` Right (TestCalculation 2 3)
        jobKind (payloadKeys retried) `shouldBe` Just "TestCalculation"
        groupKey retried `shouldBe` Just "edit-group"
        attempts retried `shouldBe` 0

    it "POST /api/v1/arbiter_servant_test/dlq/:id/retry with an empty JSON body retries the stored payload" $ do
      (jobId, dlqId) <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "as stored"))
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "Test error" jobRead
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 1 0
        pure (primaryKey jobRead, dlqPrimaryKey (head dlqs))

      postJson (rowPath "dlq" dlqId "retry") "" `shouldRespondWith` 204

      liftIO $ do
        Just retried :: Maybe (JobRead (Stored ServantTestPayload)) <-
          runSimpleDb mkEnv $ Ops.getJobById testSchema testTable jobId
        decodeStored (payload retried) `shouldBe` Right (TestMessage "as stored")

    it "POST /api/v1/arbiter_servant_test/dlq/:id/retry refuses a payload with no content type" $ do
      (jobId, dlqId) <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "as stored"))
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "Test error" jobRead
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 1 0
        pure (primaryKey jobRead, dlqPrimaryKey (head dlqs))

      post (rowPath "dlq" dlqId "retry") (encode [aesonQQ|{"payload": {"tag": "TestMessage", "contents": "edited"}}|])
        `shouldRespondWith` 400
      liftIO $ do
        retried :: Maybe (JobRead (Stored ServantTestPayload)) <-
          runSimpleDb mkEnv $ Ops.getJobById testSchema testTable jobId
        retried `shouldBe` Nothing

    it "POST /api/v1/arbiter_servant_test/dlq/:id/retry refuses a payload the queue's type rejects" $ do
      dlqId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "keep"))
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "Test error" jobRead
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 1 0
        pure $ dlqPrimaryKey (head dlqs)

      postJson (rowPath "dlq" dlqId "retry") (encode [aesonQQ|{"payload": {"tag": "NoSuchTag"}}|]) `shouldRespondWith` 400
      resp <- get (queuePath "dlq")
      liftIO $ do
        body :: DLQResponse ServantTestPayload <- decodeBody resp
        dlqTotal body `shouldBe` 1

    it "POST /api/v1/arbiter_servant_test/dlq/:id/retry with a payload keeps the DLQ row unchanged on a 409" $ do
      dlqId <- liftIO $ do
        Right (parent :| _children) <-
          runSimpleDb mkEnv
            $ HL.insertJobTree
            $ JT.rollup
              (defaultGroupedJob "edit-orphan-parent" (TestMessage "parent"))
              (JT.leaf (defaultJob (TestMessage "edit-orphan-child")) :| [])
        claimed <- runSimpleDb mkEnv $ HL.claimNextVisibleJobs 1 60 :: IO [JobRead ServantTestPayload]
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "child failed" (head claimed)
        _ <- runSimpleDb mkEnv $ HL.cancelJobCascade @ServantTestPayload (primaryKey parent)
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 1 0
        pure $ dlqPrimaryKey (head dlqs)

      postJson (rowPath "dlq" dlqId "retry") (encode [aesonQQ|{"payload": {"tag": "TestMessage", "contents": "edited"}}|])
        `shouldRespondWith` 409
      liftIO $ do
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 1 0
        map (payload . jobSnapshot) dlqs `shouldBe` [TestMessage "edit-orphan-child"]

    it "GET /api/v1/arbiter_servant_test/archive searches the payload" $ do
      liftIO $ do
        forM_ ["archived invoice", "archived receipt"] $ \message ->
          runSimpleDb mkEnv $ HL.insertJob (setArchiveFor (Just 86400) (defaultJob (TestMessage message)))
        claimed <- runSimpleDb mkEnv $ HL.claimNextVisibleJobsAs @ServantTestPayload 2 60 UUID.nil
        forM_ claimed $ runSimpleDb mkEnv . HL.ackJob
      resp <- get (queuePath "archive?payload=INVOICE")
      liftIO $ do
        body :: ArchiveResponse ServantTestPayload <- decodeBody resp
        archiveTotal body `shouldBe` 1

    it "POST /api/v1/arbiter_servant_test/archive/:id/reenqueue with a payload enqueues it" $ do
      archiveId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (setArchiveFor (Just 86400) (defaultJob (TestMessage "ran once")))
        [claimed] <- runSimpleDb mkEnv $ HL.claimNextVisibleJobsAs @ServantTestPayload 1 60 UUID.nil
        _ <- runSimpleDb mkEnv $ HL.ackJob claimed
        Just archived :: Maybe (ArchiveJob (Stored ServantTestPayload)) <-
          runSimpleDb mkEnv $ Ops.getArchivedJobById testSchema testTable (primaryKey jobRead)
        pure $ archivePrimaryKey archived

      postJson
        (rowPath "archive" archiveId "reenqueue")
        (encode [aesonQQ|{"payload": {"tag": "TestCalculation", "contents": [4, 5]}}|])
        `shouldRespondWith` 204
      post (rowPath "archive" archiveId "reenqueue") "" `shouldRespondWith` 204

      resp <- get (queuePath "jobs")
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        let queued = map ajwsJob (jobs body)
        map (decodeStored . payload) queued `shouldMatchList` [Right (TestCalculation 4 5), Right (TestMessage "ran once")]
        map (jobKind . payloadKeys) queued `shouldMatchList` [Just "TestCalculation", Just "TestMessage"]

  describe "Suspend/Resume API" $ with (cleanupDb >> pure app) $ do
    it "POST /:id/suspend suspends a job" $ do
      jobId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "suspend me"))
        pure $ primaryKey jobRead

      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/suspend") ""
        `shouldRespondWith` 204

      -- Verify the job is suspended
      liftIO $ do
        Just job :: Maybe (JobRead (Stored ServantTestPayload)) <- runSimpleDb mkEnv $ Ops.getJobById testSchema testTable jobId
        suspended job `shouldBe` True

    it "POST /:id/resume resumes a suspended job" $ do
      jobId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "resume me"))
        _ <- runSimpleDb mkEnv $ Ops.suspendJob testSchema testTable (primaryKey jobRead)
        pure $ primaryKey jobRead

      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/resume") ""
        `shouldRespondWith` 204

      -- Verify the job is no longer suspended
      liftIO $ do
        Just job :: Maybe (JobRead (Stored ServantTestPayload)) <- runSimpleDb mkEnv $ Ops.getJobById testSchema testTable jobId
        suspended job `shouldBe` False

    it "POST /:id/resume returns 404 for non-existent job" $ do
      post "/api/v1/arbiter_servant_test/jobs/99999/resume" "" `shouldRespondWith` 404

    it "POST /:id/resume returns 409 for non-suspended job" $ do
      jobId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "not suspended"))
        pure $ primaryKey jobRead

      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/resume") ""
        `shouldRespondWith` 409

    it "POST /:id/promote on suspended job returns 409 with helpful message" $ do
      jobId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "promote suspended"))
        _ <- runSimpleDb mkEnv $ Ops.suspendJob testSchema testTable (primaryKey jobRead)
        pure $ primaryKey jobRead

      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/promote") ""
        `shouldRespondWith` 409

    it "POST /:id/promote on an in-flight job says so" $ do
      jobId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "promote in-flight"))
        _ <- runSimpleDb mkEnv $ Ops.claimNextVisibleJobs @_ @ServantTestPayload testSchema testTable 1 60
        pure $ primaryKey jobRead

      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/promote") ""
        `shouldRespondWith` 409
          { matchBody =
              MatchBody
                ( \_ body ->
                    if body == "Job is in flight - wait for its lease to lapse" then Nothing else Just ("unexpected body: " <> show body)
                )
          }

    it "POST /:id/suspend on already-suspended job returns 409" $ do
      jobId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "double suspend"))
        _ <- runSimpleDb mkEnv $ Ops.suspendJob testSchema testTable (primaryKey jobRead)
        pure $ primaryKey jobRead

      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/suspend") ""
        `shouldRespondWith` 409

    it "POST /:id/suspend on in-flight job returns 409" $ do
      jobId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "suspend in-flight"))
        _ <- runSimpleDb mkEnv $ Ops.claimNextVisibleJobs @_ @ServantTestPayload testSchema testTable 1 60
        pure $ primaryKey jobRead

      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/suspend") ""
        `shouldRespondWith` 409

    it "POST /:id/promote makes a delayed job immediately visible" $ do
      futureTime <- liftIO $ truncateToMicros . addUTCTime 3600 <$> getCurrentTime
      jobId <- liftIO $ do
        let job = setNotVisibleUntil (Just futureTime) $ defaultJob (TestMessage "promote delayed")
        Just inserted <- runSimpleDb mkEnv $ HL.insertJob job
        pure $ primaryKey inserted

      -- Job is delayed and unclaimable
      liftIO $ do
        visible <- runSimpleDb mkEnv $ Ops.claimNextVisibleJobs @_ @ServantTestPayload testSchema testTable 1 60
        length visible `shouldBe` 0

      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/promote") ""
        `shouldRespondWith` 204

      -- After promote, job is now visible (claimable)
      liftIO $ do
        visible <- runSimpleDb mkEnv $ Ops.claimNextVisibleJobs @_ @ServantTestPayload testSchema testTable 1 60
        length visible `shouldBe` 1
        primaryKey (head visible) `shouldBe` jobId

  describe "Reschedule API" $ with (cleanupDb >> pure app) $ do
    let reschedule jobId at = postJson (rowPath "jobs" jobId "reschedule") (encode (object ["runAt" .= at]))
        insertedId message = liftIO $ do
          Just inserted <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage message))
          pure (primaryKey inserted)
        later = liftIO $ truncateToMicros . addUTCTime 7200 <$> getCurrentTime

    it "POST /:id/reschedule sets when the job becomes visible and clears its throttle marker" $ do
      at <- later
      jobId <- insertedId "reschedule me"
      liftIO $ setColumns "throttled_until = NOW() + interval '1 hour', not_visible_until = NOW() + interval '1 hour'" jobId

      reschedule jobId at `shouldRespondWith` 204

      liftIO $ do
        Just job :: Maybe (JobRead (Stored ServantTestPayload)) <- runSimpleDb mkEnv $ Ops.getJobById testSchema testTable jobId
        notVisibleUntil job `shouldBe` Just at
        throttleMarked jobId `shouldReturn` False

    it "POST /:id/reschedule refuses an in-flight job" $ do
      at <- later
      jobId <- insertedId "reschedule in-flight"
      liftIO . void $ runSimpleDb mkEnv $ Ops.claimNextVisibleJobs @_ @ServantTestPayload testSchema testTable 1 60
      reschedule jobId at `shouldRespondWith` statusWithBody 409 "Job is in flight - wait for its lease to lapse"

    it "POST /:id/reschedule takes a job whose lease lapsed" $ do
      at <- later
      jobId <- insertedId "reschedule lapsed"
      liftIO $ do
        void $ runSimpleDb mkEnv $ Ops.claimNextVisibleJobs @_ @ServantTestPayload testSchema testTable 1 60
        setColumns "not_visible_until = NOW() - INTERVAL '1 second'" jobId
      reschedule jobId at `shouldRespondWith` 204
      liftIO $ do
        Just job :: Maybe (JobRead (Stored ServantTestPayload)) <- runSimpleDb mkEnv $ Ops.getJobById testSchema testTable jobId
        (notVisibleUntil job, claimedBy job) `shouldBe` (Just at, Nothing)

    it "POST /:id/reschedule voids the lapsed claim" $ do
      at <- later
      jobId <- insertedId "reschedule voids"
      [claimed] <- liftIO $ runSimpleDb mkEnv $ Ops.claimNextVisibleJobs @_ @ServantTestPayload testSchema testTable 1 60
      liftIO $ setColumns "not_visible_until = NOW() - INTERVAL '1 second'" jobId
      reschedule jobId at `shouldRespondWith` 204
      liftIO $ do
        runSimpleDb mkEnv (Ops.updateJobForRetry testSchema testTable 1 "late failure" claimed) `shouldReturn` 0
        Just job :: Maybe (JobRead (Stored ServantTestPayload)) <- runSimpleDb mkEnv $ Ops.getJobById testSchema testTable jobId
        notVisibleUntil job `shouldBe` Just at

    it "POST /:id/reschedule refuses a suspended job" $ do
      at <- later
      jobId <- insertedId "reschedule suspended"
      liftIO . void $ runSimpleDb mkEnv $ Ops.suspendJob testSchema testTable jobId
      reschedule jobId at `shouldRespondWith` statusWithBody 409 "Job is suspended - use resume endpoint"

    it "POST /:id/reschedule refuses a cancel-flagged job" $ do
      at <- later
      jobId <- insertedId "reschedule cancelled"
      liftIO $ setColumns "cancel_requested_at = NOW()" jobId
      reschedule jobId at `shouldRespondWith` statusWithBody 409 "Job is cancelled - it waits for removal"

    it "POST /:id/reschedule returns 404 for non-existent job" $ do
      at <- later
      reschedule 99999 at `shouldRespondWith` 404

  describe "Groups API" $ with (cleanupDb >> pure app) $ do
    let seedGroups = liftIO $ do
          forM_ ["big-1", "big-2", "big-3"] $ \message ->
            runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "big" (TestMessage message))
          void . runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "small" (TestMessage "small-1"))
          [held] <- runSimpleDb mkEnv $ HL.claimNextVisibleJobs 1 60 :: IO [JobRead ServantTestPayload]
          pure (primaryKey held)
        groupsAt query = do
          resp <- get (queuePath ("groups" <> query))
          liftIO (decodeBody resp :: IO GroupsResponse)

    it "GET /groups lists open groups largest first with the job at each head" $ do
      heldId <- seedGroups
      body <- groupsAt ""
      liftIO $ do
        groupsTotal body `shouldBe` 2
        map gsGroupKey (groups body) `shouldBe` ["big", "small"]
        map gsJobCount (groups body) `shouldBe` [3, 1]
        map gsInFlight (groups body) `shouldBe` [True, False]
        map gsHeadStatus (groups body) `shouldBe` [Just InFlight, Just Ready]
        map gsHeadJobId (take 1 (groups body)) `shouldBe` [Just heldId]

    it "GET /groups reports the holder as the head of a held group" $ do
      heldId <- liftIO $ do
        void . runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "hd" (TestMessage "hd-1"))
        Just sibling <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "hd" (TestMessage "hd-2"))
        [held] <- runSimpleDb mkEnv $ HL.claimNextVisibleJobs 1 60 :: IO [JobRead ServantTestPayload]
        -- A visible sibling with more attempts ranks ahead of the holder in claim order.
        setColumns "attempts = 2" (primaryKey sibling)
        pure (primaryKey held)
      body <- groupsAt "?group_key=hd"
      liftIO $ do
        map gsInFlight (groups body) `shouldBe` [True]
        map gsHeadJobId (groups body) `shouldBe` [Just heldId]
        map gsHeadStatus (groups body) `shouldBe` [Just InFlight]

    it "GET /groups narrows to one key and pages" $ do
      _ <- seedGroups
      narrowed <- groupsAt "?group_key=small"
      paged <- groupsAt "?limit=1&offset=1"
      liftIO $ do
        (groupsTotal narrowed, map gsGroupKey (groups narrowed)) `shouldBe` (1, ["small"])
        (groupsTotal paged, groupsLimit paged, groupsOffset paged) `shouldBe` (2, 1, 1)
        map gsGroupKey (groups paged) `shouldBe` ["small"]

    it "GET /groups skips an exhausted row the claim skips" $ do
      nextId <- liftIO $ do
        void . runSimpleDb mkEnv $ HL.insertJob (setMaxAttempts (Just 1) (defaultGroupedJob "ex" (TestMessage "ex-1")))
        Just next <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "ex" (TestMessage "ex-2"))
        [spent] <- runSimpleDb mkEnv $ HL.claimNextVisibleJobs 1 60 :: IO [JobRead ServantTestPayload]
        -- The last attempt's lease lapses, so the row waits for the reaper.
        setColumns "not_visible_until = NOW() - INTERVAL '1 second'" (primaryKey spent)
        pure (primaryKey next)
      body <- groupsAt "?group_key=ex"
      liftIO $ do
        map gsHeadJobId (groups body) `shouldBe` [Just nextId]
        map gsHeadStatus (groups body) `shouldBe` [Just Ready]

    it "GET /groups marks a head that a full concurrency key holds back" $ do
      jobId <- liftIO $ do
        Just job <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "gated" (TestMessage "gated-1"))
        setColumns "concurrency_key = 'cc:full', concurrency_prefix = 'cc'" (primaryKey job)
        void . withResource sharedPool $ \conn ->
          PG.execute_ conn . fromString . T.unpack $
            "INSERT INTO " <> arbiterConcurrencyPoliciesTable testSchema <> " VALUES ('cc', 1, 0)"
        pure (primaryKey job)
      statsResp <- get (queuePath "stats")
      resp <- get (queuePath "groups?group_key=gated")
      liftIO $ do
        queueStats <- stats <$> (decodeBody statsResp :: IO StatsResponse)
        (Ops.readyJobs queueStats, Ops.blockedJobs queueStats) `shouldBe` (0, 1)
        body :: Value <- decodeBody resp
        let heads = case body of
              Object top | Just (Array page) <- KM.lookup "groups" top ->
                [(KM.lookup "headJobId" group, KM.lookup "headBlocked" group) | Object group <- toList page]
              _ -> []
        heads `shouldBe` [(Just (toJSON jobId), Just (Bool True))]

    it "GET /groups counts a job rescheduled into the past as ready" $ do
      jobId <- liftIO $ do
        inAnHour <- addUTCTime 3600 <$> getCurrentTime
        Just job <-
          runSimpleDb mkEnv $ HL.insertJob (setNotVisibleUntil (Just inAnHour) (defaultGroupedJob "rs" (TestMessage "rs-1")))
        pure (primaryKey job)
      past <- liftIO $ addUTCTime (-60) <$> getCurrentTime
      postJson (rowPath "jobs" jobId "reschedule") (encode (object ["runAt" .= past])) `shouldRespondWith` 204
      body <- groupsAt "?group_key=rs"
      liftIO $ map gsReadyCount (groups body) `shouldBe` [1]

  -- QueueOverview's instances also carry a gauge snapshot through the shared gate.
  describe "Job wire contract" $ do
    -- A job object as a server predating the trace and claim fields sends it.
    let olderJob =
          [aesonQQ|
            { "primaryKey": 1
            , "payload": {"tag": "TestMessage", "contents": "older server"}
            , "queueName": "arbiter_servant_test"
            , "groupKey": null
            , "insertedAt": "2026-01-01T00:00:00Z"
            , "updatedAt": "2026-01-01T00:00:00Z"
            , "attempts": 0
            , "lastError": null
            , "priority": 0
            , "lastAttemptedAt": null
            , "notVisibleUntil": null
            , "dedupKey": null
            , "maxAttempts": null
            }
          |]

    it "decodes a job object carrying none of the fields added since" $
      (claimSeq <$> decode @(JobRead ServantTestPayload) (encode olderJob)) `shouldBe` Just 0

  describe "Landing overview wire contract" $ do
    let overview =
          Ops.QueueOverview
            { Ops.overviewQueue = "greetings"
            , Ops.overviewStats =
                Ops.QueueStats
                  { Ops.totalJobs = 8
                  , Ops.readyJobs = 3
                  , Ops.inFlightJobs = 2
                  , Ops.scheduledJobs = 1
                  , Ops.backoffJobs = 1
                  , Ops.throttledJobs = 0
                  , Ops.suspendedJobs = 1
                  , Ops.cancelledJobs = 0
                  , Ops.exhaustedJobs = 0
                  , Ops.blockedJobs = 0
                  , Ops.oldestReadyAgeSeconds = Just 12.5
                  , Ops.oldestInFlightAgeSeconds = Just 4.5
                  , Ops.dlqJobs = 2
                  , Ops.kindCounts = Map.fromList [("TestMessage", 5), ("TestCalculation", 3)]
                  , Ops.dlqKindCounts = Map.fromList [("TestMessage", 2)]
                  }
            , Ops.overviewQueuePaused = True
            , Ops.overviewWorkersLive = 4
            , Ops.overviewWorkersPaused = 1
            }

    it "encodes one queue's entry exactly" $
      toJSON overview
        `shouldBe` [aesonQQ|
          { "queue": "greetings"
          , "paused": true
          , "workersLive": 4
          , "workersPaused": 1
          , "stats":
              { "totalJobs": 8
              , "readyJobs": 3
              , "inFlightJobs": 2
              , "scheduledJobs": 1
              , "backoffJobs": 1
              , "throttledJobs": 0
              , "suspendedJobs": 1
              , "cancelledJobs": 0
              , "exhaustedJobs": 0
              , "blockedJobs": 0
              , "dlqJobs": 2
              , "oldestReadyAgeSeconds": 12.5
              , "oldestInFlightAgeSeconds": 4.5
              , "kindCounts": { "TestMessage": 5, "TestCalculation": 3 }
              , "dlqKindCounts": { "TestMessage": 2 }
              }
          }
        |]

    it "round-trips, which is what the shared gauge payload relies on" $
      decode (encode overview) `shouldBe` Just overview

    -- A stats object as a replica predating the kind rollup publishes it.
    let olderStats =
          [aesonQQ|
            { "totalJobs": 8
            , "readyJobs": 3
            , "inFlightJobs": 2
            , "scheduledJobs": 1
            , "backoffJobs": 1
            , "throttledJobs": 0
            , "suspendedJobs": 1
            , "cancelledJobs": 0
            , "dlqJobs": 2
            , "oldestReadyAgeSeconds": 12.5
            , "oldestInFlightAgeSeconds": 4.5
            }
          |]

    it "decodes a stats object carrying no kind rollup" $
      (Ops.kindCounts <$> decode @Ops.QueueStats (encode olderStats)) `shouldBe` Just Map.empty

  describe "Consumer API" $ with (cleanupDb >> pure app) $ do
    it "POST /claim leases a job and returns its lease" $ do
      liftIO $ void $ runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "claim me"))

      response <- postJson "/api/v1/arbiter_servant_test/claim" "{\"maxJobs\":1,\"leaseSeconds\":30}"
      liftIO $ simpleStatus response `shouldBe` status200

      let claimed = decodeClaim response
      liftIO $ length claimed `shouldBe` 1
      liftIO $ claimedBy (head claimed) `shouldNotBe` Nothing

    it "POST /claim keeps the decoded jobs and reports a rejected row it cannot dead-letter" $ liftIO @(WaiSession ()) $ do
      logged <- newIORef []
      let capturing = LogCallback (\_ msg _ -> atomicModifyIORef' logged (\seen -> (msg : seen, ())))
          reportingApp = arbiterApp @ServantTestRegistry serverConfig {serverLogConfig = defaultLogConfig {logDestination = capturing}}
          dlqTbl = Schema.jobQueueDLQTable testSchema testTable
      claimed <- do
        Just poison <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "poison"))
        void $ runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "sibling"))
        corruptPayload (Schema.jobQueueTable testSchema testTable) "id" (primaryKey poison)
        withResource sharedPool $ \conn -> do
          void $
            PG.execute_
              conn
              ( fromString . T.unpack $
                  "ALTER TABLE "
                    <> dlqTbl
                    <> " ADD CONSTRAINT reject_poison CHECK (job_id <> "
                    <> T.pack (show (primaryKey poison))
                    <> ")"
              )
          decodeClaim
            <$> runWaiSession (postJson "/api/v1/arbiter_servant_test/claim" "{\"maxJobs\":2}") reportingApp
              `finally` PG.execute_ conn (fromString . T.unpack $ "ALTER TABLE " <> dlqTbl <> " DROP CONSTRAINT reject_poison")
      map payload claimed `shouldBe` [TestMessage "sibling"]
      messages <- readIORef logged
      messages `shouldSatisfy` any (T.isPrefixOf "Dead-letter undecodable job in arbiter_servant_test failed")

    it "POST /:id/ack completes a job the lease still holds" $ do
      liftIO $ void $ runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "ack me"))
      claimed <- decodeClaim <$> postJson "/api/v1/arbiter_servant_test/claim" "{\"maxJobs\":1}"
      let job = head claimed

      postJson (ackPath job) (leaseBody job) `shouldRespondWith` 204

      liftIO $ do
        gone :: Maybe (JobRead (Stored ServantTestPayload)) <-
          runSimpleDb mkEnv $ Ops.getJobById testSchema testTable (primaryKey job)
        gone `shouldBe` Nothing

    it "POST /:id/ack refuses a lease the caller does not hold" $ do
      liftIO $ void $ runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "not yours"))
      claimed <- decodeClaim <$> postJson "/api/v1/arbiter_servant_test/claim" "{\"maxJobs\":1}"
      let job = head claimed
          forged = encode (JobLease (claimSeq job) UUID.nil)

      postJson (ackPath job) forged `shouldRespondWith` 409

    it "POST /:id/ack keeps the result it carries for the parent rollup" $ do
      parent <- liftIO $ do
        Right (root :| _) <-
          runSimpleDb mkEnv
            $ JT.insertJobTree testSchema testTable
            $ JT.rollup
              (defaultJob (TestMessage "rollup parent"))
              (JT.leaf (defaultJob (TestMessage "rollup child")) :| [])
        pure (primaryKey root)

      claimed <- decodeClaim <$> postJson "/api/v1/arbiter_servant_test/claim" "{\"maxJobs\":1}"
      let child = head claimed
          body = encode (AckRequest (JobLease (claimSeq child) (fromMaybe UUID.nil (claimedBy child))) (Just ["done" :: Text]))

      postJson (ackPath child) body `shouldRespondWith` 204

      liftIO $ do
        (results, _, _, _) <- runSimpleDb mkEnv $ Ops.readChildResultsRaw testSchema testTable parent
        Map.lookup (primaryKey child) results `shouldBe` Just (toJSON ["done" :: Text])

    it "POST /:id/ack refuses a result the queue's type does not accept" $ do
      liftIO $ void $ runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "wrong result"))
      claimed <- decodeClaim <$> postJson "/api/v1/arbiter_servant_test/claim" "{\"maxJobs\":1}"
      let job = head claimed
          body = encode $ object ["claimSeq" .= claimSeq job, "claimedBy" .= claimedBy job, "result" .= (42 :: Int)]

      postJson (ackPath job) body `shouldRespondWith` 400

    it "POST /:id/ack refuses a job a worker pool holds" $ do
      job <- liftIO $ do
        void $ runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "pool job"))
        void $ runSimpleDb mkEnv $ Ops.registerWorker testSchema poolWorkerId testTable Nothing Nothing 300 Nothing
        head <$> runSimpleDb mkEnv (Ops.claimNextVisibleJobsAs @_ @ServantTestPayload testSchema testTable 1 60 poolWorkerId)

      postJson (ackPath job) (leaseBody job) `shouldRespondWith` 409

      liftIO $ do
        still :: Maybe (JobRead (Stored ServantTestPayload)) <-
          runSimpleDb mkEnv $ Ops.getJobById testSchema testTable (primaryKey job)
        fmap primaryKey still `shouldBe` Just (primaryKey job)

    it "POST /:id/nack hands the job back without spending an attempt" $ do
      liftIO $ void $ runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "nack me"))
      claimed <- decodeClaim <$> postJson "/api/v1/arbiter_servant_test/claim" "{\"maxJobs\":1}"
      let job = head claimed

      postJson (nackPath job) (leaseBody job) `shouldRespondWith` 204

      liftIO $ do
        Just back :: Maybe (JobRead (Stored ServantTestPayload)) <-
          runSimpleDb mkEnv $ Ops.getJobById testSchema testTable (primaryKey job)
        attempts back `shouldBe` attempts job - 1

    it "POST /maintenance runs a pass" $ do
      response <- post "/api/v1/maintenance" ""
      liftIO $ simpleStatus response `shouldBe` status200

  describe "Stats API" $ with (cleanupDb >> pure app) $ do
    it "GET /api/v1/arbiter_servant_test/stats returns zero counts for empty queue" $ do
      resp <- get "/api/v1/arbiter_servant_test/stats"
      liftIO $ do
        body :: StatsResponse <- decodeBody resp
        let queueStats = stats body
        Ops.totalJobs queueStats `shouldBe` 0
        Ops.readyJobs queueStats `shouldBe` 0
        Ops.inFlightJobs queueStats `shouldBe` 0
        Ops.scheduledJobs queueStats `shouldBe` 0
        Ops.backoffJobs queueStats `shouldBe` 0
        Ops.suspendedJobs queueStats `shouldBe` 0
        Ops.oldestReadyAgeSeconds queueStats `shouldBe` Nothing
        timestamp body `shouldSatisfy` (not . T.null)

    it "GET /api/v1/arbiter_servant_test/stats reflects inserted and claimed jobs" $ do
      -- Insert 3 jobs, claim 1
      liftIO $ do
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "stats1"))
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "stats2"))
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "stats3"))
        _ <- runSimpleDb mkEnv $ Ops.claimNextVisibleJobs @_ @ServantTestPayload testSchema testTable 1 60
        pure ()

      resp <- get "/api/v1/arbiter_servant_test/stats"
      liftIO $ do
        body :: StatsResponse <- decodeBody resp
        let queueStats = stats body
        Ops.totalJobs queueStats `shouldBe` 3
        Ops.readyJobs queueStats `shouldBe` 2
        Ops.inFlightJobs queueStats `shouldBe` 1
        Ops.scheduledJobs queueStats `shouldBe` 0
        Ops.oldestReadyAgeSeconds queueStats `shouldSatisfy` isJust

  describe "Cron API" $ with (cleanupDb >> pure app) $ do
    let seedCron name expr overlap = liftIO $ runSimpleDb mkEnv $ do
          _ <- Ops.upsertCronDefault testSchema name testTable expr overlap Nothing True
          pure ()

    it "GET /api/v1/cron/schedules returns seeded schedules" $ do
      seedCron "list-a" "0 3 * * *" "SkipOverlap"
      seedCron "list-b" "*/5 * * * *" "AllowOverlap"

      resp <- get "/api/v1/cron/schedules"
      liftIO $ do
        simpleStatus resp `shouldBe` status200
        let body = decode @(Map.Map Text [CS.CronScheduleRow]) (simpleBody resp)
        case body of
          Just decoded -> case Map.lookup "cronSchedules" decoded of
            Just rows -> length rows `shouldSatisfy` (>= 2)
            Nothing -> fail "Missing cronSchedules key"
          Nothing -> fail "Failed to decode response"

    it "PATCH /api/v1/cron/schedules/:name with empty body is a no-op" $ do
      seedCron "test-cron" "* * * * *" "AllowOverlap"

      let fields
            CS.CronScheduleRow
              { CS.defaultExpression = defaultExpr
              , CS.defaultOverlap = defaultOv
              , CS.overrideExpression = overrideExpr
              , CS.overrideOverlap = overrideOv
              , CS.overrideTimezone = overrideTz
              , CS.enabled = isEnabled
              } =
              (defaultExpr, defaultOv, overrideExpr, overrideOv, overrideTz, isEnabled)

      beforeFields <- liftIO $ do
        Just row <- runSimpleDb mkEnv $ Ops.getCronScheduleByName testSchema "test-cron"
        pure (fields row)

      resp <-
        request
          "PATCH"
          "/api/v1/cron/schedules/test-cron"
          [("Content-Type", "application/json")]
          "{}"

      liftIO $ do
        simpleStatus resp `shouldBe` status200
        case decode @CS.CronScheduleRow (simpleBody resp) of
          Nothing -> fail "Failed to decode response"
          -- No-op leaves the operator-facing fields untouched.
          Just afterRow -> fields afterRow `shouldBe` beforeFields

    it "PATCH /api/v1/cron/schedules/:name updates expression override" $ do
      seedCron "expr-test" "* * * * *" "AllowOverlap"

      resp <-
        request
          "PATCH"
          "/api/v1/cron/schedules/expr-test"
          [("Content-Type", "application/json")]
          (encode [aesonQQ|{"overrideExpression": "0 3 * * *"}|])

      liftIO $ do
        simpleStatus resp `shouldBe` status200
        case decode @CS.CronScheduleRow (simpleBody resp) of
          Just CS.CronScheduleRow {CS.overrideExpression = overrideExpr} -> overrideExpr `shouldBe` Just "0 3 * * *"
          Nothing -> fail "Failed to decode response"

    it "PATCH /api/v1/cron/schedules/:name clears override with null" $ do
      seedCron "clear-test" "* * * * *" "AllowOverlap"

      -- Set an override first
      _ <-
        request
          "PATCH"
          "/api/v1/cron/schedules/clear-test"
          [("Content-Type", "application/json")]
          (encode [aesonQQ|{"overrideExpression": "0 3 * * *"}|])

      -- Clear it with null
      resp <-
        request
          "PATCH"
          "/api/v1/cron/schedules/clear-test"
          [("Content-Type", "application/json")]
          (encode [aesonQQ|{"overrideExpression": null}|])

      liftIO $ do
        simpleStatus resp `shouldBe` status200
        case decode @CS.CronScheduleRow (simpleBody resp) of
          Just CS.CronScheduleRow {CS.overrideExpression = overrideExpr} -> overrideExpr `shouldBe` Nothing
          Nothing -> fail "Failed to decode response"

    it "PATCH /api/v1/cron/schedules/:name can disable a schedule" $ do
      seedCron "disable-test" "* * * * *" "AllowOverlap"

      resp <-
        request
          "PATCH"
          "/api/v1/cron/schedules/disable-test"
          [("Content-Type", "application/json")]
          (encode [aesonQQ|{"enabled": false}|])

      liftIO $ do
        simpleStatus resp `shouldBe` status200
        case decode @CS.CronScheduleRow (simpleBody resp) of
          Just CS.CronScheduleRow {CS.enabled = isEnabled} -> isEnabled `shouldBe` False
          Nothing -> fail "Failed to decode response"

    it "PATCH /api/v1/cron/schedules/:name rejects invalid cron expression" $ do
      seedCron "bad-expr" "* * * * *" "AllowOverlap"

      resp <-
        request
          "PATCH"
          "/api/v1/cron/schedules/bad-expr"
          [("Content-Type", "application/json")]
          (encode [aesonQQ|{"overrideExpression": "not a cron"}|])

      liftIO $ simpleStatus resp `shouldBe` status400

    it "PATCH /api/v1/cron/schedules/:name updates timezone override" $ do
      seedCron "tz-test" "0 9 * * *" "SkipOverlap"

      resp <-
        request
          "PATCH"
          "/api/v1/cron/schedules/tz-test"
          [("Content-Type", "application/json")]
          (encode [aesonQQ|{"overrideTimezone": "America/New_York"}|])

      liftIO $ do
        simpleStatus resp `shouldBe` status200
        case decode @CS.CronScheduleRow (simpleBody resp) of
          Just CS.CronScheduleRow {CS.overrideTimezone = overrideTz} ->
            overrideTz `shouldBe` Just "America/New_York"
          Nothing -> fail "Failed to decode response"

    it "PATCH /api/v1/cron/schedules/:name rejects invalid timezone" $ do
      seedCron "bad-tz" "* * * * *" "AllowOverlap"

      resp <-
        request
          "PATCH"
          "/api/v1/cron/schedules/bad-tz"
          [("Content-Type", "application/json")]
          (encode [aesonQQ|{"overrideTimezone": "Made/Up_Zone"}|])

      liftIO $ simpleStatus resp `shouldBe` status400

    it "POST /api/v1/cron/schedules/:name/run stamps a run request" $ do
      seedCron "run-me" "0 3 * * *" "SkipOverlap"

      resp <- request "POST" "/api/v1/cron/schedules/run-me/run" [] ""

      liftIO $ do
        simpleStatus resp `shouldBe` status204
        Just row <- runSimpleDb mkEnv $ Ops.getCronScheduleByName testSchema "run-me"
        CS.runRequestedAt row `shouldSatisfy` isJust

    it "POST /api/v1/cron/schedules/:name/run 409s a schedule with a run pending" $ do
      seedCron "run-twice" "0 3 * * *" "SkipOverlap"
      first <- request "POST" "/api/v1/cron/schedules/run-twice/run" [] ""
      second <- request "POST" "/api/v1/cron/schedules/run-twice/run" [] ""

      liftIO $ do
        simpleStatus first `shouldBe` status204
        simpleStatus second `shouldBe` status409
        Just row <- runSimpleDb mkEnv $ Ops.getCronScheduleByName testSchema "run-twice"
        CS.runRequestedAt row `shouldSatisfy` isJust

    it "POST /api/v1/cron/schedules/:name/run 404s an unknown schedule" $ do
      resp <- request "POST" "/api/v1/cron/schedules/no-such-schedule/run" [] ""
      liftIO $ simpleStatus resp `shouldBe` status404

    it "POST /api/v1/cron/schedules/:name/run 409s a disabled schedule" $ do
      seedCron "run-disabled" "0 3 * * *" "SkipOverlap"
      _ <-
        request
          "PATCH"
          "/api/v1/cron/schedules/run-disabled"
          [("Content-Type", "application/json")]
          (encode [aesonQQ|{"enabled": false}|])

      resp <- request "POST" "/api/v1/cron/schedules/run-disabled/run" [] ""

      liftIO $ do
        simpleStatus resp `shouldBe` status409
        Just row <- runSimpleDb mkEnv $ Ops.getCronScheduleByName testSchema "run-disabled"
        CS.runRequestedAt row `shouldBe` Nothing

    it "PATCH /api/v1/cron/schedules/:name rejects invalid overlap policy" $ do
      seedCron "bad-overlap" "* * * * *" "AllowOverlap"

      resp <-
        request
          "PATCH"
          "/api/v1/cron/schedules/bad-overlap"
          [("Content-Type", "application/json")]
          (encode [aesonQQ|{"overrideOverlap": "BadPolicy"}|])

      liftIO $ simpleStatus resp `shouldBe` status400

    it "PATCH /api/v1/cron/schedules/:name with non-existent name returns 404" $ do
      resp <-
        request
          "PATCH"
          "/api/v1/cron/schedules/does-not-exist"
          [("Content-Type", "application/json")]
          "{}"

      liftIO $ simpleStatus resp `shouldBe` status404

  describe "Queues API" $ with (cleanupDb >> pure app) $ do
    it "GET /api/v1/queues returns list of all available queues" $ do
      get "/api/v1/queues"
        `shouldRespondWith` jsonMatch [aesonQQ|{ "queues": ["arbiter_servant_test"] }|]

    it "GET /api/v1/queues/:queue/details returns null before any state, then the row" $ do
      -- No arbiter_queues row exists yet.
      noneResp <- get "/api/v1/queues/arbiter_servant_test/details"
      liftIO $ do
        body :: Maybe Q.QueueRow <- decodeBody noneResp
        body `shouldBe` Nothing

      -- Pausing creates the row lazily.
      post "/api/v1/queues/arbiter_servant_test/pause" "" `shouldRespondWith` 204
      someResp <- get "/api/v1/queues/arbiter_servant_test/details"
      liftIO $ do
        body :: Maybe Q.QueueRow <- decodeBody someResp
        fmap Q.paused body `shouldBe` Just True

    it "POST /api/v1/queues/:queue/pause then resume flips the paused flag" $ do
      post "/api/v1/queues/arbiter_servant_test/pause" "" `shouldRespondWith` 204
      liftIO $ do
        Just queueRow <- runSimpleDb mkEnv $ Ops.getQueue testSchema "arbiter_servant_test"
        Q.paused queueRow `shouldBe` True

      post "/api/v1/queues/arbiter_servant_test/resume" "" `shouldRespondWith` 204
      liftIO $ do
        Just queueRow <- runSimpleDb mkEnv $ Ops.getQueue testSchema "arbiter_servant_test"
        Q.paused queueRow `shouldBe` False

    it "POST /api/v1/queues/:queue/pause returns 404 for unknown queue" $ do
      post "/api/v1/queues/not-a-real-queue/pause" "" `shouldRespondWith` 404

    it "POST /api/v1/queues/:queue/resume returns 404 for unknown queue" $ do
      post "/api/v1/queues/not-a-real-queue/resume" "" `shouldRespondWith` 404

  describe "Workers API" $ with (cleanupDb >> pure app) $ do
    let testWorkerId = "11111111-1111-1111-1111-111111111111"
        seedWorker = liftIO $ withResource sharedPool $ \conn ->
          PG.execute
            conn
            ( fromString $
                "INSERT INTO "
                  <> T.unpack testSchema
                  <> ".arbiter_workers (worker_id, queue_name) VALUES (?, ?)"
            )
            (testWorkerId :: Text, "arbiter_servant_test" :: Text)

    it "GET /api/v1/workers lists registered workers" $ do
      _ <- seedWorker
      resp <- get "/api/v1/workers"
      liftIO $ do
        body :: WorkersResponse <- decodeBody resp
        length (workers body) `shouldBe` 1
        forM_ (workers body) $ \worker -> W.paused worker `shouldBe` False

    it "POST /api/v1/workers/:id/pause then resume flips the paused flag" $ do
      _ <- seedWorker

      post (TE.encodeUtf8 $ "/api/v1/workers/" <> testWorkerId <> "/pause") ""
        `shouldRespondWith` 204
      pausedResp <- get "/api/v1/workers"
      liftIO $ do
        body :: WorkersResponse <- decodeBody pausedResp
        map W.paused (workers body) `shouldBe` [True]

      post (TE.encodeUtf8 $ "/api/v1/workers/" <> testWorkerId <> "/resume") ""
        `shouldRespondWith` 204
      resumedResp <- get "/api/v1/workers"
      liftIO $ do
        body :: WorkersResponse <- decodeBody resumedResp
        map W.paused (workers body) `shouldBe` [False]

    it "POST /api/v1/workers/:id/pause returns 404 for unknown worker" $ do
      post "/api/v1/workers/22222222-2222-2222-2222-222222222222/pause" ""
        `shouldRespondWith` 404

    it "POST /api/v1/workers/:id/resume returns 404 for unknown worker" $ do
      post "/api/v1/workers/22222222-2222-2222-2222-222222222222/resume" ""
        `shouldRespondWith` 404

  describe "Events API" $
    it "GET /api/v1/events/stream relays notifications on the event channel" $ do
      chunks <- newIORef []
      arrived <- newEmptyMVar
      let streamRequest = defaultRequest {requestMethod = "GET", pathInfo = ["api", "v1", "events", "stream"]}
          capture builder = do
            atomicModifyIORef' chunks (\acc -> (LB.toStrict (Builder.toLazyByteString builder) : acc, ()))
            void (tryPutMVar arrived ())
          awaitChunk needle = do
            got <- timeout 5_000_000 (takeMVar arrived)
            received <- readIORef chunks
            if any (BS.isInfixOf needle) received
              then pure True
              else if isJust got then awaitChunk needle else pure False
      streaming <- forkIO . void $ app streamRequest $ \response -> do
        let (_, _, withBody) = responseToStream response
        withBody (\body -> body capture (pure ()))
        pure ResponseReceived
      flip finally (killThread streaming) $ do
        awaitChunk "\"event\":\"connected\"" `shouldReturn` True
        void . withResource sharedPool $ \conn ->
          PG.execute_ conn (fromString ("NOTIFY " <> T.unpack Schema.eventStreamingChannel <> ", '{\"event\":\"ping\"}'"))
        awaitChunk "\"event\":\"ping\"" `shouldReturn` True

  describe "Maintenance API" $ do
    (pacedEnv, pacedConfig) <- runIO $ do
      setupOnce connStr pacedSchema rateLimitTable False
      setupRateLimitPolicy connStr pacedSchema
      pacedEnv <- createSimpleEnvWithPool (Proxy @RLReg) sharedPool pacedSchema
      (,) pacedEnv <$> initArbiterServer (runSimpleDb pacedEnv)
    let pacedCleanup = withResource sharedPool $ cleanupData pacedSchema rateLimitTable

    with (pacedCleanup >> pure (arbiterApp @RLReg pacedConfig)) $
      it "POST /maintenance holds the sparse operations to their own cadence" $ do
        firstResp <- post "/api/v1/maintenance" ""
        secondResp <- post "/api/v1/maintenance" ""
        liftIO $ do
          firstPass :: MaintenanceResponse <- decodeBody firstResp
          secondPass :: MaintenanceResponse <- decodeBody secondResp
          maintenanceFailed firstPass `shouldBe` []
          maintenanceFailed secondPass `shouldBe` []
          Map.member "prune-rate-limit-buckets" (maintenanceOps firstPass) `shouldBe` True
          Map.member "prune-rate-limit-buckets" (maintenanceOps secondPass) `shouldBe` False
          Map.member "sweep-stale-workers" (maintenanceOps secondPass) `shouldBe` True

    with (pacedCleanup >> pure (arbiterApp @RLReg pacedConfig)) $ do
      let grant key amount =
            postJson
              (TE.encodeUtf8 ("/api/v1/rate-limits/rl/buckets/" <> key <> "/tokens"))
              (encode (object ["tokens" .= (amount :: Double)]))

      it "POST /rate-limits/:prefix/buckets/:key/tokens wakes the key's throttled jobs" $ do
        liftIO $ do
          void
            (runSimpleDb pacedEnv (HL.insertJobsBatch (replicate 5 (defaultJob (RLPayload "tenant" 1)))) :: IO [JobRead RLPayload])
          admitted <- runSimpleDb pacedEnv (HL.claimNextVisibleJobs 100 60) :: IO [JobRead RLPayload]
          length admitted `shouldBe` 3
        grant "rl:tenant" 2 `shouldRespondWith` jsonMatch [aesonQQ|{"woken": 2}|]
        liftIO $ do
          woken <- runSimpleDb pacedEnv (HL.claimNextVisibleJobs 100 60) :: IO [JobRead RLPayload]
          length woken `shouldBe` 2

      it "POST /rate-limits/:prefix/buckets/:key/tokens refuses an unknown prefix and a key outside it" $ do
        postJson "/api/v1/rate-limits/nope/buckets/nope:x/tokens" (encode (object ["tokens" .= (1 :: Double)]))
          `shouldRespondWith` 404
        grant "other:x" 1 `shouldRespondWith` 400

      it "POST /rate-limits/:prefix/buckets/:key/tokens refuses an amount that is not positive" $ do
        grant "rl:tenant" 0 `shouldRespondWith` 400
        grant "rl:tenant" (-5) `shouldRespondWith` 400

      it "POST /rate-limits/prune deletes full buckets idle past the given age" $ do
        -- A top-up seeds an absent bucket at full.
        grant "rl:idle" 1 `shouldRespondWith` jsonMatch [aesonQQ|{"woken": 0}|]
        post "/api/v1/rate-limits/prune" "" `shouldRespondWith` jsonMatch [aesonQQ|{"pruned": 0}|]
        post "/api/v1/rate-limits/prune?idle=0" "" `shouldRespondWith` jsonMatch [aesonQQ|{"pruned": 1}|]
        post "/api/v1/rate-limits/prune?idle=-1" "" `shouldRespondWith` 400

      it "POST /concurrency/prune reports the rows it deleted" $
        post "/api/v1/concurrency/prune" "" `shouldRespondWith` jsonMatch [aesonQQ|{"pruned": 0}|]

    missingConfig <- runIO $ do
      missingEnv <- createSimpleEnvWithPool (Proxy @ServantTestRegistry) sharedPool missingSchema
      initArbiterServer (runSimpleDb missingEnv)
    with (pure (arbiterApp @ServantTestRegistry missingConfig)) $
      it "POST /maintenance names the operations that raised" $ do
        resp <- post "/api/v1/maintenance" ""
        liftIO $ do
          simpleStatus resp `shouldBe` status200
          body :: MaintenanceResponse <- decodeBody resp
          maintenanceOps body `shouldBe` Map.empty
          maintenanceFailed body `shouldSatisfy` elem "sweep-stale-workers"
