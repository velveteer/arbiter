{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Servant.API (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ (DLQJob (..), dlqPrimaryKey)
import Arbiter.Core.Job.Types (DedupKey (..), Job (..), JobRead, defaultGroupedJob, defaultJob)
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Simple (createSimpleEnvWithPool, runSimpleDb)
import Arbiter.Test.Setup (cleanupData, setupOnce)
import Control.Monad (forM_)
import Data.Aeson (FromJSON, ToJSON, decode, encode, object, (.=))
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)
import Data.Pool (withResource)
import Data.Pool qualified as Pool
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time (UTCTime (..), addUTCTime, getCurrentTime, picosecondsToDiffTime)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Network.Wai.Test (SResponse, simpleBody)
import Test.Hspec
import Test.Hspec.Wai
import Test.Hspec.Wai.JSON

import Arbiter.Servant (arbiterApp, initArbiterServer)
import Arbiter.Servant.Types
  ( ApiJob (..)
  , ApiJobWrite (..)
  , BatchDeleteResponse (..)
  , BatchInsertRequest (..)
  , BatchInsertResponse (..)
  , DLQResponse (..)
  , JobResponse (..)
  , JobsResponse (..)
  , StatsResponse (..)
  )

-- Test schema
testSchema :: Text
testSchema = "arbiter_servant_test"

-- | Test payload type
data ServantTestPayload
  = TestMessage Text
  | TestCalculation Int Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Test registry
type ServantTestRegistry = '[ '("arbiter_servant_test", ServantTestPayload)]

-- Table name for tests
testTable :: Text
testTable = "arbiter_servant_test"

-- Create shared pool for all tests
createSharedPool :: ByteString -> IO (Pool.Pool PG.Connection)
createSharedPool connStr =
  Pool.newPool $
    Pool.setNumStripes (Just 1) $
      Pool.defaultPoolConfig
        (connectPostgreSQL connStr)
        close
        60
        5

-- | Decode a JSON response body or fail the test
decodeBody :: (FromJSON a) => SResponse -> IO a
decodeBody resp = case decode (simpleBody resp) of
  Just a -> pure a
  Nothing -> fail $ "Failed to decode JSON response: " <> show (simpleBody resp)

-- | Truncate to microsecond precision to match PostgreSQL TIMESTAMPTZ
truncateToMicros :: UTCTime -> UTCTime
truncateToMicros (UTCTime d t) =
  let micros = floor (t * 1e6) :: Integer
   in UTCTime d (picosecondsToDiffTime (micros * 1000000))

spec :: ByteString -> Spec
spec connStr = do
  runIO (setupOnce connStr testSchema testTable False)
  sharedPool <- runIO (createSharedPool connStr)
  serverConfig <- runIO (initArbiterServer (Proxy @ServantTestRegistry) connStr testSchema)
  let app = arbiterApp @ServantTestRegistry serverConfig

  let cleanupDb :: IO ()
      cleanupDb = withResource sharedPool $ \conn -> cleanupData testSchema testTable conn

  let mkEnv = createSimpleEnvWithPool (Proxy @ServantTestRegistry) sharedPool testSchema

  describe "Jobs API" $ with (cleanupDb >> pure app) $ do
    it "GET /api/v1/arbiter_servant_test/jobs returns empty list initially" $ do
      get "/api/v1/arbiter_servant_test/jobs"
        `shouldRespondWith` [json|{
          "jobs": [],
          "jobsTotal": 0,
          "jobsOffset": 0,
          "jobsLimit": 50,
          "childCounts": {},
          "pausedParents": [],
          "dlqChildCounts": {}
        }|]
          { matchStatus = 200
          }

    it "POST /api/v1/arbiter_servant_test/jobs inserts a new job" $ do
      postResp <-
        request
          "POST"
          "/api/v1/arbiter_servant_test/jobs"
          [("Content-Type", "application/json")]
          [json|{
          "payload": {"tag": "TestMessage", "contents": "test message"},
          "queueName": "arbiter_servant_test",
          "dedupKey": {"key": "test-dedup-1", "strategy": "ignore"},
          "groupKey": "group1",
          "priority": 0,
          "maxAttempts": 3
        }|]

      -- Verify POST response contains the inserted job
      liftIO $ do
        body :: JobResponse ServantTestPayload <- decodeBody postResp
        let j = unApiJob (job body)
        payload j `shouldBe` TestMessage "test message"
        groupKey j `shouldBe` Just "group1"
        dedupKey j `shouldBe` Just (IgnoreDuplicate "test-dedup-1")

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
                [ "payload" .= object ["tag" .= ("TestMessage" :: Text), "contents" .= ("delayed" :: Text)]
                , "notVisibleUntil" .= futureTime
                ]
          )

      liftIO $ do
        body :: JobResponse ServantTestPayload <- decodeBody postResp
        let j = unApiJob (job body)
        payload j `shouldBe` TestMessage "delayed"
        notVisibleUntil j `shouldBe` Just futureTime

    it "POST /api/v1/arbiter_servant_test/jobs rejects duplicate dedup key" $ do
      -- Insert first job
      request
        "POST"
        "/api/v1/arbiter_servant_test/jobs"
        [("Content-Type", "application/json")]
        [json|{
          "payload": {"tag": "TestMessage", "contents": "first"},
          "queueName": "arbiter_servant_test",
          "dedupKey": {"key": "duplicate-key", "strategy": "ignore"},
          "priority": 0
        }|]
        `shouldRespondWith` 200

      -- Try to insert duplicate
      request
        "POST"
        "/api/v1/arbiter_servant_test/jobs"
        [("Content-Type", "application/json")]
        [json|{
          "payload": {"tag": "TestMessage", "contents": "second"},
          "queueName": "arbiter_servant_test",
          "dedupKey": {"key": "duplicate-key", "strategy": "ignore"},
          "priority": 0
        }|]
        `shouldRespondWith` 409

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
          [json|{
            "payload": {"tag": "TestMessage", "contents": "existing"},
            "dedupKey": {"key": "batch-dedup", "strategy": "ignore"}
          }|]

      -- Batch insert with same dedup key — should skip the duplicate
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
                    ((defaultJob (TestMessage "duplicate")) {dedupKey = Just (IgnoreDuplicate "batch-dedup")})
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
        body :: JobResponse ServantTestPayload <- decodeBody resp
        let j = unApiJob (job body)
        payload j `shouldBe` TestMessage "get me"
        groupKey j `shouldBe` Just "group1"
        primaryKey j `shouldBe` jobId

    it "GET /api/v1/arbiter_servant_test/jobs/:id returns 404 for non-existent job" $ do
      get "/api/v1/arbiter_servant_test/jobs/99999" `shouldRespondWith` 404

    it "GET /api/v1/arbiter_servant_test/jobs supports limit parameter" $ do
      -- Insert 3 jobs
      liftIO $ do
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "g1" (TestMessage "msg1"))
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "g2" (TestMessage "msg2"))
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "g3" (TestMessage "msg3"))
        pure ()

      -- Request with limit=2 — should return 2 jobs but report total of 3
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

      -- Filter by group key — should return only groupA job
      resp <- get "/api/v1/arbiter_servant_test/jobs?group_key=groupA"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsTotal body `shouldBe` 1
        length (jobs body) `shouldBe` 1
        -- Verify only groupA jobs returned (not groupB)
        forM_ (jobs body) $ \j -> groupKey (unApiJob j) `shouldBe` Just "groupA"

    it "GET /api/v1/arbiter_servant_test/jobs returns dlqChildCounts for parent with DLQ'd children" $ do
      -- Insert parent + child
      parentId <- liftIO $ do
        Right (parent :| _children) <-
          runSimpleDb mkEnv $
            HL.insertJobTree $
              JT.rollup
                (defaultGroupedJob "dlq-count-parent" (TestMessage "parent"))
                (JT.leaf (defaultJob (TestMessage "dlq-count-child")) :| [])
        -- Claim and DLQ the child
        claimed <- runSimpleDb mkEnv $ HL.claimNextVisibleJobs 1 60 :: IO [JobRead ServantTestPayload]
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "child failed" (head claimed)
        pure $ primaryKey parent

      -- List jobs — the parent should appear with dlqChildCounts showing 1
      resp <- get "/api/v1/arbiter_servant_test/jobs"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        Map.lookup parentId (dlqChildCounts body) `shouldBe` Just 1

    it "GET /api/v1/arbiter_servant_test/jobs/in-flight returns empty list when no jobs are claimed" $ do
      resp <- get "/api/v1/arbiter_servant_test/jobs/in-flight"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsTotal body `shouldBe` 0
        jobs body `shouldBe` []

    it "GET /api/v1/arbiter_servant_test/jobs/in-flight returns claimed jobs" $ do
      liftIO $ do
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "in-flight test"))
        _ <- runSimpleDb mkEnv $ Ops.claimNextVisibleJobs @_ @ServantTestPayload testSchema testTable 1 60
        pure ()

      resp <- get "/api/v1/arbiter_servant_test/jobs/in-flight"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsTotal body `shouldBe` 1
        length (jobs body) `shouldBe` 1

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

    it "POST /api/v1/arbiter_servant_test/jobs/:id/move-to-dlq returns 404 for non-existent job" $ do
      post "/api/v1/arbiter_servant_test/jobs/99999/move-to-dlq" "" `shouldRespondWith` 404

    it "POST /api/v1/arbiter_servant_test/jobs/:id/pause-children pauses children" $ do
      -- Insert a finalizer tree — parent suspended, children unsuspended
      parentId <- liftIO $ do
        Right (parent :| _children) <-
          runSimpleDb mkEnv $
            JT.insertJobTree testSchema testTable $
              JT.rollup
                (defaultGroupedJob "pause-parent" (TestMessage "parent"))
                (JT.leaf (defaultGroupedJob "pause-child" (TestMessage "child")) :| [])
        pure $ primaryKey parent

      -- Pause children (they start unsuspended in finalizer pattern)
      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show parentId) <> "/pause-children") ""
        `shouldRespondWith` 204

      -- Verify children are actually suspended
      liftIO $ do
        allJobs :: [JobRead ServantTestPayload] <- runSimpleDb mkEnv $ Ops.listJobs testSchema testTable 10 0
        let childJobs = filter (\j -> payload j == TestMessage "child") allJobs
        length childJobs `shouldBe` 1
        forM_ childJobs $ \j -> suspended j `shouldBe` True

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
          runSimpleDb mkEnv $
            JT.insertJobTree testSchema testTable $
              JT.rollup
                (defaultGroupedJob "resume-parent" (TestMessage "parent"))
                (JT.leaf (defaultGroupedJob "resume-child" (TestMessage "child")) :| [])
        _ <- runSimpleDb mkEnv $ Ops.pauseChildren testSchema testTable (primaryKey parent)
        pure $ primaryKey parent

      -- Resume children
      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show parentId) <> "/resume-children") ""
        `shouldRespondWith` 204

      -- Verify children are no longer suspended
      liftIO $ do
        allJobs :: [JobRead ServantTestPayload] <- runSimpleDb mkEnv $ Ops.listJobs testSchema testTable 10 0
        let childJobs = filter (\j -> payload j == TestMessage "child") allJobs
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
        `shouldRespondWith` [json|{
          "dlqJobs": [],
          "dlqTotal": 0,
          "dlqOffset": 0,
          "dlqLimit": 50
        }|]
          { matchStatus = 200
          }

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

      -- Get with limit — should return 2 jobs out of 3 total
      limitResp <- get "/api/v1/arbiter_servant_test/dlq?limit=2"
      liftIO $ do
        body :: DLQResponse ServantTestPayload <- decodeBody limitResp
        dlqLimit body `shouldBe` 2
        dlqTotal body `shouldBe` 3
        length (dlqJobs body) `shouldBe` 2

      -- Get with offset — should return 2 remaining jobs
      offsetResp <- get "/api/v1/arbiter_servant_test/dlq?offset=1"
      liftIO $ do
        body :: DLQResponse ServantTestPayload <- decodeBody offsetResp
        dlqOffset body `shouldBe` 1
        dlqTotal body `shouldBe` 3
        length (dlqJobs body) `shouldBe` 2

    it "POST /api/v1/arbiter_servant_test/dlq/batch-delete deletes multiple DLQ jobs" $ do
      -- Insert 3 jobs and move to DLQ
      dlqIds <- liftIO $ do
        Just j1 <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "bd1" (TestMessage "batch del 1"))
        Just j2 <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "bd2" (TestMessage "batch del 2"))
        Just j3 <- runSimpleDb mkEnv $ HL.insertJob (defaultGroupedJob "bd3" (TestMessage "batch del 3"))
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "err" j1
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "err" j2
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "err" j3
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
        `shouldRespondWith` [json|{
          "dlqJobs": [],
          "dlqTotal": 0,
          "dlqOffset": 0,
          "dlqLimit": 50
        }|]
          { matchStatus = 200
          }

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
        `shouldRespondWith` [json|{
          "dlqJobs": [],
          "dlqTotal": 0,
          "dlqOffset": 0,
          "dlqLimit": 50
        }|]
          { matchStatus = 200
          }

      -- Verify job is back in main queue
      liftIO $ do
        allJobs :: [JobRead ServantTestPayload] <-
          runSimpleDb mkEnv $ Ops.listJobs testSchema testTable 10 0
        length allJobs `shouldBe` 1

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
        `shouldRespondWith` [json|{
          "dlqJobs": [],
          "dlqTotal": 0,
          "dlqOffset": 0,
          "dlqLimit": 50
        }|]
          { matchStatus = 200
          }

      -- Verify job is not in main queue either
      liftIO $ do
        allJobs :: [JobRead ServantTestPayload] <-
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
          runSimpleDb mkEnv $
            HL.insertJobTree $
              JT.rollup
                (defaultGroupedJob "orphan-parent" (TestMessage "parent"))
                (JT.leaf (defaultJob (TestMessage "orphan-child")) :| [])
        -- Claim and DLQ the child
        claimed <- runSimpleDb mkEnv $ HL.claimNextVisibleJobs 1 60 :: IO [JobRead ServantTestPayload]
        _ <- runSimpleDb mkEnv $ HL.moveToDLQ "child failed" (head claimed)
        -- Cancel parent (cascade) — removes the suspended parent
        _ <- runSimpleDb mkEnv $ HL.cancelJobCascade @_ @ServantTestRegistry @ServantTestPayload (primaryKey parent)
        -- Get DLQ job ID
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 1 0
        pure $ dlqPrimaryKey (head dlqs)

      -- Retry should return 409 — parent is gone
      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/dlq/" <> T.pack (show dlqId) <> "/retry") ""
        `shouldRespondWith` 409

    it "DELETE /api/v1/arbiter_servant_test/dlq/:id returns 404 for non-existent DLQ job" $ do
      delete "/api/v1/arbiter_servant_test/dlq/99999" `shouldRespondWith` 404

  describe "Suspend/Resume API" $ with (cleanupDb >> pure app) $ do
    it "POST /:id/suspend suspends a job" $ do
      jobId <- liftIO $ do
        Just jobRead <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "suspend me"))
        pure $ primaryKey jobRead

      post (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs/" <> T.pack (show jobId) <> "/suspend") ""
        `shouldRespondWith` 204

      -- Verify the job is actually suspended
      liftIO $ do
        Just job :: Maybe (JobRead ServantTestPayload) <- runSimpleDb mkEnv $ Ops.getJobById testSchema testTable jobId
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
        Just job :: Maybe (JobRead ServantTestPayload) <- runSimpleDb mkEnv $ Ops.getJobById testSchema testTable jobId
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
        let job = (defaultJob (TestMessage "promote delayed")) {notVisibleUntil = Just futureTime}
        Just inserted <- runSimpleDb mkEnv $ HL.insertJob job
        pure $ primaryKey inserted

      -- Job is delayed, not claimable
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

  describe "Stats API" $ with (cleanupDb >> pure app) $ do
    it "GET /api/v1/arbiter_servant_test/stats returns zero counts for empty queue" $ do
      resp <- get "/api/v1/arbiter_servant_test/stats"
      liftIO $ do
        body :: StatsResponse <- decodeBody resp
        let s = stats body
        Ops.totalJobs s `shouldBe` 0
        Ops.visibleJobs s `shouldBe` 0
        Ops.invisibleJobs s `shouldBe` 0
        Ops.oldestJobAgeSeconds s `shouldBe` Nothing
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
        let s = stats body
        Ops.totalJobs s `shouldBe` 3
        Ops.visibleJobs s `shouldBe` 2
        Ops.invisibleJobs s `shouldBe` 1
        Ops.oldestJobAgeSeconds s `shouldSatisfy` isJust

  describe "Queues API" $ with (cleanupDb >> pure app) $ do
    it "GET /api/v1/queues returns list of all available queues" $ do
      get "/api/v1/queues"
        `shouldRespondWith` [json|{
          "queues": ["arbiter_servant_test"]
        }|]
          { matchStatus = 200
          }
