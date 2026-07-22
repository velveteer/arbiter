{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Servant.API (spec) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ (DLQJob (..), dlqPrimaryKey)
import Arbiter.Core.Job.Types (DedupKey (..), Job (..), JobRead, JobStatus (..), defaultGroupedJob, defaultJob)
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.Queues qualified as Q
import Arbiter.Core.Worker qualified as W
import Arbiter.Simple (createSimpleEnvWithPool, runSimpleDb)
import Arbiter.Test.Setup (cleanupData, createSharedPool, setupOnce, truncateToMicros)
import Control.Monad (forM_)
import Data.Aeson (FromJSON, ToJSON, Value, decode, encode, object, (.=))
import Data.Aeson.QQ.Simple (aesonQQ)
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time (addUTCTime, getCurrentTime)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Network.HTTP.Types (status200, status204, status400, status404, status409)
import Test.Hspec
import Test.Hspec.Wai

import Arbiter.Servant (arbiterApp, initArbiterServer)
import Arbiter.Servant.Types
  ( ApiJob (..)
  , ApiJobWithStatus (..)
  , ApiJobWrite (..)
  , BatchDeleteResponse (..)
  , BatchInsertRequest (..)
  , BatchInsertResponse (..)
  , DLQResponse (..)
  , JobResponse (..)
  , JobsResponse (..)
  , StatsResponse (..)
  , WorkersResponse (..)
  )

jsonMatch :: Value -> ResponseMatcher
jsonMatch v = ResponseMatcher 200 [] (MatchBody matcher)
  where
    matcher _ body = case decode body of
      Just actual | actual == v -> Nothing
      Just actual -> Just $ "JSON mismatch:\n  expected: " <> show v <> "\n  actual: " <> show actual
      Nothing -> Just "Response body is not valid JSON"

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

-- | Decode a JSON response body or fail the test
decodeBody :: (FromJSON a) => SResponse -> IO a
decodeBody resp = case decode (simpleBody resp) of
  Just a -> pure a
  Nothing -> fail $ "Failed to decode JSON response: " <> show (simpleBody resp)

spec :: ByteString -> Spec
spec connStr = do
  runIO (setupOnce connStr testSchema testTable False)
  sharedPool <- runIO (createSharedPool connStr)
  serverConfig <- runIO (initArbiterServer (Proxy @ServantTestRegistry) connStr testSchema)
  let app = arbiterApp @ServantTestRegistry serverConfig

  let cleanupDb :: IO ()
      cleanupDb = withResource sharedPool $ \conn -> cleanupData testSchema testTable conn

  mkEnv <- runIO (createSimpleEnvWithPool (Proxy @ServantTestRegistry) sharedPool testSchema)

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
        body :: JobResponse (ApiJob ServantTestPayload) <- decodeBody postResp
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
                [ "payload"
                    .= object
                      [ "tag" .= ("TestMessage" :: Text)
                      , "contents" .= ("delayed" :: Text)
                      ]
                , "notVisibleUntil" .= futureTime
                ]
          )

      liftIO $ do
        body :: JobResponse (ApiJob ServantTestPayload) <- decodeBody postResp
        let j = unApiJob (job body)
        payload j `shouldBe` TestMessage "delayed"
        notVisibleUntil j `shouldBe` Just futureTime

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
        body :: JobResponse (ApiJob ServantTestPayload) <- decodeBody firstResp
        let j = unApiJob (job body)
        payload j `shouldBe` TestMessage "first"
        pure (primaryKey j)

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
        body :: JobResponse (ApiJob ServantTestPayload) <- decodeBody dupResp
        let j = unApiJob (job body)
        primaryKey j `shouldBe` firstId
        payload j `shouldBe` TestMessage "first"

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

      -- Batch insert with same dedup key - should skip the duplicate
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
        body :: JobResponse (ApiJob ServantTestPayload) <- decodeBody resp
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

      -- Request with limit=2 - should return 2 jobs but report total of 3
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

      -- Filter by group key - should return only groupA job
      resp <- get "/api/v1/arbiter_servant_test/jobs?group_key=groupA"
      liftIO $ do
        body :: JobsResponse ServantTestPayload <- decodeBody resp
        jobsTotal body `shouldBe` 1
        length (jobs body) `shouldBe` 1
        -- Verify only groupA jobs returned (not groupB)
        forM_ (jobs body) $ \j -> groupKey (ajwsJob j) `shouldBe` Just "groupA"

    it "GET /api/v1/arbiter_servant_test/jobs sort_by/sort_dir changes ordering" $ do
      ids <- liftIO $ do
        Just j1 <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "sort1"))
        Just j2 <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "sort2"))
        Just j3 <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "sort3"))
        pure $ map primaryKey [j1, j2, j3]
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
          runSimpleDb mkEnv $
            HL.insertJobTree $
              JT.rollup
                (defaultGroupedJob "tree-parent" (TestMessage "parent"))
                ( JT.leaf (defaultJob (TestMessage "child-a"))
                    :| [JT.leaf (defaultJob (TestMessage "child-b"))]
                )
        pure (primaryKey parent, map primaryKey children)

      -- roots_only excludes children, keeping only the root parent
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
        forM_ childIds $ \cid -> (cid `elem` returned) `shouldBe` True

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
          runSimpleDb mkEnv $
            HL.insertJobTree $
              JT.rollup
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
        Just bj <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "backoff-job"))
        claimedB <- runSimpleDb mkEnv $ HL.claimNextVisibleJobs 1 60 :: IO [JobRead ServantTestPayload]
        _ <- runSimpleDb mkEnv $ HL.updateJobForRetry 60 "boom" (head claimedB)
        -- ready
        _ <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "ready-job"))
        -- scheduled: future visibility, never attempted
        _ <- runSimpleDb mkEnv $ HL.insertJob ((defaultJob (TestMessage "scheduled-job")) {notVisibleUntil = Just future})
        -- suspended
        Just sj <- runSimpleDb mkEnv $ HL.insertJob (defaultJob (TestMessage "suspended-job"))
        _ <- runSimpleDb mkEnv $ Ops.suspendJob testSchema testTable (primaryKey sj)
        pure (primaryKey bj)

      let expectOne status pay = do
            resp <- get (TE.encodeUtf8 $ "/api/v1/arbiter_servant_test/jobs?status=" <> status)
            liftIO $ do
              body :: JobsResponse ServantTestPayload <- decodeBody resp
              jobsTotal body `shouldBe` 1
              map (payload . ajwsJob) (jobs body) `shouldBe` [pay]
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
          runSimpleDb mkEnv $
            HL.insertJobTree $
              JT.rollup
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

    it "POST /api/v1/arbiter_servant_test/jobs/:id/move-to-dlq returns 404 for non-existent job" $ do
      post "/api/v1/arbiter_servant_test/jobs/99999/move-to-dlq" "" `shouldRespondWith` 404

    it "POST /api/v1/arbiter_servant_test/jobs/:id/pause-children pauses children" $ do
      -- Insert a finalizer tree - parent suspended, children unsuspended
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

      -- Get with limit - should return 2 jobs out of 3 total
      limitResp <- get "/api/v1/arbiter_servant_test/dlq?limit=2"
      liftIO $ do
        body :: DLQResponse ServantTestPayload <- decodeBody limitResp
        dlqLimit body `shouldBe` 2
        dlqTotal body `shouldBe` 3
        length (dlqJobs body) `shouldBe` 2

      -- Get with offset - should return 2 remaining jobs
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
        `shouldRespondWith` jsonMatch [aesonQQ|{ "dlqJobs": [], "dlqTotal": 0, "dlqOffset": 0, "dlqLimit": 50 }|]

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
        -- Cancel parent (cascade) - removes the suspended parent
        _ <- runSimpleDb mkEnv $ HL.cancelJobCascade @_ @ServantTestRegistry @ServantTestPayload (primaryKey parent)
        -- Get DLQ job ID
        dlqs :: [DLQJob ServantTestPayload] <- runSimpleDb mkEnv $ HL.listDLQJobs 1 0
        pure $ dlqPrimaryKey (head dlqs)

      -- Retry should return 409 - parent is gone
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
        Ops.readyJobs s `shouldBe` 0
        Ops.inFlightJobs s `shouldBe` 0
        Ops.scheduledJobs s `shouldBe` 0
        Ops.backoffJobs s `shouldBe` 0
        Ops.suspendedJobs s `shouldBe` 0
        Ops.oldestReadyAgeSeconds s `shouldBe` Nothing
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
        Ops.readyJobs s `shouldBe` 2
        Ops.inFlightJobs s `shouldBe` 1
        Ops.scheduledJobs s `shouldBe` 0
        Ops.oldestReadyAgeSeconds s `shouldSatisfy` isJust

  describe "Cron API" $ with (cleanupDb >> pure app) $ do
    let seedCron name expr ov = liftIO $ runSimpleDb mkEnv $ do
          _ <- Ops.upsertCronDefault testSchema name testTable expr ov Nothing
          pure ()

    it "GET /api/v1/cron/schedules returns seeded schedules" $ do
      seedCron "list-a" "0 3 * * *" "SkipOverlap"
      seedCron "list-b" "*/5 * * * *" "AllowOverlap"

      resp <- get "/api/v1/cron/schedules"
      liftIO $ do
        simpleStatus resp `shouldBe` status200
        let body = decode @(Map.Map Text [CS.CronScheduleRow]) (simpleBody resp)
        case body of
          Just m -> case Map.lookup "cronSchedules" m of
            Just rows -> length rows `shouldSatisfy` (>= 2)
            Nothing -> fail "Missing cronSchedules key"
          Nothing -> fail "Failed to decode response"

    it "PATCH /api/v1/cron/schedules/:name with empty body is a no-op" $ do
      seedCron "test-cron" "* * * * *" "AllowOverlap"

      let fields
            CS.CronScheduleRow
              { CS.defaultExpression = de
              , CS.defaultOverlap = dov
              , CS.overrideExpression = oe
              , CS.overrideOverlap = oo
              , CS.overrideTimezone = ot
              , CS.enabled = en
              } =
              (de, dov, oe, oo, ot, en)

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
          Just CS.CronScheduleRow {CS.overrideExpression = oe} -> oe `shouldBe` Just "0 3 * * *"
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
          Just CS.CronScheduleRow {CS.overrideExpression = oe} -> oe `shouldBe` Nothing
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
          Just CS.CronScheduleRow {CS.enabled = en} -> en `shouldBe` False
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
          Just CS.CronScheduleRow {CS.overrideTimezone = ot} ->
            ot `shouldBe` Just "America/New_York"
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
        Just q <- runSimpleDb mkEnv $ Ops.getQueue testSchema "arbiter_servant_test"
        Q.paused q `shouldBe` True

      post "/api/v1/queues/arbiter_servant_test/resume" "" `shouldRespondWith` 204
      liftIO $ do
        Just q <- runSimpleDb mkEnv $ Ops.getQueue testSchema "arbiter_servant_test"
        Q.paused q `shouldBe` False

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
        forM_ (workers body) $ \w -> W.paused w `shouldBe` False

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
