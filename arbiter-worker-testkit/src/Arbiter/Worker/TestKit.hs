{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

-- | Parameterized worker-pool test suite, instantiated for each 'MonadArbiter' backend.
module Arbiter.Worker.TestKit
  ( workerSpec
  , listenerSpec
  , multiQueueListenerSpec
  ) where

import Arbiter.Core.Exceptions
  ( throwBranchCancel
  , throwJobStolen
  , throwNack
  , throwPermanent
  , throwRetryable
  , throwTreeCancel
  )
import Arbiter.Core.HasArbiterSchema (HasArbiterSchema (..), ResultOf)
import Arbiter.Core.HighLevel (QueueOperation, RegistryAdmissionPolicies)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Archive qualified as Archive
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types
  ( Job (..)
  , JobRead
  , ObservabilityHooks (..)
  , dayRetention
  , defaultJob
  , defaultObservabilityHooks
  )
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.Listen qualified as Listen
import Arbiter.Core.MonadArbiter (JobHandler, getListener, withDbTransaction)
import Arbiter.Core.QueueRegistry (RegistryTables)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config
  ( BatchCallbacks
  , WorkerConfig (..)
  , ack
  , ackAll
  , ackAllWith
  , ackWith
  , cancelBranch
  , cancelTree
  , defaultBatchedWorkerConfig
  , failPermanent
  , failRetry
  , nack
  , transactionalWorkerConfig
  )
import Control.Concurrent (threadDelay)
import Control.Monad (unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (toJSON)
import Data.ByteString (ByteString)
import Data.Foldable (toList, traverse_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.List (find, partition)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (isJust, isNothing, listToMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Database.PostgreSQL.Simple (Only (..), close, connectPostgreSQL)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Types (Identifier (..))
import Test.Hspec
import UnliftIO (MonadUnliftIO, atomically, bracket)
import UnliftIO.Async (withAsync)

-- | Build a worker-pool test suite for the given 'MonadArbiter' runner.
--
-- @mkSimple@/@mkFailing@ construct the backend's payload, @mkHandler@ adapts a
-- plain job action into the backend's 'JobHandler' shape (some backends pass a
-- connection, others do not), and @runM@ runs a backend action in 'IO'.
--
-- The queue under test declares @Maybe [Text]@ as its result type.
workerSpec
  :: forall payload m env
   . ( Eq payload
     , MonadUnliftIO m
     , QueueOperation m payload
     , RegistryAdmissionPolicies (RegistryOf m)
     , RegistryTables (RegistryOf m)
     , ResultOf m payload ~ Maybe [Text]
     , Show payload
     )
  => (Text -> payload)
  -- ^ Construct a simple task payload
  -> (Int -> payload)
  -- ^ Construct a failing task payload
  -> (forall r. (JobRead payload -> m r) -> JobHandler m payload r)
  -- ^ Adapt a job action into the backend's handler shape, result-polymorphic
  -> (forall a. env -> m a -> IO a)
  -- ^ Runner function (e.g. runSimpleDb env or runOrvilleTest env)
  -> SpecWith env
workerSpec mkSimple mkFailing mkHandler runM = do
  describe "Worker Pool" $ do
    it "processes jobs successfully" $ \env -> do
      completedRef <- newIORef []
      config <- mkConfig $ \job ->
        liftIO $ atomicModifyIORef' completedRef $ \jobs -> (payload job : jobs, ())
      let jobs =
            [ (defaultJob (mkSimple "Job 1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "Job 2")) {groupKey = Just "g2"}
            , (defaultJob (mkSimple "Job 3")) {groupKey = Just "g3"}
            ]

      runM env $ traverse_ HL.insertJob jobs

      withAsync (runM env $ runWorkerPool config {workerCount = 3, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ (== 3) . length <$> readIORef completedRef
        completed <- readIORef completedRef
        length completed `shouldBe` 3
        completed `shouldMatchList` [mkSimple "Job 1", mkSimple "Job 2", mkSimple "Job 3"]

    it "respects worker count concurrency limit" $ \env -> do
      activeRef <- newIORef (0 :: Int)
      maxActiveRef <- newIORef (0 :: Int)
      completedRef <- newIORef (0 :: Int)
      config <- mkConfig $ \_job -> do
        active <- liftIO $ atomicModifyIORef' activeRef $ \n -> let n' = n + 1 in (n', n')
        liftIO $ atomicModifyIORef' maxActiveRef $ \maxN -> (max maxN active, ())
        liftIO $ threadDelay 500_000
        liftIO $ atomicModifyIORef' activeRef $ \n -> (n - 1, ())
        liftIO $ atomicModifyIORef' completedRef $ \n -> (n + 1, ())
      let jobs =
            map
              (\i -> (defaultJob (mkSimple (T.pack $ "Job " <> show @Int i))) {groupKey = Just (T.pack $ "g" <> show i)})
              [1 .. 10]

      runM env $ traverse_ HL.insertJob jobs

      withAsync (runM env $ runWorkerPool config {workerCount = 3, pollInterval = 0.05}) $ \_ -> do
        waitUntil 10_000 $ (== 10) <$> readIORef completedRef
        completed <- readIORef completedRef
        completed `shouldBe` 10
        maxActive <- readIORef maxActiveRef
        maxActive `shouldSatisfy` (> 1)
        maxActive `shouldSatisfy` (<= 3)

    it "retries failed jobs up to max attempts" $ \env -> do
      attemptsRef <- newIORef (0 :: Int)
      -- mkFailing 3 marks a job that throws on its first two attempts.
      config <- mkConfig $ \_job -> do
        n <- liftIO $ atomicModifyIORef' attemptsRef $ \k -> (k + 1, k + 1)
        when (n < 3) $ throwRetryable "Not yet!"
      let job = (defaultJob (mkFailing 3)) {groupKey = Just "g1", maxAttempts = Just 5}

      void $ runM env $ HL.insertJob job

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1, jitter = NoJitter}) $ \_ -> do
        waitUntil 15_000 $ (== 3) <$> readIORef attemptsRef
        attempts' <- readIORef attemptsRef
        attempts' `shouldBe` 3

    it "moves jobs to DLQ after max attempts" $ \env -> do
      config <- mkConfig $ \_job -> throwRetryable "Always fails"
      let job = (defaultJob (mkSimple "Doomed")) {groupKey = Just "g1", maxAttempts = Just 1}

      void $ runM env $ HL.insertJob job

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ do
          dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
          pure (length dlqJobs == 1)
        dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
        length dlqJobs `shouldBe` 1
        let dlqJob = head dlqJobs
        payload (DLQ.jobSnapshot dlqJob) `shouldBe` mkSimple "Doomed"
        attempts (DLQ.jobSnapshot dlqJob) `shouldBe` 1

    it "archives a completed job when archiveFor is set" $ \env -> do
      config <- mkConfig $ \_job -> pure ()
      void $
        runM env $
          HL.insertJob ((defaultJob (mkSimple "arch-done")) {groupKey = Just "g1", archiveFor = Just dayRetention})

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (any ((== mkSimple "arch-done") . payload . Archive.jobSnapshot) arch)
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        map (payload . Archive.jobSnapshot) arch `shouldBe` [mkSimple "arch-done"]

    it "fetches an archived job by id" $ \env -> do
      config <- mkConfig $ \_job -> pure ()
      void $
        runM env $
          HL.insertJob ((defaultJob (mkSimple "arch-byid")) {groupKey = Just "gk", archiveFor = Just dayRetention})

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (any ((== mkSimple "arch-byid") . payload . Archive.jobSnapshot) arch)
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        let jid = primaryKey (Archive.jobSnapshot (head arch))
        found <- runM env $ HL.getArchivedJobById @payload jid
        fmap (payload . Archive.jobSnapshot) found `shouldBe` Just (mkSimple "arch-byid")
        miss <- runM env $ HL.getArchivedJobById @payload 999_999
        (miss :: Maybe (Archive.ArchiveJob payload)) `shouldBe` Nothing

    it "lists archived jobs by group key" $ \env -> do
      config <- mkConfig $ \_job -> pure ()
      let jobs =
            [ (defaultJob (mkSimple "g-a")) {groupKey = Just "ga", archiveFor = Just dayRetention}
            , (defaultJob (mkSimple "g-b")) {groupKey = Just "ga", archiveFor = Just dayRetention}
            , (defaultJob (mkSimple "other")) {groupKey = Just "gb", archiveFor = Just dayRetention}
            ]
      runM env $ traverse_ HL.insertJob jobs

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ do
          inGa <- runM env $ HL.listArchivedJobsByGroupKey @payload "ga" 100 0
          pure (length inGa == 2)
        inGa <- runM env $ HL.listArchivedJobsByGroupKey @payload "ga" 100 0
        map (payload . Archive.jobSnapshot) inGa `shouldMatchList` [mkSimple "g-a", mkSimple "g-b"]

    it "does not archive a job whose archiveFor is Nothing" $ \env -> do
      doneRef <- newIORef False
      config <- mkConfig $ \_job -> liftIO $ writeIORef doneRef True
      void $ runM env $ HL.insertJob ((defaultJob (mkSimple "no-arch")) {groupKey = Just "g1", archiveFor = Nothing})

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ readIORef doneRef
        threadDelay 300_000
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        map (payload . Archive.jobSnapshot) arch `shouldBe` []

    it "reaper purges archived jobs past their archiveFor window" $ \env -> do
      config <- mkConfig $ \_job -> pure ()
      void $ runM env $ HL.insertJob ((defaultJob (mkSimple "arch-purge")) {groupKey = Just "g1", archiveFor = Just 1})

      withAsync
        (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1, reaperInterval = 0.5})
        $ \_ -> do
          -- First archived, then purged once it ages past the 1s window.
          waitUntil 10_000 $ do
            arch <- runM env $ HL.listArchiveJobs @payload 100 0
            pure (any ((== mkSimple "arch-purge") . payload . Archive.jobSnapshot) arch)
          waitUntil 15_000 $ do
            arch <- runM env $ HL.listArchiveJobs @payload 100 0
            pure (not (any ((== mkSimple "arch-purge") . payload . Archive.jobSnapshot) arch))

    it "re-enqueues an archived job, keeping the archive row" $ \env -> do
      config <- mkConfig $ \_job -> pure ()
      void $
        runM env $
          HL.insertJob ((defaultJob (mkSimple "re-job")) {groupKey = Just "rg", archiveFor = Just dayRetention})

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        let countReJob arch = length (filter ((== mkSimple "re-job") . payload . Archive.jobSnapshot) arch)
        waitUntil 10_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (countReJob arch == 1)
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        let pk = Archive.archivePrimaryKey (head arch)
        reEnq <- runM env $ HL.reEnqueueFromArchive @payload pk
        (payload <$> reEnq) `shouldBe` Just (mkSimple "re-job")
        -- Original archive row is kept. The re-run is processed and archived too.
        waitUntil 10_000 $ do
          arch2 <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (countReJob arch2 == 2)

    it "purges an archived job by id" $ \env -> do
      config <- mkConfig $ \_job -> pure ()
      void $
        runM env $
          HL.insertJob ((defaultJob (mkSimple "purge-one")) {groupKey = Just "pg", archiveFor = Just dayRetention})

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (any ((== mkSimple "purge-one") . payload . Archive.jobSnapshot) arch)
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        let pk = Archive.archivePrimaryKey (head arch)
        deleted <- runM env $ HL.deleteArchiveJob @payload pk
        deleted `shouldBe` 1
        arch2 <- runM env $ HL.listArchiveJobs @payload 100 0
        map (payload . Archive.jobSnapshot) arch2 `shouldBe` []

    it "bulk-purges archived jobs" $ \env -> do
      config <- mkConfig $ \_job -> pure ()
      let jobs =
            [ (defaultJob (mkSimple "bp-1")) {groupKey = Just "b1", archiveFor = Just dayRetention}
            , (defaultJob (mkSimple "bp-2")) {groupKey = Just "b2", archiveFor = Just dayRetention}
            ]
      runM env $ traverse_ HL.insertJob jobs

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (length arch == 2)
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        let pks = map Archive.archivePrimaryKey arch
        deleted <- runM env $ HL.deleteArchiveJobsBatch @payload pks
        deleted `shouldBe` 2
        arch2 <- runM env $ HL.listArchiveJobs @payload 100 0
        map (payload . Archive.jobSnapshot) arch2 `shouldBe` []

    it "DLQ-retried job retains archiveFor and is archived on later success" $ \env -> do
      callsRef <- newIORef (0 :: Int)
      config <- mkConfig $ \_job -> do
        n <- liftIO $ atomicModifyIORef' callsRef (\c -> (c + 1, c + 1))
        when (n == 1) $ throwPermanent "fail first time"
      void $
        runM env $
          HL.insertJob
            ((defaultJob (mkSimple "dlq-arch")) {groupKey = Just "da", maxAttempts = Just 1, archiveFor = Just dayRetention})

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ do
          dlq <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
          pure (any ((== mkSimple "dlq-arch") . payload . DLQ.jobSnapshot) dlq)
        dlq <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
        let dlqId = DLQ.dlqPrimaryKey (head dlq)
        void $ runM env $ HL.retryFromDLQ @payload dlqId
        -- On the retry it succeeds. archive_for survived the DLQ round-trip, so it is archived.
        waitUntil 10_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (any ((== mkSimple "dlq-arch") . payload . Archive.jobSnapshot) arch)

    it "archives the row but stores no result for a Nothing result" $ \env -> do
      cfg :: WorkerConfig m payload <-
        transactionalWorkerConfig 1 (mkHandler (\_job -> pure (Nothing :: Maybe [Text])))
      void $ runM env $ HL.insertJob ((defaultJob (mkSimple "null-result")) {archiveFor = Just dayRetention})

      withAsync (runM env $ runWorkerPool cfg {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (any ((== mkSimple "null-result") . payload . Archive.jobSnapshot) arch)
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        let mine = find ((== mkSimple "null-result") . payload . Archive.jobSnapshot) arch
        (Archive.archivedResult =<< mine) `shouldBe` Nothing

    it "stores the wrapped value for a Just result" $ \env -> do
      cfg :: WorkerConfig m payload <-
        transactionalWorkerConfig 1 (mkHandler (\_job -> pure (Just ["kept"] :: Maybe [Text])))
      void $ runM env $ HL.insertJob ((defaultJob (mkSimple "just-result")) {archiveFor = Just dayRetention})

      withAsync (runM env $ runWorkerPool cfg {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (any ((== mkSimple "just-result") . payload . Archive.jobSnapshot) arch)
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        let mine = find ((== mkSimple "just-result") . payload . Archive.jobSnapshot) arch
        (Archive.archivedResult =<< mine) `shouldBe` Just (toJSON ["kept" :: Text])

    it "does not archive a result when archiveFor is unset" $ \env -> do
      doneRef <- newIORef False
      cfg :: WorkerConfig m payload <-
        transactionalWorkerConfig
          1
          (mkHandler (\_job -> liftIO (writeIORef doneRef True) >> pure (Just ["unkept"] :: Maybe [Text])))
      void $ runM env $ HL.insertJob (defaultJob (mkSimple "no-arch-result"))

      withAsync (runM env $ runWorkerPool cfg {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ readIORef doneRef
        threadDelay 300_000
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        map (payload . Archive.jobSnapshot) arch `shouldBe` []

    it "stores a child's result in the tree, not on its archive row" $ \env -> do
      cfg :: WorkerConfig m payload <-
        transactionalWorkerConfig 1 (mkHandler (\_job -> pure (Just ["child-result"] :: Maybe [Text])))
      let child = (defaultJob (mkSimple "arch-child")) {archiveFor = Just dayRetention}
      void $ runM env $ HL.insertJobTree $ defaultJob (mkSimple "arch-root") <~~ (child :| [])

      withAsync (runM env $ runWorkerPool cfg {workerCount = 2, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (any ((== mkSimple "arch-child") . payload . Archive.jobSnapshot) arch)
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        let childRow = find ((== mkSimple "arch-child") . payload . Archive.jobSnapshot) arch
        (Archive.archivedResult =<< childRow) `shouldBe` Nothing

    it "preserves the attempt count on the archived row" $ \env -> do
      callsRef <- newIORef (0 :: Int)
      config <- mkConfig $ \_job -> do
        n <- liftIO $ atomicModifyIORef' callsRef (\c -> (c + 1, c + 1))
        when (n == 1) $ throwRetryable "fail once"
      void $
        runM env $
          HL.insertJob
            ((defaultJob (mkSimple "arch-attempts")) {groupKey = Just "aa", maxAttempts = Just 5, archiveFor = Just dayRetention})

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1, jitter = NoJitter}) $ \_ -> do
        waitUntil 15_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (any ((== mkSimple "arch-attempts") . payload . Archive.jobSnapshot) arch)
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        let mine = find ((== mkSimple "arch-attempts") . payload . Archive.jobSnapshot) arch
        -- Claimed twice (one retryable failure, then success), so the ack-copy
        -- must carry attempts = 2, not reset it to 0.
        fmap (attempts . Archive.jobSnapshot) mine `shouldBe` Just 2

    it "preserves a child's parent linkage on its archived row" $ \env -> do
      cfg :: WorkerConfig m payload <-
        transactionalWorkerConfig 1 (mkHandler (\_job -> pure (Just ["ok"] :: Maybe [Text])))
      let child = (defaultJob (mkSimple "pl-child")) {archiveFor = Just dayRetention}
          root = (defaultJob (mkSimple "pl-root")) {archiveFor = Just dayRetention}
      void $ runM env $ HL.insertJobTree $ root <~~ (child :| [])

      withAsync (runM env $ runWorkerPool cfg {workerCount = 2, pollInterval = 0.1}) $ \_ -> do
        let snap p arch = Archive.jobSnapshot <$> find ((== mkSimple p) . payload . Archive.jobSnapshot) arch
        waitUntil 10_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (isJust (snap "pl-child" arch) && isJust (snap "pl-root" arch))
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        -- The child's archived parent_id must point at the root's original job id.
        (parentId =<< snap "pl-child" arch) `shouldBe` (primaryKey <$> snap "pl-root" arch)

    it "reaper purges only expired archived rows, keeping live ones" $ \env -> do
      config <- mkConfig $ \_job -> pure ()
      runM env $
        traverse_
          HL.insertJob
          [ (defaultJob (mkSimple "purge-expired")) {groupKey = Just "pe1", archiveFor = Just 1}
          , (defaultJob (mkSimple "purge-kept")) {groupKey = Just "pe2", archiveFor = Just dayRetention}
          ]

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1, reaperInterval = 0.5}) $ \_ -> do
        let has p arch = any ((== mkSimple p) . payload . Archive.jobSnapshot) arch
        waitUntil 10_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (has "purge-expired" arch && has "purge-kept" arch)
        waitUntil 15_000 $ do
          arch <- runM env $ HL.listArchiveJobs @payload 100 0
          pure (not (has "purge-expired" arch))
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        has "purge-kept" arch `shouldBe` True

    it "does not archive a job whose archiveFor is zero" $ \env -> do
      doneRef <- newIORef False
      config <- mkConfig $ \_job -> liftIO $ writeIORef doneRef True
      void $ runM env $ HL.insertJob ((defaultJob (mkSimple "zero-arch")) {groupKey = Just "z1", archiveFor = Just 0})

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ -> do
        waitUntil 10_000 $ readIORef doneRef
        threadDelay 300_000
        arch <- runM env $ HL.listArchiveJobs @payload 100 0
        map (payload . Archive.jobSnapshot) arch `shouldBe` []

    it "processes all jobs from different groups" $ \env -> do
      processingRef <- newIORef []
      config <- mkConfig $ \job -> do
        liftIO $ atomicModifyIORef' processingRef $ \groups -> (groupKey job : groups, ())
        liftIO $ threadDelay 1_000_000
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G2-1")) {groupKey = Just "g2"}
            , (defaultJob (mkSimple "G3-1")) {groupKey = Just "g3"}
            ]

      runM env $ traverse_ HL.insertJob jobs

      withAsync (runM env $ runWorkerPool config {workerCount = 3, pollInterval = 0.05}) $ \_ -> do
        waitUntil 10_000 $ (== 3) . length <$> readIORef processingRef
        processed <- readIORef processingRef
        length processed `shouldBe` 3
        processed `shouldMatchList` [Just "g1", Just "g2", Just "g3"]

    it "retries once then DLQs at maxAttempts 2" $ \env -> do
      attemptsRef <- newIORef (0 :: Int)
      retryRef <- newIORef (0 :: Int)
      dlqRef <- newIORef (0 :: Int)
      let hooks =
            defaultObservabilityHooks
              { onJobRetry = \_ _ -> liftIO $ atomicModifyIORef' retryRef (\n -> (n + 1, ()))
              , onJobFailedAndMovedToDLQ = \_ _ -> liftIO $ atomicModifyIORef' dlqRef (\n -> (n + 1, ()))
              }
      config <- mkConfig $ \_job -> do
        liftIO $ atomicModifyIORef' attemptsRef (\n -> (n + 1, ()))
        throwRetryable "always fails"

      let job = (defaultJob (mkSimple "Boundary")) {groupKey = Just "g1", maxAttempts = Just 2}
      void $ runM env $ HL.insertJob job

      withAsync
        (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1, jitter = NoJitter, observabilityHooks = hooks})
        $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef retryRef
          dlqAfterFirst <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
          length dlqAfterFirst `shouldBe` 0

          waitUntil 10_000 $ (== 1) <$> readIORef dlqRef
          dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
          length dlqJobs `shouldBe` 1
          attempts (DLQ.jobSnapshot (head dlqJobs)) `shouldBe` 2
          readIORef attemptsRef `shouldReturn` 2
          readIORef retryRef `shouldReturn` 1

    it "permanent exception goes straight to DLQ on first attempt" $ \env -> do
      retryCalls <- newIORef (0 :: Int)
      dlqCalls <- newIORef (0 :: Int)
      let hooks =
            defaultObservabilityHooks
              { onJobRetry = \_ _ -> liftIO $ atomicModifyIORef' retryCalls (\n -> (n + 1, ()))
              , onJobFailedAndMovedToDLQ = \_ _ -> liftIO $ atomicModifyIORef' dlqCalls (\n -> (n + 1, ()))
              }
      config <- mkConfig $ \_job -> throwPermanent "Unrecoverable error"

      void $ runM env $ HL.insertJob (defaultJob (mkSimple "PermanentFail"))

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1, observabilityHooks = hooks}) $ \_ -> do
        waitUntil 10_000 $ (== 1) <$> readIORef dlqCalls
        retryCount <- readIORef retryCalls
        retryCount `shouldBe` 0
        dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
        length dlqJobs `shouldBe` 1
        payload (DLQ.jobSnapshot (head dlqJobs)) `shouldBe` mkSimple "PermanentFail"
        attempts (DLQ.jobSnapshot (head dlqJobs)) `shouldBe` 1

    it "heartbeat keeps leases alive when every worker holds a long transaction" $ \env -> do
      -- workerCnt held below the shared pool size so heartbeat queries are not
      -- starved by the long-held handler transactions.
      let workerCnt = 3
      let jobNames = ["long-" <> T.pack (show @Int i) | i <- [1 .. workerCnt]]
      runM env $ traverse_ (\name -> void $ HL.insertJob (defaultJob (mkSimple name))) jobNames
      config <- mkConfig $ \_job -> liftIO $ threadDelay 10_000_000

      withAsync
        ( runM env $
            runWorkerPool
              config
                { workerCount = workerCnt
                , visibilityTimeout = 3
                , jobHeartbeatInterval = 1
                , pollInterval = 0.1
                , jitter = NoJitter
                }
        )
        $ \_ -> do
          threadDelay 8_000_000
          reclaimed <-
            runM env $
              HL.claimNextVisibleJobs @payload workerCnt 3
          length reclaimed `shouldBe` 0

    it "completing a job acks it and fires onJobSuccess" $ \env -> do
      successRef <- newIORef ([] :: [Int64])
      let handler (job :| _) cbs = ack cbs job
          hooks =
            defaultObservabilityHooks
              { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \js -> (primaryKey job : js, ())
              }
      void $ runM env $ HL.insertJob (defaultJob (mkSimple "ManualAckSuccess"))
      config <- mkBatchedConfig 1 1 handler
      let manualConfig = config {pollInterval = 0.1, observabilityHooks = hooks}

      withAsync (runM env $ runWorkerPool manualConfig) $ \_ ->
        waitUntil 10_000 $ (== 0) <$> runM env (HL.countJobs @payload)

      runM env (HL.countJobs @payload) `shouldReturn` 0
      successes <- readIORef successRef
      length successes `shouldBe` 1

    it "completing a reclaimed job skips without firing onJobSuccess" $ \env -> do
      successRef <- newIORef ([] :: [Int64])
      let handler (job :| _) cbs = do
            void $ HL.ackJob job
            ack cbs job
          hooks =
            defaultObservabilityHooks
              { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \js -> (primaryKey job : js, ())
              }
      void $ runM env $ HL.insertJob (defaultJob (mkSimple "ManualReclaimed"))
      config <- mkBatchedConfig 1 1 handler
      let manualConfig = config {pollInterval = 0.1, observabilityHooks = hooks}

      withAsync (runM env $ runWorkerPool manualConfig) $ \_ -> do
        waitUntil 10_000 $ (== 0) <$> runM env (HL.countJobs @payload)
        threadDelay 200_000

      successes <- readIORef successRef
      length successes `shouldBe` 0

    it "ackWith and ackAllWith store each job's result on its archive row" $ \env -> do
      let handler jobs cbs = do
            let (single, batch) = partition ((== mkSimple "aw-1") . payload) (toList jobs)
            traverse_ (\j -> ackWith cbs j (Just ["first"])) single
            ackAllWith cbs (map (\j -> (j, Just ["rest"])) batch)
      runM env $
        traverse_
          (\n -> void $ HL.insertJob ((defaultJob (mkSimple n)) {groupKey = Just "aw", archiveFor = Just dayRetention}))
          ["aw-1", "aw-2", "aw-3"]
      config <- mkBatchedConfig 1 3 handler

      withAsync (runM env $ runWorkerPool config {pollInterval = 0.1, jitter = NoJitter}) $ \_ ->
        waitUntil 10_000 $ (== 0) <$> runM env (HL.countJobs @payload)

      arch <- runM env $ HL.listArchiveJobs @payload 100 0
      let resultFor p = Archive.archivedResult =<< find ((== mkSimple p) . payload . Archive.jobSnapshot) arch
      resultFor "aw-1" `shouldBe` Just (toJSON ["first" :: Text])
      resultFor "aw-2" `shouldBe` Just (toJSON ["rest" :: Text])
      resultFor "aw-3" `shouldBe` Just (toJSON ["rest" :: Text])

  describe "Group Ordering" $ do
    it "processes jobs in the same group serially" $ \env -> do
      orderRef <- newIORef []
      activeRef <- newIORef (0 :: Int)
      maxActiveRef <- newIORef (0 :: Int)
      config <- mkConfig $ \job -> do
        active <- liftIO $ atomicModifyIORef' activeRef $ \n -> let n' = n + 1 in (n', n')
        liftIO $ atomicModifyIORef' maxActiveRef $ \maxN -> (max maxN active, ())
        liftIO $ threadDelay 200_000
        liftIO $ atomicModifyIORef' orderRef $ \order -> (payload job : order, ())
        liftIO $ atomicModifyIORef' activeRef $ \n -> (n - 1, ())
      let jobs =
            [ (defaultJob (mkSimple "First")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "Second")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "Third")) {groupKey = Just "g1"}
            ]

      runM env $ traverse_ HL.insertJob jobs

      withAsync (runM env $ runWorkerPool config {workerCount = 3, pollInterval = 0.05}) $ \_ -> do
        waitUntil 10_000 $ (== 3) . length <$> readIORef orderRef
        order <- readIORef orderRef
        length order `shouldBe` 3
        reverse order `shouldBe` [mkSimple "First", mkSimple "Second", mkSimple "Third"]
        maxActive <- readIORef maxActiveRef
        maxActive `shouldBe` 1

  describe "Observability Hooks" $ do
    it "onJobClaimed is called with start time when job is claimed" $ \env -> do
      claimedRef <- newIORef []
      successRef <- newIORef []
      let hooks =
            defaultObservabilityHooks
              { onJobClaimed = \job startTime -> liftIO $ atomicModifyIORef' claimedRef $ \jobs -> ((primaryKey job, startTime) : jobs, ())
              , onJobSuccess = \_ startTime _ -> liftIO $ atomicModifyIORef' successRef $ \ts -> (startTime : ts, ())
              }
      config <- mkConfig $ \_job -> pure ()
      let configWithHooks = config {observabilityHooks = hooks, workerCount = 1, pollInterval = 0.1}

      let job = (defaultJob (mkSimple "Test")) {groupKey = Just "g1"}
      Just inserted <- runM env $ HL.insertJob job

      withAsync (runM env $ runWorkerPool configWithHooks) $ \_ -> do
        waitUntil 10_000 $ (== 1) . length <$> readIORef claimedRef
        claimed <- readIORef claimedRef
        length claimed `shouldBe` 1
        waitUntil 10_000 $ (== 1) . length <$> readIORef successRef
        successTimes <- readIORef successRef
        let (claimedId, claimedStart) = head claimed
        -- onJobClaimed fires for the claimed job, before processing starts.
        claimedId `shouldBe` primaryKey inserted
        claimedStart `shouldSatisfy` (<= head successTimes)

    it "onJobSuccess is called with start and end times" $ \env -> do
      successRef <- newIORef []
      let hooks =
            defaultObservabilityHooks
              { onJobSuccess = \job startTime endTime ->
                  liftIO $ atomicModifyIORef' successRef $ \results -> ((primaryKey job, startTime, endTime) : results, ())
              }
      config <- mkConfig $ \_job -> liftIO $ threadDelay 200_000
      let configWithHooks = config {observabilityHooks = hooks, workerCount = 1, pollInterval = 0.1}

      let job = (defaultJob (mkSimple "Test")) {groupKey = Just "g1"}
      void $ runM env $ HL.insertJob job

      withAsync (runM env $ runWorkerPool configWithHooks) $ \_ -> do
        waitUntil 10_000 $ (== 1) . length <$> readIORef successRef
        results <- readIORef successRef
        length results `shouldBe` 1
        let (_, startTime, endTime) = head results
        endTime `shouldSatisfy` (> startTime)

    it "onJobFailure is called with start and end times on failure" $ \env -> do
      failureRef <- newIORef []
      successRef <- newIORef (0 :: Int)
      let hooks =
            defaultObservabilityHooks
              { onJobFailure = \job _err startTime endTime ->
                  liftIO $ atomicModifyIORef' failureRef $ \results -> ((primaryKey job, startTime, endTime) : results, ())
              , onJobSuccess = \_ _ _ -> liftIO $ atomicModifyIORef' successRef (\n -> (n + 1, ()))
              }
      config <- mkConfig $ \_job -> do
        liftIO $ threadDelay 200_000
        throwRetryable "Test failure"
      let configWithHooks = config {observabilityHooks = hooks, workerCount = 1, pollInterval = 0.1}

      let job = (defaultJob (mkSimple "Test")) {groupKey = Just "g1", maxAttempts = Just 1}
      void $ runM env $ HL.insertJob job

      withAsync (runM env $ runWorkerPool configWithHooks) $ \_ -> do
        waitUntil 10_000 $ (== 1) . length <$> readIORef failureRef
        results <- readIORef failureRef
        length results `shouldBe` 1
        let (_, startTime, endTime) = head results
        endTime `shouldSatisfy` (> startTime)
        -- A failing job never fires the success hook.
        readIORef successRef >>= (`shouldBe` 0)

    it "onJobHeartbeat is called periodically during job execution" $ \env -> do
      heartbeatRef <- newIORef (0 :: Int)
      let hooks =
            defaultObservabilityHooks
              { onJobHeartbeat = \_ _currentTime _startTime ->
                  liftIO $ atomicModifyIORef' heartbeatRef $ \count -> (count + 1, ())
              }
      config <- mkConfig $ \_job -> liftIO $ threadDelay 3_000_000
      let configWithHooks =
            config
              { observabilityHooks = hooks
              , workerCount = 1
              , pollInterval = 0.1
              , jobHeartbeatInterval = 1
              }

      let job = (defaultJob (mkSimple "LongRunning")) {groupKey = Just "g1"}
      void $ runM env $ HL.insertJob job

      withAsync (runM env $ runWorkerPool configWithHooks) $ \_ -> do
        waitUntil 10_000 $ (>= 2) <$> readIORef heartbeatRef
        heartbeatCount <- readIORef heartbeatRef
        heartbeatCount `shouldSatisfy` (>= 2)

    it "onJobRetry is called with backoff delay on retriable failure" $ \env -> do
      retryRef <- newIORef []
      let hooks =
            defaultObservabilityHooks
              { onJobRetry = \job backoff ->
                  liftIO $ atomicModifyIORef' retryRef $ \rs -> ((primaryKey job, backoff) : rs, ())
              }
      config <- mkConfig $ \_job -> throwRetryable "retry me"
      let configWithHooks = config {observabilityHooks = hooks, workerCount = 1, pollInterval = 0.1}

      let job = (defaultJob (mkSimple "RetryHook")) {groupKey = Just "g1", maxAttempts = Just 3}
      void $ runM env $ HL.insertJob job

      withAsync (runM env $ runWorkerPool configWithHooks) $ \_ -> do
        waitUntil 15_000 $ (== 2) . length <$> readIORef retryRef
        retries <- readIORef retryRef
        length retries `shouldBe` 2
        traverse_ (\(_, backoff) -> backoff `shouldSatisfy` (> 0)) retries

    it "onJobFailedAndMovedToDLQ is called when job exhausts retries" $ \env -> do
      dlqRef <- newIORef []
      let hooks =
            defaultObservabilityHooks
              { onJobFailedAndMovedToDLQ = \errMsg job ->
                  liftIO $ atomicModifyIORef' dlqRef $ \rs -> ((errMsg, primaryKey job) : rs, ())
              }
      config <- mkConfig $ \_job -> throwRetryable "always fails"
      let configWithHooks = config {observabilityHooks = hooks, workerCount = 1, pollInterval = 0.1}

      let job = (defaultJob (mkSimple "DLQHook")) {groupKey = Just "g1", maxAttempts = Just 1}
      void $ runM env $ HL.insertJob job

      withAsync (runM env $ runWorkerPool configWithHooks) $ \_ -> do
        waitUntil 10_000 $ (== 1) . length <$> readIORef dlqRef
        dlqCalls <- readIORef dlqRef
        length dlqCalls `shouldBe` 1
        let (errMsg, _) = head dlqCalls
        errMsg `shouldBe` "always fails"

  describe "Batched Job Mode" $ do
    it "processes a batch of jobs from the same group together" $ \env -> do
      batchesRef <- newIORef []
      let batchHandler jobs cbs = do
            let jobPayloads = map payload (toList jobs)
            liftIO $ atomicModifyIORef' batchesRef $ \batches -> (jobPayloads : batches, ())
            traverse_ (ack cbs) jobs
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-3")) {groupKey = Just "g1"}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler
      let batchedConfig = config {pollInterval = 0.050}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        waitUntil 10_000 $ (== 1) . length <$> readIORef batchesRef
        batches <- readIORef batchesRef
        length batches `shouldBe` 1
        let processedJobs = head batches
        processedJobs `shouldMatchList` [mkSimple "G1-1", mkSimple "G1-2", mkSimple "G1-3"]

    it "processes multiple groups as separate batches" $ \env -> do
      batchesRef <- newIORef []
      let batchHandler jobs cbs = do
            let jobPayloads = map payload (toList jobs)
            liftIO $ atomicModifyIORef' batchesRef $ \batches -> (jobPayloads : batches, ())
            traverse_ (ack cbs) jobs
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G2-1")) {groupKey = Just "g2"}
            , (defaultJob (mkSimple "G2-2")) {groupKey = Just "g2"}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 2 10 batchHandler
      let batchedConfig = config {pollInterval = 0.050}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        waitUntil 10_000 $ (== 2) . length <$> readIORef batchesRef
        batches <- readIORef batchesRef
        length batches `shouldBe` 2

    it "respects batch size limit" $ \env -> do
      batchSizesRef <- newIORef []
      let batchHandler jobs cbs = do
            let batchSize = length jobs
            liftIO $ atomicModifyIORef' batchSizesRef $ \sizes -> (batchSize : sizes, ())
            traverse_ (ack cbs) jobs
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-3")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-4")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-5")) {groupKey = Just "g1"}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 2 batchHandler
      let batchedConfig = config {pollInterval = 0.050}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        waitUntil 10_000 $ (== 3) . length <$> readIORef batchSizesRef
        batchSizes <- readIORef batchSizesRef
        batchSizes `shouldMatchList` [2, 2, 1]

    it "retries entire batch on failure" $ \env -> do
      attemptsRef <- newIORef (0 :: Int)
      let batchHandler jobs cbs = do
            atts <- liftIO $ atomicModifyIORef' attemptsRef $ \n -> (n + 1, n + 1)
            if atts < 2 then throwRetryable "Batch failed!" else traverse_ (ack cbs) jobs
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1", maxAttempts = Just 3}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1", maxAttempts = Just 3}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler
      let batchedConfig = config {pollInterval = 0.050, jitter = NoJitter}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        waitUntil 15_000 $ (== 2) <$> readIORef attemptsRef
        atts <- readIORef attemptsRef
        atts `shouldBe` 2

    it "moves entire batch to DLQ on max failures" $ \env -> do
      let batchHandler _jobs _cbs = throwRetryable "Always fails"
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1", maxAttempts = Just 1}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1", maxAttempts = Just 1}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler
      let batchedConfig = config {pollInterval = 0.050}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        waitUntil 10_000 $ do
          dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
          pure (length dlqJobs == 2)
        dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
        length dlqJobs `shouldBe` 2
        map (payload . DLQ.jobSnapshot) dlqJobs `shouldMatchList` [mkSimple "G1-1", mkSimple "G1-2"]

    it "uses each job's own maxAttempts in a batch" $ \env -> do
      let batchHandler _jobs _cbs = throwRetryable "Always fails"
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1", maxAttempts = Just 2}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1", maxAttempts = Just 3}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler
      let batchedConfig = config {pollInterval = 0.050, jitter = NoJitter}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        waitUntil 15_000 $ do
          dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
          pure (length dlqJobs == 2)
        dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
        length dlqJobs `shouldBe` 2
        let dlqFor name = head $ filter ((== mkSimple name) . payload . DLQ.jobSnapshot) dlqJobs
        attempts (DLQ.jobSnapshot (dlqFor "G1-1")) `shouldBe` 2
        attempts (DLQ.jobSnapshot (dlqFor "G1-2")) `shouldBe` 3

    it "completed jobs survive a throwNack while the rest reprocess" $ \env -> do
      callCountRef <- newIORef (0 :: Int)
      seenRef <- newIORef ([] :: [[payload]])
      completedRef <- newIORef ([] :: [payload])
      let batchHandler jobs cbs = do
            n <- liftIO $ atomicModifyIORef' callCountRef $ \c -> (c + 1, c + 1)
            liftIO $ atomicModifyIORef' seenRef $ \bs -> (map payload (toList jobs) : bs, ())
            let finish j = do
                  ack cbs j
                  liftIO $ atomicModifyIORef' completedRef $ \xs -> (payload j : xs, ())
            if n == 1
              then do
                traverse_ finish (filter ((== mkSimple "G1-1") . payload) (toList jobs))
                throwNack
              else traverse_ finish jobs
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-3")) {groupKey = Just "g1"}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler
      let batchedConfig = config {pollInterval = 0.1, visibilityTimeout = 2, jobHeartbeatInterval = 1}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        let names = map mkSimple ["G1-1", "G1-2", "G1-3"]
        waitUntil 15_000 $ do
          completed <- readIORef completedRef
          pure $ all (`elem` completed) names
        seen <- readIORef seenRef
        let occurrences p = length (filter (p `elem`) seen)
        occurrences (mkSimple "G1-1") `shouldBe` 1
        occurrences (mkSimple "G1-2") `shouldSatisfy` (>= 2)
        occurrences (mkSimple "G1-3") `shouldSatisfy` (>= 2)

    it "throwNack in single-job mode reprocesses without recording a failure" $ \env -> do
      callsRef <- newIORef (0 :: Int)
      config <- mkConfig $ \_job -> do
        n <- liftIO $ atomicModifyIORef' callsRef (\c -> (c + 1, c + 1))
        when (n == 1) throwNack
      void $ runM env $ HL.insertJob (defaultJob (mkSimple "tn-single"))

      withAsync
        (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1, visibilityTimeout = 2, jobHeartbeatInterval = 1})
        $ \_ -> do
          waitUntil 15_000 $ (>= 2) <$> readIORef callsRef
          dlq <- runM env (HL.listDLQJobs 100 0) :: IO [DLQ.DLQJob payload]
          filter (== mkSimple "tn-single") (map (payload . DLQ.jobSnapshot) dlq) `shouldBe` []

    it "calls heartbeat for all jobs in batch" $ \env -> do
      heartbeatJobsRef <- newIORef []
      let hooks =
            defaultObservabilityHooks
              { onJobHeartbeat = \job _currentTime _startTime ->
                  liftIO $ atomicModifyIORef' heartbeatJobsRef $ \jobs -> (primaryKey job : jobs, ())
              }
      let batchHandler jobs cbs = do
            liftIO $ threadDelay 6_000_000
            traverse_ (ack cbs) jobs
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1"}
            ]
      insertedJobs <- runM env $ HL.insertJobsBatch jobs
      let jobIds = map primaryKey insertedJobs
      config <- mkBatchedConfig 1 10 batchHandler
      let batchedConfig = config {observabilityHooks = hooks, pollInterval = 0.050, jobHeartbeatInterval = 1}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        let countFor jid = length . filter (== jid) <$> readIORef heartbeatJobsRef
        waitUntil 10_000 $ do
          job1Count <- countFor (head jobIds)
          job2Count <- countFor (jobIds !! 1)
          pure (job1Count >= 2 && job2Count >= 2)
        job1Before <- countFor (head jobIds)
        job2Before <- countFor (jobIds !! 1)
        threadDelay 1_200_000
        job1After <- countFor (head jobIds)
        job2After <- countFor (jobIds !! 1)
        job1After `shouldSatisfy` (> job1Before)
        job2After `shouldSatisfy` (> job2Before)

    it "a throw fails only the jobs not yet completed" $ \env -> do
      failuresRef <- newIORef ([] :: [Int64])
      successRef <- newIORef ([] :: [Int64])
      let hooks =
            defaultObservabilityHooks
              { onJobFailure = \job _ _ _ ->
                  liftIO $ atomicModifyIORef' failuresRef $ \fs -> (primaryKey job : fs, ())
              , onJobSuccess = \job _ _ ->
                  liftIO $ atomicModifyIORef' successRef $ \ss -> (primaryKey job : ss, ())
              }
      let batchHandler jobs cbs = do
            traverse_ (ack cbs) (take 2 (toList jobs))
            throwRetryable "Third job failed"
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-3")) {groupKey = Just "g1"}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler
      let batchedConfig = config {pollInterval = 0.050, observabilityHooks = hooks, jitter = NoJitter}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        waitUntil 10_000 $ (== 1) . length <$> readIORef failuresRef
        failures <- readIORef failuresRef
        length failures `shouldBe` 1
        successes <- readIORef successRef
        length successes `shouldBe` 2

    it "completing every job in a batch removes them from the queue" $ \env -> do
      processedRef <- newIORef False
      let batchHandler jobs cbs = do
            traverse_ (ack cbs) jobs
            liftIO $ atomicModifyIORef' processedRef $ \_ -> (True, ())
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1"}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler
      let batchedConfig = config {pollInterval = 0.050}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        waitUntil 10_000 $ readIORef processedRef
        processed <- readIORef processedRef
        processed `shouldBe` True
        remainingJobs <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
        length remainingJobs `shouldBe` 0

    it "ackAll bulk-acks the batch and fires onJobSuccess for each" $ \env -> do
      successRef <- newIORef ([] :: [Int64])
      let hooks =
            defaultObservabilityHooks
              { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \js -> (primaryKey job : js, ())
              }
      let batchHandler jobs cbs = ackAll cbs (toList jobs)
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-3")) {groupKey = Just "g1"}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler
      let batchedConfig = config {pollInterval = 0.050, observabilityHooks = hooks}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        waitUntil 10_000 $ (== 3) . length <$> readIORef successRef
        successes <- readIORef successRef
        length successes `shouldBe` 3
        runM env (HL.countJobs @payload) `shouldReturn` 0

    it "failPermanent callback DLQs one job while the rest complete" $ \env -> do
      successRef <- newIORef ([] :: [payload])
      let hooks =
            defaultObservabilityHooks
              { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \xs -> (payload job : xs, ())
              }
      let batchHandler jobs cbs =
            traverse_
              (\j -> if payload j == mkSimple "fp-bad" then failPermanent cbs j "bad input" else ack cbs j)
              (toList jobs)
      let jobs =
            [ (defaultJob (mkSimple "fp-good1")) {groupKey = Just "fp"}
            , (defaultJob (mkSimple "fp-bad")) {groupKey = Just "fp"}
            , (defaultJob (mkSimple "fp-good2")) {groupKey = Just "fp"}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler

      withAsync (runM env $ runWorkerPool config {pollInterval = 0.05, observabilityHooks = hooks}) $ \_ -> do
        waitUntil 10_000 $ (== 2) . length <$> readIORef successRef
        dlq <- runM env (HL.listDLQJobs 100 0) :: IO [DLQ.DLQJob payload]
        filter (== mkSimple "fp-bad") (map (payload . DLQ.jobSnapshot) dlq) `shouldBe` [mkSimple "fp-bad"]
        successes <- readIORef successRef
        successes `shouldMatchList` [mkSimple "fp-good1", mkSimple "fp-good2"]

    it "failRetry callback retries a job and DLQs it at its maxAttempts" $ \env -> do
      callsRef <- newIORef (0 :: Int)
      let batchHandler jobs cbs = do
            liftIO $ atomicModifyIORef' callsRef (\n -> (n + 1, ()))
            traverse_ (\j -> failRetry cbs j "transient") (toList jobs)
      let jobs = [(defaultJob (mkSimple "fr-job")) {groupKey = Just "fr", maxAttempts = Just 2}]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler

      withAsync (runM env $ runWorkerPool config {pollInterval = 0.05, jitter = NoJitter}) $ \_ -> do
        waitUntil 15_000 $ do
          dlq <- runM env (HL.listDLQJobs 100 0) :: IO [DLQ.DLQJob payload]
          pure (any ((== mkSimple "fr-job") . payload . DLQ.jobSnapshot) dlq)
        dlq <- runM env (HL.listDLQJobs 100 0) :: IO [DLQ.DLQJob payload]
        let mine = filter ((== mkSimple "fr-job") . payload . DLQ.jobSnapshot) dlq
        attempts (DLQ.jobSnapshot (head mine)) `shouldBe` 2
        calls <- readIORef callsRef
        calls `shouldBe` 2

    it "nack callback leaves a job to be reprocessed with no failure" $ \env -> do
      callsRef <- newIORef (0 :: Int)
      let batchHandler jobs cbs = do
            n <- liftIO $ atomicModifyIORef' callsRef (\c -> (c + 1, c + 1))
            if n == 1 then traverse_ (nack cbs) (toList jobs) else traverse_ (ack cbs) (toList jobs)
      let jobs = [(defaultJob (mkSimple "nk-job")) {groupKey = Just "nk"}]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler

      withAsync
        (runM env $ runWorkerPool config {pollInterval = 0.1, visibilityTimeout = 2, jobHeartbeatInterval = 1})
        $ \_ -> do
          waitUntil 15_000 $ (>= 2) <$> readIORef callsRef
          dlq <- runM env (HL.listDLQJobs 100 0) :: IO [DLQ.DLQJob payload]
          filter (== mkSimple "nk-job") (map (payload . DLQ.jobSnapshot) dlq) `shouldBe` []

    it "nack hands back the attempt the claim consumed" $ \env -> do
      callsRef <- newIORef (0 :: Int)
      seenAttemptsRef <- newIORef (Nothing :: Maybe Int)
      let batchHandler jobs cbs = do
            n <- liftIO $ atomicModifyIORef' callsRef (\c -> (c + 1, c + 1))
            let (j :| _) = jobs
            if n == 1
              then traverse_ (nack cbs) (toList jobs)
              else do
                liftIO $ writeIORef seenAttemptsRef (Just (fromIntegral (attempts j)))
                traverse_ (ack cbs) (toList jobs)
      let jobs = [(defaultJob (mkSimple "nk-attempts")) {groupKey = Just "nka"}]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler

      withAsync
        (runM env $ runWorkerPool config {pollInterval = 0.1, visibilityTimeout = 2, jobHeartbeatInterval = 1})
        $ \_ -> do
          waitUntil 15_000 $ (>= 2) <$> readIORef callsRef
          -- The claim on call 1 set attempts to 1. The nack gave that attempt
          -- back, so the reclaim on call 2 sees attempts == 1 again, not 2.
          seen <- readIORef seenAttemptsRef
          seen `shouldBe` Just 1

    it "nack does not fire onJobSuccess" $ \env -> do
      successRef <- newIORef ([] :: [Int64])
      callsRef <- newIORef (0 :: Int)
      let hooks =
            defaultObservabilityHooks
              { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \js -> (primaryKey job : js, ())
              }
      let batchHandler jobs cbs = do
            n <- liftIO $ atomicModifyIORef' callsRef (\c -> (c + 1, c + 1))
            if n == 1 then traverse_ (nack cbs) (toList jobs) else traverse_ (ack cbs) (toList jobs)
      let jobs = [(defaultJob (mkSimple "nk-success")) {groupKey = Just "nks"}]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler

      withAsync
        ( runM env $
            runWorkerPool config {pollInterval = 0.1, visibilityTimeout = 2, jobHeartbeatInterval = 1, observabilityHooks = hooks}
        )
        $ \_ -> do
          waitUntil 15_000 $ (>= 2) <$> readIORef callsRef
          threadDelay 300_000
          successes <- readIORef successRef
          -- The nack (call 1) fires no success hook. Only the ack (call 2) does.
          length successes `shouldBe` 1

    it "a thrown JobStolenException skips retry and leaves the job to reprocess" $ \env -> do
      callsRef <- newIORef (0 :: Int)
      let batchHandler jobs cbs = do
            n <- liftIO $ atomicModifyIORef' callsRef (\c -> (c + 1, c + 1))
            if n == 1
              then throwJobStolen "stolen mid-batch"
              else traverse_ (ack cbs) (toList jobs)
      let jobs = [(defaultJob (mkSimple "stolen-job")) {groupKey = Just "st"}]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler

      withAsync
        (runM env $ runWorkerPool config {pollInterval = 0.1, visibilityTimeout = 2, jobHeartbeatInterval = 1})
        $ \_ -> do
          -- The steal skips retry, not a failure, so the job reprocesses and the
          -- second pass acks it. It never lands in the DLQ.
          waitUntil 15_000 $ (>= 2) <$> readIORef callsRef
          waitUntil 15_000 $ (== 0) <$> runM env (HL.countJobs @payload)
          dlq <- runM env (HL.listDLQJobs 100 0) :: IO [DLQ.DLQJob payload]
          filter (== mkSimple "stolen-job") (map (payload . DLQ.jobSnapshot) dlq) `shouldBe` []

    it "an outer rollback after ack reprocesses the job" $ \env -> do
      callsRef <- newIORef (0 :: Int)
      let batchHandler jobs cbs = do
            n <- liftIO $ atomicModifyIORef' callsRef (\c -> (c + 1, c + 1))
            if n == 1
              then withDbTransaction $ do
                traverse_ (ack cbs) (toList jobs)
                throwRetryable "rolling back the ack"
              else traverse_ (ack cbs) (toList jobs)
      let jobs = [(defaultJob (mkSimple "sp-job")) {groupKey = Just "sp", maxAttempts = Just 5}]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler

      withAsync
        (runM env $ runWorkerPool config {pollInterval = 0.1, visibilityTimeout = 2, jobHeartbeatInterval = 1, jitter = NoJitter})
        $ \_ -> do
          waitUntil 15_000 $ (>= 2) <$> readIORef callsRef
          waitUntil 15_000 $ (== 0) <$> runM env (HL.countJobs @payload)

    it "cancelTree callback deletes the entire tree" $ \env -> do
      Right (root :| _) <-
        runM env $
          HL.insertJobTree $
            defaultJob (mkSimple "ct-root")
              <~~ (defaultJob (mkSimple "ct-c1") :| [defaultJob (mkSimple "ct-c2")])
      let rootId = primaryKey root
      let batchHandler jobs cbs = traverse_ (\j -> cancelTree cbs j "abort") (toList jobs)
      config <- mkBatchedConfig 1 10 batchHandler

      withAsync (runM env $ runWorkerPool config {pollInterval = 0.05}) $ \_ ->
        waitUntil 10_000 $ do
          mJob <- runM env $ HL.getJobById @payload rootId
          pure (isNothing mJob)

    it "cancelBranch callback deletes the job's branch" $ \env -> do
      Right (root :| _) <-
        runM env $
          HL.insertJobTree $
            defaultJob (mkSimple "cb-root")
              <~~ (defaultJob (mkSimple "cb-c1") :| [defaultJob (mkSimple "cb-c2")])
      let rootId = primaryKey root
      let batchHandler jobs cbs = traverse_ (\j -> cancelBranch cbs j "branch failed") (toList jobs)
      config <- mkBatchedConfig 1 10 batchHandler

      withAsync (runM env $ runWorkerPool config {pollInterval = 0.05}) $ \_ ->
        waitUntil 10_000 $ do
          mJob <- runM env $ HL.getJobById @payload rootId
          pure (isNothing mJob)

    it "uncompleted jobs remain in the queue after the handler returns" $ \env -> do
      processedRef <- newIORef False
      let batchHandler jobs cbs = do
            ack cbs (head (toList jobs))
            liftIO $ atomicModifyIORef' processedRef $ \_ -> (True, ())
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1"}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 1 10 batchHandler
      let batchedConfig = config {pollInterval = 0.050}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ ->
        waitUntil 10_000 $ readIORef processedRef

      readIORef processedRef >>= (`shouldBe` True)
      runM env (HL.countJobs @payload) `shouldReturn` 1

    it "ungrouped jobs are batched together (not as singletons)" $ \env -> do
      batchSizesRef <- newIORef []
      let batchHandler jobs cbs = do
            let batchSize = length jobs
            liftIO $ atomicModifyIORef' batchSizesRef $ \sizes -> (batchSize : sizes, ())
            traverse_ (ack cbs) jobs
      let jobs =
            [ defaultJob (mkSimple "Ungrouped-1")
            , defaultJob (mkSimple "Ungrouped-2")
            , defaultJob (mkSimple "Ungrouped-3")
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 3 10 batchHandler
      let batchedConfig = config {pollInterval = 0.050}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        waitUntil 10_000 $ (== 1) . length <$> readIORef batchSizesRef
        batchSizes <- readIORef batchSizesRef
        length batchSizes `shouldBe` 1
        batchSizes `shouldMatchList` [3]

    it "mixed grouped and ungrouped jobs are batched correctly" $ \env -> do
      batchPayloadsRef <- newIORef []
      let batchHandler jobs cbs = do
            let payloads = map payload (toList jobs)
            liftIO $ atomicModifyIORef' batchPayloadsRef $ \batches -> (payloads : batches, ())
            traverse_ (ack cbs) jobs
      let jobs =
            [ (defaultJob (mkSimple "G1-1")) {groupKey = Just "g1"}
            , (defaultJob (mkSimple "G1-2")) {groupKey = Just "g1"}
            , defaultJob (mkSimple "Ungrouped-1")
            , defaultJob (mkSimple "Ungrouped-2")
            , (defaultJob (mkSimple "G2-1")) {groupKey = Just "g2"}
            ]
      runM env $ traverse_ HL.insertJob jobs
      config <- mkBatchedConfig 4 10 batchHandler
      let batchedConfig = config {pollInterval = 0.050}
      threadDelay 100_000

      withAsync (runM env $ runWorkerPool batchedConfig) $ \_ -> do
        waitUntil 10_000 $ (== 3) . length <$> readIORef batchPayloadsRef
        batches <- readIORef batchPayloadsRef
        length batches `shouldBe` 3
        let batchSizes = map length batches
        batchSizes `shouldMatchList` [2, 2, 1]
        let g1Batch = filter (\b -> mkSimple "G1-1" `elem` b) batches
        length g1Batch `shouldBe` 1
        head g1Batch `shouldMatchList` [mkSimple "G1-1", mkSimple "G1-2"]

  describe "Tree and Branch Cancel" $ do
    it "throwTreeCancel deletes the entire tree (not DLQ'd)" $ \env -> do
      runM env $
        void $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkSimple "tc-root"))
              ( JT.rollup
                  (defaultJob (mkSimple "tc-mid"))
                  (JT.leaf (defaultJob (mkSimple "tc-leaf1")) :| [JT.leaf (defaultJob (mkSimple "tc-leaf2"))])
                  :| []
              )
      config <- mkConfig $ \job ->
        when (payload job == mkSimple "tc-leaf1") $ liftIO (throwTreeCancel "abort everything")

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ ->
        waitUntil 10_000 $ null <$> runM env (HL.listJobs @payload 100 0)

      let treePayloads =
            [ mkSimple "tc-root"
            , mkSimple "tc-mid"
            , mkSimple "tc-leaf1"
            , mkSimple "tc-leaf2"
            ]
      jobs <- runM env $ HL.listJobs @payload 100 0
      traverse_ (\p -> map payload jobs `shouldNotContain` [p]) treePayloads
      dlqJobs <- runM env $ HL.listDLQJobs @payload 100 0
      traverse_ (\p -> map (payload . DLQ.jobSnapshot) dlqJobs `shouldNotContain` [p]) treePayloads

    it "throwBranchCancel deletes branch but resumes grandparent" $ \env -> do
      rootProcessedRef <- newIORef False
      runM env $
        void $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkSimple "bc-root"))
              ( JT.rollup
                  (defaultJob (mkSimple "bc-mid"))
                  (JT.leaf (defaultJob (mkSimple "bc-leaf1")) :| [JT.leaf (defaultJob (mkSimple "bc-leaf2"))])
                  :| []
              )
      config <- mkConfig $ \job ->
        if payload job == mkSimple "bc-leaf1"
          then liftIO (throwBranchCancel "abort this branch")
          else when (payload job == mkSimple "bc-root") $
            liftIO $
              atomicModifyIORef' rootProcessedRef $
                \_ -> (True, ())

      withAsync (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1}) $ \_ ->
        waitUntil 10_000 $ readIORef rootProcessedRef

      readIORef rootProcessedRef `shouldReturn` True
      dlqJobs <- runM env $ HL.listDLQJobs @payload 100 0
      let allPayloads = [mkSimple "bc-root", mkSimple "bc-mid", mkSimple "bc-leaf1", mkSimple "bc-leaf2"]
      traverse_ (\p -> map (payload . DLQ.jobSnapshot) dlqJobs `shouldNotContain` [p]) allPayloads
  where
    mkConfig :: (JobRead payload -> m ()) -> IO (WorkerConfig m payload)
    mkConfig h = transactionalWorkerConfig 10 (mkHandler (\job -> h job >> pure (Nothing :: Maybe [Text])))
    mkBatchedConfig
      :: Int
      -> Int
      -> (NonEmpty (JobRead payload) -> BatchCallbacks m payload (ResultOf m payload) -> m ())
      -> IO (WorkerConfig m payload)
    mkBatchedConfig = defaultBatchedWorkerConfig

-- | Env-owned LISTEN hub test suite, instantiated for each backend. A high
-- @pollInterval@ makes the NOTIFY the only thing that could wake the dispatcher
-- in time, so completion proves the listener fired.
listenerSpec
  :: forall payload m env
   . ( MonadUnliftIO m
     , QueueOperation m payload
     , RegistryAdmissionPolicies (RegistryOf m)
     , RegistryTables (RegistryOf m)
     , ResultOf m payload ~ ()
     )
  => Text
  -- ^ Schema\/table name, also the LISTEN channel prefix
  -> ByteString
  -- ^ Connection string, for terminating the listener backend
  -> (Text -> payload)
  -- ^ Construct a task payload
  -> IO env
  -- ^ Create an env whose listener is enabled
  -> IO env
  -- ^ Create an env with the listener disabled (poll-only)
  -> (env -> IO ())
  -- ^ Release an env built by the actions above
  -> ((JobRead payload -> m ()) -> JobHandler m payload ())
  -- ^ Adapt a job action into the backend's handler shape
  -> (forall a. env -> m a -> IO a)
  -- ^ Runner function
  -> Spec
listenerSpec schema connStr mkPayload mkEnv mkEnvPollOnly destroyEnv mkHandler runM =
  describe "listener" $ do
    around (bracket mkEnv destroyEnv) $ do
      it "wakes the dispatcher on NOTIFY under a high poll interval" $ \env -> do
        ref <- newIORef (0 :: Int)
        config :: WorkerConfig m payload <- runM env $ transactionalWorkerConfig 1 (mkHandler (counting ref))
        let workerConfig = config {workerCount = 1, pollInterval = 300, jitter = NoJitter}
        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          threadDelay 1_000_000
          runM env $ void $ HL.insertJob (defaultJob (mkPayload "notify")) {groupKey = Just "g1"}
          waitUntil 5_000 $ (== 1) <$> readIORef ref
          readIORef ref >>= (`shouldBe` 1)

      it "re-subscribes after a reconnect under a high poll interval" $ \env -> do
        ref <- newIORef (0 :: Int)
        config :: WorkerConfig m payload <- runM env $ transactionalWorkerConfig 1 (mkHandler (counting ref))
        let workerConfig = config {workerCount = 1, pollInterval = 300, jitter = NoJitter}
        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          threadDelay 1_000_000
          killListener connStr schema
          threadDelay 3_000_000
          runM env $ void $ HL.insertJob (defaultJob (mkPayload "after-reconnect")) {groupKey = Just "g1"}
          waitUntil 8_000 $ (== 1) <$> readIORef ref
          readIORef ref >>= (`shouldBe` 1)

      it "shares one listener connection across registrants on the same env" $ \env ->
        withSharedListener env $ \listener -> do
          let chanA = TE.encodeUtf8 (schema <> "_dedup_a")
              chanB = TE.encodeUtf8 (schema <> "_dedup_b")
          Listen.withChannels listener quietHubLog [(chanA, ignoreNotif)] $ \readyA ->
            Listen.withChannels listener quietHubLog [(chanB, ignoreNotif)] $ \readyB -> do
              waitUntil 5_000 (atomically readyA)
              waitUntil 5_000 (atomically readyB)
              listenerConnectionCount connStr schema >>= (`shouldBe` 1)

      it "keeps a shared channel live after an overlapping registrant leaves" $ \env ->
        withSharedListener env $ \listener -> do
          ref <- newIORef (0 :: Int)
          let sharedName = schema <> "_shrink_shared"
              extraName = schema <> "_shrink_extra"
              sharedChan = TE.encodeUtf8 sharedName
              extraChan = TE.encodeUtf8 extraName
          Listen.withChannels listener quietHubLog [(sharedChan, bumpNotif ref)] $ \readyOuter -> do
            waitUntil 5_000 (atomically readyOuter)
            -- The inner registrant adds an extra channel, then leaves, shrinking the
            -- desired set back to the shared channel and issuing an UNLISTEN.
            Listen.withChannels listener quietHubLog [(sharedChan, ignoreNotif), (extraChan, ignoreNotif)] $ \readyInner ->
              waitUntil 5_000 (atomically readyInner)
            notifyChannel connStr sharedName
            waitUntil 5_000 $ (== 1) <$> readIORef ref
            readIORef ref >>= (`shouldBe` 1)

      it "isolates a throwing channel handler from the others" $ \env ->
        withSharedListener env $ \listener -> do
          good <- newIORef (0 :: Int)
          warned <- newIORef (0 :: Int)
          let hubLog = Listen.HubLog (\_ -> bumpRef warned) (const (pure ()))
              goodName = schema <> "_iso_good"
              badName = schema <> "_iso_bad"
              handlers =
                [ (TE.encodeUtf8 badName, \_ -> ioError (userError "boom"))
                , (TE.encodeUtf8 goodName, bumpNotif good)
                ]
          Listen.withChannels listener hubLog handlers $ \ready -> do
            waitUntil 5_000 (atomically ready)
            notifyChannel connStr badName
            waitUntil 5_000 $ (== 1) <$> readIORef warned
            notifyChannel connStr goodName
            waitUntil 5_000 $ (== 1) <$> readIORef good
            readIORef good >>= (`shouldBe` 1)

    around (bracket mkEnvPollOnly destroyEnv) $
      it "processes jobs poll-only when the listener is disabled" $ \env -> do
        ref <- newIORef (0 :: Int)
        config :: WorkerConfig m payload <- runM env $ transactionalWorkerConfig 1 (mkHandler (counting ref))
        let workerConfig = config {workerCount = 1, pollInterval = 0.2, jitter = NoJitter}
        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          runM env $ void $ HL.insertJob (defaultJob (mkPayload "poll")) {groupKey = Just "g1"}
          waitUntil 10_000 $ (== 1) <$> readIORef ref
          readIORef ref >>= (`shouldBe` 1)
  where
    counting :: IORef Int -> JobRead payload -> m ()
    counting ref _job = liftIO (bumpRef ref)
    withSharedListener env k = do
      mListener <- runM env getListener
      case mListener of
        Nothing -> expectationFailure "expected a shared listener, got poll-only"
        Just listener -> k listener

-- | A hub logger that swallows warn and error output.
quietHubLog :: Listen.HubLog
quietHubLog = Listen.HubLog (const (pure ())) (const (pure ()))

-- | Channel handler that ignores the notification.
ignoreNotif :: Listen.Notification -> IO ()
ignoreNotif = const (pure ())

-- | Channel handler that bumps a counter.
bumpNotif :: IORef Int -> Listen.Notification -> IO ()
bumpNotif ref = const (bumpRef ref)

bumpRef :: IORef Int -> IO ()
bumpRef ref = atomicModifyIORef' ref (\n -> (n + 1, ()))

-- | Count distinct backends holding a LISTEN on any of this schema's channels.
listenerConnectionCount :: ByteString -> Text -> IO Int
listenerConnectionCount connStr schema =
  bracket (connectPostgreSQL connStr) close $ \conn -> do
    rows <-
      PG.query @_ @(Only Int)
        conn
        "SELECT count(DISTINCT pid)::int \
        \FROM pg_stat_activity \
        \WHERE datname = current_database() \
        \  AND query LIKE 'LISTEN%' \
        \  AND query LIKE ?"
        (Only ("%" <> schema <> "%" :: Text))
    pure (maybe 0 fromOnly (listToMaybe rows))

-- | Terminate the env's listener backend, forcing the hub to reconnect.
killListener :: ByteString -> Text -> IO ()
killListener connStr schema =
  bracket (connectPostgreSQL connStr) close $ \conn ->
    void $
      PG.query @_ @(Only Bool)
        conn
        "SELECT pg_terminate_backend(pid) \
        \FROM pg_stat_activity \
        \WHERE pid <> pg_backend_pid() \
        \  AND datname = current_database() \
        \  AND query LIKE ?"
        (Only ("LISTEN%" <> schema <> "%" :: Text))

-- | Env-owned LISTEN hub test suite for two queues sharing one env, one worker
-- pool per queue. Each queue's job-arrival channel is derived from its table
-- name, so these check that a shared hub wakes each pool for its own queue and
-- never spuriously for the other's jobs. The registry must map @payloadA@ to the
-- @tableA@ queue and @payloadB@ to @tableB@.
multiQueueListenerSpec
  :: forall payloadA payloadB m env
   . ( MonadUnliftIO m
     , QueueOperation m payloadA
     , QueueOperation m payloadB
     , RegistryAdmissionPolicies (RegistryOf m)
     , RegistryTables (RegistryOf m)
     , ResultOf m payloadA ~ ()
     , ResultOf m payloadB ~ ()
     )
  => Text
  -- ^ Queue A table name, also its LISTEN channel prefix
  -> Text
  -- ^ Queue B table name, also its LISTEN channel prefix
  -> ByteString
  -- ^ Connection string, for issuing a raw NOTIFY
  -> (Text -> payloadA)
  -- ^ Construct a queue A payload
  -> (Text -> payloadB)
  -- ^ Construct a queue B payload
  -> IO env
  -- ^ Create an env whose listener is enabled, with both queue tables set up
  -> (env -> IO ())
  -- ^ Release an env built by the action above
  -> (forall p. (JobRead p -> m ()) -> JobHandler m p ())
  -- ^ Adapt a job action into the backend's handler shape
  -> (forall a. env -> m a -> IO a)
  -- ^ Runner function
  -> Spec
multiQueueListenerSpec tableA tableB connStr mkPayloadA mkPayloadB mkEnv destroyEnv mkHandler runM =
  around (bracket mkEnv destroyEnv) $
    describe "multi-queue listener" $ do
      it "wakes each pool only for its own queue's jobs under a high poll interval" $ \env -> do
        refA <- newIORef (0 :: Int)
        refB <- newIORef (0 :: Int)
        cfgA :: WorkerConfig m payloadA <-
          runM env $ transactionalWorkerConfig 1 (mkHandler (bumping refA :: JobRead payloadA -> m ()))
        cfgB :: WorkerConfig m payloadB <-
          runM env $ transactionalWorkerConfig 1 (mkHandler (bumping refB :: JobRead payloadB -> m ()))
        let poolA = cfgA {workerCount = 1, pollInterval = 300, jitter = NoJitter}
            poolB = cfgB {workerCount = 1, pollInterval = 300, jitter = NoJitter}
        withAsync (runM env $ runWorkerPool poolA) $ \_ ->
          withAsync (runM env $ runWorkerPool poolB) $ \_ -> do
            threadDelay 1_000_000
            runM env $ void $ HL.insertJob (defaultJob (mkPayloadA "a1"))
            waitUntil 5_000 $ (== 1) <$> readIORef refA
            threadDelay 500_000
            readIORef refB >>= (`shouldBe` 0)

            runM env $ void $ HL.insertJob (defaultJob (mkPayloadB "b1"))
            waitUntil 5_000 $ (== 1) <$> readIORef refB
            threadDelay 500_000
            readIORef refA >>= (`shouldBe` 1)

      it "routes a NOTIFY only to the owning queue's channel handler" $ \env -> do
        mListener <- runM env getListener
        case mListener of
          Nothing -> expectationFailure "expected a shared listener, got poll-only"
          Just listener -> do
            refA <- newIORef (0 :: Int)
            refB <- newIORef (0 :: Int)
            let chanA = TE.encodeUtf8 (Schema.notificationChannelForTable tableA)
                chanB = TE.encodeUtf8 (Schema.notificationChannelForTable tableB)
                handlers =
                  [ (chanA, bumpNotif refA)
                  , (chanB, bumpNotif refB)
                  ]
            Listen.withChannels listener quietHubLog handlers $ \ready -> do
              waitUntil 5_000 (atomically ready)
              notifyChannel connStr (Schema.notificationChannelForTable tableA)
              waitUntil 5_000 $ (== 1) <$> readIORef refA
              threadDelay 500_000
              readIORef refA >>= (`shouldBe` 1)
              readIORef refB >>= (`shouldBe` 0)
  where
    bumping :: IORef Int -> JobRead p -> m ()
    bumping ref _job = liftIO (bumpRef ref)

-- | Issue a raw @NOTIFY@ on a channel over a throwaway connection.
notifyChannel :: ByteString -> Text -> IO ()
notifyChannel connStr chan =
  bracket (connectPostgreSQL connStr) close $ \conn ->
    void $ PG.execute conn "NOTIFY ?" (Only (Identifier chan))

-- | Poll every 100 ms until the predicate returns 'True'.
-- Fails with 'expectationFailure' after @timeoutMs@ milliseconds.
waitUntil :: (HasCallStack) => Int -> IO Bool -> IO ()
waitUntil timeoutMs check = go (max 1 (timeoutMs `div` 100))
  where
    go :: (HasCallStack) => Int -> IO ()
    go 0 = expectationFailure "waitUntil: timed out waiting for condition"
    go n = do
      ok <- check
      unless ok $ do
        threadDelay 100_000
        go (n - 1)
