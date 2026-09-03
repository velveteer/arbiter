{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Covers two paths 'Arbiter.Worker.TestKit.workerSpec' cannot reach. One is a
-- plain non-@Maybe@ result type. The other is a @Queue@ entry whose
-- 'Arbiter.Core.ResultOf' is @()@.
module Test.Arbiter.Worker.PlainResult (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Archive qualified as Archive
import Arbiter.Core.Job.Types
  ( JobRead
  , dayRetention
  , defaultJob
  , isRollup
  , payload
  , primaryKey
  , setArchiveFor
  )
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Simple (SimpleDb, createSimpleEnv, destroySimpleEnv, runSimpleDb)
import Arbiter.Test.Poll (waitUntil, withLinkedAsync)
import Arbiter.Test.Setup (addQueueTable, cleanupData, setupOnce)
import Arbiter.Worker (mergedChildResults, runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config
  ( BatchCallbacks (..)
  , WorkerConfig (..)
  , defaultBatchedWorkerConfig
  , transactionalWorkerConfig
  )
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON, toJSON)
import Data.ByteString (ByteString)
import Data.Foldable (toList, traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.List (find, partition)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import GHC.Generics (Generic)
import Test.Hspec (Spec, beforeAll, describe, it, shouldBe, shouldMatchList, shouldReturn)
import UnliftIO (bracket)

newtype NoResultPayload = NoResultTask Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype PlainResultPayload = PlainResultTask Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type PlainRegistry =
  '[ Queue "plain_noresult" NoResultPayload
   , QueueWithResult "plain_result" PlainResultPayload [Text]
   ]

testSchema :: Text
testSchema = "arbiter_plain_result_test"

noResultTable :: Text
noResultTable = "plain_noresult"

resultTable :: Text
resultTable = "plain_result"

cleanup :: ByteString -> IO ()
cleanup connStr = do
  conn <- connectPostgreSQL connStr
  cleanupData testSchema noResultTable conn
  cleanupData testSchema resultTable conn
  close conn

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr testSchema noResultTable True >> addQueueTable connStr testSchema resultTable True) $ do
    describe "unparameterized ack callbacks" $
      it "acks a batch through ack and ackAll, storing no result" $ do
        cleanup connStr
        withEnv $ \env -> do
          ackedRef <- newIORef (0 :: Int)
          let handler
                :: NonEmpty (JobRead NoResultPayload)
                -> BatchCallbacks (SimpleDb PlainRegistry IO) NoResultPayload ()
                -> SimpleDb PlainRegistry IO ()
              handler jobs cbs = case toList jobs of
                [] -> pure ()
                (firstJob : rest) -> do
                  ack cbs firstJob
                  ackAll cbs rest
                  liftIO $ atomicModifyIORef' ackedRef $ \count -> (count + 1 + length rest, ())
          cfg <- defaultBatchedWorkerConfig 1 10 handler
          runSimpleDb env $
            traverse_
              (\name -> void $ HL.insertJob (setArchiveFor (Just dayRetention) $ defaultJob (NoResultTask name)))
              ["a", "b", "c"]
          withLinkedAsync (runSimpleDb env $ runWorkerPool cfg {pollInterval = 0.1, jitter = NoJitter}) $ \_ ->
            waitUntil 10_000 $ (== 3) <$> readIORef ackedRef
          readIORef ackedRef >>= (`shouldBe` 3)
          runSimpleDb env (HL.countJobs @NoResultPayload) `shouldReturn` 0
          arch <- runSimpleDb env $ HL.listArchiveJobs @NoResultPayload 100 0
          map (payload . Archive.jobSnapshot) arch
            `shouldMatchList` [NoResultTask "a", NoResultTask "b", NoResultTask "c"]
          map Archive.archivedResult arch `shouldBe` [Nothing, Nothing, Nothing]

    describe "insertResult on a queue that stores nothing" $
      it "writes no row and reports 0" $ do
        cleanup connStr
        withEnv $ \env -> do
          Right (parent :| [child]) <-
            runSimpleDb env
              $ HL.insertJobTree
              $ JT.rollup
                (defaultJob (NoResultTask "declining-parent"))
                (JT.leaf (defaultJob (NoResultTask "declining-child")) :| [])
          rowsInserted <-
            runSimpleDb env $
              HL.insertResult @NoResultPayload (primaryKey parent) (primaryKey child) ()
          rowsInserted `shouldBe` 0
          results <- runSimpleDb env $ HL.getResultsByParent @NoResultPayload (primaryKey parent)
          results `shouldBe` Map.empty

    describe "plain result type" $ do
      it "stores a non-Maybe result on the archive row" $ do
        cleanup connStr
        withEnv $ \env -> do
          let handler :: JobHandler (SimpleDb PlainRegistry IO) PlainResultPayload [Text]
              handler _conn _job = pure ["alpha", "beta"]
          cfg <- transactionalWorkerConfig 1 handler
          void
            $ runSimpleDb env
            $ HL.insertJob (setArchiveFor (Just dayRetention) $ defaultJob (PlainResultTask "archived"))
          withLinkedAsync (runSimpleDb env $ runWorkerPool cfg {pollInterval = 0.1, jitter = NoJitter}) $ \_ ->
            waitUntil 10_000 $ do
              arch <- runSimpleDb env $ HL.listArchiveJobs @PlainResultPayload 100 0
              pure (any ((== PlainResultTask "archived") . payload . Archive.jobSnapshot) arch)
          arch <- runSimpleDb env $ HL.listArchiveJobs @PlainResultPayload 100 0
          let mine = find ((== PlainResultTask "archived") . payload . Archive.jobSnapshot) arch
          (Archive.archivedResult =<< mine) `shouldBe` Just (toJSON ["alpha", "beta" :: Text])

      it "passes plain ackWith and ackAllWith results to a rollup parent" $ do
        cleanup connStr
        withEnv $ \env -> do
          mergedRef <- newIORef (Nothing :: Maybe [Text])
          let taskName job = case payload job of PlainResultTask name -> name
              handler
                :: NonEmpty (JobRead PlainResultPayload)
                -> BatchCallbacks (SimpleDb PlainRegistry IO) PlainResultPayload [Text]
                -> SimpleDb PlainRegistry IO ()
              handler jobs cbs = do
                let (parents, children) = partition isRollup (toList jobs)
                case children of
                  [] -> pure ()
                  (firstChild : rest) -> do
                    ackWith cbs firstChild ["from-" <> taskName firstChild]
                    ackAllWith cbs (map (\child -> (child, ["from-" <> taskName child])) rest)
                traverse_
                  ( \parent -> do
                      (merged, _) <- mergedChildResults parent
                      liftIO $ writeIORef mergedRef (Just merged)
                      ack cbs parent
                  )
                  parents
          cfg <- defaultBatchedWorkerConfig 1 10 handler
          void
            $ runSimpleDb env
            $ HL.insertJobTree
            $ defaultJob (PlainResultTask "parent")
              <~~ (defaultJob (PlainResultTask "kid1") :| [defaultJob (PlainResultTask "kid2")])
          withLinkedAsync (runSimpleDb env $ runWorkerPool cfg {pollInterval = 0.1, jitter = NoJitter}) $ \_ ->
            waitUntil 10_000 $ isJust <$> readIORef mergedRef
          readIORef mergedRef >>= (`shouldBe` Just ["from-kid1", "from-kid2"])
  where
    withEnv = bracket (createSimpleEnv (Proxy @PlainRegistry) connStr testSchema) destroySimpleEnv
