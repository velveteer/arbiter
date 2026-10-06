{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Hasql.Operations (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
import Arbiter.Core.MonadArbiter (executeQuery, runHandlerWithConnection, withDbTransaction)
import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query)
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Operations (operationsSpec)
import Arbiter.Test.Setup (cleanupOnce, setupOnce)
import Control.Exception (SomeException, bracket, catch, throwIO)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Hasql.Connection qualified as Hasql
import Test.Hspec

import Arbiter.Hasql.Compat (acquireConnect)
import Arbiter.Hasql.Compat qualified as Compat
import Arbiter.Hasql.HasqlDb
  ( HasqlConfig (..)
  , HasqlDb
  , createHasqlEnvWithPool
  , inTransaction
  , inTransactionWith
  , runHasqlDb
  )
import Test.Arbiter.Hasql.TestHelpers (createHasqlPool, testConnect)

testSchema :: Text
testSchema = "arbiter_hasql_ops_test"

type HasqlOpsTestRegistry = '[QueueWithResult "arbiter_hasql_ops_test" TestPayload [Text]]

testTable :: Text
testTable = "arbiter_hasql_ops_test"

type HasqlOpsDb = HasqlDb HasqlOpsTestRegistry IO

preparedCountSQL :: Query Int64
preparedCountSQL = [sql|SELECT @{n :: CInt8} FROM (SELECT count(*) AS n FROM pg_prepared_statements) counted|]

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  sharedPool <- runIO (createHasqlPool 5 connStr)
  mkEnv <- runIO (createHasqlEnvWithPool (Proxy @HasqlOpsTestRegistry) sharedPool testSchema)
  around (\action -> cleanupOnce connStr testSchema testTable >> action mkEnv) $ do
    operationsSpec @TestPayload TestMessage pure runHasqlDb

    describe "Handler connection" $ do
      it "runs a handler on a borrowed pool connection when none is pinned" $ \env -> do
        result <- runHasqlDb env $ do
          _ <- HL.insertJob (defaultJob (TestMessage "Unpinned"))
          [job] <- HL.claimNextVisibleJobs 1 60 :: HasqlOpsDb [JobRead TestPayload]
          runHandlerWithConnection (\conn _ -> ["ran"] <$ liftIO (Compat.runSQL conn "SELECT 1")) job
        result `shouldBe` ["ran"]

    describe "Transaction Participation (inTransaction)" $ do
      it "commits job insertion within user transaction" $ \env -> do
        let job = defaultJob (TestMessage "InTx")

        withResource sharedPool $ \conn -> do
          Compat.runSQL conn "BEGIN"
          inTransaction @HasqlOpsTestRegistry conn testSchema $ do
            _ <- HL.insertJob job
            pure ()
          Compat.runSQL conn "COMMIT"
          pure ()

        claimed <- runHasqlDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 1
        payload (head claimed) `shouldBe` TestMessage "InTx"

      it "prepares no statement when the config disables prepared statements" $ \_ -> do
        bracket (acquireConnect (testConnect connStr) >>= either fail pure) Hasql.release $ \conn -> do
          Compat.runSQL conn "BEGIN"
          prepared <- inTransactionWith @HasqlOpsTestRegistry (HasqlConfig False) conn testSchema $ do
            _ <- HL.claimNextVisibleJobs 1 60 :: HasqlOpsDb [JobRead TestPayload]
            executeQuery preparedCountSQL
          Compat.runSQL conn "ROLLBACK"
          prepared `shouldBe` [0]

      it "rolls back job insertion when user transaction fails" $ \env -> do
        let job = defaultJob (TestMessage "RollbackTest")

        result <-
          ( withResource sharedPool $ \conn -> do
              Compat.runSQL conn "BEGIN"
              inTransaction @HasqlOpsTestRegistry conn testSchema $ do
                _ <- HL.insertJob job
                pure ()
              Compat.runSQL conn "ROLLBACK"
              pure ("rolled back" :: String)
          )
            `catch` \(exception :: SomeException) -> pure (show exception)

        result `shouldContain` "rolled back"

        claimed <- runHasqlDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 0

      it "shares transaction with user's database operations" $ \env -> do
        let job1 = setGroupKey (Just "g1") $ defaultJob (TestMessage "SharedTx")
        let job2 = setGroupKey (Just "g2") $ defaultJob (TestMessage "SharedTx2")

        runHasqlDb env $ do
          withDbTransaction $ do
            _ <- HL.insertJob job1
            _ <- HL.insertJob job2
            pure ()

        claimed <- runHasqlDb env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 2
        map payload claimed `shouldMatchList` [TestMessage "SharedTx", TestMessage "SharedTx2"]

      it "rolls back both user operations and job when transaction fails" $ \env -> do
        let job1 = setGroupKey (Just "g1") $ defaultJob (TestMessage "FirstJob")
        let job2 = setGroupKey (Just "g2") $ defaultJob (TestMessage "SecondJob")

        result <-
          ( runHasqlDb env $ do
              withDbTransaction $ do
                _ <- HL.insertJob job1
                _ <- HL.insertJob job2
                liftIO $ throwIO (userError "Force rollback")
          )
            `catch` \(exception :: SomeException) -> pure (show exception)

        result `shouldContain` "Force rollback"

        claimed <- runHasqlDb env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 0
