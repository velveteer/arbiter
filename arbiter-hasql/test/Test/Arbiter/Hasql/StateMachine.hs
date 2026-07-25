{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

-- | Hasql backend wrapper for the shared state-machine property suite.
module Test.Arbiter.Hasql.StateMachine (spec) where

import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Test.Setup (createSharedPool, setupOnce)
import Arbiter.Test.StateMachine (SMPayload, stateMachineSpec)
import Data.ByteString (ByteString)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Arbiter.Hasql.HasqlDb (HasqlDb, createHasqlEnvWithPool, runHasqlDb, setPreparedStatements)
import Test.Arbiter.Hasql.TestHelpers (cleanupHasqlTest, createHasqlPool)

testSchema :: Text
testSchema = "arbiter_hasql_sm_test"

testTable :: Text
testTable = "arbiter_hasql_sm_test"

type SMRegistry = '[Queue "arbiter_hasql_sm_test" SMPayload]

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  pool <- runIO (createHasqlPool 40 connStr)
  pgPool <- runIO (createSharedPool connStr)
  -- Prepared claims on, so this suite covers the prepared statement path.
  env <- runIO (setPreparedStatements True <$> createHasqlEnvWithPool (Proxy @SMRegistry) pool testSchema)
  let run :: forall a. HasqlDb SMRegistry IO a -> IO a
      run = runHasqlDb env
      withConn :: forall a. (PG.Connection -> IO a) -> IO a
      withConn = withResource pgPool
      reset = cleanupHasqlTest connStr testSchema testTable
  stateMachineSpec @(HasqlDb SMRegistry IO) @SMRegistry
    run
    testSchema
    testTable
    withConn
    reset
