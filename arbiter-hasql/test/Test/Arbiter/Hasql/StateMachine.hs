{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Hasql backend wrapper for the shared state-machine property suite.
module Test.Arbiter.Hasql.StateMachine (spec) where

import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Test.Setup (cleanupOnce, createSharedPool, setupOnce, stateMachinePoolSize)
import Arbiter.Test.StateMachine (SMPayload, stateMachineSpec)
import Data.ByteString (ByteString)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Arbiter.Hasql.HasqlDb (HasqlDb, createHasqlEnvWithPool, runHasqlDb)
import Test.Arbiter.Hasql.TestHelpers (createHasqlPool)

testSchema :: Text
testSchema = "arbiter_hasql_sm_test"

testTable :: Text
testTable = "arbiter_hasql_sm_test"

type SMRegistry = '[Queue "arbiter_hasql_sm_test" SMPayload]

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  pool <- runIO (createHasqlPool stateMachinePoolSize connStr)
  pgPool <- runIO (createSharedPool connStr)
  env <- runIO (createHasqlEnvWithPool (Proxy @SMRegistry) pool testSchema)
  let run :: forall a. HasqlDb SMRegistry IO a -> IO a
      run = runHasqlDb env
      withConn :: forall a. (PG.Connection -> IO a) -> IO a
      withConn = withResource pgPool
      reset = cleanupOnce connStr testSchema testTable
  stateMachineSpec @(HasqlDb SMRegistry IO)
    run
    testSchema
    testTable
    withConn
    reset
