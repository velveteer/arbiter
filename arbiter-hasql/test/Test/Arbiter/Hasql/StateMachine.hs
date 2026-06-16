{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Hasql backend wrapper for the shared state-machine property suite.
module Test.Arbiter.Hasql.StateMachine (spec) where

import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Setup (setupOnce)
import Arbiter.Test.StateMachine (stateMachineSpec)
import Control.Exception (bracket)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Arbiter.Hasql.HasqlDb (HasqlDb, createHasqlEnvWithPool, runHasqlDb)
import Test.Arbiter.Hasql.TestHelpers (cleanupHasqlTest, createHasqlPool)

testSchema :: Text
testSchema = "arbiter_hasql_sm_test"

testTable :: Text
testTable = "arbiter_hasql_sm_test"

type SMRegistry = '[ '("arbiter_hasql_sm_test", TestPayload)]

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  pool <- runIO (createHasqlPool 40 connStr)
  let env = createHasqlEnvWithPool (Proxy @SMRegistry) pool testSchema
      run :: forall a. HasqlDb SMRegistry IO a -> IO a
      run = runHasqlDb env
      withConn :: forall a. (PG.Connection -> IO a) -> IO a
      withConn = bracket (PG.connectPostgreSQL connStr) PG.close
      reset = cleanupHasqlTest connStr testSchema testTable
  stateMachineSpec @(HasqlDb SMRegistry IO) @SMRegistry @TestPayload
    TestMessage
    run
    testSchema
    testTable
    withConn
    reset
