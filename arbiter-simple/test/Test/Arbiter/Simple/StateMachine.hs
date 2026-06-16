{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Simple backend wrapper for the shared state-machine property suite.
module Test.Arbiter.Simple.StateMachine (spec) where

import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Setup (setupOnce)
import Arbiter.Test.StateMachine (stateMachineSpec)
import Data.ByteString (ByteString)
import Data.Maybe (fromJust)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Arbiter.Simple.MonadArbiter (SimpleConnectionPool (..))
import Arbiter.Simple.SimpleDb (SimpleDb, SimpleEnv (..), createSimpleEnvWithPool, runSimpleDb)
import Test.Arbiter.Simple.TestHelpers (cleanupSimpleTest, createSimplePool)

testSchema :: Text
testSchema = "arbiter_simple_sm_test"

testTable :: Text
testTable = "arbiter_simple_sm_test"

type SMRegistry = '[ '("arbiter_simple_sm_test", TestPayload)]

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  pool <- runIO (createSimplePool 40 connStr)
  let env = createSimpleEnvWithPool (Proxy @SMRegistry) pool testSchema
      run :: forall a. SimpleDb SMRegistry IO a -> IO a
      run = runSimpleDb env
      withConn :: forall a. (PG.Connection -> IO a) -> IO a
      withConn = withResource (fromJust (connectionPool (simplePool env)))
      reset = cleanupSimpleTest env testSchema testTable
  stateMachineSpec @(SimpleDb SMRegistry IO) @SMRegistry @TestPayload
    TestMessage
    run
    testSchema
    testTable
    withConn
    reset
