{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Simple backend wrapper for the shared state-machine property suite.
module Test.Arbiter.Simple.StateMachine (spec) where

import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Test.Setup (cleanupData, createPoolOf, setupOnce)
import Arbiter.Test.StateMachine (SMPayload, stateMachineSpec)
import Data.ByteString (ByteString)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Arbiter.Simple.SimpleDb (SimpleDb, createSimpleEnvWithPool, runSimpleDb)

testSchema :: Text
testSchema = "arbiter_simple_sm_test"

testTable :: Text
testTable = "arbiter_simple_sm_test"

type SMRegistry = '[Queue "arbiter_simple_sm_test" SMPayload]

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  pool <- runIO (createPoolOf 40 connStr)
  env <- runIO (createSimpleEnvWithPool (Proxy @SMRegistry) pool testSchema)
  let run :: forall a. SimpleDb SMRegistry IO a -> IO a
      run = runSimpleDb env
      withConn :: forall a. (PG.Connection -> IO a) -> IO a
      withConn = withResource pool
      reset = withConn (cleanupData testSchema testTable)
  stateMachineSpec @(SimpleDb SMRegistry IO)
    run
    testSchema
    testTable
    withConn
    reset
