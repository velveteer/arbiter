{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Orville backend wrapper for the shared state-machine property suite.
module Test.Arbiter.Orville.StateMachine (spec) where

import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Test.Setup (createSharedPool, setupOnce)
import Arbiter.Test.StateMachine (SMPayload, stateMachineSpec)
import Data.ByteString (ByteString)
import Data.Pool (withResource)
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Test.Arbiter.Orville.TestHelpers
  ( TestOrville
  , cleanupOrvilleTest
  , createOrvilleTestEnv
  , runOrvilleTest
  )

testSchema :: Text
testSchema = "arbiter_orville_sm_test"

testTable :: Text
testTable = "arbiter_orville_sm_test"

type SMRegistry = '[Queue "arbiter_orville_sm_test" SMPayload]

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  env <- runIO (createOrvilleTestEnv connStr testSchema testTable 40)
  pgPool <- runIO (createSharedPool connStr)
  let run :: forall a. TestOrville SMRegistry a -> IO a
      run = runOrvilleTest env
      withConn :: forall a. (PG.Connection -> IO a) -> IO a
      withConn = withResource pgPool
      reset = cleanupOrvilleTest env
  stateMachineSpec @(TestOrville SMRegistry)
    run
    testSchema
    testTable
    withConn
    reset
