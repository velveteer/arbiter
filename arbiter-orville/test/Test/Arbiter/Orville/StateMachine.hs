{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Orville backend wrapper for the shared state-machine property suite.
module Test.Arbiter.Orville.StateMachine (spec) where

import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.StateMachine (stateMachineSpec)
import Control.Exception (bracket)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Test.Arbiter.Orville.TestHelpers
  ( TestOrville
  , cleanupOrvilleTest
  , runOrvilleTest
  , setupOrvilleTest
  )

testSchema :: Text
testSchema = "arbiter_orville_sm_test"

testTable :: Text
testTable = "arbiter_orville_sm_test"

type SMRegistry = '[ '("arbiter_orville_sm_test", TestPayload)]

spec :: ByteString -> Spec
spec connStr = do
  env <- runIO (setupOrvilleTest connStr testSchema testTable 40)
  let run :: forall a. TestOrville SMRegistry a -> IO a
      run = runOrvilleTest env
      withConn :: forall a. (PG.Connection -> IO a) -> IO a
      withConn = bracket (PG.connectPostgreSQL connStr) PG.close
      reset = cleanupOrvilleTest env
  stateMachineSpec @(TestOrville SMRegistry) @SMRegistry @TestPayload
    TestMessage
    run
    testSchema
    testTable
    withConn
    reset
