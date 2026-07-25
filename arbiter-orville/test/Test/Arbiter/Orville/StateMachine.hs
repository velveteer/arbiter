{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

-- | Orville backend wrapper for the shared state-machine property suite.
module Test.Arbiter.Orville.StateMachine (spec) where

import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Test.Setup (createSharedPool)
import Arbiter.Test.StateMachine (SMPayload, stateMachineSpec)
import Data.ByteString (ByteString)
import Data.Pool (withResource)
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

type SMRegistry = '[ 'Queue "arbiter_orville_sm_test" SMPayload]

spec :: ByteString -> Spec
spec connStr = do
  env <- runIO (setupOrvilleTest connStr testSchema testTable 40)
  pgPool <- runIO (createSharedPool connStr)
  let run :: forall a. TestOrville SMRegistry a -> IO a
      run = runOrvilleTest env
      withConn :: forall a. (PG.Connection -> IO a) -> IO a
      withConn = withResource pgPool
      reset = cleanupOrvilleTest env
  stateMachineSpec @(TestOrville SMRegistry) @SMRegistry
    run
    testSchema
    testTable
    withConn
    reset
