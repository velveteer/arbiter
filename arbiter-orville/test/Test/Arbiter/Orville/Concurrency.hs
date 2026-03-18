{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Orville.Concurrency (spec) where

import Arbiter.Test.Concurrency
  ( concurrencySpec
  , groupsConsistencyStressSpec
  , inFlightConcurrencySpec
  , raceConditionSpec
  )
import Arbiter.Test.Fixtures (TestPayload (..))
import Control.Exception (bracket)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Test.Arbiter.Orville.TestHelpers
  ( OrvilleTestEnv (testConnStr)
  , cleanupOrvilleTest
  , runOrvilleTest
  , setupOrvilleTest
  )

testSchema :: Text
testSchema = "arbiter_orville_concurrency_test"

type OrvilleConcurrencyTestRegistry = '[ '("arbiter_orville_concurrency_test", TestPayload)]

testTable :: Text
testTable = "arbiter_orville_concurrency_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOrvilleTest connStr testSchema testTable 10) $ beforeWith (\env -> cleanupOrvilleTest env >> pure env) $ do
  let withConn :: OrvilleTestEnv OrvilleConcurrencyTestRegistry -> (PG.Connection -> IO ()) -> IO ()
      withConn env f = bracket (PG.connectPostgreSQL (testConnStr env)) PG.close f
  concurrencySpec @TestPayload @OrvilleConcurrencyTestRegistry
    TestMessage
    (runOrvilleTest @OrvilleConcurrencyTestRegistry)
  raceConditionSpec @TestPayload @OrvilleConcurrencyTestRegistry
    TestMessage
    (runOrvilleTest @OrvilleConcurrencyTestRegistry)
  groupsConsistencyStressSpec @TestPayload @OrvilleConcurrencyTestRegistry
    testSchema
    testTable
    TestMessage
    (runOrvilleTest @OrvilleConcurrencyTestRegistry)
    withConn
  inFlightConcurrencySpec @TestPayload @OrvilleConcurrencyTestRegistry
    testSchema
    testTable
    TestMessage
    (runOrvilleTest @OrvilleConcurrencyTestRegistry)
    withConn
