{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Orville.Concurrency (spec) where

import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Test.Concurrency
  ( concurrencySpec
  , raceConditionSpec
  )
import Arbiter.Test.Fixtures (TestPayload (..))
import Data.ByteString (ByteString)
import Data.Text (Text)
import Test.Hspec

import Test.Arbiter.Orville.TestHelpers
  ( cleanupOrvilleTest
  , runOrvilleTest
  , setupOrvilleTest
  )

testSchema :: Text
testSchema = "arbiter_orville_concurrency_test"

type OrvilleConcurrencyTestRegistry = '[Queue "arbiter_orville_concurrency_test" TestPayload]

testTable :: Text
testTable = "arbiter_orville_concurrency_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOrvilleTest connStr testSchema testTable 10) $ beforeWith (\env -> cleanupOrvilleTest env >> pure env) $ do
  concurrencySpec @TestPayload @OrvilleConcurrencyTestRegistry
    TestMessage
    (runOrvilleTest @OrvilleConcurrencyTestRegistry)
  raceConditionSpec @TestPayload @OrvilleConcurrencyTestRegistry
    TestMessage
    (runOrvilleTest @OrvilleConcurrencyTestRegistry)
