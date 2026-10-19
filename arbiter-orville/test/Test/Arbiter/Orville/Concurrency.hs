{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Orville.Concurrency (spec) where

import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Test.Concurrency
  ( concurrencySpec
  , raceConditionSpec
  )
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Setup (concurrencyPoolSize)
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
spec connStr = beforeAll (setupOrvilleTest connStr testSchema testTable concurrencyPoolSize) $ beforeWith (\env -> cleanupOrvilleTest env >> pure env) $ do
  concurrencySpec @TestPayload
    TestMessage
    (runOrvilleTest @OrvilleConcurrencyTestRegistry)
  raceConditionSpec @TestPayload
    TestMessage
    (runOrvilleTest @OrvilleConcurrencyTestRegistry)
