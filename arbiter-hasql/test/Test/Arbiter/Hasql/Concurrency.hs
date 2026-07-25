{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Hasql.Concurrency (spec) where

import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Test.Concurrency
  ( concurrencySpec
  , raceConditionSpec
  )
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Setup (setupOnce)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Test.Hspec

import Arbiter.Hasql.HasqlDb (createHasqlEnvWithPool, runHasqlDb)
import Test.Arbiter.Hasql.TestHelpers (cleanupHasqlTest, createHasqlPool)

testSchema :: Text
testSchema = "arbiter_hasql_concurrency_test"

type HasqlConcurrencyTestRegistry = '[ 'Queue "arbiter_hasql_concurrency_test" TestPayload]

testTable :: Text
testTable = "arbiter_hasql_concurrency_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False >> createHasqlPool 10 connStr) $ beforeWith (\pool -> cleanupHasqlTest connStr testSchema testTable >> pure pool) $ do
  let run pool act = do
        env <- createHasqlEnvWithPool (Proxy @HasqlConcurrencyTestRegistry) pool testSchema
        runHasqlDb env act
  concurrencySpec @TestPayload @HasqlConcurrencyTestRegistry
    TestMessage
    run
  raceConditionSpec @TestPayload @HasqlConcurrencyTestRegistry
    TestMessage
    run
