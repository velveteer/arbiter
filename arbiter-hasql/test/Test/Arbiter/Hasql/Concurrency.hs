{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Hasql.Concurrency (spec) where

import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Test.Concurrency
  ( concurrencySpec
  , raceConditionSpec
  )
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Setup (cleanupOnce, concurrencyPoolSize, setupOnce)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Test.Hspec

import Arbiter.Hasql.HasqlDb (createHasqlEnvWithPool, runHasqlDb)
import Test.Arbiter.Hasql.TestHelpers (createHasqlPool)

testSchema :: Text
testSchema = "arbiter_hasql_concurrency_test"

type HasqlConcurrencyTestRegistry = '[Queue "arbiter_hasql_concurrency_test" TestPayload]

testTable :: Text
testTable = "arbiter_hasql_concurrency_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  sharedPool <- runIO (createHasqlPool concurrencyPoolSize connStr)
  sharedEnv <- runIO (createHasqlEnvWithPool (Proxy @HasqlConcurrencyTestRegistry) sharedPool testSchema)
  around (\action -> cleanupOnce connStr testSchema testTable >> action sharedEnv) $ do
    concurrencySpec @TestPayload TestMessage runHasqlDb
    raceConditionSpec @TestPayload TestMessage runHasqlDb
