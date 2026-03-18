{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Hasql.Concurrency (spec) where

import Arbiter.Test.Concurrency
  ( concurrencySpec
  , groupsConsistencyStressSpec
  , inFlightConcurrencySpec
  , raceConditionSpec
  )
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Setup (setupOnce)
import Control.Exception (bracket)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Arbiter.Hasql.HasqlDb (createHasqlEnvWithPool, runHasqlDb)
import Test.Arbiter.Hasql.TestHelpers (cleanupHasqlTest, createHasqlPool)

testSchema :: Text
testSchema = "arbiter_hasql_concurrency_test"

type HasqlConcurrencyTestRegistry = '[ '("arbiter_hasql_concurrency_test", TestPayload)]

testTable :: Text
testTable = "arbiter_hasql_concurrency_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False >> createHasqlPool 10 connStr) $ beforeWith (\pool -> cleanupHasqlTest connStr testSchema testTable >> pure pool) $ do
  let mkEnv pool = createHasqlEnvWithPool (Proxy @HasqlConcurrencyTestRegistry) pool testSchema
      run pool = runHasqlDb (mkEnv pool)
      withConn _ f = bracket (PG.connectPostgreSQL connStr) PG.close f
  concurrencySpec @TestPayload @HasqlConcurrencyTestRegistry
    TestMessage
    run
  raceConditionSpec @TestPayload @HasqlConcurrencyTestRegistry
    TestMessage
    run
  groupsConsistencyStressSpec @TestPayload @HasqlConcurrencyTestRegistry
    testSchema
    testTable
    TestMessage
    run
    withConn
  inFlightConcurrencySpec @TestPayload @HasqlConcurrencyTestRegistry
    testSchema
    testTable
    TestMessage
    run
    withConn
