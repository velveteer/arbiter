{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Test.Arbiter.Hasql.ConcurrencyLimit (spec) where

import Arbiter.Test.ConcurrencyLimit (CLReg, concurrencyLimitSpec, concurrencyTable)
import Arbiter.Test.ConcurrencyModel (concurrencyModelSpec)
import Arbiter.Test.Setup (createSharedPool, setupOnce)
import Data.ByteString (ByteString)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Arbiter.Hasql.HasqlDb (HasqlDb, createHasqlEnvWithPool, runHasqlDb, setPreparedStatements)
import Test.Arbiter.Hasql.TestHelpers (cleanupHasqlTest, createHasqlPool)

testSchema :: Text
testSchema = "arbiter_hasql_concurrency_limit_test"

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr testSchema concurrencyTable False) $ do
    sharedPool <- runIO (createHasqlPool 5 connStr)
    pgPool <- runIO (createSharedPool connStr)
    -- This suite runs with prepared claims on.
    mkEnv <- runIO (setPreparedStatements True <$> createHasqlEnvWithPool (Proxy @CLReg) sharedPool testSchema)
    let run :: forall a. HasqlDb CLReg IO a -> IO a
        run = runHasqlDb mkEnv
        withConn :: forall a. (PG.Connection -> IO a) -> IO a
        withConn = withResource pgPool
    around (\action -> cleanupHasqlTest connStr testSchema concurrencyTable >> action mkEnv) $
      concurrencyLimitSpec runHasqlDb
    concurrencyModelSpec run withConn testSchema
