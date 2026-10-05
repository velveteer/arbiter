{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Test.Arbiter.Simple.ConcurrencyLimit (spec) where

import Arbiter.Test.ConcurrencyLimit (CLReg, concurrencyLimitSpec, concurrencyTable)
import Arbiter.Test.ConcurrencyModel (concurrencyModelSpec)
import Arbiter.Test.Setup (cleanupData, createSharedPool, setupOnce)
import Data.ByteString (ByteString)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Arbiter.Simple.SimpleDb (SimpleDb, createSimpleEnvWithPool, runSimpleDb)

testSchema :: Text
testSchema = "arbiter_simple_concurrency_limit_test"

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr testSchema concurrencyTable False) $ do
    sharedPool <- runIO (createSharedPool connStr)
    mkEnv <- runIO (createSimpleEnvWithPool (Proxy @CLReg) sharedPool testSchema)
    let run :: forall a. SimpleDb CLReg IO a -> IO a
        run = runSimpleDb mkEnv
        withConn :: forall a. (PG.Connection -> IO a) -> IO a
        withConn = withResource sharedPool
    around (\action -> withConn (cleanupData testSchema concurrencyTable) >> action mkEnv) $
      concurrencyLimitSpec runSimpleDb
    concurrencyModelSpec run testSchema withConn
