{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Test.Arbiter.Orville.ConcurrencyLimit (spec) where

import Arbiter.Test.ConcurrencyLimit (CLReg, concurrencyLimitSpec, concurrencyTable)
import Arbiter.Test.ConcurrencyModel (concurrencyModelSpec)
import Arbiter.Test.Setup (createSharedPool)
import Data.ByteString (ByteString)
import Data.Pool (withResource)
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Test.Arbiter.Orville.TestHelpers (TestOrville, cleanupOrvilleTest, runOrvilleTest, setupOrvilleTest)

testSchema :: Text
testSchema = "arbiter_orville_concurrency_limit_test"

spec :: ByteString -> Spec
spec connStr = do
  env <- runIO (setupOrvilleTest @CLReg connStr testSchema concurrencyTable 5)
  pgPool <- runIO (createSharedPool connStr)
  let run :: forall a. TestOrville CLReg a -> IO a
      run = runOrvilleTest env
      withConn :: forall a. (PG.Connection -> IO a) -> IO a
      withConn = withResource pgPool
  before (cleanupOrvilleTest env >> pure env) $
    concurrencyLimitSpec (runOrvilleTest @CLReg)
  concurrencyModelSpec run withConn testSchema
