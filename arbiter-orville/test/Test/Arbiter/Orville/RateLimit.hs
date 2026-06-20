{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Test.Arbiter.Orville.RateLimit (spec) where

import Arbiter.Test.RateLimit (RLReg, rateLimitSpec, rateLimitTable, setupRateLimitPolicy)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Test.Hspec

import Test.Arbiter.Orville.TestHelpers (cleanupOrvilleTest, runOrvilleTest, setupOrvilleTest)

testSchema :: Text
testSchema = "arbiter_orville_ratelimit_test"

spec :: ByteString -> Spec
spec connStr = do
  env <- runIO (setupOrvilleTest @RLReg connStr testSchema rateLimitTable 5)
  runIO (setupRateLimitPolicy connStr testSchema)
  before (cleanupOrvilleTest env >> pure env) $
    rateLimitSpec (runOrvilleTest @RLReg)
