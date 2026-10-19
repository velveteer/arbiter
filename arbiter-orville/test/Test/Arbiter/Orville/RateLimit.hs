{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Orville.RateLimit (spec) where

import Arbiter.Test.RateLimit (RLReg, rateLimitSpec, rateLimitTable, setupRateLimitPolicy)
import Arbiter.Test.Setup (setupOnce, sharedPoolSize)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Test.Hspec

import Test.Arbiter.Orville.TestHelpers (cleanupOrvilleTest, createOrvilleTestEnv, runOrvilleTest)

testSchema :: Text
testSchema = "arbiter_orville_ratelimit_test"

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr testSchema rateLimitTable False >> setupRateLimitPolicy connStr testSchema) $ do
    env <- runIO (createOrvilleTestEnv @RLReg connStr testSchema rateLimitTable sharedPoolSize)
    before (cleanupOrvilleTest env >> pure env) $
      rateLimitSpec (runOrvilleTest @RLReg)
