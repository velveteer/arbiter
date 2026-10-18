{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Simple.RateLimit (spec) where

import Arbiter.Test.RateLimit (RLReg, rateLimitSpec, rateLimitTable, setupRateLimitPolicy)
import Arbiter.Test.Setup (cleanupData, createSharedPool, setupOnce)
import Data.ByteString (ByteString)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Test.Hspec

import Arbiter.Simple.SimpleDb (createSimpleEnvWithPool, runSimpleDb)

testSchema :: Text
testSchema = "arbiter_simple_ratelimit_test"

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr testSchema rateLimitTable False >> setupRateLimitPolicy connStr testSchema) $ do
    sharedPool <- runIO (createSharedPool connStr)
    mkEnv <- runIO (createSimpleEnvWithPool (Proxy @RLReg) sharedPool testSchema)
    around (\action -> withResource sharedPool (cleanupData testSchema rateLimitTable) >> action mkEnv) $
      rateLimitSpec runSimpleDb
