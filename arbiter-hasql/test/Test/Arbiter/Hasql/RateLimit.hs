{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Hasql.RateLimit (spec) where

import Arbiter.Test.RateLimit (RLReg, rateLimitSpec, rateLimitTable, setupRateLimitPolicy)
import Arbiter.Test.Setup (cleanupOnce, setupOnce)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Test.Hspec

import Arbiter.Hasql.HasqlDb (createHasqlEnvWithPool, runHasqlDb)
import Test.Arbiter.Hasql.TestHelpers (createHasqlPool)

testSchema :: Text
testSchema = "arbiter_hasql_ratelimit_test"

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr testSchema rateLimitTable False >> setupRateLimitPolicy connStr testSchema) $ do
    sharedPool <- runIO (createHasqlPool 5 connStr)
    mkEnv <- runIO (createHasqlEnvWithPool (Proxy @RLReg) sharedPool testSchema)
    around (\action -> cleanupOnce connStr testSchema rateLimitTable >> action mkEnv) $
      rateLimitSpec runHasqlDb
