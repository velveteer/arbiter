{-# LANGUAGE OverloadedStrings #-}

-- | Test configuration shared across arbiter test suites.
module Arbiter.Test.Config
  ( -- * Connection Configuration
    getTestConnectionString
  ) where

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS8
import System.Environment (lookupEnv)

-- | Get the test database connection string.
--
-- Reads from @ARBITER_TEST_CONN_STRING@ environment variable if set,
-- otherwise falls back to the local compose.yml configuration.
getTestConnectionString :: IO ByteString
getTestConnectionString = do
  mConnStr <- lookupEnv "ARBITER_TEST_CONN_STRING"
  pure $ maybe defaultConnString BS8.pack mConnStr
  where
    defaultConnString = "host=localhost port=54324 user=postgres password=master dbname=postgres"
