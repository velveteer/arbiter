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
--
-- The default carries a @connect_timeout@ because libpq waits forever without one.
-- A host that resolves to an address nothing answers on then hangs the suite with
-- no output, rather than failing with the host in the message.
getTestConnectionString :: IO ByteString
getTestConnectionString = do
  mConnStr <- lookupEnv "ARBITER_TEST_CONN_STRING"
  pure $ maybe defaultConnString BS8.pack mConnStr
  where
    defaultConnString =
      "host=localhost port=5432 user=postgres password=master dbname=postgres connect_timeout=10"
