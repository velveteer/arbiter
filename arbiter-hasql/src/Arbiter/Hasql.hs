{-# OPTIONS_GHC -Wno-missing-import-lists #-}

-- | Convenience re-exports for the @hasql@ backend.
--
-- @
-- import Arbiter.Hasql
-- import Data.Proxy (Proxy (..))
--
-- main :: IO ()
-- main = do
--   env <- createHasqlEnv (Proxy \@MyRegistry) connStr "arbiter"
--   runHasqlDb env $ do
--     insertJob (defaultJob myPayload)
-- @
module Arbiter.Hasql
  ( -- * Re-exports
    module Arbiter.Hasql.MonadArbiter
  , module Arbiter.Hasql.HasqlDb
  ) where

import Arbiter.Hasql.HasqlDb
import Arbiter.Hasql.MonadArbiter
