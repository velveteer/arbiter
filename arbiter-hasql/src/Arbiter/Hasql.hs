{-# OPTIONS_GHC -Wno-missing-import-lists #-}

-- | Convenience re-exports for the @hasql@ backend.
--
-- @
-- import Arbiter.Core
-- import Arbiter.Hasql
-- import Data.Proxy (Proxy (..))
-- import Pqi.Ffi qualified as Ffi
--
-- main :: IO ()
-- main = do
--   env <- createHasqlEnv (Proxy \@MyRegistry) (toHasqlConnect Ffi.adapter connStr) "arbiter"
--   runHasqlDb env $ do
--     insertJob (defaultJob myPayload)
-- @
--
-- On hasql 1.x 'toHasqlConnect' takes only the connection string.
module Arbiter.Hasql
  ( -- * Re-exports
    module Arbiter.Hasql.MonadArbiter
  , module Arbiter.Hasql.HasqlDb
  ) where

import Arbiter.Hasql.HasqlDb
import Arbiter.Hasql.MonadArbiter
