{-# OPTIONS_GHC -Wno-missing-import-lists #-}

-- | Convenience re-exports for the @hasql@ backend.
--
-- @
-- import Arbiter.Core
-- import Arbiter.Hasql
-- import Control.Monad (void)
-- import Data.Proxy (Proxy (..))
-- import Pqi.Ffi qualified as Ffi
--
-- main :: IO ()
-- main = do
--   env <- createHasqlEnv (Proxy \@MyRegistry) (toHasqlConnect Ffi.adapter connStr) "arbiter"
--   runHasqlDb env $ do
--     void $ insertJob (defaultJob myPayload)
-- @
--
-- @Pqi.Ffi@ is in the pqi-ffi package. On hasql 1.x 'toHasqlConnect' takes only the connection string.
--
-- "Arbiter.Hasql.MonadArbiter" has the primitives for an application's own instance.
module Arbiter.Hasql
  ( -- * Re-exports
    module Arbiter.Hasql.MonadArbiter
  , module Arbiter.Hasql.HasqlDb
  ) where

import Arbiter.Hasql.HasqlDb
import Arbiter.Hasql.MonadArbiter
