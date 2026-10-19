-- | Convenience re-exports for the @hasql@ backend.
--
-- @
-- import Arbiter.Core qualified as Arb
-- import Arbiter.Hasql qualified as ArbH
-- import Control.Monad (void)
-- import Data.Proxy (Proxy (..))
-- import Pqi.Ffi qualified as Ffi
--
-- main :: IO ()
-- main = do
--   env <- ArbH.createHasqlEnv (Proxy \@MyRegistry) (ArbH.toHasqlConnect Ffi.adapter connStr) "arbiter"
--   ArbH.runHasqlDb env $ do
--     void $ Arb.insertJob (Arb.defaultJob myPayload)
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
