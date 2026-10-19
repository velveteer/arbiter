-- | Convenience re-exports for the @postgresql-simple@ backend.
--
-- @
-- import Arbiter.Core qualified as Arb
-- import Arbiter.Simple qualified as ArbS
-- import Control.Monad (void)
-- import Data.Proxy (Proxy (..))
--
-- main :: IO ()
-- main = do
--   env <- ArbS.createSimpleEnv (Proxy \@MyRegistry) connStr "arbiter"
--   ArbS.runSimpleDb env $ do
--     void $ Arb.insertJob (Arb.defaultJob myPayload)
-- @
--
-- "Arbiter.Simple.MonadArbiter" has the primitives for an application's own instance.
module Arbiter.Simple
  ( -- * Re-exports
    module Arbiter.Simple.MonadArbiter
  , module Arbiter.Simple.SimpleDb
  ) where

import Arbiter.Simple.MonadArbiter
import Arbiter.Simple.SimpleDb
