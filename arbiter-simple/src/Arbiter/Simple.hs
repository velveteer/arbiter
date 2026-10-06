{-# OPTIONS_GHC -Wno-missing-import-lists #-}

-- | Convenience re-exports for the @postgresql-simple@ backend.
--
-- @
-- import Arbiter.Core
-- import Arbiter.Simple
-- import Control.Monad (void)
-- import Data.Proxy (Proxy (..))
--
-- main :: IO ()
-- main = do
--   env <- createSimpleEnv (Proxy \@MyRegistry) connStr "arbiter"
--   runSimpleDb env $ do
--     void $ insertJob (defaultJob myPayload)
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
