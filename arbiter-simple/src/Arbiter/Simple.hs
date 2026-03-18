{-# OPTIONS_GHC -Wno-missing-import-lists #-}

-- | Convenience re-exports for the @postgresql-simple@ backend.
--
-- @
-- import Arbiter.Simple
-- import Data.Proxy (Proxy (..))
--
-- main :: IO ()
-- main = do
--   env <- createSimpleEnv (Proxy @MyRegistry) connStr "public"
--   runSimpleDb env $ do
--     insertJob (defaultJob myPayload)
-- @
module Arbiter.Simple
  ( -- * Re-exports
    module Arbiter.Simple.MonadArbiter
  , module Arbiter.Simple.SimpleDb
  ) where

import Arbiter.Simple.MonadArbiter
import Arbiter.Simple.SimpleDb
