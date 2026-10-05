{-# OPTIONS_GHC -Wno-missing-import-lists #-}

-- | Convenience re-exports for the @orville-postgresql@ backend.
--
-- @
-- import Arbiter.Core
-- import Arbiter.Orville
--
-- enqueue :: (MonadOrville m, MonadUnliftIO m) => m ()
-- enqueue =
--   runOrvilleDb \@MyRegistry (OrvilleEnv "arbiter" Nothing) $
--     insertJob (defaultJob myPayload)
-- @
--
-- "Arbiter.Orville.MonadArbiter" has the primitives for an application's own instance.
-- "Arbiter.Orville.Worker" adapts handlers and hooks written in the application monad.
module Arbiter.Orville
  ( -- * Re-exports
    module Arbiter.Orville.MonadArbiter
  , module Arbiter.Orville.OrvilleDb
  ) where

import Arbiter.Orville.MonadArbiter
import Arbiter.Orville.OrvilleDb
