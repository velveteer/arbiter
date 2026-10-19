-- | Convenience re-exports for the @orville-postgresql@ backend.
--
-- @
-- import Arbiter.Core qualified as Arb
-- import Arbiter.Orville qualified as ArbO
-- import Control.Monad (void)
-- import Orville.PostgreSQL (MonadOrville)
-- import UnliftIO (MonadUnliftIO)
--
-- enqueue :: (MonadOrville m, MonadUnliftIO m) => m ()
-- enqueue =
--   ArbO.runOrvilleDb \@MyRegistry ArbO.OrvilleEnv {ArbO.schema = "arbiter", ArbO.listener = Nothing} $
--     void $ Arb.insertJob (Arb.defaultJob myPayload)
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
