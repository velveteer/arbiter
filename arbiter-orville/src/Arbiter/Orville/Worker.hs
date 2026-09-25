-- | Run a worker in 'OrvilleDb' with its handlers and hooks written in the base monad:
--
-- @
-- import Arbiter.Orville.Worker
--
-- config <- manualWorkerConfig 5 (orvilleBatchedHandler processJob)
-- let pool = config {observabilityHooks = orvilleHooks appHooks}
-- @
module Arbiter.Orville.Worker
  ( orvilleBatchedHandler
  , orvilleHooks
  ) where

import Arbiter.Core.Job.Types (ObservabilityHooks, hoistObservabilityHooks)
import Arbiter.Worker.Config (BatchCallbacks, hoistBatchCallbacks)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Reader (ReaderT (..))

import Arbiter.Orville.OrvilleDb (OrvilleDb (..), runOrvilleDb)

-- | A batched or manual handler written in the base monad. Its callbacks run in the
-- worker's own 'OrvilleDb' env, on the handler's connection and inside its transaction.
orvilleBatchedHandler
  :: (jobs -> BatchCallbacks m payload result -> m ())
  -> jobs
  -> BatchCallbacks (OrvilleDb registry m) payload result
  -> OrvilleDb registry m ()
orvilleBatchedHandler handler jobs callbacks =
  OrvilleDb . ReaderT $ \env ->
    handler jobs (hoistBatchCallbacks (runOrvilleDb env) callbacks)

-- | Hooks written in the base monad.
orvilleHooks :: (Monad m) => ObservabilityHooks m payload -> ObservabilityHooks (OrvilleDb registry m) payload
orvilleHooks = hoistObservabilityHooks lift
