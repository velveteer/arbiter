{-# OPTIONS_GHC -Wno-missing-import-lists #-}

-- | Convenience re-exports for the @orville-postgresql@ backend.
--
-- @
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

    -- * Helper Functions
  , createOrvilleConnectionOptions
  ) where

import Arbiter.Core.PoolConfig (PoolConfig (..))
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS8
import Orville.PostgreSQL qualified as O

import Arbiter.Orville.MonadArbiter
import Arbiter.Orville.OrvilleDb

-- | Orville @ConnectionOptions@ from an arbiter 'Arbiter.Core.PoolConfig.PoolConfig'.
createOrvilleConnectionOptions
  :: ByteString
  -- ^ PostgreSQL connection string
  -> PoolConfig
  -- ^ Arbiter pool configuration
  -> O.ConnectionOptions
createOrvilleConnectionOptions connStr config =
  let stripes = maybe O.OneStripePerCapability O.StripeCount (poolStripes config)
   in O.ConnectionOptions
        { O.connectionString = BS8.unpack connStr
        , O.connectionNoticeReporting = O.DisableNoticeReporting
        , O.connectionPoolStripes = stripes
        , O.connectionPoolLingerTime = fromIntegral (poolIdleTimeout config)
        , O.connectionPoolMaxConnections = O.MaxConnectionsTotal (poolSize config)
        }
