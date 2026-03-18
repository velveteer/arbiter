{-# OPTIONS_GHC -Wno-missing-import-lists #-}

-- | Convenience re-exports for the @orville-postgresql@ backend.
--
-- See "Arbiter.Orville.MonadArbiter" for integration details.
module Arbiter.Orville
  ( -- * Re-exports
    module Arbiter.Orville.MonadArbiter

    -- * Helper Functions
  , createOrvilleConnectionOptions
  ) where

import Arbiter.Core.PoolConfig (PoolConfig (..))
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS8
import Orville.PostgreSQL qualified as O

import Arbiter.Orville.MonadArbiter

-- | Create Orville ConnectionOptions from an Arbiter PoolConfig
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
