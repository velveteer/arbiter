-- | Connection pool configuration.
module Arbiter.Core.PoolConfig
  ( PoolConfig (..)
  , defaultPoolConfig
  , poolConfigForWorkers
  ) where

import Control.Monad.IO.Class (MonadIO, liftIO)
import GHC.Conc (getNumCapabilities)

-- | Connection pool configuration.
data PoolConfig = PoolConfig
  { poolSize :: Int
  -- ^ Maximum connections
  , poolIdleTimeout :: Int
  -- ^ Idle timeout (seconds)
  , poolStripes :: Maybe Int
  -- ^ Number of stripes (sub-pools). Reduces lock contention on connection checkout.
  --
  -- * @Nothing@: Auto-detect based on CPU count
  -- * @Just n@: Use n stripes
  }
  deriving stock (Eq, Show)

-- | Default pool configuration: 10 connections, 300s idle timeout, 1 stripe.
--
-- Conservative defaults suitable for producer-only workloads. For worker pools,
-- use 'poolConfigForWorkers' to size the pool based on worker count.
defaultPoolConfig :: PoolConfig
defaultPoolConfig =
  PoolConfig
    { poolSize = 10
    , poolIdleTimeout = 300
    , poolStripes = Just 1
    }

-- | Creates a pool configuration sized for a worker pool.
--
-- Sizing: @workerCount + 5@ connections (worker threads plus headroom for
-- dispatcher and heartbeats). Stripes set to min(capabilities, poolSize).
poolConfigForWorkers :: (MonadIO m) => Int -> m PoolConfig
poolConfigForWorkers workerCnt = do
  caps <- liftIO getNumCapabilities
  let size = workerCnt + 5
  -- Ensure stripes doesn't exceed pool size (resource-pool requirement)
  let stripes = min caps size
  pure
    PoolConfig
      { poolSize = size
      , poolIdleTimeout = 300
      , poolStripes = Just stripes
      }
