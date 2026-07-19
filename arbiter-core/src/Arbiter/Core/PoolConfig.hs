-- | Connection pool configuration.
module Arbiter.Core.PoolConfig
  ( PoolConfig (..)
  , defaultPoolConfig
  ) where

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

-- | 10 connections, 300s idle timeout, 1 stripe. For workers, size the pool
-- with 'Arbiter.Worker.poolConfigForWorkers' instead.
defaultPoolConfig :: PoolConfig
defaultPoolConfig =
  PoolConfig
    { poolSize = 10
    , poolIdleTimeout = 300
    , poolStripes = Just 1
    }
