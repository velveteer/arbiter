-- | Connection pool configuration.
module Arbiter.Core.PoolConfig
  ( PoolConfig (..)
  , defaultPoolConfig
  , poolConfigForWorkers
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

-- | 10 connections, 300s idle timeout, 1 stripe. For workers, use 'poolConfigForWorkers'.
defaultPoolConfig :: PoolConfig
defaultPoolConfig =
  PoolConfig
    { poolSize = 10
    , poolIdleTimeout = 300
    , poolStripes = Just 1
    }

-- | Pool sized for a worker pool of @workerCnt@ threads.
--
-- Returns a single-stripe pool with @2 * workerCnt@ connections (floor of 2).
--
-- A single stripe is intentional. @Data.Pool.withResource@ pins each thread
-- to one stripe based on its capability and does not search other stripes
-- when its own is exhausted. With multiple stripes, threads from the same
-- worker pool can cluster on one stripe and starve it of free connections
-- even when other stripes are idle.
poolConfigForWorkers :: Int -> PoolConfig
poolConfigForWorkers workerCnt =
  PoolConfig
    { poolSize = max 2 (2 * workerCnt)
    , poolIdleTimeout = 300
    , poolStripes = Just 1
    }
