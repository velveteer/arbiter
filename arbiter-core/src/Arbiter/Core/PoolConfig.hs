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
  -- ^ Sub-pools, which spread checkout contention. 'Nothing' picks one per CPU.
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
