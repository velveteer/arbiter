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
  -- ^ Sub-pools. 'Nothing' picks one per CPU.
  }
  deriving stock (Eq, Show)

-- | 10 connections, 300s idle timeout, 1 stripe. For workers, size the pool
-- with 'Arbiter.Worker.poolConfigForWorkers'.
defaultPoolConfig :: PoolConfig
defaultPoolConfig =
  PoolConfig
    { poolSize = 10
    , poolIdleTimeout = 300
    , poolStripes = Just 1
    }
