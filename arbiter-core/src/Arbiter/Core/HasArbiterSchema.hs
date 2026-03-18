{-# LANGUAGE FunctionalDependencies #-}

-- | Type class for monads that provide the Arbiter schema name
module Arbiter.Core.HasArbiterSchema
  ( HasArbiterSchema (..)
  ) where

import Data.Text (Text)

import Arbiter.Core.QueueRegistry (JobPayloadRegistry)

-- | Type class for monads that provide the PostgreSQL schema name for Arbiter tables
--
-- The functional dependency @m -> registry@ ensures that a monad uniquely determines
-- which registry (type-level table mapping) it uses. This allows the high-level API
-- to perform compile-time table lookups based on payload types.
class (Monad m) => HasArbiterSchema m (registry :: JobPayloadRegistry) | m -> registry where
  getSchema :: m Text
