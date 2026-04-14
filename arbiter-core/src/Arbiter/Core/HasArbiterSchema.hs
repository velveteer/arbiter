{-# LANGUAGE FunctionalDependencies #-}

-- | Associates a monad with its PostgreSQL schema name and queue registry.
module Arbiter.Core.HasArbiterSchema
  ( HasArbiterSchema (..)
  ) where

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)

-- | Links a monad to a schema name and registry. The fundep @m -> registry@
-- lets the high-level API resolve table names from payload types at compile time.
class (Monad m) => HasArbiterSchema m (registry :: JobPayloadRegistry) | m -> registry where
  -- | The schema name for this monad's Arbiter tables.
  getSchema :: m SchemaName
