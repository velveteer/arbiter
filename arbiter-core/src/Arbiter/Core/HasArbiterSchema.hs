{-# LANGUAGE TypeFamilies #-}

-- | Associates a monad with its PostgreSQL schema name and queue registry.
module Arbiter.Core.HasArbiterSchema
  ( HasArbiterSchema (..)
  , ArbiterSchema
  , ResultOf
  ) where

import Data.Kind (Type)

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.QueueRegistry (JobPayloadRegistry, ResultFor)

-- | Links a monad to a schema name and registry. 'RegistryOf' lets the
-- high-level API resolve table names from payload types at compile time.
class (Monad m) => HasArbiterSchema m where
  -- | This monad's registry.
  type RegistryOf m :: JobPayloadRegistry

  -- | The schema name for this monad's Arbiter tables.
  getSchema :: m SchemaName

-- | 'HasArbiterSchema' with the registry named, for signatures that mention it.
type ArbiterSchema m (registry :: JobPayloadRegistry) =
  (HasArbiterSchema m, RegistryOf m ~ registry)

-- | The result type declared by @payload@'s registry entry.
type ResultOf m (payload :: Type) = ResultFor payload (RegistryOf m)
