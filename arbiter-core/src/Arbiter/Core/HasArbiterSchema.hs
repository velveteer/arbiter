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
--
-- Not injective. A signature mentioning @ResultOf@ without naming @payload@
-- concretely is ambiguous, and the error names 'ResultFor' and
-- 'Arbiter.Core.QueueRegistry.SpecForPayload' rather than the user's own code.
-- Add an equality constraint:
--
-- @
-- -- rejected
-- runAll :: (HasArbiterSchema m) => ResultOf m payload -> m ()
-- -- accepted
-- runAll :: (HasArbiterSchema m, ResultOf m payload ~ r) => r -> m ()
-- @
--
-- The worker types take @result@ as an ordinary parameter for this reason, so
-- only code that names @ResultOf@ itself, such as
-- 'Arbiter.Worker.mergedChildResults', needs the constraint.
type ResultOf m (payload :: Type) = ResultFor payload (RegistryOf m)
