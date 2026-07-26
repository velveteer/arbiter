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
-- Not injective, so a signature that mentions @ResultOf@ without pinning
-- @payload@ elsewhere is ambiguous, and the error names 'ResultFor' and
-- 'Arbiter.Core.QueueRegistry.SpecForPayload' rather than the user's own code.
-- An equality constraint does not settle it, since @payload@ stays free on both
-- sides. Another argument has to determine @payload@:
--
-- @
-- -- rejected
-- runAll :: (HasArbiterSchema m) => ResultOf m payload -> m ()
-- runAll :: (HasArbiterSchema m, ResultOf m payload ~ r) => r -> m ()
-- -- accepted
-- runAll :: (HasArbiterSchema m) => JobRead payload -> ResultOf m payload -> m ()
-- @
--
-- Otherwise pin the result type itself with @ResultOf m payload ~ ()@. Worker
-- configs take @result@ as an ordinary parameter, so this reaches only code
-- that names @ResultOf@: 'Arbiter.Worker.mergedChildResults' directly, or a
-- polymorphic wrapper around 'Arbiter.Worker.transactionalWorkerConfig' and
-- 'Arbiter.Worker.defaultBatchedWorkerConfig', which return it.
type ResultOf m (payload :: Type) = ResultFor payload (RegistryOf m)
