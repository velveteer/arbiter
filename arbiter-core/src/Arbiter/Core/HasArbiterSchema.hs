{-# LANGUAGE TypeFamilies #-}

-- | Associates a monad with its PostgreSQL schema name and queue registry.
module Arbiter.Core.HasArbiterSchema
  ( HasArbiterSchema (..)
  , HasRegistry
  , ResultOf
  ) where

import Data.Kind (Type)
import GHC.TypeLits (ErrorMessage (..), TypeError)

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.QueueRegistry (JobPayloadRegistry, ResultFor)

-- | Links a monad to a schema name and registry. 'RegistryOf' lets the
-- high-level API resolve table names from payload types at compile time.
class (Monad m) => HasArbiterSchema m where
  -- | This monad's registry. The default reports an instance that omits it,
  -- which would otherwise surface as an irreducible 'RegistryOf' application.
  type RegistryOf m :: JobPayloadRegistry

  type
    RegistryOf m =
      TypeError
        ( 'Text "No registry declared for "
            ':<>: 'ShowType m
            ':$$: 'Text "Its HasArbiterSchema instance is missing the RegistryOf definition."
            ':$$: 'Text "Add to the instance:  type RegistryOf "
            ':<>: 'ShowType m
            ':<>: 'Text " = YourRegistry"
        )

  -- | The schema name for this monad's Arbiter tables.
  getSchema :: m SchemaName

-- | 'HasArbiterSchema' with the registry named, for signatures that mention it.
type HasRegistry m (registry :: JobPayloadRegistry) =
  (HasArbiterSchema m, RegistryOf m ~ registry)

-- | The result type declared by @payload@'s registry entry. Not injective, so a
-- signature naming it needs another argument to determine @payload@.
type ResultOf m (payload :: Type) = ResultFor payload (RegistryOf m)
