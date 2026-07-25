{-# LANGUAGE TypeData #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

-- | Type-level utilities for job queue registry validation.
--
-- The registry enforces at compile-time that:
--
--   1. Each payload type maps to exactly one queue name (via 'TableForPayload')
--   2. All queue names are unique (via 'AllQueuesUnique')
--   3. All payload types are unique (also via 'AllQueuesUnique')
--   4. Workers can only claim jobs for payloads they're registered to handle
--   5. Each queue has one handler result type (via 'ResultFor')
module Arbiter.Core.QueueRegistry
  ( -- * Registry type
    JobPayloadRegistry
  , QueueSpec (..)
  , Queue

    -- * Registry lookups
  , TableForPayload
  , ResultFor
  , SpecForPayload
  , SpecName
  , SpecPayload
  , SpecResult

    -- * Registry validation
  , AllQueuesUnique

    -- * Runtime utilities
  , RegistryTables (..)
  ) where

import Data.Kind (Constraint, Type)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import GHC.TypeLits (ErrorMessage (..), KnownSymbol, Symbol, TypeError, symbolVal)

-- | A queue's table name, payload type, and handler result type.
--
-- This is a @type data@ constructor, so it lives in the type namespace and takes
-- no promotion tick.
type data QueueSpec = QueueWithResult Symbol Type Type

-- | A queue whose handlers store no result. Type errors and haddock print the
-- expanded 'QueueWithResult' form.
--
-- A module with its own @Queue@ type needs @import Arbiter.Core hiding (Queue)@
-- or a qualified import.
type Queue (table :: Symbol) (payload :: Type) = QueueWithResult table payload ()

-- | A type-level registry mapping table names to payload types.
--
-- Example:
-- @
-- type MyAppRegistry =
--   '[ Queue "email_jobs" EmailPayload
--    , QueueWithResult "image_jobs" ImagePayload Score
--    ]
-- @
type JobPayloadRegistry = [QueueSpec]

-- | A queue's table name.
type family SpecName (spec :: QueueSpec) :: Symbol where
  SpecName (QueueWithResult table _ _) = table

-- | A queue's payload type.
type family SpecPayload (spec :: QueueSpec) :: Type where
  SpecPayload (QueueWithResult _ payload _) = payload

-- | A queue's result type. A 'Queue' entry produces @()@.
type family SpecResult (spec :: QueueSpec) :: Type where
  SpecResult (QueueWithResult _ _ result) = result

-- | Look up a payload type's registry entry. Compile-time error if not
-- registered. Takes the first match, which 'AllQueuesUnique' makes the only one.
type family SpecForPayload (payload :: Type) (registry :: JobPayloadRegistry) :: QueueSpec where
  SpecForPayload payload (QueueWithResult table payload result ': _) = QueueWithResult table payload result
  SpecForPayload payload (_ ': rest) = SpecForPayload payload rest
  SpecForPayload payload '[] = PayloadNotRegistered payload

type family PayloadNotRegistered (payload :: Type) :: QueueSpec where
  PayloadNotRegistered payload =
    TypeError
      ( 'Text "Payload type "
          ':<>: 'ShowType payload
          ':<>: 'Text " not found in registry"
          ':$$: 'Text "Add a Queue entry, or QueueWithResult to store a result."
      )

-- | Look up the table name for a payload type. Compile-time error if not registered.
type family TableForPayload (payload :: Type) (registry :: JobPayloadRegistry) :: Symbol where
  TableForPayload payload registry = SpecName (SpecForPayload payload registry)

-- | The result type a queue's handlers produce.
type family ResultFor (payload :: Type) (registry :: JobPayloadRegistry) :: Type where
  ResultFor payload registry = SpecResult (SpecForPayload payload registry)

-- | Compile-time check that no two entries share a queue name or a payload type.
-- The payload half matters because 'SpecForPayload' takes the first match.
type family AllQueuesUnique (registry :: JobPayloadRegistry) :: Constraint where
  AllQueuesUnique '[] = ()
  AllQueuesUnique (spec ': rest) =
    ( NotInTables (SpecName spec) rest
    , NotInPayloads (SpecPayload spec) rest
    , AllQueuesUnique rest
    )

-- | Check that a table name doesn't appear in the rest of the registry.
type family NotInTables (table :: Symbol) (registry :: JobPayloadRegistry) :: Constraint where
  NotInTables _ '[] = ()
  NotInTables table (QueueWithResult table _ _ ': _) = DuplicateTable table
  NotInTables table (_ ': rest) = NotInTables table rest

-- | Check that a payload type doesn't appear in the rest of the registry.
type family NotInPayloads (payload :: Type) (registry :: JobPayloadRegistry) :: Constraint where
  NotInPayloads _ '[] = ()
  NotInPayloads payload (QueueWithResult _ payload _ ': _) = DuplicatePayload payload
  NotInPayloads payload (_ ': rest) = NotInPayloads payload rest

type family DuplicateTable (table :: Symbol) :: Constraint where
  DuplicateTable table =
    TypeError
      ( 'Text "Duplicate table name: "
          ':<>: 'ShowType table
          ':<>: 'Text ""
          ':$$: 'Text "Each table can only be used once in the registry."
          ':$$: 'Text "Hint: Multiple payload types cannot share the same table."
      )

type family DuplicatePayload (payload :: Type) :: Constraint where
  DuplicatePayload payload =
    TypeError
      ( 'Text "Duplicate payload type: "
          ':<>: 'ShowType payload
          ':$$: 'Text "Each payload type can only be used once in the registry."
          ':$$: 'Text "Hint: a payload's table name and result type resolve to its"
          ':$$: 'Text "first registry entry, so a second entry would never be used."
      )

-- | Extract table names from a type-level registry at runtime (used by migrations).
class (AllQueuesUnique registry) => RegistryTables (registry :: JobPayloadRegistry) where
  registryTableNames :: Proxy registry -> [Text]

instance RegistryTables '[] where
  registryTableNames _ = []

instance
  ( KnownSymbol (SpecName spec)
  , NotInPayloads (SpecPayload spec) rest
  , NotInTables (SpecName spec) rest
  , RegistryTables rest
  )
  => RegistryTables (spec ': rest)
  where
  registryTableNames _ =
    T.pack (symbolVal (Proxy @(SpecName spec))) : registryTableNames (Proxy @rest)
