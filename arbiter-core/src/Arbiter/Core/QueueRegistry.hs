{-# LANGUAGE TypeData #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

-- | The type-level registry and the lookups over it. The lookups resolve a payload's
-- queue name and handler result type at compile time. Queue names and payload types
-- must be unique. 'SpecForPayload' checks the entry it resolves against the whole
-- registry. 'AllQueuesUnique' checks the registry up front.
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

import Arbiter.Core.Job.Kind (HasKind (..))

-- | A queue's table name, payload type, and handler result type. A @type data@
-- constructor. It takes no promotion tick.
type data QueueSpec = QueueWithResult Symbol Type Type

-- | A queue whose handlers store no result. A module with its own @Queue@ type
-- needs @import Arbiter.Core hiding (Queue)@ or a qualified import.
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

-- | Look up a payload type's registry entry.
type family SpecForPayload (payload :: Type) (registry :: JobPayloadRegistry) :: QueueSpec where
  SpecForPayload payload registry = MatchIn payload '[] registry

-- | Walk to the entry for @payload@. Carries the entries already passed over
-- and compares the match with all other entries.
type family
  MatchIn (payload :: Type) (seen :: JobPayloadRegistry) (rest :: JobPayloadRegistry)
    :: QueueSpec
  where
  MatchIn payload seen (QueueWithResult table payload result ': rest) =
    OnlyMatch payload (QueueWithResult table payload result) (AppendSpecs seen rest)
  MatchIn payload seen (spec ': rest) = MatchIn payload (spec ': seen) rest
  MatchIn payload _ '[] = PayloadNotRegistered payload

-- | Registry concatenation.
type family
  AppendSpecs (xs :: JobPayloadRegistry) (ys :: JobPayloadRegistry)
    :: JobPayloadRegistry
  where
  AppendSpecs '[] ys = ys
  AppendSpecs (x ': xs) ys = x ': AppendSpecs xs ys

-- | The match, unless another entry reuses its payload type or table name.
type family
  OnlyMatch (payload :: Type) (spec :: QueueSpec) (others :: JobPayloadRegistry)
    :: QueueSpec
  where
  OnlyMatch _ spec '[] = spec
  OnlyMatch payload _ (QueueWithResult _ payload _ ': _) = TypeError (DuplicatePayloadMsg payload)
  OnlyMatch _ (QueueWithResult table _ _) (QueueWithResult table _ _ ': _) =
    TypeError (DuplicateTableMsg table)
  OnlyMatch payload spec (_ ': rest) = OnlyMatch payload spec rest

type DuplicatePayloadMsg (payload :: Type) =
  'Text "Duplicate payload type in registry: " ':<>: 'ShowType payload

type DuplicateTableMsg (table :: Symbol) =
  'Text "Duplicate table name in registry: " ':<>: 'ShowType table

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
  NotInTables table (QueueWithResult table _ _ ': _) = TypeError (DuplicateTableMsg table)
  NotInTables table (_ ': rest) = NotInTables table rest

-- | Check that a payload type doesn't appear in the rest of the registry.
type family NotInPayloads (payload :: Type) (registry :: JobPayloadRegistry) :: Constraint where
  NotInPayloads _ '[] = ()
  NotInPayloads payload (QueueWithResult _ payload _ ': _) = TypeError (DuplicatePayloadMsg payload)
  NotInPayloads payload (_ ': rest) = NotInPayloads payload rest

-- | Extract table names from a type-level registry at runtime (used by migrations).
class (AllQueuesUnique registry) => RegistryTables (registry :: JobPayloadRegistry) where
  registryTableNames :: Proxy registry -> [Text]

  -- | Each queue with the labels its payload declares.
  registryQueueKinds :: Proxy registry -> [(Text, [Text])]

instance RegistryTables '[] where
  registryTableNames _ = []
  registryQueueKinds _ = []

instance
  ( HasKind (SpecPayload spec)
  , KnownSymbol (SpecName spec)
  , NotInPayloads (SpecPayload spec) rest
  , NotInTables (SpecName spec) rest
  , RegistryTables rest
  )
  => RegistryTables (spec ': rest)
  where
  registryTableNames _ =
    T.pack (symbolVal (Proxy @(SpecName spec))) : registryTableNames (Proxy @rest)
  registryQueueKinds _ =
    (T.pack (symbolVal (Proxy @(SpecName spec))), kindsFor @(SpecPayload spec))
      : registryQueueKinds (Proxy @rest)
