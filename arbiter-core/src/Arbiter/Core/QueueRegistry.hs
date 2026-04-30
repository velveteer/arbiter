{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

-- | Type-level utilities for job queue registry validation.
--
-- The registry enforces at compile-time that:
--
--   1. Each payload type maps to exactly one queue name (via 'TableForPayload')
--   2. All queue names are unique (via 'AllQueuesUnique')
--   3. Workers can only claim jobs for payloads they're registered to handle
module Arbiter.Core.QueueRegistry
  ( -- * Registry type
    JobPayloadRegistry

    -- * Registry validation
  , TableForPayload
  , AllQueuesUnique

    -- * Runtime utilities
  , RegistryTables (..)
  ) where

import Data.Kind (Constraint, Type)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import GHC.TypeLits (ErrorMessage (..), KnownSymbol, Symbol, TypeError, symbolVal)

-- | A type-level registry mapping table names to payload types.
--
-- Example:
-- @
-- type MyAppRegistry =
--   '[ '("email_jobs", EmailPayload)
--    , '("image_jobs", ImagePayload)
--    ]
-- @
type JobPayloadRegistry = [(Symbol, Type)]

-- | Look up the table name for a payload type. Compile-time error if not registered.
type family TableForPayload (payload :: Type) (registry :: JobPayloadRegistry) :: Symbol where
  TableForPayload payload ('(table, payload) ': _) = table
  TableForPayload payload ('(_, _) ': rest) =
    TableForPayload payload rest
  TableForPayload payload '[] =
    TypeError ('Text "Payload type " ':<>: 'ShowType payload ':<>: 'Text " not found in registry")

-- | Compile-time check that no two payload types share a queue name.
type family AllQueuesUnique (registry :: JobPayloadRegistry) :: Constraint where
  AllQueuesUnique '[] = ()
  AllQueuesUnique ('(table, _) ': rest) =
    (NotInTables table rest, AllQueuesUnique rest)

-- | Check that a table name doesn't appear in the rest of the registry.
type family NotInTables (table :: Symbol) (registry :: JobPayloadRegistry) :: Constraint where
  NotInTables _ '[] = ()
  NotInTables table ('(table, _) ': _) =
    TypeError
      ( 'Text "Duplicate table name: "
          ':<>: 'ShowType table
          ':<>: 'Text ""
          ':$$: 'Text "Each table can only be used once in the registry."
          ':$$: 'Text "Hint: Multiple payload types cannot share the same table."
      )
  NotInTables table ('(_, _) ': rest) = NotInTables table rest

-- | Extract table names from a type-level registry at runtime (used by migrations).
class (AllQueuesUnique registry) => RegistryTables (registry :: JobPayloadRegistry) where
  registryTableNames :: Proxy registry -> [Text]

instance RegistryTables '[] where
  registryTableNames _ = []

instance (KnownSymbol table, NotInTables table rest, RegistryTables rest) => RegistryTables ('(table, payload) ': rest) where
  registryTableNames _ =
    T.pack (symbolVal (Proxy @table)) : registryTableNames (Proxy @rest)
