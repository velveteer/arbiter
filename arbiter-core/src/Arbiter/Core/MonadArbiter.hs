{-# LANGUAGE TypeFamilies #-}

-- | The database primitives every backend implements.
module Arbiter.Core.MonadArbiter
  ( MonadArbiter (..)
  , JobHandler
  , HasRegistry
  , ResultOf
  , Params
  , SomeParam (..)
  , ParamType (..)
  , Query (..)
  , mkQuery
  ) where

import Data.Int (Int64)
import Data.Kind (Type)
import GHC.TypeLits (ErrorMessage (..), TypeError)
import UnliftIO (MonadUnliftIO)

import Arbiter.Core.Codec (ParamType (..), Params, SomeParam (..))
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.Listen (Listener)
import Arbiter.Core.QueueRegistry (JobPayloadRegistry, ResultFor)
import Arbiter.Core.Sql.Query (Query (..), mkQuery)

-- | Database abstraction for job queue operations. Each backend (postgresql-simple,
-- hasql, orville) provides an instance that maps queries to its native driver.
--
-- The instance also names the monad's schema and queue registry. The high-level
-- API resolves table names and result types from them at compile time.
class (MonadUnliftIO m) => MonadArbiter m where
  -- | This monad's registry. The default reports an instance that omits it.
  type RegistryOf m :: JobPayloadRegistry

  type
    RegistryOf m =
      TypeError
        ( 'Text "No registry declared for "
            ':<>: 'ShowType m
            ':$$: 'Text "Its MonadArbiter instance is missing the RegistryOf definition."
            ':$$: 'Text "Add to the instance:  type RegistryOf "
            ':<>: 'ShowType m
            ':<>: 'Text " = YourRegistry"
        )

  -- | Backend-specific handler shape (e.g. @Connection -> job -> m result@).
  -- 'JobHandler' instantiates it at one queue's job and result types.
  type Handler m job result :: Type

  -- | The schema name for this monad's Arbiter tables.
  getSchema :: m SchemaName

  -- | Run a query and decode the result rows. The text, its parameters, and the
  -- decoder travel together in the 'Query'.
  executeQuery :: Query a -> m [a]

  -- | 'executeQuery' for a hot statement whose text is stable across calls. A
  -- backend may prepare it once per connection.
  executeQueryPrepared :: Query a -> m [a]
  executeQueryPrepared = executeQuery

  -- | Run a statement, returning the number of affected rows. The 'Query'
  -- decoder is ignored.
  executeStatement :: Query a -> m Int64

  -- | Run an action in a transaction. Nesting creates savepoints.
  withDbTransaction :: m a -> m a

  -- | Run a job handler with a database connection from the pool.
  runHandlerWithConnection
    :: JobHandler m payload (ResultOf m payload)
    -> JobRead payload
    -> m (ResultOf m payload)

  -- | The env's shared LISTEN/NOTIFY listener, or 'Nothing' for poll-only.
  getListener :: m (Maybe Listener)

-- | A handler for @payload@'s queue. @result@ is what its registry entry declares.
type JobHandler m (payload :: Type) result = Handler m (JobRead payload) result

-- | 'MonadArbiter' with the registry named, for signatures that mention it.
type HasRegistry m (registry :: JobPayloadRegistry) =
  (MonadArbiter m, RegistryOf m ~ registry)

-- | The result type declared by @payload@'s registry entry. It is not injective.
-- A signature naming it needs another argument to determine @payload@.
type ResultOf m (payload :: Type) = ResultFor payload (RegistryOf m)
