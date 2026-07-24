{-# LANGUAGE TypeFamilies #-}

module Arbiter.Core.MonadArbiter
  ( MonadArbiter (..)
  , Params
  , SomeParam (..)
  , ParamType (..)
  , Query (..)
  , JobHandler
  , BatchedJobHandler
  ) where

import Control.Monad.IO.Class (MonadIO)
import Data.Int (Int64)
import Data.Kind (Type)
import Data.List.NonEmpty (NonEmpty)

import Arbiter.Core.Codec (ParamType (..), Params, SomeParam (..))
import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.Listen (Listener)
import Arbiter.Core.Sql.Query (Query (..))

-- | Database abstraction for job queue operations. Each backend (postgresql-simple,
-- hasql, orville) provides an instance that maps queries to its native driver.
class (Monad m, MonadIO m) => MonadArbiter m where
  -- | Backend-specific handler type (e.g., @Connection -> jobs -> IO result@).
  type Handler m jobs result :: Type

  -- | Run a query and decode the result rows. The text, its parameters, and the
  -- decoder all travel together in the 'Query', so they cannot drift.
  executeQuery :: Query a -> m [a]

  -- | 'executeQuery' for a hot statement whose text is stable across calls, so a
  -- backend may prepare it once per connection and reuse the plan.
  executeQueryPrepared :: Query a -> m [a]
  executeQueryPrepared = executeQuery

  -- | Run a statement, returning the number of affected rows. The 'Query'
  -- decoder is ignored.
  executeStatement :: Query a -> m Int64

  -- | Run an action in a transaction. Nesting creates savepoints.
  withDbTransaction :: m a -> m a

  -- | Run a handler with a database connection from the pool.
  runHandlerWithConnection :: Handler m jobs result -> jobs -> m result

  -- | The env's shared LISTEN/NOTIFY listener, or 'Nothing' for poll-only.
  getListener :: m (Maybe Listener)

type JobHandler m payload result = Handler m (JobRead payload) result
type BatchedJobHandler m payload result = Handler m (NonEmpty (JobRead payload)) result
