{-# LANGUAGE TypeFamilies #-}

module Arbiter.Core.MonadArbiter
  ( MonadArbiter (..)
  , Params
  , SomeParam (..)
  , ParamType (..)
  , JobHandler
  , BatchedJobHandler
  ) where

import Control.Monad.IO.Class (MonadIO)
import Data.Int (Int64)
import Data.Kind (Type)
import Data.List.NonEmpty (NonEmpty)
import Data.Text (Text)

import Arbiter.Core.Codec (ParamType (..), Params, RowCodec, SomeParam (..))
import Arbiter.Core.Job.Types (JobRead)

-- | Database abstraction for job queue operations. Each backend (postgresql-simple,
-- hasql, orville) provides an instance that maps queries to its native driver.
class (Monad m, MonadIO m) => MonadArbiter m where
  -- | Backend-specific handler type (e.g., @Connection -> jobs -> IO result@).
  type Handler m jobs result :: Type

  -- | Run a parameterized query and decode the result rows.
  executeQuery :: Text -> Params -> RowCodec a -> m [a]

  -- | Run a parameterized statement, returning the number of affected rows.
  executeStatement :: Text -> Params -> m Int64

  -- | Run an action in a transaction. Nesting creates savepoints.
  withDbTransaction :: m a -> m a

  -- | Run a handler with a database connection from the pool.
  runHandlerWithConnection :: Handler m jobs result -> jobs -> m result

type JobHandler m payload result = Handler m (JobRead payload) result
type BatchedJobHandler m payload result = Handler m (NonEmpty (JobRead payload)) result
