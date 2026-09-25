{-# LANGUAGE TypeFamilies #-}

-- | A 'MonadArbiter' transformer over the application's Orville monad:
--
-- @
-- import Arbiter.Core
-- import Arbiter.Orville
--
-- O.withTransaction $ do
--   O.insertEntity ordersTable order
--   runOrvilleDb \@MyRegistry (OrvilleEnv "arbiter" Nothing) $
--     insertJob (defaultJob (ProcessOrder orderId))
-- @
--
-- Queries run on the base monad's connection and join its open transaction.
module Arbiter.Orville.OrvilleDb
  ( OrvilleDb (..)
  , OrvilleEnv (..)
  , runOrvilleDb
  ) where

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Listen (Listener)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Class (MonadTrans (..))
import Control.Monad.Trans.Reader (ReaderT (..), asks)
import Orville.PostgreSQL qualified as O
import UnliftIO (MonadUnliftIO)

import Arbiter.Orville.MonadArbiter
  ( orvilleExecuteQuery
  , orvilleExecuteStatement
  , orvilleWithDbTransaction
  )

-- | The schema of the Arbiter tables and an optional LISTEN/NOTIFY listener.
data OrvilleEnv (registry :: JobPayloadRegistry) = OrvilleEnv
  { schema :: SchemaName
  , listener :: Maybe Listener
  -- ^ 'Nothing' for poll-only
  }

-- | The Orville database monad. Handlers run in the base monad.
newtype OrvilleDb (registry :: JobPayloadRegistry) m a = OrvilleDb {unOrvilleDb :: ReaderT (OrvilleEnv registry) m a}
  deriving newtype
    ( Applicative
    , Functor
    , Monad
    , MonadCatch
    , MonadFail
    , MonadIO
    , MonadMask
    , MonadThrow
    , MonadUnliftIO
    , O.HasOrvilleState
    , O.MonadOrvilleControl
    )

instance MonadTrans (OrvilleDb registry) where
  lift = OrvilleDb . lift

instance (O.MonadOrville m) => O.MonadOrville (OrvilleDb registry m)

instance (MonadUnliftIO m, O.MonadOrville m) => MonadArbiter (OrvilleDb registry m) where
  type RegistryOf (OrvilleDb registry m) = registry
  type Handler (OrvilleDb registry m) job result = job -> m result
  getSchema = OrvilleDb $ asks schema
  executeQuery = orvilleExecuteQuery
  executeStatement = orvilleExecuteStatement
  withDbTransaction = orvilleWithDbTransaction
  runHandlerWithConnection handler = lift . handler
  getListener = OrvilleDb $ asks listener

-- | Run an 'OrvilleDb' action in the base monad.
runOrvilleDb :: OrvilleEnv registry -> OrvilleDb registry m a -> m a
runOrvilleDb env = flip runReaderT env . unOrvilleDb
