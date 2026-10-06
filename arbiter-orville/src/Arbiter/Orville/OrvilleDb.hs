{-# LANGUAGE TypeFamilies #-}

-- | A 'MonadArbiter' transformer over the application's Orville monad:
--
-- @
-- import Arbiter.Core
-- import Arbiter.Orville
-- import Orville.PostgreSQL qualified as O
--
-- O.withTransaction $ do
--   O.insertEntity ordersTable order
--   runOrvilleDb \@MyRegistry OrvilleEnv {schema = "arbiter", listener = Nothing} $
--     insertJob (defaultJob (ProcessOrder orderId))
-- @
--
-- Queries run on the base monad's connection and join its open transaction.
module Arbiter.Orville.OrvilleDb
  ( -- * Database monad
    OrvilleDb (..)
  , OrvilleEnv (..)
  , runOrvilleDb

    -- * Connection options
  , toOrvilleConnectionOptions
  ) where

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Listen (Listener)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Class (MonadTrans (..))
import Control.Monad.Trans.Reader (ReaderT (..), asks)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS8
import Orville.PostgreSQL qualified as O
import UnliftIO (MonadUnliftIO)

import Arbiter.Orville.MonadArbiter
  ( orvilleExecuteQuery
  , orvilleExecuteStatement
  , orvilleWithDbTransaction
  )

-- | The schema of the Arbiter tables and an optional LISTEN\/NOTIFY listener.
data OrvilleEnv (registry :: JobPayloadRegistry) = OrvilleEnv
  { schema :: SchemaName
  -- ^ The schema the arbiter tables live in.
  , listener :: Maybe Listener
  -- ^ A listener such as @Arbiter.LibPQ.newLibPQListener@ from arbiter-libpq. 'Nothing' for poll-only.
  }

-- | The Orville database monad. Handlers run in the base monad.
newtype OrvilleDb (registry :: JobPayloadRegistry) m a = OrvilleDb
  { unOrvilleDb :: ReaderT (OrvilleEnv registry) m a
  -- ^ The action as a reader over its env.
  }
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

-- | Orville @ConnectionOptions@ from an arbiter 'Arbiter.Core.PoolConfig.PoolConfig'.
-- Notice reporting is off.
toOrvilleConnectionOptions
  :: ByteString
  -- ^ PostgreSQL connection string
  -> PoolConfig
  -- ^ Arbiter pool configuration
  -> O.ConnectionOptions
toOrvilleConnectionOptions connStr config =
  let stripes = maybe O.OneStripePerCapability O.StripeCount (poolStripes config)
   in O.ConnectionOptions
        { O.connectionString = BS8.unpack connStr
        , O.connectionNoticeReporting = O.DisableNoticeReporting
        , O.connectionPoolStripes = stripes
        , O.connectionPoolLingerTime = fromIntegral (poolIdleTimeout config)
        , O.connectionPoolMaxConnections = O.MaxConnectionsTotal (poolSize config)
        }
