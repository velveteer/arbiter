{-# LANGUAGE OverloadedStrings #-}

-- | STM handlers for the worker pool's NOTIFY channels (pause, force-cancel).
-- Each handler decodes the JSON payload and reacts only if the message
-- addresses this worker.
module Arbiter.Worker.ChannelHandlers
  ( RunningJobs
  , JobForceCancelled (..)
  , handlePauseNotif
  , handleCancelNotif
  , withRegisteredJobs
  ) where

import Arbiter.Core.Job.Types ()
import Control.Concurrent (forkIO)
import Control.Exception
  ( Exception (..)
  , SomeException
  , asyncExceptionFromException
  , asyncExceptionToException
  )
import Control.Monad (unless, void)
import Data.Aeson qualified as Aeson
import Data.Foldable (traverse_)
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.UUID (UUID)
import Database.PostgreSQL.Simple.Notification qualified as PS
import UnliftIO (MonadUnliftIO, atomically, liftIO)
import UnliftIO.Async qualified as Async
import UnliftIO.Exception (throwTo)
import UnliftIO.STM (TVar, readTVar)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Config (WorkerConfig (..))
import Arbiter.Worker.WorkerState (WorkerState (..))

type RunningJobs = TVar (Map.Map Int64 (Async.Async ()))

-- | Decode the pause payload and, if it addresses this worker, write 'pauseVar'.
handlePauseNotif
  :: (MonadUnliftIO m)
  => WorkerConfig n payload result
  -> PS.Notification
  -> m ()
handlePauseNotif config notif =
  case Aeson.decodeStrict (PS.notificationData notif) :: Maybe PausePayload of
    Just (PausePayload wid p) | wid == workerId config -> atomically $ do
      st <- STM.readTVar (workerStateVar config)
      unless (st == ShuttingDown) $ STM.writeTVar (pauseVar config) p
    _ -> pure ()

-- | If the cancel payload targets a job on this worker, 'throwTo'
-- 'JobForceCancelled' into its handler thread.
handleCancelNotif
  :: (MonadUnliftIO m)
  => WorkerConfig n payload result
  -> RunningJobs
  -> PS.Notification
  -> m ()
handleCancelNotif config runningJobs notif =
  case Aeson.decodeStrict (PS.notificationData notif) :: Maybe CancelPayload of
    Just (CancelPayload wid jid) | wid == workerId config -> do
      mAsync <- atomically $ Map.lookup jid <$> readTVar runningJobs
      traverse_ fireCancel mAsync
    _ -> pure ()
  where
    -- Fork so throwTo can't block the listener on the target unwinding.
    fireCancel a =
      liftIO . void . forkIO $ throwTo (Async.asyncThreadId a) JobForceCancelled

-- | Run @work@ as an async registered in 'RunningJobs' for its lifetime.
withRegisteredJobs
  :: forall m
   . (MonadUnliftIO m)
  => RunningJobs
  -> [Int64]
  -> m ()
  -> m (Either SomeException ())
withRegisteredJobs runningJobs jobIds work = do
  startGate <- STM.newEmptyTMVarIO
  let gated :: (forall c. m c -> m c) -> m ()
      gated unmask = unmask $ do
        atomically (STM.readTMVar startGate)
        work
  Async.withAsyncWithUnmask gated $ \a -> do
    atomically $ do
      STM.modifyTVar' runningJobs $ \m ->
        foldl' (\acc jid -> Map.insert jid a acc) m jobIds
      STM.putTMVar startGate ()
    Async.waitCatch a

data PausePayload = PausePayload UUID Bool

instance Aeson.FromJSON PausePayload where
  parseJSON = Aeson.withObject "PausePayload" $ \o ->
    PausePayload <$> o Aeson..: "worker_id" <*> o Aeson..: "paused"

data CancelPayload = CancelPayload UUID Int64

instance Aeson.FromJSON CancelPayload where
  parseJSON = Aeson.withObject "CancelPayload" $ \o ->
    CancelPayload <$> o Aeson..: "worker_id" <*> o Aeson..: "job_id"

-- | Async exception for user-initiated force-cancel.
data JobForceCancelled = JobForceCancelled
  deriving stock (Show)

instance Exception JobForceCancelled where
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException
