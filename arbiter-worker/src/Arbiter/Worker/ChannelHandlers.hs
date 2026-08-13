{-# LANGUAGE OverloadedStrings #-}

-- | STM handlers for the worker pool's NOTIFY channels.
-- Each handler decodes the payload and reacts only if the message
-- addresses this worker.
module Arbiter.Worker.ChannelHandlers
  ( RunningJobs
  , handlePauseNotif
  , handleCancelNotif
  , handleCronRunNotif
  , withRegisteredJobs
  ) where

import Arbiter.Core.Exceptions (JobForceCancelled (..))
import Arbiter.Core.Listen (Notification, notificationData)
import Control.Concurrent (forkIO)
import Control.Exception (SomeException)
import Control.Monad (unless, void, when)
import Data.Aeson qualified as Aeson
import Data.Foldable (traverse_)
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8Lenient)
import Data.UUID (UUID)
import UnliftIO (MonadUnliftIO, atomically, liftIO)
import UnliftIO.Async qualified as Async
import UnliftIO.Exception (finally, throwTo)
import UnliftIO.STM (TVar, readTVar)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Config (WorkerConfig (..))
import Arbiter.Worker.WorkerState (WorkerState (..))

type RunningJobs = TVar (Map.Map Int64 (Async.Async ()))

-- | Decode the pause payload and, if it addresses this worker, write 'pauseVar'.
handlePauseNotif
  :: (MonadUnliftIO m)
  => WorkerConfig n payload
  -> Notification
  -> m ()
handlePauseNotif config notif =
  case Aeson.decodeStrict (notificationData notif) :: Maybe PausePayload of
    Just (PausePayload wid p) | wid == workerId config -> atomically $ do
      st <- STM.readTVar (workerStateVar config)
      unless (st == ShuttingDown) $ STM.writeTVar (pauseVar config) p
    _ -> pure ()

-- | If the cancel payload targets a job on this worker, 'throwTo'
-- 'JobForceCancelled' into its handler thread.
handleCancelNotif
  :: (MonadUnliftIO m)
  => WorkerConfig n payload
  -> RunningJobs
  -> Notification
  -> m ()
handleCancelNotif config runningJobs notif =
  case Aeson.decodeStrict (notificationData notif) :: Maybe CancelPayload of
    Just (CancelPayload wid jid) | wid == workerId config -> do
      mAsync <- atomically $ Map.lookup jid <$> readTVar runningJobs
      traverse_ (fireCancel jid) mAsync
    _ -> pure ()
  where
    -- Fork so throwTo can't block the listener on the target unwinding.
    fireCancel jid a =
      liftIO . void . forkIO $ throwTo (Async.asyncThreadId a) (JobForceCancelled [jid] [])

-- | Signal the scheduler when a run-now NOTIFY names a schedule this pool owns.
-- The cron-run channel is per-schema, so pools that do not own the named
-- schedule ignore the wake instead of issuing a useless pending-runs scan.
handleCronRunNotif
  :: (MonadUnliftIO m)
  => Set Text
  -- ^ This pool's own cron schedule names
  -> TVar Bool
  -> Notification
  -> m ()
handleCronRunNotif ownNames runNowVar notif =
  when (Set.member (decodeUtf8Lenient (notificationData notif)) ownNames) $
    atomically $
      STM.writeTVar runNowVar True

-- | Run @work@ as an async registered in 'RunningJobs' for its lifetime, dropping
-- only the entries this call made.
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
      unregister a =
        atomically $
          STM.modifyTVar' runningJobs $ \m ->
            foldl' (\acc jid -> Map.update (\b -> if b == a then Nothing else Just b) jid acc) m jobIds
  Async.withAsyncWithUnmask gated $ \a ->
    flip finally (unregister a) $ do
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
