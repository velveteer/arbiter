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

import Arbiter.Worker.Config (WorkerConfig (..), workerStateVar, writePause)
import Arbiter.Worker.WorkerState (WorkerState (..))

-- | The handler threads in flight, by job id.
type RunningJobs = TVar (Map.Map Int64 (Async.Async ()))

-- | Decode the pause payload and, if it addresses this worker, write 'pauseVar'.
handlePauseNotif
  :: (MonadUnliftIO m)
  => WorkerConfig n payload
  -> Notification
  -> m ()
handlePauseNotif config notif =
  case Aeson.decodeStrict (notificationData notif) :: Maybe PausePayload of
    Just (PausePayload wid paused) | wid == workerId config -> atomically $ do
      state <- STM.readTVar (workerStateVar config)
      unless (state == ShuttingDown) $ writePause config paused
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
    -- Fork the throwTo off the listener thread.
    fireCancel jid handlerAsync =
      liftIO . void . forkIO $ throwTo (Async.asyncThreadId handlerAsync) (JobForceCancelled [jid] [])

-- | Signal the scheduler when a run-now NOTIFY names a schedule this pool owns.
handleCronRunNotif
  :: (MonadUnliftIO m)
  => Set Text
  -- ^ This pool's own cron schedule names
  -> TVar Bool
  -> Notification
  -> m ()
handleCronRunNotif ownNames runNowVar notif =
  when (Set.member (decodeUtf8Lenient (notificationData notif)) ownNames)
    $ atomically
    $ STM.writeTVar runNowVar True

-- | Run @work@ as an async registered in 'RunningJobs' for its lifetime. Cleanup
-- removes the entries this call made.
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
      unregister handlerAsync =
        atomically $
          STM.modifyTVar' runningJobs $ \running ->
            foldl'
              (\acc jid -> Map.update (\registered -> if registered == handlerAsync then Nothing else Just registered) jid acc)
              running
              jobIds
  Async.withAsyncWithUnmask gated $ \handlerAsync ->
    flip finally (unregister handlerAsync) $ do
      atomically $ do
        STM.modifyTVar' runningJobs $ \running ->
          foldl' (\acc jid -> Map.insert jid handlerAsync acc) running jobIds
        STM.putTMVar startGate ()
      Async.waitCatch handlerAsync

data PausePayload = PausePayload UUID Bool

instance Aeson.FromJSON PausePayload where
  parseJSON = Aeson.withObject "PausePayload" $ \obj ->
    PausePayload <$> obj Aeson..: "worker_id" <*> obj Aeson..: "paused"

data CancelPayload = CancelPayload UUID Int64

instance Aeson.FromJSON CancelPayload where
  parseJSON = Aeson.withObject "CancelPayload" $ \obj ->
    CancelPayload <$> obj Aeson..: "worker_id" <*> obj Aeson..: "job_id"
