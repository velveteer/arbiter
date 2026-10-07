{-# LANGUAGE OverloadedStrings #-}

-- | STM handlers for the worker pool's NOTIFY channels.
-- Each handler decodes the payload and reacts only if the message
-- addresses this worker.
module Arbiter.Worker.ChannelHandlers
  ( handlePauseNotif
  , handleCancelNotif
  , handleCronRunNotif
  ) where

import Arbiter.Core.Listen (Notification, notificationData)
import Control.Monad (unless, when)
import Data.Aeson qualified as Aeson
import Data.Int (Int64)
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8Lenient)
import Data.UUID (UUID)
import UnliftIO (MonadUnliftIO, atomically)
import UnliftIO.STM (TVar)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Config (WorkerConfig (..), workerStateVar, writePause)
import Arbiter.Worker.Heartbeat (PoolGuard, recheckJob)
import Arbiter.Worker.WorkerState (WorkerState (..))

-- | Decode the pause payload and, if it addresses this worker, write 'Arbiter.Worker.Config.pauseVar'.
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

-- | If the cancel payload targets this worker, extend the job's batch now. The
-- extend finds the cancel and the guard stops the handler.
handleCancelNotif
  :: (MonadUnliftIO m)
  => WorkerConfig n payload
  -> PoolGuard payload
  -> Notification
  -> m ()
handleCancelNotif config guard notif =
  case Aeson.decodeStrict (notificationData notif) :: Maybe CancelPayload of
    Just (CancelPayload wid jid) | wid == workerId config -> recheckJob guard jid
    _ -> pure ()

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

data PausePayload = PausePayload UUID Bool

instance Aeson.FromJSON PausePayload where
  parseJSON = Aeson.withObject "PausePayload" $ \obj ->
    PausePayload <$> obj Aeson..: "worker_id" <*> obj Aeson..: "paused"

data CancelPayload = CancelPayload UUID Int64

instance Aeson.FromJSON CancelPayload where
  parseJSON = Aeson.withObject "CancelPayload" $ \obj ->
    CancelPayload <$> obj Aeson..: "worker_id" <*> obj Aeson..: "job_id"
