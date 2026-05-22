{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.NotificationListener
  ( withNotificationLoop
  ) where

import Arbiter.Core.Job.Schema (quoteIdentifier)
import Control.Monad (forever, void)
import Data.ByteString.Char8 qualified as BSC
import Data.Maybe (fromMaybe)
import Data.String (fromString)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Database.PostgreSQL.Simple qualified as PS
import Database.PostgreSQL.Simple.Notification qualified as PS
import UnliftIO (MonadUnliftIO, liftIO)
import UnliftIO.Async (race_)
import UnliftIO.Exception (bracket)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Logger (LogConfig, defaultLogConfig)
import Arbiter.Worker.Retry (retryOnException)
import Arbiter.Worker.WorkerState (WorkerState (..))

data ListenerCtx
  = ListenerCtx
  { lcProcessStatus :: STM.TVar WorkerState
  , lcPollDelay :: NominalDiffTime
  , lcNotificationVar :: STM.TVar (Maybe PS.Notification)
  , lcConnection :: PS.Connection
  , lcWakeTrigger :: Maybe (STM.STM ())
  }

type Action m a = Maybe PS.Notification -> m a

-- | Runs the provided action when a notification is received on the specified
-- channel or when the poll delay timer expires. Forks a linked thread that listens
-- for Postgres notifications and communicates with the handler loop via a TVar.
-- If the connection is lost, automatically reconnects with backoff. Only exits
-- when the worker state is set to 'ShuttingDown' or an async exception is received.
withNotificationLoop
  :: (MonadUnliftIO m)
  => String
  -- ^ Postgres connection string
  -> String
  -- ^ Notification channel name (e.g., "email_jobs_created")
  -> STM.TVar WorkerState
  -- ^ Signal for worker state (Running, Paused, ShuttingDown)
  -> NominalDiffTime
  -- ^ Poll delay in seconds - action fires on this interval if no
  -- notifications are received. Also serves as the liveness heartbeat.
  -> Maybe LogConfig
  -- ^ Optional log configuration for internal errors
  -> Maybe (STM.STM ())
  -- ^ Optional wake trigger (e.g., worker finished signal)
  -> m ()
  -- ^ Action to run once after LISTEN is established, before entering the
  -- main loop. Lets callers do an initial claim without racing NOTIFYs.
  -> Action m ()
  -- ^ Action to run on each main-loop iteration
  -> m ()
withNotificationLoop connStr channel pSt polDel mLogCfg mWakeTrigger onReady action =
  retryOnException pSt logCfg "Notification listener"
    $ bracket
      (liftIO $ connectToDb connStr)
      (liftIO . PS.close)
    $ \conn -> do
      nVar <- STM.newTVarIO Nothing
      let ctx = ListenerCtx pSt polDel nVar conn mWakeTrigger
      liftIO $ subscribeToChannel (lcConnection ctx) channel
      onReady
      race_
        (mainLoop ctx action)
        (notificationLoop ctx)
  where
    logCfg = fromMaybe defaultLogConfig mLogCfg

data Command
  = Halt
  | PauseCmd
  | NotificationRecv PS.Notification
  | TimerExpired

-- | The main wait/dispatch loop.
--
-- Each iteration registers a fresh poll-delay timer and then awaits — in a
-- single 'atomically' — any of: a state change away from 'Running', an
-- inbound notification, the poll timer expiring, or the optional wake
-- trigger firing. Paused state suspends the loop until state changes.
mainLoop :: (MonadUnliftIO m) => ListenerCtx -> Action m a -> m ()
mainLoop ctx action = loop
  where
    pollMicros = round (lcPollDelay ctx * 1_000_000)

    loop = do
      cmd <- nextCommand
      case cmd of
        Halt -> pure ()
        PauseCmd -> awaitUnpause >> loop
        NotificationRecv n -> action (Just n) *> loop
        TimerExpired -> action Nothing *> loop

    nextCommand = do
      delayVar <- STM.registerDelay pollMicros
      STM.atomically $ do
        status <- STM.readTVar (lcProcessStatus ctx)
        case status of
          ShuttingDown -> pure Halt
          Paused -> pure PauseCmd
          Running ->
            consumeNotification
              `STM.orElse` timerExpired delayVar
              `STM.orElse` waitWakeTrigger
              `STM.orElse` watchStateChange

    -- Block until we're either ShuttingDown or back to Running.
    awaitUnpause =
      STM.atomically $ do
        s <- STM.readTVar (lcProcessStatus ctx)
        case s of
          Paused -> STM.retrySTM
          _ -> pure ()

    consumeNotification = do
      mNotif <- STM.readTVar (lcNotificationVar ctx)
      case mNotif of
        Just n -> do
          STM.writeTVar (lcNotificationVar ctx) Nothing
          pure (NotificationRecv n)
        Nothing -> STM.retrySTM

    timerExpired delayVar = do
      isExpired <- STM.readTVar delayVar
      if isExpired then pure TimerExpired else STM.retrySTM

    waitWakeTrigger = case lcWakeTrigger ctx of
      Nothing -> STM.retrySTM
      Just trigger -> trigger >> pure TimerExpired

    -- Wake when status leaves Running so we can re-check at the top.
    watchStateChange = do
      s <- STM.readTVar (lcProcessStatus ctx)
      case s of
        ShuttingDown -> pure Halt
        Paused -> pure PauseCmd
        Running -> STM.retrySTM

-- Block on receiving a Postgres notification. When a notification is received,
-- add it to the notification var and loop.
notificationLoop :: (MonadUnliftIO m) => ListenerCtx -> m ()
notificationLoop ctx = forever $ do
  n <- liftIO $ PS.getNotification (lcConnection ctx)
  void . STM.atomically $ STM.swapTVar (lcNotificationVar ctx) (Just n)

connectToDb :: String -> IO PS.Connection
connectToDb = PS.connectPostgreSQL . BSC.pack

-- | Issue a LISTEN command to the database for a specific notification channel.
subscribeToChannel :: PS.Connection -> String -> IO ()
subscribeToChannel conn channel =
  void . PS.execute_ conn . fromString $
    T.unpack $
      "LISTEN " <> quoteIdentifier (T.pack channel)
