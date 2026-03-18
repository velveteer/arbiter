{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.NotificationListener
  ( withNotificationLoop
  ) where

import Arbiter.Core.Job.Schema (quoteIdentifier)
import Control.Applicative (Alternative ((<|>)))
import Control.Concurrent (threadDelay)
import Control.Monad (forever, void)
import Data.ByteString.Char8 qualified as BSC
import Data.String (fromString)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Database.PostgreSQL.Simple qualified as PS
import Database.PostgreSQL.Simple.Notification qualified as PS
import UnliftIO (MonadUnliftIO, liftIO)
import UnliftIO.Async (Concurrently (..), race_)
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
  -- ^ Poll delay in seconds — action fires on this interval if no
  -- notifications are received. Also serves as the liveness heartbeat.
  -> Maybe LogConfig
  -- ^ Optional log configuration for internal errors
  -> Maybe (STM.STM ())
  -- ^ Optional wake trigger (e.g., worker finished signal)
  -> Action m ()
  -- ^ Action to run
  -> m ()
withNotificationLoop connStr channel pSt polDel mLogCfg mWakeTrigger action =
  retryOnException pSt logCfg "Notification listener"
    $ bracket
      (liftIO $ connectToDb connStr)
      (liftIO . PS.close)
    $ \conn -> do
      nVar <- STM.newTVarIO Nothing
      let ctx = ListenerCtx pSt polDel nVar conn mWakeTrigger
      liftIO $ subscribeToChannel (lcConnection ctx) channel
      race_
        (mainLoop ctx action)
        (notificationLoop ctx)
  where
    logCfg = maybe defaultLogConfig id mLogCfg

mainLoop :: (MonadUnliftIO m) => ListenerCtx -> Action m a -> m ()
mainLoop ctx action = loop
  where
    loop = do
      status <- STM.readTVarIO $ lcProcessStatus ctx

      -- check status first so there is no race if the process wants to shut down
      case status of
        ShuttingDown -> pure ()
        Paused -> do
          -- When paused, wait for state to change to Running or ShuttingDown
          newStatus <- STM.atomically $ do
            s <- STM.readTVar (lcProcessStatus ctx)
            case s of
              Paused -> STM.retrySTM -- Block until state changes
              _ -> pure s
          case newStatus of
            ShuttingDown -> pure ()
            _ -> loop
        Running -> do
          -- Cancel waiting for a notification when the app shuts down or paused.
          -- There is also an optional timer that, if it expires, fires the
          -- action even if no notification has been received. This provides
          -- assurance that we won't miss anything.
          command <-
            runConcurrently $
              Concurrently (checkStateChange ctx)
                <|> Concurrently (waitForNotification $ lcNotificationVar ctx)
                <|> Concurrently (messageWaitTimer ctx)
                <|> Concurrently (waitForWakeTrigger ctx)

          case command of
            Halt -> pure ()
            PauseCmd -> loop -- Go back to top, will re-check state
            NotificationRecv n -> action (Just n) *> loop
            -- run the action even though there was no message
            TimerExpired -> action Nothing *> loop

data Command
  = Halt
  | PauseCmd
  | NotificationRecv PS.Notification
  | TimerExpired

-- | Blocks until the process status changes from Running to Paused or ShuttingDown.
-- Returns the appropriate command when state changes.
checkStateChange :: (MonadUnliftIO m) => ListenerCtx -> m Command
checkStateChange ctx =
  STM.atomically $ do
    status <- STM.readTVar (lcProcessStatus ctx)
    case status of
      ShuttingDown -> pure Halt
      Paused -> pure PauseCmd
      Running -> STM.retrySTM -- Block until state changes

-- | Block until a notification is received from the notification TVar.
-- Then block until the result is True.
waitForNotification :: (MonadUnliftIO m) => STM.TVar (Maybe PS.Notification) -> m Command
waitForNotification notificationVar = STM.atomically $ do
  mNotificationVar <- STM.readTVar notificationVar
  case mNotificationVar of
    Just n -> do
      STM.writeTVar notificationVar Nothing
      pure $ NotificationRecv n
    Nothing -> STM.retrySTM

-- Block on receiving a Postgres notification. When a notification is received,
-- add it to the notification var and loop.
notificationLoop :: (MonadUnliftIO m) => ListenerCtx -> m ()
notificationLoop ctx = forever $ do
  n <- liftIO $ PS.getNotification (lcConnection ctx)
  void . STM.atomically $ STM.swapTVar (lcNotificationVar ctx) (Just n)

-- | Blocks for the duration of the poll delay.
messageWaitTimer :: (MonadUnliftIO m) => ListenerCtx -> m Command
messageWaitTimer ctx = do
  let microSecs = round (lcPollDelay ctx * 1_000_000)
  delay <- STM.registerDelay microSecs
  STM.atomically $ do
    isExpired <- STM.readTVar delay
    if isExpired
      then pure TimerExpired
      else STM.retrySTM

-- | Blocks until the wake trigger fires, or forever if no trigger is configured.
waitForWakeTrigger :: (MonadUnliftIO m) => ListenerCtx -> m Command
waitForWakeTrigger ctx = case lcWakeTrigger ctx of
  Nothing -> liftIO $ forever $ threadDelay maxBound
  Just trigger -> STM.atomically $ trigger >> pure TimerExpired

connectToDb :: String -> IO PS.Connection
connectToDb = PS.connectPostgreSQL . BSC.pack

-- | Issue a LISTEN command to the database for a specific notification channel.
subscribeToChannel :: PS.Connection -> String -> IO ()
subscribeToChannel conn channel =
  void . PS.execute_ conn . fromString $
    T.unpack $
      "LISTEN " <> quoteIdentifier (T.pack channel)
