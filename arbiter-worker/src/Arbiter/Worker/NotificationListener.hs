{-# LANGUAGE OverloadedStrings #-}

-- | Postgres LISTEN/NOTIFY plumbing: a multi-channel listener thread, and a
-- pause-aware consumer loop the dispatcher drives off it.
module Arbiter.Worker.NotificationListener
  ( ChannelHandler
  , runMultiChannelListener
  , runNotificationConsumer
  ) where

import Arbiter.Core.Job.Schema (quoteIdentifier)
import Control.Monad (forever, void, when)
import Data.ByteString.Char8 qualified as BSC
import Data.Foldable (for_, traverse_)
import Data.Map.Strict qualified as Map
import Data.String (fromString)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Database.PostgreSQL.Simple qualified as PS
import Database.PostgreSQL.Simple.Notification qualified as PS
import UnliftIO (MonadUnliftIO, liftIO)
import UnliftIO.Exception (bracket, tryAny)
import UnliftIO.STM (TVar)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Logger (LogConfig, LogLevel (..))
import Arbiter.Worker.Logger.Internal (tryLog)
import Arbiter.Worker.WorkerState (WorkerState (..))

type ChannelHandler m = PS.Notification -> m ()

type Action m a = Maybe PS.Notification -> m a

-- | LISTEN on every registered channel and dispatch notifications.
runMultiChannelListener
  :: (MonadUnliftIO m)
  => BSC.ByteString
  -> [(String, ChannelHandler m)]
  -> LogConfig
  -> TVar Bool
  -- ^ Set to True once every channel is subscribed.
  -> m ()
runMultiChannelListener connStr handlers logCfg ready =
  bracket
    (liftIO $ PS.connectPostgreSQL connStr)
    (liftIO . PS.close)
    $ \conn -> do
      for_ (Map.keys handlerMap) (liftIO . subscribe conn)
      STM.atomically $ STM.writeTVar ready True
      forever $ do
        n <- liftIO $ PS.getNotification conn
        let chan = BSC.unpack (PS.notificationChannel n)
        traverse_ (dispatch n) (Map.lookup chan handlerMap)
  where
    handlerMap = Map.fromList handlers

    dispatch n h = do
      result <- tryAny (h n)
      case result of
        Right () -> pure ()
        Left e ->
          tryLog logCfg Warning $
            "Channel handler exception: " <> T.pack (show e)

    subscribe conn channel =
      void $
        PS.execute_
          conn
          (fromString ("LISTEN " <> T.unpack (quoteIdentifier (T.pack channel))))

-- | Loop until 'ShuttingDown'. Per iteration wait on notification, poll timer,
-- wake trigger, or state change. Fires @action Nothing@ once at startup if the
-- state is 'Running', so callers don't have to wait for the first event.
runNotificationConsumer
  :: (MonadUnliftIO m)
  => STM.STM WorkerState
  -> NominalDiffTime
  -> STM.TVar (Maybe PS.Notification)
  -> Maybe (STM.STM ())
  -> Action m ()
  -> m ()
runNotificationConsumer readState polDel notifVar mWakeTrigger action = do
  st <- STM.atomically readState
  when (st == Running) (action Nothing)
  loop
  where
    pollMicros = round (polDel * 1_000_000)

    loop = do
      cmd <- nextCommand
      case cmd of
        Halt -> pure ()
        PauseCmd -> do
          next <- awaitUnpause
          case next of
            Halt -> pure ()
            _ -> action Nothing *> loop
        NotificationRecv n -> action (Just n) *> loop
        TimerExpired -> action Nothing *> loop

    nextCommand = do
      delayVar <- STM.registerDelay pollMicros
      STM.atomically $ do
        status <- readState
        case status of
          ShuttingDown -> pure Halt
          Paused -> pure PauseCmd
          Running ->
            consumeNotification
              `STM.orElse` timerExpired delayVar
              `STM.orElse` waitWakeTrigger
              `STM.orElse` watchStateChange

    awaitUnpause =
      STM.atomically $ do
        s <- readState
        case s of
          Paused -> STM.retrySTM
          ShuttingDown -> pure Halt
          Running -> pure TimerExpired

    consumeNotification = do
      mNotif <- STM.readTVar notifVar
      case mNotif of
        Just n -> do
          STM.writeTVar notifVar Nothing
          pure (NotificationRecv n)
        Nothing -> STM.retrySTM

    timerExpired delayVar = do
      isExpired <- STM.readTVar delayVar
      if isExpired then pure TimerExpired else STM.retrySTM

    waitWakeTrigger = case mWakeTrigger of
      Nothing -> STM.retrySTM
      Just trigger -> trigger >> pure TimerExpired

    watchStateChange = do
      s <- readState
      case s of
        ShuttingDown -> pure Halt
        Paused -> pure PauseCmd
        Running -> STM.retrySTM

data Command
  = Halt
  | PauseCmd
  | NotificationRecv PS.Notification
  | TimerExpired
