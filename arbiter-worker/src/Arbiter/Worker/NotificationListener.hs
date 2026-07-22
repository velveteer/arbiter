-- | Pause-aware consumer loop the dispatcher drives off the shared listener.
module Arbiter.Worker.NotificationListener
  ( runNotificationConsumer
  ) where

import Arbiter.Core.Listen (Notification)
import Control.Monad (when)
import Data.Time (NominalDiffTime)
import UnliftIO (MonadUnliftIO)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.WorkerState (WorkerState (..))

type Action m a = Maybe Notification -> m a

-- | Loop until 'ShuttingDown'. Per iteration wait on notification, poll timer,
-- wake trigger, or state change. Fires @action Nothing@ once at startup if the
-- state is 'Running', so callers don't have to wait for the first event.
runNotificationConsumer
  :: (MonadUnliftIO m)
  => STM.STM WorkerState
  -> NominalDiffTime
  -> STM.TVar (Maybe Notification)
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
  | NotificationRecv Notification
  | TimerExpired
