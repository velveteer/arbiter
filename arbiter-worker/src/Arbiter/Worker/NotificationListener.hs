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
-- state is 'Running'.
runNotificationConsumer
  :: (MonadUnliftIO m)
  => STM.STM WorkerState
  -> NominalDiffTime
  -> STM.TVar (Maybe Notification)
  -> Maybe (STM.STM ())
  -> Action m ()
  -> m ()
runNotificationConsumer readState pollDelay notifVar mWakeTrigger action = do
  state <- STM.atomically readState
  when (state == Running) (action Nothing)
  loop
  where
    pollMicros = round (pollDelay * 1_000_000)

    loop = do
      command <- nextCommand
      case command of
        Halt -> pure ()
        PauseCmd -> do
          next <- awaitUnpause
          case next of
            Halt -> pure ()
            _ -> action Nothing *> loop
        NotificationRecv notification -> action (Just notification) *> loop
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
        state <- readState
        case state of
          Paused -> STM.retrySTM
          ShuttingDown -> pure Halt
          Running -> pure TimerExpired

    consumeNotification = do
      mNotif <- STM.readTVar notifVar
      case mNotif of
        Just notification -> do
          STM.writeTVar notifVar Nothing
          pure (NotificationRecv notification)
        Nothing -> STM.retrySTM

    timerExpired delayVar = do
      isExpired <- STM.readTVar delayVar
      if isExpired then pure TimerExpired else STM.retrySTM

    waitWakeTrigger = case mWakeTrigger of
      Nothing -> STM.retrySTM
      Just trigger -> trigger >> pure TimerExpired

    watchStateChange = do
      state <- readState
      case state of
        ShuttingDown -> pure Halt
        Paused -> pure PauseCmd
        Running -> STM.retrySTM

data Command
  = Halt
  | PauseCmd
  | NotificationRecv Notification
  | TimerExpired
