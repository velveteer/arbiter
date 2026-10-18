-- | Pause-aware consumer loop the dispatcher drives off the shared listener.
module Arbiter.Worker.NotificationListener
  ( runNotificationConsumer
  ) where

import Arbiter.Core.Listen (Notification)
import Arbiter.Core.Operations qualified as Ops
import Control.Monad (when)
import Data.Time (NominalDiffTime)
import UnliftIO (MonadUnliftIO)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.WorkerState (WorkerState (..))

-- | Loop until 'ShuttingDown'. Per iteration wait on notification, poll timer,
-- wake trigger, or state change. Fires @action Nothing@ once at startup if the
-- state is 'Running'.
runNotificationConsumer
  :: (MonadUnliftIO m)
  => STM.STM WorkerState
  -> NominalDiffTime
  -> STM.TVar (Maybe Notification)
  -> STM.STM ()
  -> (Maybe Notification -> m ())
  -> m ()
runNotificationConsumer readState pollDelay notifVar wakeTrigger action = do
  state <- STM.atomically readState
  when (state == Running) (action Nothing)
  loop
  where
    pollMicros = Ops.micros pollDelay

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
              `STM.orElse` (TimerExpired <$ wakeTrigger)

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

data Command
  = Halt
  | PauseCmd
  | NotificationRecv Notification
  | TimerExpired
