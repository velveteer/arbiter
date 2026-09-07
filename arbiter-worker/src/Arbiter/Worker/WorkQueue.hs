-- | The pool's batch queue, with the counts the free-worker reading and the drain
-- need, and the signal a freed slot sends the dispatcher.
module Arbiter.Worker.WorkQueue
  ( WorkQueue
  , newWorkQueue
  , pushWork
  , popWork
  , finishWork
  , awaitFinished
  , queuedCount
  , busyCount
  , inFlight
  ) where

import Control.Monad.IO.Class (MonadIO)
import Data.Foldable (traverse_)
import UnliftIO (MonadUnliftIO, mask_)
import UnliftIO.Chan (Chan, newChan, readChan, writeChan)
import UnliftIO.STM (STM, TVar, atomically, checkSTM, modifyTVar', newTVarIO, readTVar, writeTVar)

-- | The channel, the batches queued on it, the batches a worker holds, and
-- whether a slot freed since the dispatcher last looked.
data WorkQueue a = WorkQueue
  { wqChan :: Chan a
  , wqQueued :: TVar Int
  , wqBusy :: TVar Int
  , wqFinished :: TVar Bool
  }

newWorkQueue :: (MonadIO m) => m (WorkQueue a)
newWorkQueue = WorkQueue <$> newChan <*> newTVarIO 0 <*> newTVarIO 0 <*> newTVarIO False

-- | Enqueue in order. The count rises before the writes, so a reading never overshoots.
pushWork :: (MonadUnliftIO m) => WorkQueue a -> [a] -> m ()
pushWork _ [] = pure ()
pushWork queue items = mask_ $ do
  atomically (modifyTVar' (wqQueued queue) (+ length items))
  traverse_ (writeChan (wqChan queue)) items

-- | Take the next item, moving it from queued to busy in one transaction.
popWork :: (MonadIO m) => WorkQueue a -> m a
popWork queue = do
  item <- readChan (wqChan queue)
  item <$ atomically (modifyTVar' (wqQueued queue) (subtract 1) *> modifyTVar' (wqBusy queue) (+ 1))

-- | Free the busy slot a 'popWork' took and signal the dispatcher.
finishWork :: WorkQueue a -> STM ()
finishWork queue = modifyTVar' (wqBusy queue) (subtract 1) *> writeTVar (wqFinished queue) True

-- | Block until a slot freed, consuming the signal.
awaitFinished :: WorkQueue a -> STM ()
awaitFinished queue = readTVar (wqFinished queue) >>= checkSTM >> writeTVar (wqFinished queue) False

queuedCount :: WorkQueue a -> STM Int
queuedCount = readTVar . wqQueued

busyCount :: WorkQueue a -> STM Int
busyCount = readTVar . wqBusy

-- | Batches queued or held by a worker.
inFlight :: WorkQueue a -> STM Int
inFlight queue = (+) <$> queuedCount queue <*> busyCount queue
