{-# LANGUAGE OverloadedStrings #-}

-- | The handler side of the guard: registering a batch and asking it to stop.
--
-- A signal is thrown from a courier thread, so a masked handler cannot stall
-- the guard. Unregister kills the couriers, which revokes a signal still in
-- flight, and drains one that already landed. A second signal within one beat
-- is dropped.
module Arbiter.Worker.Heartbeat.Guard.Signal
  ( guardBatch
  , signal
  ) where

import Control.Concurrent.Class.MonadSTM (MonadSTM, atomically, modifyTVar', newTVarIO, stateTVar)
import Control.Exception (asyncExceptionFromException, asyncExceptionToException)
import Control.Monad (unless, void, when)
import Control.Monad.Class.MonadFork (MonadFork (..), MonadThread (..))
import Control.Monad.Class.MonadThrow (Exception (..), MonadCatch (..), MonadMask (..), MonadThrow (..), SomeException)
import Control.Monad.Class.MonadTime.SI (MonadMonotonicTime (..), MonadTime (..), Time, addTime, diffTime, diffUTCTime)
import Data.Foldable (toList, traverse_)
import Data.List (partition)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, maybeToList)

import Arbiter.Worker.Heartbeat.Guard.State
import Arbiter.Worker.Logger (LogLevel (..))

-- | A guard signal, rethrown as sync at the handler boundary.
newtype GuardSignal = GuardSignal SomeException
  deriving stock (Show)

instance Exception GuardSignal where
  backtraceDesired _ = False
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

-- | Run @action@ on the calling thread with the batch registered with the guard.
guardBatch
  :: (MonadFork n, MonadMask n, MonadMonotonicTime n, MonadSTM n, MonadTime n)
  => HeartbeatGuard n job
  -> Batch n job
  -> n a
  -> n a
{-# SPECIALIZE guardBatch :: HeartbeatGuard IO job -> Batch IO job -> IO a -> IO a #-}
guardBatch guard batch action =
  asSync (bracket register unregister (const action))
  where
    config = guardConfig guard
    register = do
      wallNow <- getCurrentTime
      monoNow <- getMonotonicTime
      handler <- myThreadId
      let elapsed = toDiffTime (diffUTCTime wallNow (batchStart batch))
          -- The rows' own deadlines, which the claim set before the batch started.
          rowLeases =
            [ (job, addTime (toDiffTime (diffUTCTime at wallNow)) monoNow)
            | job <- toList (batchJobs batch)
            , Just at <- [configLease config job]
            ]
          -- A lease already past at registration indicates clock skew.
          (past, current) = partition ((<= monoNow) . snd) rowLeases
          leaseUntil = minimum (addTime (configTimeout config - elapsed) monoNow : map snd current)
          firstBeat = addTime (heartbeatWait (configInterval config) True (leaseUntil `diffTime` monoNow)) monoNow
          deadline = (`addTime` monoNow) <$> configMaxDuration config
      unless (null past) $
        configLog
          config
          Warning
          (map fst past)
          "Lease already expired as the batch registered. The worker and database clocks disagree."
      status <-
        newTVarIO
          Status
            { leaseAt = leaseUntil
            , beatAt = firstBeat
            , leaseLapsed = False
            , deadlineSent = False
            , signalledAt = Nothing
            , couriers = Just []
            }
      atomically $ do
        token <- stateTVar (guardNextToken guard) (\next -> (next, next + 1))
        let entry = Guarded token batch handler deadline status
        modifyTVar' (guardEntries guard) (Map.insert token entry)
        wakeFor guard (minimum (leaseUntil : firstBeat : maybeToList deadline))
        pure entry

    asSync act = act `catch` (\(GuardSignal exc) -> throwIO exc)

    unregister entry = do
      carrying <- uninterruptibleMask_ $ do
        carrying <- atomically $ do
          modifyTVar' (guardEntries guard) (Map.delete (guardedToken entry))
          stateTVar (guardedStatus entry) (\status -> (fromMaybe [] (couriers status), status {couriers = Nothing}))
        carrying <$ traverse_ killThread carrying
      -- A signal a courier had already sent lands here, inside the boundary, and is dropped.
      unless (null carrying) $ interruptible (pure ()) `catch` \GuardSignal {} -> pure ()

-- | Ask the handler to stop, from a courier thread. A second ask within one beat is dropped.
signal :: (MonadFork n, MonadSTM n) => HeartbeatGuard n job -> Time -> Guarded n job -> SomeException -> n ()
signal guard now entry exc = do
  fresh <- atomically $ stateTVar (guardedStatus entry) $ \status ->
    let due = maybe True (\at -> now >= addTime (configInterval (guardConfig guard)) at) (signalledAt status)
     in (due, if due then status {signalledAt = Just now} else status)
  when fresh . void . forkIO $ do
    courier <- myThreadId
    claimed <- atomically $ stateTVar (guardedStatus entry) $ \status -> case couriers status of
      Nothing -> (False, status)
      Just carrying -> (True, status {couriers = Just (courier : carrying)})
    when claimed (throwTo (guardedHandler entry) (GuardSignal exc))
