{-# LANGUAGE OverloadedStrings #-}

-- | Backend-agnostic LISTEN/NOTIFY hub over one libpq connection per env.
module Arbiter.Core.Listen
  ( Notification (..)
  , Listener
  , RunningHub
  , HubLog (..)
  , withChannels
  , newPoolListener

    -- * Dedicated-connection listener
  , DedicatedListen
  , newDedicatedListen
  , dedicatedListener
  ) where

import Control.Concurrent (threadDelay, threadWaitRead, threadWaitWrite)
import Control.Concurrent.MVar
  ( MVar
  , modifyMVar
  , newEmptyMVar
  , newMVar
  , putMVar
  , readMVar
  )
import Control.Concurrent.STM
  ( STM
  , TVar
  , atomically
  , check
  , modifyTVar'
  , newTVarIO
  , orElse
  , readTVar
  , readTVarIO
  , writeTVar
  )
import Control.Exception (bracket, onException, uninterruptibleMask_)
import Control.Monad (unless, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.Foldable (for_, traverse_)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Database.PostgreSQL.LibPQ qualified as PQ
import GHC.Clock (getMonotonicTime)
import GHC.Conc (threadWaitReadSTM)
import UnliftIO (MonadUnliftIO, withRunInIO)
import UnliftIO.Async qualified as Async
import UnliftIO.Exception (tryAny)

import Arbiter.Core.Exceptions (displayEx, throwInternal)
import Arbiter.Core.SqlLiterals (quoteIdentifier)
import Arbiter.Core.Threads (labelArbiterThread)

-- | A received notification: the channel it arrived on and its payload.
data Notification = Notification
  { notificationChannel :: ByteString
  , notificationData :: ByteString
  }
  deriving stock (Eq, Show)

-- | A registrant's warn and error loggers, carrying its logging context.
data HubLog = HubLog
  { hubWarn :: Text -> IO ()
  , hubError :: Text -> IO ()
  }

-- | Everything the worker needs to run a shared listener for one env.
data Listener = Listener
  { listenerSlot :: MVar (Maybe RunningHub)
  -- ^ Rendezvous, lazily started and refcounted. Shared across an env's pools.
  , listenerWithConn :: (PQ.Connection -> IO ()) -> IO ()
  -- ^ Run the loop with a libpq connection, for the connection's lifetime.
  }

-- | Build a pool-backed 'Listener' with a fresh hub slot from a connection runner.
newPoolListener :: ((PQ.Connection -> IO ()) -> IO ()) -> IO Listener
newPoolListener withConn = do
  slot <- newMVar Nothing
  pure (Listener slot withConn)

-- | Mutable hub state behind the slot. Opaque to callers.
data RunningHub = RunningHub
  { hubHandlers :: TVar (Map ByteString [(Int, HubLog, Notification -> IO ())])
  , hubRefs :: TVar Int
  , hubNextId :: TVar Int
  , hubSubscribed :: TVar (Set ByteString)
  , hubThread :: MVar (Async.Async ())
  }

-- | Register this pool's channels on the env's shared hub for the duration of
-- @k@. The 'STM' action reports 'True' once they are all subscribed.
withChannels
  :: (MonadUnliftIO m)
  => Listener
  -> HubLog
  -> [(ByteString, Notification -> m ())]
  -> (STM Bool -> m a)
  -> m a
withChannels listener logger handlers k =
  withRunInIO $ \runInIO -> do
    let ioHandlers = [(chan, runInIO . h) | (chan, h) <- handlers]
    bracket
      (registerHub listener logger ioHandlers)
      (\(regId, _) -> deregisterHub listener regId)
      (\(_, ready) -> runInIO (k ready))

registerHub
  :: Listener
  -> HubLog
  -> [(ByteString, Notification -> IO ())]
  -> IO (Int, STM Bool)
registerHub listener logger ioHandlers =
  modifyMVar (listenerSlot listener) $ \mhub ->
    -- Keep this section non-blocking.
    uninterruptibleMask_ $ do
      hub <- maybe (startHub listener) pure mhub
      res <- atomically $ do
        regId <- readTVar (hubNextId hub)
        writeTVar (hubNextId hub) $! regId + 1
        modifyTVar' (hubHandlers hub) $ \m ->
          foldl' (\acc (chan, h) -> Map.insertWith (++) chan [(regId, logger, h)] acc) m ioHandlers
        modifyTVar' (hubRefs hub) (+ 1)
        let myChans = map fst ioHandlers
            ready = do
              subd <- readTVar (hubSubscribed hub)
              pure (all (`Set.member` subd) myChans)
        pure (regId, ready)
      pure (Just hub, res)

-- | Drop a registration, tearing the hub down when the last one leaves. The
-- 'Async.cancel' runs after the slot lock is released and waits for the hub to stop.
deregisterHub :: Listener -> Int -> IO ()
deregisterHub listener regId = do
  mthread <- modifyMVar (listenerSlot listener) $ \mhub -> case mhub of
    Nothing -> pure (Nothing, Nothing)
    Just hub -> do
      remaining <- atomically $ do
        modifyTVar' (hubHandlers hub) (dropReg regId)
        modifyTVar' (hubRefs hub) (subtract 1)
        readTVar (hubRefs hub)
      if remaining <= 0
        then do
          thread <- readMVar (hubThread hub)
          pure (Nothing, Just thread)
        else pure (Just hub, Nothing)
  traverse_ Async.cancel mthread
  where
    dropReg rid =
      Map.mapMaybe $ \hs ->
        case filter (\(r, _, _) -> r /= rid) hs of
          [] -> Nothing
          hs' -> Just hs'

startHub :: Listener -> IO RunningHub
startHub listener = do
  handlersV <- newTVarIO Map.empty
  refsV <- newTVarIO 0
  nextV <- newTVarIO 0
  subV <- newTVarIO Set.empty
  threadVar <- newEmptyMVar
  let hub =
        RunningHub
          { hubHandlers = handlersV
          , hubRefs = refsV
          , hubNextId = nextV
          , hubSubscribed = subV
          , hubThread = threadVar
          }
  thread <- Async.asyncWithUnmask $ \unmask -> unmask (labelArbiterThread "listener" Nothing >> hubLoop listener hub)
  putMVar threadVar thread
  pure hub

baseBackoff, maxBackoff :: Int
baseBackoff = 1_000_000
maxBackoff = 30_000_000

-- | Uptime past which a failure restarts backoff at the base.
stableSeconds :: Double
stableSeconds = 30

hubLoop :: Listener -> RunningHub -> IO ()
hubLoop listener hub = go baseBackoff
  where
    go backoff = do
      connectedAt <- newIORef Nothing
      -- Clear before each attempt so a failed reconnect does not leave stale subscriptions.
      atomically $ writeTVar (hubSubscribed hub) Set.empty
      result <-
        tryAny $
          listenerWithConn listener $ \conn -> do
            getMonotonicTime >>= writeIORef connectedAt . Just
            connectionLoop hub conn
      let restart msg = do
            mStart <- readIORef connectedAt
            ended <- getMonotonicTime
            let uptime = maybe 0 (ended -) mStart
            logErrorAll hub msg
            threadDelay backoff
            go (if uptime >= stableSeconds then baseBackoff else min maxBackoff (backoff * 2))
      restart (either displayEx (const "arbiter listener: connection loop exited unexpectedly") result)

-- | Reconcile on the first iteration and whenever the desired channel set changes.
connectionLoop :: RunningHub -> PQ.Connection -> IO ()
connectionLoop hub conn = reconcile hub conn >>= loop
  where
    loop desired = do
      mNotify <- PQ.notifies conn
      case mNotify of
        Just n -> dispatch hub (toNotification n) >> loop desired
        Nothing -> do
          mfd <- PQ.socket conn
          case mfd of
            Nothing -> throwInternal "arbiter listener: connection has no socket"
            Just fd -> do
              changed <-
                bracket (threadWaitReadSTM fd) snd $ \(waitRead, _) ->
                  atomically $
                    (False <$ waitRead)
                      `orElse` (readTVar (hubHandlers hub) >>= \hs -> True <$ check (Map.keysSet hs /= desired))
              ok <- PQ.consumeInput conn
              unless ok $ throwInternal "arbiter listener: consumeInput failed"
              if changed then reconcile hub conn >>= loop else loop desired

-- | Bring the wire's subscriptions in line with the registered channels.
reconcile :: RunningHub -> PQ.Connection -> IO (Set ByteString)
reconcile hub conn = do
  (desired, subd) <-
    atomically $
      (,)
        <$> (Map.keysSet <$> readTVar (hubHandlers hub))
        <*> readTVar (hubSubscribed hub)
  issue "LISTEN" (Set.toList (Set.difference desired subd))
  issue "UNLISTEN" (Set.toList (Set.difference subd desired))
  atomically $ writeTVar (hubSubscribed hub) desired
  pure desired
  where
    issue _ [] = pure ()
    issue verb chans = do
      escaped <- traverse escapeChannel chans
      mres <- PQ.exec conn (BSC.intercalate "; " [verb <> " " <> e | e <- escaped])
      case mres of
        Nothing ->
          throwInternal $
            "arbiter listener: "
              <> T.pack (BSC.unpack verb)
              <> " returned no result"
        Just res -> do
          st <- PQ.resultStatus res
          when (st /= PQ.CommandOk)
            $ throwInternal
            $ "arbiter listener: "
              <> T.pack (BSC.unpack verb)
              <> " failed with "
              <> T.pack (show st)
    escapeChannel chan =
      fromMaybe (quoteChannel chan) <$> PQ.escapeIdentifier conn chan

dispatch :: RunningHub -> Notification -> IO ()
dispatch hub n = do
  hs <- Map.findWithDefault [] (notificationChannel n) <$> readTVarIO (hubHandlers hub)
  for_ hs $ \(_, lg, h) ->
    tryAny (h n) >>= either (hubWarn lg . ("channel handler exception: " <>) . displayEx) pure

-- | Report a connection failure to every registered pool.
logErrorAll :: RunningHub -> Text -> IO ()
logErrorAll hub msg = do
  handlers <- readTVarIO (hubHandlers hub)
  let loggers = Map.fromList [(regId, lg) | hs <- Map.elems handlers, (regId, lg, _) <- hs]
  traverse_ (`hubError` msg) (Map.elems loggers)

toNotification :: PQ.Notify -> Notification
toNotification n =
  Notification
    { notificationChannel = PQ.notifyRelname n
    , notificationData = PQ.notifyExtra n
    }

-- | Quote a channel name as a SQL identifier, doubling embedded quotes.
quoteChannel :: ByteString -> ByteString
quoteChannel = TE.encodeUtf8 . quoteIdentifier . TE.decodeUtf8

-- | A listener over its own libpq connection opened from a connection string, not a pool slot.
data DedicatedListen = DedicatedListen
  { dedicatedSlot :: MVar (Maybe RunningHub)
  , dedicatedConnStr :: ByteString
  }

-- | Allocate a 'DedicatedListen' from a connection string, once at startup.
newDedicatedListen :: (MonadIO m) => ByteString -> m DedicatedListen
newDedicatedListen connStr = liftIO $ do
  slot <- newMVar Nothing
  pure (DedicatedListen slot connStr)

-- | The 'Listener' for a 'DedicatedListen'.
dedicatedListener :: DedicatedListen -> Listener
dedicatedListener d =
  Listener
    { listenerSlot = dedicatedSlot d
    , listenerWithConn = \action ->
        bracket (interruptibleConnectDb (dedicatedConnStr d)) PQ.finish $ \conn -> do
          st <- PQ.status conn
          case st of
            PQ.ConnectionOk -> action conn
            _ -> do
              merr <- PQ.errorMessage conn
              throwInternal $
                "arbiter listener: connect failed"
                  <> foldMap ((": " <>) . T.pack . BSC.unpack) merr
    }

-- | Open a libpq connection asynchronously so a teardown cancel is not lost in the uninterruptible connect FFI.
interruptibleConnectDb :: ByteString -> IO PQ.Connection
interruptibleConnectDb connStr = do
  conn <- PQ.connectStart connStr
  st <- PQ.status conn
  case st of
    PQ.ConnectionBad -> pure conn
    _ -> (poll conn >> pure conn) `onException` PQ.finish conn
  where
    poll conn = do
      status <- PQ.connectPoll conn
      case status of
        PQ.PollingReading -> waitSocket conn threadWaitRead >> poll conn
        PQ.PollingWriting -> waitSocket conn threadWaitWrite >> poll conn
        _ -> pure ()
    waitSocket conn wait =
      PQ.socket conn >>= \case
        Just fd -> wait fd
        Nothing -> throwInternal "arbiter listener: connection has no socket during connect"
