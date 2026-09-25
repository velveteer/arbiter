{-# LANGUAGE OverloadedStrings #-}

-- | Backend-agnostic LISTEN/NOTIFY hub over one connection per env.
module Arbiter.Core.Listen
  ( Notification (..)
  , ListenConn (..)
  , Listener
  , RunningHub
  , HubLog (..)
  , withChannels
  , newListener
  ) where

import Control.Concurrent (threadDelay)
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
import Control.Exception (bracket, uninterruptibleMask_)
import Control.Monad (unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.Foldable (for_, traverse_)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.List.NonEmpty qualified as NE
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time (NominalDiffTime)
import GHC.Clock (getMonotonicTime)
import GHC.Conc (threadWaitReadSTM)
import System.Posix.Types (Fd)
import UnliftIO (MonadUnliftIO, withRunInIO)
import UnliftIO.Async qualified as Async
import UnliftIO.Exception (tryAny)

import Arbiter.Core.Exceptions (displayEx, throwInternal)
import Arbiter.Core.FailureGate
  ( clearFailure
  , defaultFailureRepeatInterval
  , holdFailure
  , newFailureGate
  )
import Arbiter.Core.SqlLiterals (quoteIdentifier)
import Arbiter.Core.Threads (labelArbiterThread)

-- | A received notification. The channel it arrived on and its payload.
data Notification = Notification
  { notificationChannel :: ByteString
  , notificationData :: ByteString
  }
  deriving stock (Eq, Show)

-- | Hub event reporting preferences for a registrant.
data HubLog = HubLog
  { hubRecovered :: Text -> IO ()
  -- ^ The hub's connection came back.
  , hubWarn :: Text -> IO ()
  -- ^ This registrant's own channel handler raised.
  , hubError :: Text -> IO ()
  -- ^ The hub's connection failed.
  , hubRepeatInterval :: NominalDiffTime
  -- ^ Repeat interval for a persistent connection failure.
  }

-- | Shared listener configuration for one environment.
data Listener = Listener
  { listenerSlot :: MVar (Maybe RunningHub)
  -- ^ Rendezvous, lazily started and refcounted. Shared across an env's pools.
  , listenerWithConn :: (ListenConn -> IO ()) -> IO ()
  -- ^ Run the loop with a connection, for the connection's lifetime.
  }

-- | The connection operations the hub loop needs.
data ListenConn = ListenConn
  { listenNotifies :: IO (Maybe Notification)
  , listenSocket :: IO (Maybe Fd)
  , listenConsumeInput :: IO Bool
  , listenExec :: ByteString -> IO (Either Text ())
  -- ^ Run a command. A failure comes back as its reason.
  , listenEscapeIdentifier :: ByteString -> IO (Maybe ByteString)
  }

-- | A 'Listener' with a fresh hub slot over a connection runner.
newListener :: ((ListenConn -> IO ()) -> IO ()) -> IO Listener
newListener withConn = do
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
withChannels listener logger handlers body =
  withRunInIO $ \runInIO -> do
    let ioHandlers = [(chan, runInIO . handler) | (chan, handler) <- handlers]
    bracket
      (registerHub listener logger ioHandlers)
      (\(regId, _) -> deregisterHub listener regId)
      (\(_, ready) -> runInIO (body ready))

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
        modifyTVar' (hubHandlers hub) $ \handlerMap ->
          foldl' (\acc (chan, handler) -> Map.insertWith (++) chan [(regId, logger, handler)] acc) handlerMap ioHandlers
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
      Map.mapMaybe $ \registrations ->
        case filter (\(candidateId, _, _) -> candidateId /= rid) registrations of
          [] -> Nothing
          kept -> Just kept

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
hubLoop listener hub = newFailureGate >>= go baseBackoff
  where
    go backoff gate = do
      -- Set once the channels are subscribed.
      readyAt <- newIORef Nothing
      -- Clear before each attempt.
      atomically $ writeTVar (hubSubscribed hub) Set.empty
      result <-
        tryAny $
          listenerWithConn listener $ \conn ->
            connectionLoop hub conn $ do
              getMonotonicTime >>= writeIORef readyAt . Just
              recovered <- clearFailure gate
              when recovered . void $ logHub hubRecovered hub "arbiter listener: reconnected"
      let restart msg = do
            mStart <- readIORef readyAt
            ended <- getMonotonicTime
            let uptime = maybe 0 (ended -) mStart
            repeatAfter <- hubRepeatAfter hub
            worth <- holdFailure gate repeatAfter msg
            when worth $ reportFailure gate msg
            threadDelay backoff
            go (if uptime >= stableSeconds then baseBackoff else min maxBackoff (backoff * 2)) gate
      restart $ "arbiter listener: " <> either displayEx (const "connection loop exited unexpectedly") result
    -- Not held when it reached no one.
    reportFailure gate msg =
      logHub hubError hub msg >>= \sent -> unless sent (void (clearFailure gate))

-- | The tightest repeat cadence the registrants ask for.
hubRepeatAfter :: RunningHub -> IO NominalDiffTime
hubRepeatAfter hub =
  maybe defaultFailureRepeatInterval minimum . NE.nonEmpty . map hubRepeatInterval <$> hubLoggers hub

-- | Reconcile on the first iteration and whenever the desired channel set changes.
-- @onReady@ runs once the first reconcile has subscribed.
connectionLoop :: RunningHub -> ListenConn -> IO () -> IO ()
connectionLoop hub conn onReady = do
  desired <- reconcile hub conn
  onReady
  loop desired
  where
    loop desired = do
      mNotify <- listenNotifies conn
      case mNotify of
        Just notification -> dispatch hub notification >> loop desired
        Nothing -> do
          mfd <- listenSocket conn
          case mfd of
            Nothing -> throwInternal "connection has no socket"
            Just socketFd -> do
              changed <-
                bracket (threadWaitReadSTM socketFd) snd $ \(waitRead, _) ->
                  atomically $
                    (False <$ waitRead)
                      `orElse` (readTVar (hubHandlers hub) >>= \handlers -> True <$ check (Map.keysSet handlers /= desired))
              consumed <- listenConsumeInput conn
              unless consumed $ throwInternal "consumeInput failed"
              if changed then reconcile hub conn >>= loop else loop desired

-- | Synchronize wire subscriptions with registered channels.
reconcile :: RunningHub -> ListenConn -> IO (Set ByteString)
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
      listenExec conn (BSC.intercalate "; " [verb <> " " <> ident | ident <- escaped])
        >>= either (\reason -> throwInternal (T.pack (BSC.unpack verb) <> " " <> reason)) pure
    escapeChannel chan =
      fromMaybe (quoteChannel chan) <$> listenEscapeIdentifier conn chan

dispatch :: RunningHub -> Notification -> IO ()
dispatch hub notification = do
  handlers <- Map.findWithDefault [] (notificationChannel notification) <$> readTVarIO (hubHandlers hub)
  for_ handlers $ \(_, logger, handler) ->
    tryAny (handler notification)
      >>= either (void . tryAny . hubWarn logger . ("channel handler exception: " <>) . displayEx) pure

-- | Report a hub event through the first registrant whose logger takes it. False
-- when it reached no one.
logHub :: (HubLog -> Text -> IO ()) -> RunningHub -> Text -> IO Bool
logHub channel hub msg = hubLoggers hub >>= emitFirst
  where
    emitFirst [] = pure False
    emitFirst (logger : rest) =
      tryAny (channel logger msg) >>= either (const (emitFirst rest)) (const (pure True))

-- | One logger per registration.
hubLoggers :: RunningHub -> IO [HubLog]
hubLoggers hub = registrants <$> readTVarIO (hubHandlers hub)
  where
    registrants handlers =
      Map.elems (Map.fromList [(regId, logger) | registrations <- Map.elems handlers, (regId, logger, _) <- registrations])

-- | Quote a channel name as a SQL identifier, doubling embedded quotes.
quoteChannel :: ByteString -> ByteString
quoteChannel = TE.encodeUtf8 . quoteIdentifier . TE.decodeUtf8
