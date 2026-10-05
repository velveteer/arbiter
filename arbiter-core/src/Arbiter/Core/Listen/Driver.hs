{-# LANGUAGE OverloadedStrings #-}

-- | A 'ListenConn' and an interruptible connect over any libpq-shaped driver.
module Arbiter.Core.Listen.Driver
  ( ConnectDriver (..)
  , ConnStatus (..)
  , Polling (..)
  , execOutcome
  , withDriverListenConn
  ) where

import Control.Concurrent (threadWaitRead, threadWaitWrite)
import Control.Exception (bracket, onException)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.Foldable (traverse_)
import Data.Text (Text)
import Data.Text qualified as T
import System.Posix.Types (Fd)

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.Listen (ListenConn (..))

-- | Where an asynchronous connect stands.
data ConnStatus
  = -- | The connection is open.
    ConnOk
  | -- | The connect failed.
    ConnBad
  | -- | The connect is still in progress.
    ConnPending
  deriving stock (Eq, Show)

-- | Wait target from a connection poll.
data Polling
  = -- | Wait until the socket is readable.
    PollReading
  | -- | Wait until the socket is writable.
    PollWriting
  | -- | The connect has finished.
    PollDone
  deriving stock (Eq, Show)

-- | The driver calls that open, inspect and close a connection.
data ConnectDriver conn = ConnectDriver
  { connectStart :: ByteString -> IO conn
  -- ^ Start an asynchronous connect from a connection string.
  , connectPoll :: conn -> IO Polling
  -- ^ Advance the connect. Tells what to wait for next.
  , status :: conn -> IO ConnStatus
  -- ^ Where the connection stands.
  , finish :: conn -> IO ()
  -- ^ Close the connection.
  , errorMessage :: conn -> IO (Maybe ByteString)
  -- ^ The driver's last error message, if any.
  }

-- | Check command status against the driver's success status.
execOutcome :: (Eq status, Show status) => status -> (result -> IO status) -> Maybe result -> IO (Either Text ())
execOutcome okStatus resultStatus = maybe (pure (Left "returned no result")) (fmap judge . resultStatus)
  where
    judge st
      | st == okStatus = Right ()
      | otherwise = Left ("failed with " <> T.pack (show st))

-- | Run an action on a connection of its own, opened from a connection string.
withDriverListenConn
  :: ConnectDriver conn
  -> (conn -> ListenConn)
  -> ByteString
  -> (ListenConn -> IO a)
  -> IO a
withDriverListenConn connector toConn connStr action =
  bracket (interruptibleConnect connector (listenSocket . toConn) connStr) (finish connector) $ \conn -> do
    st <- status connector conn
    if st == ConnOk
      then action (toConn conn)
      else do
        merr <- errorMessage connector conn
        throwInternal $ "connect failed" <> foldMap ((": " <>) . T.pack . BSC.unpack) merr

-- | Open a connection asynchronously. A teardown cancel interrupts the connect.
interruptibleConnect :: ConnectDriver conn -> (conn -> IO (Maybe Fd)) -> ByteString -> IO conn
interruptibleConnect connector socketOf connStr = do
  conn <- connectStart connector connStr
  st <- status connector conn
  if st == ConnBad
    then pure conn
    else (poll conn >> pure conn) `onException` finish connector conn
  where
    poll conn = connectPoll connector conn >>= traverse_ (\wait -> waitSocket conn wait >> poll conn) . waitFor
    waitFor PollReading = Just threadWaitRead
    waitFor PollWriting = Just threadWaitWrite
    waitFor PollDone = Nothing
    waitSocket conn wait =
      socketOf conn >>= \case
        Just socketFd -> wait socketFd
        Nothing -> throwInternal "connection has no socket during connect"
