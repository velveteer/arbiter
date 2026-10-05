-- | LISTEN\/NOTIFY connections and listeners over libpq.
module Arbiter.LibPQ
  ( libpqListenConn
  , withLibPQListenConn
  , newLibPQListener
  ) where

import Arbiter.Core.Listen (ListenConn (..), Listener, Notification (..), newListener)
import Arbiter.Core.Listen.Driver
  ( ConnStatus (..)
  , ConnectDriver (..)
  , Polling (..)
  , execOutcome
  , withDriverListenConn
  )
import Control.Monad ((>=>))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Database.PostgreSQL.LibPQ qualified as PQ

connectDriver :: ConnectDriver PQ.Connection
connectDriver =
  ConnectDriver
    { connectStart = PQ.connectStart
    , connectPoll = fmap polling . PQ.connectPoll
    , connectStatus = fmap connStatus . PQ.status
    , connectFinish = PQ.finish
    , connectErrorMessage = PQ.errorMessage
    }
  where
    polling PQ.PollingReading = PollReading
    polling PQ.PollingWriting = PollWriting
    polling _ = PollDone
    connStatus PQ.ConnectionOk = ConnOk
    connStatus PQ.ConnectionBad = ConnBad
    connStatus _ = ConnPending

-- | A 'ListenConn' over a libpq connection.
libpqListenConn :: PQ.Connection -> ListenConn
libpqListenConn conn =
  ListenConn
    { listenNotifies = fmap (fmap (Notification <$> PQ.notifyRelname <*> PQ.notifyExtra)) (PQ.notifies conn)
    , listenSocket = PQ.socket conn
    , listenConsumeInput = PQ.consumeInput conn
    , listenExec = PQ.exec conn >=> execOutcome PQ.CommandOk PQ.resultStatus
    , listenEscapeIdentifier = PQ.escapeIdentifier conn
    }

-- | Run an action on a libpq connection of its own, opened from a connection string.
withLibPQListenConn :: ByteString -> (ListenConn -> IO a) -> IO a
withLibPQListenConn = withDriverListenConn connectDriver libpqListenConn

-- | A 'Listener' over its own libpq connection, opened from a connection string.
newLibPQListener :: (MonadIO m) => ByteString -> m Listener
newLibPQListener connStr = liftIO (newListener (withLibPQListenConn connStr))
