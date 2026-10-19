{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP, except for the names
-- that "Arbiter.Hasql.HasqlDb" re-exports.
--
-- Every hasql version difference that arbiter-hasql depends on.
module Arbiter.Hasql.Compat
  ( runSQL
  , connectionInTransaction
  , withHasqlListenConn
  , hasqlSettings
  , HasqlSettings
  , HasqlConnect
  , toHasqlConnect
  , acquireConnect
  , withDedicatedListenConn
  ) where

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.Listen (ListenConn)
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.Either (fromRight)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Text.Encoding.Error qualified as TE
import Hasql.Connection qualified as Hasql
import Hasql.Connection.Settings qualified as Settings
import Hasql.Session qualified as Session

#if MIN_VERSION_hasql(2,0,0)
import Arbiter.Core.Listen (ListenConn (..), Notification (..))
import Arbiter.Core.Listen.Driver
  ( ConnStatus (..)
  , ConnectDriver (..)
  , Polling (..)
  , execOutcome
  , withDriverListenConn
  )
import Control.Monad ((>=>))
import Data.Either (isRight)
import Pqi qualified as PQ
#else
import Arbiter.LibPQ (libPQListenConn, withLibPQListenConn)
import Database.PostgreSQL.LibPQ qualified as PQ
#endif

-- | Run a bare SQL command, such as @BEGIN@ or @COMMIT@.
runSQL :: Hasql.Connection -> ByteString -> IO ()
runSQL conn sql =
  Hasql.use conn (Session.script (lenientText sql))
    >>= either (\err -> throwInternal $ "hasql runSQL error: " <> T.pack (show err)) pure

-- | Decode UTF-8, replacing invalid bytes.
lenientText :: ByteString -> T.Text
lenientText = TE.decodeUtf8With TE.lenientDecode

#if MIN_VERSION_hasql(2,0,0)
-- | Connection settings for hasql 2: transport adapter (for example @Pqi.Ffi.adapter@) and connection string.
data HasqlConnect = HasqlConnect PQ.Adapter ByteString

-- | Connection settings from a pqi adapter and a connection string. With the @hasql2@
-- flag off, the hasql 1.x shape takes only the connection string.
toHasqlConnect :: PQ.Adapter -> ByteString -> HasqlConnect
toHasqlConnect = HasqlConnect

-- | Open a connection. A failure comes back as its description.
acquireConnect :: HasqlConnect -> IO (Either String Hasql.Connection)
acquireConnect (HasqlConnect adapter connStr) = first show <$> Hasql.acquire adapter (hasqlSettings connStr)

-- | Run the listener loop on a driver connection of its own.
withDedicatedListenConn :: HasqlConnect -> (ListenConn -> IO a) -> IO a
withDedicatedListenConn (HasqlConnect adapter connStr) = withDriverListenConn (pqiConnectDriver adapter) toListenConn connStr
#else
-- | Connection settings for hasql 1.x: connection string.
newtype HasqlConnect = HasqlConnect ByteString

-- | Connection settings from a connection string. With the @hasql2@ flag on, the
-- hasql 2 shape also takes a pqi adapter first.
toHasqlConnect :: ByteString -> HasqlConnect
toHasqlConnect = HasqlConnect

-- | Open a connection. A failure comes back as its description.
acquireConnect :: HasqlConnect -> IO (Either String Hasql.Connection)
acquireConnect (HasqlConnect connStr) = first show <$> Hasql.acquire (hasqlSettings connStr)

-- | Run the listener loop on a driver connection of its own.
withDedicatedListenConn :: HasqlConnect -> (ListenConn -> IO a) -> IO a
withDedicatedListenConn (HasqlConnect connStr) = withLibPQListenConn connStr
#endif

-- | Whether the connection is in a transaction block, valid or aborted.
connectionInTransaction :: Hasql.Connection -> IO Bool
connectionInTransaction conn =
  fromRight False
    <$> Hasql.use
      conn
      ( Session.onLibpqConnection $ \libpq -> do
          status <- PQ.transactionStatus libpq
          pure (Right (txStatusNeedsRollback status), libpq)
      )

-- | Run the listener loop on the connection's driver handle. The loop runs outside the
-- session.
withHasqlListenConn :: Hasql.Connection -> (ListenConn -> IO a) -> IO a
withHasqlListenConn conn action = do
  result <- Hasql.use conn $ Session.onLibpqConnection $ \libpq -> pure (Right libpq, libpq)
  either (const (throwInternal "connection lost")) (action . toListenConn) result

#if MIN_VERSION_hasql(2,0,0)
-- | A 'ListenConn' over a pqi connection. The native transport reads the socket only
-- inside a query, so an empty query drains it.
toListenConn :: PQ.Connection -> ListenConn
toListenConn conn =
  ListenConn
    { listenNotifies = fmap (Notification <$> PQ.notifyRelname <*> PQ.notifyExtra) <$> PQ.notifies conn
    , listenSocket = PQ.socket conn
    , listenConsumeInput = PQ.exec conn "" >>= fmap isRight . execOutcome PQ.EmptyQuery PQ.resultStatus
    , listenExec = PQ.exec conn >=> execOutcome PQ.CommandOk PQ.resultStatus
    , listenEscapeIdentifier = PQ.escapeIdentifier conn
    }

pqiConnectDriver :: PQ.Adapter -> ConnectDriver PQ.Connection
pqiConnectDriver adapter =
  ConnectDriver
    { connectStart = PQ.connectStart adapter
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
#else
toListenConn :: PQ.Connection -> ListenConn
toListenConn = libPQListenConn
#endif

-- | @TransInTrans@ and @TransInError@ accept a @ROLLBACK@ without warning.
txStatusNeedsRollback :: PQ.TransactionStatus -> Bool
txStatusNeedsRollback PQ.TransInTrans = True
txStatusNeedsRollback PQ.TransInError = True
txStatusNeedsRollback _ = False

-- | hasql connection settings.
type HasqlSettings = Settings.Settings

-- | Convert a connection string ByteString to hasql settings.
hasqlSettings :: ByteString -> HasqlSettings
hasqlSettings = Settings.connectionString . lenientText
