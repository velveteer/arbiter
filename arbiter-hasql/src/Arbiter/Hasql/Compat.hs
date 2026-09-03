{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Every hasql version difference that arbiter-hasql depends on.
module Arbiter.Hasql.Compat
  ( runSQL
  , connectionInTransaction
  , withHasqlLibPQConnection
  , hasqlSettings
  , HasqlSettings
  ) where

import Arbiter.Core.Exceptions (throwInternal)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Text.Encoding.Error qualified as TE
import Database.PostgreSQL.LibPQ qualified as LibPQ
import Hasql.Connection qualified as Hasql
import Hasql.Session qualified as Session
import UnliftIO (MonadUnliftIO)

#if MIN_VERSION_hasql(1,10,0)
import Hasql.Connection.Settings qualified as Settings
#else
import Hasql.Connection.Setting qualified as Setting
import Hasql.Connection.Setting.Connection qualified as ConnSetting
#endif

-- | Run a bare SQL command, such as @BEGIN@ or @COMMIT@.
runSQL :: (MonadUnliftIO m) => Hasql.Connection -> ByteString -> m ()
runSQL conn sql =
  liftIO (Hasql.use conn (runScript (TE.decodeUtf8With TE.lenientDecode sql)))
    >>= either (\err -> throwInternal $ "hasql runSQL error: " <> T.pack (show err)) pure

#if MIN_VERSION_hasql(1,10,0)
runScript :: T.Text -> Session.Session ()
runScript = Session.script
#else
runScript :: T.Text -> Session.Session ()
runScript = Session.sql
#endif

-- | Whether the connection is in a transaction block, valid or aborted.
connectionInTransaction :: Hasql.Connection -> IO Bool
#if MIN_VERSION_hasql(1,10,0)
connectionInTransaction conn = do
  result <- Hasql.use conn $ Session.onLibpqConnection $ \libpq -> do
    status <- LibPQ.transactionStatus libpq
    pure (Right (txStatusNeedsRollback status), libpq)
  case result of
    Right inTx -> pure inTx
    Left _ -> pure False
#else
connectionInTransaction conn =
  Hasql.withLibPQConnection conn $ \libpq -> do
    status <- LibPQ.transactionStatus libpq
    pure (txStatusNeedsRollback status)
#endif

-- | Run an action with the underlying libpq connection, for LISTEN/NOTIFY.
withHasqlLibPQConnection :: Hasql.Connection -> (LibPQ.Connection -> IO a) -> IO a
#if MIN_VERSION_hasql(1,10,0)
withHasqlLibPQConnection conn action = do
  result <- Hasql.use conn $ Session.onLibpqConnection $ \libpq -> do
    actionResult <- action libpq
    pure (Right actionResult, libpq)
  either (const (throwInternal "connection lost")) pure result
#else
withHasqlLibPQConnection = Hasql.withLibPQConnection
#endif

-- | @TransInTrans@ and @TransInError@ accept a @ROLLBACK@ without warning.
txStatusNeedsRollback :: LibPQ.TransactionStatus -> Bool
txStatusNeedsRollback LibPQ.TransInTrans = True
txStatusNeedsRollback LibPQ.TransInError = True
txStatusNeedsRollback _ = False

#if MIN_VERSION_hasql(1,10,0)
-- | Connection settings, whose representation follows the hasql version.
type HasqlSettings = Settings.Settings

-- | Convert a connection string ByteString to hasql settings.
hasqlSettings :: ByteString -> HasqlSettings
hasqlSettings = Settings.connectionString . TE.decodeUtf8With TE.lenientDecode
#else
-- | Connection settings, whose representation follows the hasql version.
type HasqlSettings = [Setting.Setting]

-- | Convert a connection string ByteString to hasql settings.
hasqlSettings :: ByteString -> HasqlSettings
hasqlSettings connStr = [Setting.connection (ConnSetting.string (TE.decodeUtf8With TE.lenientDecode connStr))]
#endif
