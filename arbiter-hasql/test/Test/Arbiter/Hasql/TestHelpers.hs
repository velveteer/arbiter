{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Hasql.TestHelpers
  ( createHasqlPool
  , createHasqlPoolWith
  , runHasqlCommand
  , testConnect
  , nativeConnect
  , refusingCancelConnect
  ) where

import Arbiter.Core.Backend (withConn)
import Arbiter.Test.Setup (createPoolWith)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Pool (Pool)
import Data.Text (Text)
import Data.Text.Encoding qualified as TE
import Hasql.Connection qualified as Hasql

import Arbiter.Hasql.Compat (acquireConnect, runSQL)
import Arbiter.Hasql.HasqlDb (HasqlConnect, HasqlDb, toHasqlConnect)

#if MIN_VERSION_hasql(2,0,0)
import Data.ByteString.Char8 qualified as BS8
import Pqi qualified as PQ
import Pqi.Ffi qualified as Ffi
import Pqi.Native qualified as Native
#endif

-- | The test connect. On hasql 2 it uses the libpq adapter.
testConnect :: ByteString -> HasqlConnect
#if MIN_VERSION_hasql(2,0,0)
testConnect = toHasqlConnect Ffi.adapter
#else
testConnect = toHasqlConnect
#endif

-- | The pure Haskell transport, which rejects @connect_timeout@. Only hasql 2 has one.
nativeConnect :: Maybe (ByteString -> HasqlConnect)
#if MIN_VERSION_hasql(2,0,0)
nativeConnect = Just (toHasqlConnect Native.adapter . withoutConnectTimeout)
  where
    withoutConnectTimeout = BS8.unwords . filter (not . BS8.isPrefixOf "connect_timeout=") . BS8.words
#else
nativeConnect = Nothing
#endif

-- | The libpq adapter whose connections refuse to cancel, so a session interrupted mid-flight
-- fails its cleanup. Only hasql 2 exposes the adapter.
refusingCancelConnect :: Maybe (ByteString -> HasqlConnect)
#if MIN_VERSION_hasql(2,0,0)
refusingCancelConnect = Just (toHasqlConnect refusingCancelAdapter)
  where
    refusingCancelAdapter =
      Ffi.adapter
        { PQ.connectdb = fmap refuseCancel . PQ.connectdb Ffi.adapter
        , PQ.connectStart = fmap refuseCancel . PQ.connectStart Ffi.adapter
        }
    refuseCancel conn = conn {PQ.getCancel = pure (Just PQ.Cancel {PQ.cancel = pure (Left "cancel refused")})}
#else
refusingCancelConnect = Nothing
#endif

createHasqlPool :: Int -> ByteString -> IO (Pool Hasql.Connection)
createHasqlPool = createHasqlPoolWith testConnect

-- | A pool over a connect of the caller's choice.
createHasqlPoolWith :: (ByteString -> HasqlConnect) -> Int -> ByteString -> IO (Pool Hasql.Connection)
createHasqlPoolWith connect numConnections connStr =
  createPoolWith numConnections acquire Hasql.release
  where
    acquire = acquireConnect (connect connStr) >>= either (fail . ("hasql test: connection failed: " <>)) pure

-- | Run a command as a bare script on the monad's current connection.
runHasqlCommand :: Text -> HasqlDb registry IO ()
runHasqlCommand sql = withConn $ \conn -> liftIO (runSQL conn (TE.encodeUtf8 sql))
