module Test.Arbiter.Simple.TestHelpers
  ( createSimplePool
  , cleanupSimpleTest
  ) where

import Arbiter.Test.Setup (cleanupData)
import Data.ByteString (ByteString)
import Data.Maybe (fromJust)
import Data.Pool (Pool, defaultPoolConfig, newPool, setNumStripes, withResource)
import Data.Text (Text)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import Database.PostgreSQL.Simple qualified as PG

import Arbiter.Simple.MonadArbiter (SimpleConnectionPool (..))
import Arbiter.Simple.SimpleDb (SimpleEnv (..))

createSimplePool :: Int -> ByteString -> IO (Pool PG.Connection)
createSimplePool numConnections connStr =
  newPool $
    setNumStripes (Just 1) $
      defaultPoolConfig
        (connectPostgreSQL connStr)
        close
        60
        numConnections

cleanupSimpleTest :: SimpleEnv registry -> Text -> Text -> IO ()
cleanupSimpleTest env schema tableName =
  let connPool = fromJust (connectionPool (simplePool env))
   in withResource connPool $ \conn -> cleanupData schema tableName conn
