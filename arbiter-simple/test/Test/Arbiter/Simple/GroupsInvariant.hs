{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Simple.GroupsInvariant (spec) where

import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.GroupsInvariant (groupsInvariantSpec)
import Arbiter.Test.Setup (setupOnce)
import Data.ByteString (ByteString)
import Data.Maybe (fromJust)
import Data.Pool (Pool, withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec

import Arbiter.Simple.MonadArbiter (SimpleConnectionPool (..))
import Arbiter.Simple.SimpleDb (SimpleEnv (..), createSimpleEnvWithPool, runSimpleDb)
import Test.Arbiter.Simple.TestHelpers (cleanupSimpleTest, createSimplePool)

testSchema :: Text
testSchema = "arbiter_simple_groups_test"

type SimpleGITestRegistry = '[ '("arbiter_simple_groups_test", TestPayload)]

testTable :: Text
testTable = "arbiter_simple_groups_test"

withCleanup
  :: Pool PG.Connection -> (SimpleEnv SimpleGITestRegistry -> IO a) -> IO a
withCleanup sharedPool action = do
  let env = createSimpleEnvWithPool (Proxy @SimpleGITestRegistry) sharedPool testSchema
  cleanupSimpleTest env testSchema testTable
  action env

withConn :: SimpleEnv SimpleGITestRegistry -> (PG.Connection -> IO ()) -> IO ()
withConn env f =
  let connPool = fromJust (connectionPool (simplePool env))
   in withResource connPool f

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  sharedPool <- runIO (createSimplePool 5 connStr)
  around (withCleanup sharedPool) $ do
    groupsInvariantSpec @TestPayload
      testSchema
      testTable
      TestMessage
      runSimpleDb
      withConn
