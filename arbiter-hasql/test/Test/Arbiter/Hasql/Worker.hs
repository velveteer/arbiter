{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Hasql.Worker (spec) where

import Arbiter.Test.Setup (setupOnce)
import Arbiter.Worker.TestKit (workerSpec)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import GHC.Generics (Generic)
import Test.Hspec

import Arbiter.Hasql.HasqlDb (createHasqlEnvWithPool, runHasqlDb)
import Test.Arbiter.Hasql.TestHelpers (cleanupHasqlTest, createHasqlPool)

workerTestSchemaName :: Text
workerTestSchemaName = "arbiter_hasql_worker_test"

data HasqlWorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type HasqlWorkerTestRegistry = '[ '("arbiter_hasql_worker_test", HasqlWorkerTestPayload)]

testTable :: Text
testTable = "arbiter_hasql_worker_test"

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr workerTestSchemaName testTable False >> createHasqlPool 10 connStr) $
    beforeWith (\pool -> cleanupHasqlTest connStr workerTestSchemaName testTable >> pure pool) $ do
      let runM pool = runHasqlDb (createHasqlEnvWithPool (Proxy @HasqlWorkerTestRegistry) pool workerTestSchemaName)
      workerSpec @HasqlWorkerTestPayload @HasqlWorkerTestRegistry connStr SimpleTask FailingTask (\f _conn job -> f job) runM
