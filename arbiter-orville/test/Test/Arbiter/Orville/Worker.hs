{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Orville.Worker
  ( withOrvilleBackend
  , spec
  , deadlineSpec
  , cronSpec
  , reclaimSpec
  , connectionRecoverySpec
  , lifecycleSpec
  ) where

import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Setup qualified as TestSetup
import Arbiter.Worker.TestKit qualified as TestKit
import Data.ByteString (ByteString)
import Data.Text (Text)
import Test.Hspec (Spec, afterAll_, beforeAll, runIO)

import Test.Arbiter.Orville.TestHelpers
  ( OrvilleTestEnv
  , TestOrville
  , cleanupOrvilleTest
  , createOrvilleTestEnv
  , destroyOrvilleTestEnv
  , disableOrvilleListener
  , orvillePoolSize
  , orvilleTestHandler
  , runOrvilleTest
  )

workerTestSchemaName :: Text
workerTestSchemaName = "arbiter_orville_worker_test"

type OrvilleWorkerTestRegistry =
  '[QueueWithResult "arbiter_orville_worker_test" WorkerTestPayload (Maybe [Text])]

spec :: ByteString -> Spec
spec connStr = withOrvilleBackend @OrvilleWorkerTestRegistry connStr workerTestSchemaName TestKit.workerSpec

-- | Build the schema and one env over a shared pool, then run a suite over the backend.
withOrvilleBackend
  :: forall registry
   . ByteString
  -> Text
  -> (TestKit.TestBackend WorkerTestPayload (TestOrville registry) (OrvilleTestEnv registry) -> Spec)
  -> Spec
withOrvilleBackend connStr schema suite =
  beforeAll (TestSetup.setupOnce connStr schema schema True) $ do
    env <- runIO (createOrvilleTestEnv connStr schema schema orvillePoolSize)
    afterAll_ (destroyOrvilleTestEnv env) $ suite (orvilleBackend connStr schema env)

orvilleBackend
  :: forall registry
   . ByteString
  -> Text
  -> OrvilleTestEnv registry
  -> TestKit.TestBackend WorkerTestPayload (TestOrville registry) (OrvilleTestEnv registry)
orvilleBackend connStr schema env =
  TestKit.TestBackend
    { schema
    , table = schema
    , connStr
    , mkSimple = SimpleTask
    , mkFailing = FailingTask
    , mkEnv = cleanupOrvilleTest env >> pure env
    , pollOnly = disableOrvilleListener
    , mkFreshEnv = TestSetup.cleanupOnce connStr schema schema >> createOrvilleTestEnv connStr schema schema orvillePoolSize
    , destroyEnv = destroyOrvilleTestEnv
    , mkHandler = orvilleTestHandler schema
    , runCommand = TestKit.statementCommand
    , runM = runOrvilleTest
    }

deadlineSchema :: Text
deadlineSchema = "arbiter_orville_deadline_test"

type OrvilleDeadlineRegistry = '[Queue "arbiter_orville_deadline_test" WorkerTestPayload]

deadlineSpec :: ByteString -> Spec
deadlineSpec connStr = withOrvilleBackend @OrvilleDeadlineRegistry connStr deadlineSchema TestKit.deadlineSpec

cronSchema :: Text
cronSchema = "arbiter_orville_cron_test"

type OrvilleCronRegistry = '[Queue "arbiter_orville_cron_test" WorkerTestPayload]

cronSpec :: ByteString -> Spec
cronSpec connStr = withOrvilleBackend @OrvilleCronRegistry connStr cronSchema TestKit.cronSpec

reclaimSchema :: Text
reclaimSchema = "arbiter_orville_reclaim_test"

type OrvilleReclaimRegistry = '[Queue "arbiter_orville_reclaim_test" WorkerTestPayload]

reclaimSpec :: ByteString -> Spec
reclaimSpec connStr = withOrvilleBackend @OrvilleReclaimRegistry connStr reclaimSchema TestKit.reclaimSpec

recoverySchema :: Text
recoverySchema = "arbiter_orville_recovery_test"

type OrvilleRecoveryRegistry = '[Queue "arbiter_orville_recovery_test" WorkerTestPayload]

connectionRecoverySpec :: ByteString -> Spec
connectionRecoverySpec connStr = withOrvilleBackend @OrvilleRecoveryRegistry connStr recoverySchema TestKit.connectionRecoverySpec

lifecycleSchema :: Text
lifecycleSchema = "arbiter_orville_lifecycle_test"

type OrvilleLifecycleRegistry =
  '[QueueWithResult "arbiter_orville_lifecycle_test" WorkerTestPayload (Maybe [Text])]

lifecycleSpec :: ByteString -> Spec
lifecycleSpec connStr = withOrvilleBackend @OrvilleLifecycleRegistry connStr lifecycleSchema TestKit.lifecycleSpec
