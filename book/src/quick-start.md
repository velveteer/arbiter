# Quick Start

## Payload and Result Types

Define payload types with `ToJSON` and `FromJSON` instances. A queue whose
handlers produce a [result](features/results.md) needs the same instances on
the result type.

```haskell
data EmailPayload
  = SendWelcome Text Text
  | SendReceipt Text Int
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)

data ImagePayload
  = ResizeImage Text Int Int
  | GenerateThumbnail Text
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)

data Score = Score
  { sharpness :: Double
  , sizeBytes :: Int
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)
```

## Type-Level Registry

Map queue table names to payload types at the type level. `Queue` declares a
queue whose handlers store no result. `QueueWithResult` adds the
[result type](features/results.md) its handlers produce.

```haskell
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))

type AppRegistry =
  '[ Queue "email_queue" EmailPayload
   , QueueWithResult "image_queue" ImagePayload Score
   ]
```

The compiler checks the registry. Each payload type maps to one table. A
duplicate table name or payload type is a type error.

## Migrations

```haskell
import Arbiter.Migrations qualified as Mig
import Data.Proxy (Proxy (..))
import System.Exit (die)

main :: IO ()
main = do
  result <- Mig.runMigrationsForRegistry (Proxy @AppRegistry) connStr "arbiter" Mig.defaultMigrationConfig
  case result of
    Mig.MigrationSuccess -> putStrLn "Migrations complete"
    Mig.MigrationError err -> die $ "Migration failed: " <> err
```

If the database user lacks `CREATE` privilege on the schema, create it manually
first:

```sql
CREATE SCHEMA IF NOT EXISTS arbiter;
GRANT USAGE, CREATE ON SCHEMA arbiter TO your_app_user;
```

Migrations reconcile `enableNotifications` and `enableEventStreaming`. Run the
migrations after you change either option. The migration installs or removes
the applicable triggers. Do not edit the migration history.

Multiple replicas can run migrations concurrently. `migrationLockTimeout`
limits the wait for the migration lock. Arbiter returns `MigrationError` if the
wait exceeds this limit. The default has no time limit. Use a direct database
connection for migrations. A pooler in transaction mode cannot serialize
migration sessions.

## Inserting Jobs

```haskell
import Arbiter.Core qualified as Arb
import Arbiter.Simple qualified as ArbS
import Data.Proxy (Proxy (..))

-- A producer needs no worker-pool config, the defaults are enough
env <- ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"

ArbS.runSimpleDb env $ do
  -- Ungrouped: processed concurrently by any available worker
  _ <- Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")

  -- Grouped: jobs with the same group key are processed one at a time
  _ <- Arb.insertJob (Arb.defaultGroupedJob "user-42" $ SendReceipt "alice@example.com" 1001)
```

`insertJob` returns `Maybe (JobRead payload)`. It returns `Nothing` when a
deduplication key causes Arbiter to skip the insert.

## Configuring a Job

Start from `defaultJob`/`defaultGroupedJob` and apply setters:

```haskell
job =
  Arb.defaultJob (SendWelcome "alice@example.com" "Alice")
    & Arb.setPriority 10
    & Arb.setMaxAttempts (Just 3)
    & Arb.setArchiveFor (Just Arb.dayRetention)
```

## Processing Jobs

```haskell
import Arbiter.Core qualified as Arb
import Arbiter.Simple qualified as ArbS
import Arbiter.Worker qualified as Worker
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Proxy (Proxy (..))
import Database.PostgreSQL.Simple qualified as PG

main :: IO ()
main = do
  -- One pool with five worker threads, using the arbiter-simple backend
  config <- Worker.transactionalWorkerConfig 5 processEmail
  let workers = [Worker.namedWorkerPool config]
  poolCfg <- Worker.poolConfigForWorkers workers
  env <- ArbS.createSimpleEnvWithConfig (Proxy @AppRegistry) connStr "arbiter" poolCfg
  ArbS.runSimpleDb env $ Worker.runWorkerPools workers

processEmail :: Arb.JobHandler (ArbS.SimpleDb AppRegistry IO) EmailPayload ()
processEmail conn job = do
  case Arb.payload job of
    SendWelcome recipient name -> do
      result <- liftIO $ sendEmail recipient ("Welcome, " <> name)
      case result of
        Left err -> Arb.throwRetryable err
        Right () -> pure ()

    SendReceipt recipient orderId -> do
      -- Transactional: this INSERT and the job ack commit together
      void $ liftIO $ PG.execute conn
        "INSERT INTO email_log (recipient, order_id) VALUES (?, ?)"
        (recipient, orderId)
```

`transactionalWorkerConfig` wraps each handler in a transaction and acks for
you. If the handler returns, the job and the handler's writes commit together.
If it throws, the transaction rolls back and the job is retried or moved to the
DLQ.

`manualWorkerConfig` does not start a transaction. It supplies callbacks to ack,
fail, cancel, or reprocess the job:

```haskell
config <- Worker.manualWorkerConfig 5 processEmail

processEmail
    :: Arb.JobRead EmailPayload
    -> Worker.BatchCallbacks (ArbS.SimpleDb AppRegistry IO) EmailPayload ()
    -> ArbS.SimpleDb AppRegistry IO ()
processEmail job cbs = do
  liftIO $ deliverEmail (Arb.payload job)
  Worker.ack cbs job
```

[Worker Configuration](worker/configuration.md) compares the two, and
[Batched Handlers](worker/batched-handlers.md) covers taking several jobs per
invocation.
