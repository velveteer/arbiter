# Quick Start

## Payload and Result Types

Payload and [result](features/results.md) types need `ToJSON` and `FromJSON`:

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

`Queue` has no result. `QueueWithResult` names the result type.

```haskell
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))

type AppRegistry =
  '[ Queue "email_queue" EmailPayload
   , QueueWithResult "image_queue" ImagePayload Score
   ]
```

A duplicate table name or payload type is a type error.

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

Rerun the migrations after changing `enableNotifications` or
`enableEventStreaming`. Do not edit the migration history.

Replicas can migrate concurrently. `migrationLockTimeout` bounds the wait for
the migration lock, unbounded by default. A transaction-mode pooler cannot
serialize migration sessions.

## Inserting Jobs

```haskell
import Arbiter.Core qualified as Arb
import Arbiter.Simple qualified as ArbS
import Data.Proxy (Proxy (..))

-- A producer needs no worker-pool config
env <- ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"

ArbS.runSimpleDb env $ do
  -- Ungrouped: processed concurrently by any available worker
  _ <- Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")

  -- Grouped: jobs with the same group key are processed one at a time
  _ <- Arb.insertJob (Arb.defaultGroupedJob "user-42" $ SendReceipt "alice@example.com" 1001)
```

`insertJob` returns `Nothing` when a [dedup key](features/deduplication.md)
skips the insert.

## Configuring a Job

Configure `defaultJob` or `defaultGroupedJob` with setters:

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
  -- One pool of five worker threads, each handler in a transaction
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

`transactionalWorkerConfig` commits the handler's writes and the ack together.
An exception rolls back, then retries or moves to the DLQ.

`manualWorkerConfig` runs no transaction and passes callbacks:

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

Configuration details: [Worker Configuration](worker/configuration.md),
[Batched Handlers](worker/batched-handlers.md).
