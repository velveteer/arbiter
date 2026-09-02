<div align="center">

<h1>
<img src="./arbiter.png" width="120" height="120" alt="" /><br />
Arbiter
</h1>

A PostgreSQL job queue for Haskell applications.

<a href="https://demo.arbiterq.dev/"><img src="https://img.shields.io/badge/Live_Demo-0e7490?style=for-the-badge&amp;logo=rocket&amp;logoColor=white" alt="Live Demo" /></a>
<a href="https://arbiterq.dev/docs/"><img src="https://img.shields.io/badge/Guide-0f766e?style=for-the-badge&amp;logo=readthedocs&amp;logoColor=white" alt="Guide" /></a>
<a href="https://arbiterq.dev/packages.html"><img src="https://img.shields.io/badge/API_Docs-5e5086?style=for-the-badge&amp;logo=haskell&amp;logoColor=white" alt="API Docs" /></a>
<a href="https://github.com/velveteer/arbiter/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/velveteer/arbiter/ci.yml?branch=main&amp;style=for-the-badge&amp;label=CI" alt="CI" /></a>

</div>

## Installation

Install directly from GitHub:

**Cabal** - add to your `cabal.project`:

```
source-repository-package
  type: git
  location: https://github.com/velveteer/arbiter.git
  tag: <commit-sha>
  subdir:
    arbiter-core
    arbiter-worker
    arbiter-simple
    arbiter-migrations
```

**Stack** - add to your `stack.yaml`:

```yaml
extra-deps:
  - git: https://github.com/velveteer/arbiter.git
    commit: <commit-sha>
    subdirs:
      - arbiter-core
      - arbiter-worker
      - arbiter-simple
      - arbiter-migrations
```

Replace `arbiter-simple` with `arbiter-orville` or `arbiter-hasql` depending on your backend.

## Quick Start

**Shared** - the payload and the registry, imported by both sides:

```haskell
import Arbiter.Core.QueueRegistry (Queue)

data EmailPayload = SendWelcome Text Text
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)

-- Each queue table maps to one payload type, checked at compile time.
type AppRegistry = '[ Queue "email_queue" EmailPayload ]
```

**Migrations** - once per deploy, before either process starts:

```haskell
import Arbiter.Migrations qualified as Mig
import Data.Proxy (Proxy (..))

Mig.runMigrationsForRegistry (Proxy @AppRegistry) connStr "arbiter" Mig.defaultMigrationConfig
```

**Producer** - enqueues only, so it needs no worker configuration:

```haskell
import Arbiter.Core qualified as Arb
import Arbiter.Simple qualified as ArbS

env <- ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"

ArbS.runSimpleDb env $
  void $ Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")
```

**Worker** - a separate process, with a connection pool sized for the worker
pools it runs:

```haskell
import Arbiter.Worker qualified as Worker

main :: IO ()
main = do
  -- 1 pool of 5 worker threads, each handler wrapped in a transaction
  config <- Worker.transactionalWorkerConfig 5 processEmail
  let workers = [Worker.namedWorkerPool config]
  poolCfg <- Worker.poolConfigForWorkers workers
  env <- ArbS.createSimpleEnvWithConfig (Proxy @AppRegistry) connStr "arbiter" poolCfg
  ArbS.runSimpleDb env $ Worker.runWorkerPools workers

processEmail :: Arb.JobHandler (ArbS.SimpleDb AppRegistry IO) EmailPayload ()
processEmail _conn job = case Arb.payload job of
  SendWelcome recipient name -> liftIO $ sendEmail recipient ("Welcome, " <> name)
```
