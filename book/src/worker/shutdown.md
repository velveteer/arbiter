# Graceful Shutdown

Install signal handlers after you construct the worker configs. For one pool or
several, call `shutdownPools` with the same list you pass to `runWorkerPools`:

```haskell
import System.Posix.Signals qualified as Signals

emailConfig <- Worker.transactionalWorkerConfig 3 processEmail
imageConfig <- Worker.transactionalWorkerConfig 2 processImage

let workers = [Worker.namedWorkerPool emailConfig, Worker.namedWorkerPool imageConfig]
    shutdown = Signals.Catch $ Worker.shutdownPools workers
void $ Signals.installHandler Signals.sigTERM shutdown Nothing
void $ Signals.installHandler Signals.sigINT shutdown Nothing

poolCfg <- Worker.poolConfigForWorkers workers
env <- ArbS.createSimpleEnvWithConfig (Proxy @AppRegistry) connStr "arbiter" poolCfg
ArbS.runSimpleDb env $ Worker.runWorkerPools workers
```

The dispatcher stops claiming and in-flight jobs drain. `runWorkerPools`
returns once they finish, or once `gracefulShutdownTimeout` runs out, whichever
comes first. A job still running at the timeout is left alone and redelivered
after its visibility lapses.
