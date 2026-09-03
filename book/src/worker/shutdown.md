# Graceful Shutdown

Install signal handlers after construction of the worker configurations. Pass
the same pool list to `shutdownPools` and `runWorkerPools`:

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

The dispatcher stops new claims and waits for in-flight jobs.
`runWorkerPools` returns when those jobs finish or when
`gracefulShutdownTimeout` expires. Arbiter does not finalize a job that still
runs at the timeout. It redelivers the job after its visibility period expires.
