# arbiter-orville

`OrvilleDb` runs Arbiter over a `MonadOrville` application monad, on its connection and inside its transaction. The Orville primitives it is built from are exported for applications that write their own `MonadArbiter` instance. `Arbiter.Orville.Worker` adapts worker handlers and hooks written in the application monad.

See the [Arbiter guide](https://arbiterq.dev/docs/) for installation, setup, and examples.
