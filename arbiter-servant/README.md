# arbiter-servant

REST API for managing and monitoring Arbiter job queues, built on Servant.

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Arbiter.Servant (Queue, createArbiterServer, runArbiterAPI)
import Arbiter.Simple (createSimpleEnv, runSimpleDb)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import GHC.Generics (Generic)

data EmailPayload = SendEmail {to :: Text, subject :: Text, body :: Text}
  deriving stock (Generic)
  deriving anyclass (FromJSON, ToJSON)

type AppRegistry = '[Queue "email_queue" EmailPayload]

connStr :: ByteString
connStr = "host=localhost dbname=arbiter"

main :: IO ()
main = do
  -- Run the Arbiter migrations first. "arbiter" is the migrated schema.
  -- Live SSE updates also need enableEventStreaming = True.
  env <- createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
  config <- createArbiterServer (runSimpleDb env)
  runArbiterAPI 8080 config
```

`createArbiterServer` takes any backend runner, so a hasql application passes
`runHasqlDb env` and the server shares that env's pool and listener.

A queue with a handler result is `QueueWithResult "email_queue" EmailPayload
Report`. Import `QueueSpec (..)` for the constructor.

See the [Arbiter guide](https://arbiterq.dev/docs/) for installation, setup, and examples.
