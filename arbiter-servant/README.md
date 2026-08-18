# arbiter-servant

REST API for managing and monitoring Arbiter job queues using Servant.

## Installation

Add to your `package.yaml` or `.cabal` file:

```yaml
dependencies:
  - arbiter-servant
  - arbiter-simple
```

## Quick Start

### Basic API Server

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Arbiter.Servant (Queue, initArbiterServer, runArbiterAPI)
import Data.Aeson (FromJSON, ToJSON)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import GHC.Generics (Generic)

-- Define your job types
data EmailJob = SendEmail {to :: Text, subject :: Text, body :: Text}
  deriving stock (Generic)
  deriving anyclass (FromJSON, ToJSON)

type MyRegistry = '[Queue "email_jobs" EmailJob]

main :: IO ()
main = do
  -- Run Arbiter migrations first. connStr is your libpq connection string and
  -- "public" is the migrated schema. Live SSE updates also need
  -- enableEventStreaming = True.
  config <- initArbiterServer (Proxy @MyRegistry) connStr "public"
  runArbiterAPI 8080 config
```

A queue that stores a handler result uses `QueueWithResult "email_jobs" EmailJob
Report` in place of `Queue`, importing `QueueSpec (..)` for the constructor. See
the [project documentation](https://github.com/velveteer/arbiter#readme) for
the worker side and the full feature set.
