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
{-# LANGUAGE TypeApplications #-}

import Arbiter.Servant
import Data.Proxy (Proxy(..))

-- Define your job types
data EmailJob = SendEmail { to :: Text, subject :: Text, body :: Text }
  deriving (Generic, ToJSON, FromJSON)

type MyRegistry = '[ '("email_jobs", EmailJob) ]

main :: IO ()
main = do
  config <- initArbiterServer (Proxy @MyRegistry) connStr "public"
  runArbiterAPI 8080 config
```
