{-# LANGUAGE DuplicateRecordFields #-}

-- | Servant REST API for Arbiter job queue administration. See 'ArbiterAPI'
-- for the route tree.
--
-- = Quick Start
--
-- @
-- import Arbiter.Servant
-- import Data.Proxy (Proxy(..))
--
-- main :: IO ()
-- main = do
--   -- Create server config (creates its own pool and registers event triggers)
--   config <- initArbiterServer (Proxy @MyRegistry) connStr "public"
--
--   -- Start API server on port 8080
--   runArbiterAPI 8080 config
-- @
module Arbiter.Servant
  ( -- * Registry
    QueueSpec (..)
  , Queue

    -- * Server
  , arbiterServer
  , arbiterApp
  , runArbiterAPI
  , ArbiterServerConfig (..)
  , initArbiterServer
  , defaultQueueStatsCacheTtl
  , BuildServer (..)

    -- * API Types
  , ArbiterAPI
  , RegistryToAPI
  , TableAPI (..)
  , SharedAPI
  , EventsAPI
  , QueuesAPI (..)
  , JobsAPI (..)
  , DLQAPI (..)
  , ArchiveAPI (..)
  , StatsAPI (..)
  , CronAPI (..)
  , WorkersAPI (..)
  , RateLimitsAPI (..)
  , ConcurrencyAPI (..)

    -- * Request and response types
  , module Arbiter.Servant.Types
  ) where

import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))

import Arbiter.Servant.API
import Arbiter.Servant.Server
import Arbiter.Servant.Types
