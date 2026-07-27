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
  , BuildServer (..)

    -- * API Types
  , ArbiterAPI
  , RegistryToAPI
  , TableAPI (..)
  , EventsAPI
  , QueuesAPI (..)
  , JobsAPI (..)
  , DLQAPI (..)
  , StatsAPI (..)
  , CronAPI (..)
  , WorkersAPI (..)

    -- * Request Types
  , ApiJobWrite (..)

    -- * Response Types
  , QueuesResponse (..)
  , QueueRow (..)
  , WorkersResponse (..)
  , WorkerRow (..)
  , JobResponse (..)
  , JobsResponse (..)
  , DLQResponse (..)
  , StatsResponse (..)
  , CronSchedulesResponse (..)
  , CronScheduleRow (..)
  , CronScheduleUpdate (..)
  ) where

import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))

import Arbiter.Servant.API
import Arbiter.Servant.Server
import Arbiter.Servant.Types
