{-# LANGUAGE DuplicateRecordFields #-}

-- | Servant REST API for Arbiter job queue administration.
--
-- Includes endpoints for:
--
-- * Job management (list, get, cancel, promote)
-- * Dead letter queue operations (list, retry, delete)
-- * Queue statistics and monitoring
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
  ( -- * Server
    arbiterServer
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

    -- * Request Types
  , ApiJobWrite (..)

    -- * Response Types
  , QueuesResponse (..)
  , JobResponse (..)
  , JobsResponse (..)
  , DLQResponse (..)
  , StatsResponse (..)
  , CronSchedulesResponse (..)
  , CronScheduleRow (..)
  , CronScheduleUpdate (..)
  ) where

import Arbiter.Servant.API
import Arbiter.Servant.Server
import Arbiter.Servant.Types
