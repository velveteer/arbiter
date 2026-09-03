{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | Cross-process coordination for database gauge scans.
module Arbiter.Otel.Gauges.Coordination
  ( RefreshSource (..)
  , prepareRefreshSource
  ) where

import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.Operations (Shared (..), gateNameFor, micros, runGatedShared)
import Arbiter.Worker (LogConfig, LogLevel (Warning), tryLog)
import Control.Exception (SomeException)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import System.Timeout (timeout)
import UnliftIO.Exception (tryAny)

import Arbiter.Otel.Gauges.Cache (Snapshot)
import Arbiter.Otel.Gauges.Scan (scanSnapshot)

-- | A prepared refresh operation and its timing values. A refresh that says nothing
-- about the database yields @Right Nothing@.
data RefreshSource = RefreshSource
  { runRefresh :: IO (Either SomeException (Maybe (Shared Snapshot)))
  , minimumDelay :: NominalDiffTime
  }

-- | The gate interval over the refresh interval. Under the loop's period, so a
-- replica can win the gate on consecutive ticks.
gateIntervalFactor :: NominalDiffTime
gateIntervalFactor = 0.9

-- | Prepare a coordinated refresh operation for one queue set.
prepareRefreshSource
  :: (MonadArbiter m)
  => LogConfig
  -> (forall a. m a -> IO a)
  -> SchemaName
  -> [(TableName, [Text])]
  -> NominalDiffTime
  -> NominalDiffTime
  -- ^ How old a reading may be before it is stale.
  -> IO Bool
  -- ^ Whether the local cache is stale.
  -> IO RefreshSource
prepareRefreshSource logCfg runDb schema queueKinds refreshInterval freshnessWindow cacheIsStale = do
  gateCache <- newIORef Nothing
  pure
    RefreshSource
      { runRefresh = refresh gateCache
      , minimumDelay = gateInterval
      }
  where
    gateInterval = gateIntervalFactor * refreshInterval
    queueTables = map fst queueKinds
    scan = scanSnapshot freshnessWindow schema queueKinds

    refresh gateCache = tryAny (resolveGate gateCache) >>= either (pure . Left) sharedScan

    sharedScan gate =
      bounded (runDb (runGatedShared schema gate gateInterval freshnessWindow scan)) >>= \case
        Right (Just (Unreadable why)) -> localScan why
        r -> pure r

    -- Fixed for the registration, and a query of its own when it is long.
    resolveGate gateCache =
      readIORef gateCache
        >>= maybe (runDb (gateNameFor "refresh-gauges" queueTables) >>= \gate -> gate <$ writeIORef gateCache (Just gate)) pure

    -- A scan that outlives the freshness window is abandoned, so long as a fresh
    -- reading stands in for it. Only unblocks a driver that yields to the runtime.
    bounded :: IO (Maybe (Shared Snapshot)) -> IO (Either SomeException (Maybe (Shared Snapshot)))
    bounded act = do
      stale <- cacheIsStale
      tryAny (if stale then Just <$> act else timeout (micros freshnessWindow) act)
        >>= traverse (maybe (Nothing <$ tryLog logCfg Warning abandoned) pure)
    abandoned = "Gauge scan outlived the freshness window, abandoned"

    -- Only an unreadable payload falls back, and only with nothing fresh of its own.
    localScan why = do
      tryLog logCfg Warning ("Shared gauge payload unreadable, scanning locally: " <> why)
      stale <- cacheIsStale
      if stale then tryAny (Just . Ran <$> runDb scan) else pure (Right Nothing)
