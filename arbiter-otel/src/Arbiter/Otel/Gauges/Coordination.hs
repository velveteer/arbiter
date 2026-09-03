{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | Cross-process coordination for database gauge scans.
module Arbiter.Otel.Gauges.Coordination
  ( RefreshedSnapshot (..)
  , RefreshResult (..)
  , RefreshSource (..)
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

-- | Origin and value of a shared snapshot.
data RefreshedSnapshot
  = Scanned Snapshot
  | ReadShared Double Snapshot

-- | Result of one coordinated refresh.
data RefreshResult
  = RefreshReady RefreshedSnapshot
  | RefreshSkipped
  | RefreshFailed SomeException

-- | A prepared refresh operation and its timing values.
data RefreshSource = RefreshSource
  { runRefresh :: IO RefreshResult
  , refreshInterval :: NominalDiffTime
  , minimumDelay :: NominalDiffTime
  }

-- | Prepare a coordinated refresh operation for one queue set.
prepareRefreshSource
  :: (MonadArbiter m)
  => LogConfig
  -> (forall a. m a -> IO a)
  -> SchemaName
  -> [(TableName, [Text])]
  -> NominalDiffTime
  -> IO Bool
  -- ^ Whether the local cache is stale.
  -> IO RefreshSource
prepareRefreshSource logCfg runDb schema queueKinds refreshInterval cacheIsStale = do
  gateCache <- newIORef Nothing
  pure
    RefreshSource
      { runRefresh = refresh gateCache
      , refreshInterval = refreshInterval
      , minimumDelay = gateInterval
      }
  where
    gateInterval = 0.9 * refreshInterval
    freshnessWindow = 3 * refreshInterval
    queueTables = map fst queueKinds
    scan = scanSnapshot freshnessWindow schema queueKinds

    refresh gateCache =
      tryAny (resolveGate gateCache) >>= \case
        Left err -> pure (RefreshFailed err)
        Right gate -> bounded (runDb (runGatedShared schema gate gateInterval freshnessWindow scan)) >>= classify

    resolveGate gateCache = do
      cached <- readIORef gateCache
      case cached of
        Just gate -> pure gate
        Nothing -> do
          gate <- runDb (gateNameFor "refresh-gauges" queueTables)
          writeIORef gateCache (Just gate)
          pure gate

    bounded action = do
      stale <- cacheIsStale
      result <- tryAny (if stale then Just <$> action else timeout (micros freshnessWindow) action)
      case result of
        Left err -> pure (Left err)
        Right Nothing -> do
          tryLog logCfg Warning "Gauge scan exceeded the freshness window"
          pure (Right Nothing)
        Right (Just shared) -> pure (Right shared)

    classify = \case
      Left err -> pure (RefreshFailed err)
      Right Nothing -> pure RefreshSkipped
      Right (Just (Ran snapshot)) -> pure (RefreshReady (Scanned snapshot))
      Right (Just (Published age snapshot)) -> pure (RefreshReady (ReadShared age snapshot))
      Right (Just (Unreadable why)) -> localScan why

    -- Only an unreadable payload falls back, and only with nothing fresh of its own.
    localScan why = do
      tryLog logCfg Warning ("Shared gauge payload unreadable, scanning locally: " <> why)
      stale <- cacheIsStale
      if stale
        then either RefreshFailed (RefreshReady . Scanned) <$> tryAny (runDb scan)
        else pure RefreshSkipped
