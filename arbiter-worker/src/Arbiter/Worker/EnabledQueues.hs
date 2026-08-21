{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Resolving which queues a worker process should run from
-- @ARBITER_ENABLED_QUEUES@.
module Arbiter.Worker.EnabledQueues
  ( enabledQueuesEnvVar
  , getEnabledQueues
  , enabledQueuesForMonad
  , requestedQueues
  , requestedQueuesForMonad
  ) where

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.MonadArbiter (RegistryOf)
import Arbiter.Core.QueueRegistry (RegistryTables (..))
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import System.Environment (lookupEnv)

-- | The environment variable naming the queues a worker process should run.
enabledQueuesEnvVar :: String
enabledQueuesEnvVar = "ARBITER_ENABLED_QUEUES"

-- | The comma-separated queue names an environment variable asks for, every one of them
-- checked against the registry. Unset or blank gives the registry's whole queue set, and
-- a name outside it throws.
getEnabledQueues
  :: (RegistryTables registry)
  => String
  -- ^ Environment variable name
  -> Proxy registry
  -- ^ Registry proxy
  -> IO [Text]
getEnabledQueues envVar registry =
  fromMaybe (registryTableNames registry) <$> requestedQueues envVar registry

-- | The queue names an environment variable asks for, or 'Nothing' when it is
-- unset or blank. Names are validated against the registry.
requestedQueues
  :: (RegistryTables registry)
  => String
  -- ^ Environment variable name
  -> Proxy registry
  -- ^ Registry proxy
  -> IO (Maybe [Text])
requestedQueues envVar registry = do
  mVal <- lookupEnv envVar
  case T.strip . T.pack <$> mVal of
    Just tval | not (T.null tval) -> Just <$> validate tval
    _ -> pure Nothing
  where
    validate tval =
      let allQueues = registryTableNames registry
          requested = filter (not . T.null) . map T.strip $ T.splitOn "," tval
          invalid = filter (`notElem` allQueues) requested
       in case (requested, invalid) of
            ([], _) -> throwInternal $ T.pack envVar <> " is set but names no queues"
            (_, []) -> pure requested
            _ -> throwInternal $ "Unknown queue names: " <> T.intercalate ", " invalid

-- | 'getEnabledQueues' for @ARBITER_ENABLED_QUEUES@, resolving the registry from
-- the monad through 'RegistryOf' instead of a passed 'Proxy'.
enabledQueuesForMonad
  :: forall m
   . (RegistryTables (RegistryOf m))
  => IO [Text]
enabledQueuesForMonad = getEnabledQueues enabledQueuesEnvVar (Proxy @(RegistryOf m))

-- | 'requestedQueues' for @ARBITER_ENABLED_QUEUES@, resolving the registry from
-- the monad through 'RegistryOf' instead of a passed 'Proxy'.
requestedQueuesForMonad
  :: forall m
   . (RegistryTables (RegistryOf m))
  => IO (Maybe [Text])
requestedQueuesForMonad = requestedQueues enabledQueuesEnvVar (Proxy @(RegistryOf m))
