{-# LANGUAGE RankNTypes #-}

-- | One backend's answers to what the generic worker suites need.
module Arbiter.Worker.TestKit.Backend (TestBackend (..)) where

import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.MonadArbiter (JobHandler, ResultOf)
import Data.ByteString (ByteString)
import Data.Text (Text)

-- | A backend's payloads, envs, and runner for the generic worker suites.
data TestBackend payload m env = TestBackend
  { schema :: Text
  -- ^ Schema name, also the LISTEN channel prefix.
  , table :: Text
  -- ^ The queue table under test.
  , connStr :: ByteString
  -- ^ For raw side connections.
  , mkSimple :: Text -> payload
  -- ^ A payload the suites' handlers complete.
  , mkFailing :: Int -> payload
  -- ^ A payload the suites tag as failing. The Int tells tagged payloads apart.
  -- Each suite's handler decides how the job fails.
  , mkEnv :: IO env
  -- ^ The suite's shared env over an emptied queue table.
  , pollOnly :: env -> env
  -- ^ The env with its listener removed.
  , mkFreshEnv :: IO env
  -- ^ An env over a pool of its own and an emptied queue table, for tests that kill connections.
  , destroyEnv :: env -> IO ()
  -- ^ Release a fresh env's pool.
  , mkHandler :: (JobRead payload -> m (ResultOf m payload)) -> JobHandler m payload (ResultOf m payload)
  -- ^ Adapt a plain job action into the backend's 'JobHandler' shape. Some
  -- backends pass a connection, others do not.
  , runCommand :: Text -> m ()
  -- ^ Run one SQL command on the monad's current connection. It can report no row count.
  , runM :: forall a. env -> m a -> IO a
  -- ^ Run a backend action in 'IO'.
  }
