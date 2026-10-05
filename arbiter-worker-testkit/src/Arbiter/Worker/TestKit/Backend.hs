{-# LANGUAGE RankNTypes #-}

-- | The backend record the worker suites run over.
module Arbiter.Worker.TestKit.Backend (TestBackend (..)) where

import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.MonadArbiter (JobHandler, ResultOf)
import Data.ByteString (ByteString)
import Data.Text (Text)

-- | A backend's payloads, envs, and runner for the generic worker suites.
data TestBackend payload m env = TestBackend
  { schema :: Text
  -- ^ Schema name.
  , table :: Text
  -- ^ The queue table under test.
  , connStr :: ByteString
  -- ^ For raw side connections.
  , mkSimple :: Text -> payload
  -- ^ A payload told apart by its text.
  , mkFailing :: Int -> payload
  -- ^ A payload tagged by an @Int@, for jobs a suite's handler fails.
  , mkEnv :: IO env
  -- ^ The suite's shared env over an emptied queue table.
  , pollOnly :: env -> env
  -- ^ The env with its listener removed.
  , mkFreshEnv :: IO env
  -- ^ An env over a pool of its own and an emptied queue table.
  , destroyEnv :: env -> IO ()
  -- ^ Release a fresh env's pool.
  , mkHandler :: (JobRead payload -> m (ResultOf m payload)) -> JobHandler m payload (ResultOf m payload)
  -- ^ Adapt a plain job action into the backend's 'JobHandler' shape.
  , runCommand :: Text -> m ()
  -- ^ Run one SQL command, such as @COMMIT@, on the monad's current connection.
  , runM :: forall a. env -> m a -> IO a
  -- ^ Run a backend action in 'IO'.
  }
