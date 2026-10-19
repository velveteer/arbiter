-- | Polling helpers for tests.
module Arbiter.Test.Poll
  ( waitUntil
  , withLinkedAsync
  , bumpRef
  ) where

import Control.Concurrent (threadDelay)
import Control.Monad (unless)
import Data.IORef (IORef, atomicModifyIORef')
import GHC.Stack (HasCallStack)
import Test.Hspec (expectationFailure)
import UnliftIO (MonadUnliftIO)
import UnliftIO.Async (Async, link, withAsync)

-- | Milliseconds between polls of 'waitUntil'.
pollIntervalMs :: Int
pollIntervalMs = 100

-- | Poll every 'pollIntervalMs' until the predicate returns 'True'.
-- Fails with 'expectationFailure' after @timeoutMs@ milliseconds.
waitUntil :: (HasCallStack) => Int -> IO Bool -> IO ()
waitUntil timeoutMs check = go (max 1 (timeoutMs `div` pollIntervalMs))
  where
    go :: (HasCallStack) => Int -> IO ()
    go 0 = expectationFailure "waitUntil: timed out waiting for condition"
    go remaining = do
      satisfied <- check
      unless satisfied $ do
        threadDelay (pollIntervalMs * microsPerMilli)
        go (remaining - 1)

microsPerMilli :: Int
microsPerMilli = 1_000

-- | Run 'withAsync' with a linked handle. Raise a child-thread failure in the
-- caller and stop the associated 'waitUntil'.
withLinkedAsync :: (MonadUnliftIO m) => m a -> (Async a -> m b) -> m b
withLinkedAsync run body = withAsync run $ \handle -> link handle >> body handle

-- | Add one to a counter.
bumpRef :: (Num a) => IORef a -> IO ()
bumpRef ref = atomicModifyIORef' ref (\count -> (count + 1, ()))
