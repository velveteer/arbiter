-- | Polling helpers for tests.
--
-- Replace fixed @threadDelay@ waits with polling that completes as soon as
-- the condition is met, falling back to a timeout.
module Arbiter.Test.Poll
  ( waitUntil
  , withLinkedAsync
  ) where

import Control.Concurrent (threadDelay)
import Control.Monad (unless)
import GHC.Stack (HasCallStack)
import Test.Hspec (expectationFailure)
import UnliftIO (MonadUnliftIO)
import UnliftIO.Async (Async, link, withAsync)

-- | Poll every 100 ms until the predicate returns 'True'.
-- Fails with 'expectationFailure' after @timeoutMs@ milliseconds.
waitUntil :: (HasCallStack) => Int -> IO Bool -> IO ()
waitUntil timeoutMs check = go (max 1 (timeoutMs `div` 100))
  where
    go :: (HasCallStack) => Int -> IO ()
    go 0 = expectationFailure "waitUntil: timed out waiting for condition"
    go n = do
      ok <- check
      unless ok $ do
        threadDelay 100_000
        go (n - 1)

-- | Run 'withAsync' with a linked handle. Raise a child-thread failure in the
-- caller and stop the associated 'waitUntil'.
withLinkedAsync :: (MonadUnliftIO m) => m a -> (Async a -> m b) -> m b
withLinkedAsync run body = withAsync run $ \handle -> link handle >> body handle
