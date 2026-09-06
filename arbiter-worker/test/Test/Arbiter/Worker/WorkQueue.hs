-- | The pool's batch queue when a push is interrupted.
module Test.Arbiter.Worker.WorkQueue (spec) where

import Control.Monad (replicateM_)
import Data.Maybe (isJust)
import Test.Hspec (Spec, describe, it, shouldBe, shouldSatisfy)
import UnliftIO (timeout)
import UnliftIO.Async (async, cancel)
import UnliftIO.STM (atomically, checkSTM)

import Arbiter.Worker.WorkQueue (newWorkQueue, popWork, pushWork, queuedCount)

-- | Enough items that the signal lands while the push is still writing.
pushSize :: Int
pushSize = 100_000

-- | How long the drain has to take the count the push reported.
drainTimeoutMicros :: Int
drainTimeoutMicros = 5_000_000

spec :: Spec
spec = describe "Work queue" $
  it "writes every batch it counted when a push is interrupted" $ do
    queue <- newWorkQueue
    pusher <- async (pushWork queue [1 .. pushSize :: Int])
    -- Wake as the count rises, so the signal lands inside the writes.
    atomically (queuedCount queue >>= checkSTM . (> 0))
    cancel pusher
    queued <- atomically (queuedCount queue)
    queued `shouldBe` pushSize
    drained <- timeout drainTimeoutMicros (replicateM_ queued (popWork queue))
    drained `shouldSatisfy` isJust
