{-# LANGUAGE OverloadedStrings #-}

import Test.Hspec

import Test.Arbiter.Worker.BackoffStrategy qualified as BackoffStrategy
import Test.Arbiter.Worker.BatchSim qualified as BatchSim
import Test.Arbiter.Worker.Cron qualified as Cron
import Test.Arbiter.Worker.GuardSim qualified as GuardSim
import Test.Arbiter.Worker.Logging qualified as Logging
import Test.Arbiter.Worker.MultiQueue qualified as MultiQueue
import Test.Arbiter.Worker.WorkQueue qualified as WorkQueue

main :: IO ()
main =
  hspec $ do
    BackoffStrategy.spec
    describe "Cron Scheduler" Cron.spec
    BatchSim.spec
    GuardSim.spec
    Logging.spec
    MultiQueue.spec
    WorkQueue.spec
