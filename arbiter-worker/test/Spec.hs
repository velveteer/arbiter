{-# LANGUAGE OverloadedStrings #-}

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Worker.Concurrency qualified as Concurrency
import Test.Arbiter.Worker.ConnectionRecovery qualified as ConnRecovery
import Test.Arbiter.Worker.Cron qualified as Cron
import Test.Arbiter.Worker.Deadline qualified as Deadline
import Test.Arbiter.Worker.MultiQueue qualified as MultiQueue
import Test.Arbiter.Worker.PoolSizing qualified as PoolSizing

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Concurrency & Exception Safety" $
      Concurrency.spec connStr
    describe "Connection Recovery" $
      ConnRecovery.spec connStr
    describe "Cron Scheduler" $
      Cron.spec connStr
    describe "Deadlines" $
      Deadline.spec connStr
    describe "Pool Sizing" $
      PoolSizing.spec connStr
    MultiQueue.spec
