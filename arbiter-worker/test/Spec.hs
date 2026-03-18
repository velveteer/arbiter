{-# LANGUAGE OverloadedStrings #-}

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Worker qualified as Worker
import Test.Arbiter.Worker.Concurrency qualified as Concurrency
import Test.Arbiter.Worker.ConnectionRecovery qualified as ConnRecovery
import Test.Arbiter.Worker.Cron qualified as Cron

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Worker Pool Integration Tests" $
      Worker.spec connStr
    describe "Concurrency & Exception Safety" $
      Concurrency.spec connStr
    describe "Connection Recovery" $
      ConnRecovery.spec connStr
    describe "Cron Scheduler" $
      Cron.spec connStr
