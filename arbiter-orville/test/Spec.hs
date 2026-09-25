{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Orville.Concurrency qualified as Concurrency
import Test.Arbiter.Orville.ConcurrencyLimit qualified as ConcurrencyLimit
import Test.Arbiter.Orville.Listener qualified as Listener
import Test.Arbiter.Orville.Operations qualified as Operations
import Test.Arbiter.Orville.RateLimit qualified as RateLimit
import Test.Arbiter.Orville.StateMachine qualified as StateMachine
import Test.Arbiter.Orville.Worker qualified as Worker
import Test.Arbiter.Orville.WorkerAdapters qualified as WorkerAdapters

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Arbiter.Orville.Operations" $ Operations.spec connStr
    describe "Arbiter.Orville.Concurrency" $ Concurrency.spec connStr
    describe "Arbiter.Orville.StateMachine" $ StateMachine.spec connStr
    describe "Arbiter.Orville.Worker" $ Worker.spec connStr
    describe "Arbiter.Orville.Listener" $ Listener.listenerSpec connStr
    describe "Arbiter.Orville.MultiQueueListener" $ Listener.multiQueueSpec connStr
    describe "Arbiter.Orville.Deadline" $ Worker.deadlineSpec connStr
    describe "Arbiter.Orville.Cron" $ Worker.cronSpec connStr
    describe "Arbiter.Orville.Reclaim" $ Worker.reclaimSpec connStr
    describe "Arbiter.Orville.ConnectionRecovery" $ Worker.connectionRecoverySpec connStr
    describe "Arbiter.Orville.Lifecycle" $ Worker.lifecycleSpec connStr
    describe "Arbiter.Orville.WorkerAdapters" $ WorkerAdapters.spec connStr
    describe "Arbiter.Orville.RateLimit" $ RateLimit.spec connStr
    describe "Arbiter.Orville.ConcurrencyLimit" $ ConcurrencyLimit.spec connStr
