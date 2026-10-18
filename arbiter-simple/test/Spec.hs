module Main (main) where

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Simple.Concurrency qualified as Concurrency
import Test.Arbiter.Simple.ConcurrencyLimit qualified as ConcurrencyLimit
import Test.Arbiter.Simple.Operations qualified as Operations
import Test.Arbiter.Simple.PlainResult qualified as PlainResult
import Test.Arbiter.Simple.PoolSizing qualified as PoolSizing
import Test.Arbiter.Simple.RateLimit qualified as RateLimit
import Test.Arbiter.Simple.StateMachine qualified as StateMachine
import Test.Arbiter.Simple.Worker qualified as Worker

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Arbiter.Simple.Operations" $ Operations.spec connStr
    describe "Arbiter.Simple.Concurrency" $ Concurrency.spec connStr
    describe "Arbiter.Simple.StateMachine" $ StateMachine.spec connStr
    describe "Arbiter.Simple.Worker" $ Worker.spec connStr
    describe "Arbiter.Simple.Listener" $ Worker.listenerSpec connStr
    describe "Arbiter.Simple.MultiQueueListener" $ Worker.multiQueueSpec connStr
    describe "Arbiter.Simple.Deadline" $ Worker.deadlineSpec connStr
    describe "Arbiter.Simple.Cron" $ Worker.cronSpec connStr
    describe "Arbiter.Simple.Reclaim" $ Worker.reclaimSpec connStr
    describe "Arbiter.Simple.ConnectionRecovery" $ Worker.connectionRecoverySpec connStr
    describe "Arbiter.Simple.Lifecycle" $ Worker.lifecycleSpec connStr
    describe "Arbiter.Simple.PoolSizing" $ PoolSizing.spec connStr
    describe "Arbiter.Simple.PlainResult" $ PlainResult.spec connStr
    describe "Arbiter.Simple.RateLimit" $ RateLimit.spec connStr
    describe "Arbiter.Simple.ConcurrencyLimit" $ ConcurrencyLimit.spec connStr
