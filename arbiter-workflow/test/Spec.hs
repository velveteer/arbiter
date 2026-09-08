module Main (main) where

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec (hspec)

import Test.Arbiter.Workflow.Builder qualified as Builder
import Test.Arbiter.Workflow.Checkpoints qualified as Checkpoints
import Test.Arbiter.Workflow.EndToEnd qualified as EndToEnd
import Test.Arbiter.Workflow.Example qualified as Example
import Test.Arbiter.Workflow.LockSim qualified as LockSim
import Test.Arbiter.Workflow.LockTrace qualified as LockTrace
import Test.Arbiter.Workflow.Runs qualified as Runs

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    Builder.spec
    Example.spec
    Checkpoints.spec connStr
    LockSim.spec
    LockTrace.spec connStr
    Runs.spec connStr
    EndToEnd.spec connStr
