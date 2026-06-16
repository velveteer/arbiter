{-# LANGUAGE OverloadedStrings #-}

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Simple.Concurrency qualified as Concurrency
import Test.Arbiter.Simple.Operations qualified as Operations
import Test.Arbiter.Simple.StateMachine qualified as StateMachine

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Arbiter.Simple.Operations" $
      Operations.spec connStr

    describe "Arbiter.Simple.Concurrency" $
      Concurrency.spec connStr

    describe "Arbiter.Simple.StateMachine" $
      StateMachine.spec connStr
