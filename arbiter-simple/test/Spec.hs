{-# LANGUAGE OverloadedStrings #-}

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Simple.Concurrency qualified as Concurrency
import Test.Arbiter.Simple.GroupsInvariant qualified as GroupsInvariant
import Test.Arbiter.Simple.Operations qualified as Operations

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Arbiter.Simple.Operations" $
      Operations.spec connStr

    describe "Arbiter.Simple.GroupsInvariant" $
      GroupsInvariant.spec connStr

    describe "Arbiter.Simple.Concurrency" $
      Concurrency.spec connStr
