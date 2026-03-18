{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Orville.Concurrency qualified as Concurrency
import Test.Arbiter.Orville.Operations qualified as Operations
import Test.Arbiter.Orville.Worker qualified as Worker

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Arbiter.Orville.Operations" $ Operations.spec connStr
    describe "Arbiter.Orville.Concurrency" $ Concurrency.spec connStr
    describe "Arbiter.Orville.Worker" $ Worker.spec connStr
