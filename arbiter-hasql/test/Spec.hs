{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Hasql.Concurrency qualified as Concurrency
import Test.Arbiter.Hasql.Operations qualified as Operations
import Test.Arbiter.Hasql.Worker qualified as Worker

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Arbiter.Hasql.Operations" $ Operations.spec connStr
    describe "Arbiter.Hasql.Concurrency" $ Concurrency.spec connStr
    describe "Arbiter.Hasql.Worker" $ Worker.spec connStr
