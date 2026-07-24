{-# LANGUAGE OverloadedStrings #-}

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Worker qualified as Worker
import Test.Arbiter.Worker.MultiQueueListener qualified as MultiQueueListener
import Test.Arbiter.Worker.SharedListener qualified as SharedListener

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Worker Pool Integration Tests" $
      Worker.spec connStr
    describe "Shared Listener" $
      SharedListener.spec connStr
    describe "Multi-Queue Shared Listener" $
      MultiQueueListener.spec connStr
