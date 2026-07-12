{-# LANGUAGE OverloadedStrings #-}

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Servant.API qualified as API
import Test.Arbiter.Servant.Consumer qualified as Consumer

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Arbiter.Servant.API" $
      API.spec connStr
    Consumer.spec connStr
