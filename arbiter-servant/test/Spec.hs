{-# LANGUAGE OverloadedStrings #-}

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Servant.API qualified as API
import Test.Arbiter.Servant.Client qualified as Client

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Arbiter.Servant.API" $
      API.spec connStr
    describe "Arbiter.Servant.Client" Client.spec
