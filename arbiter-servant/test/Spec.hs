{-# LANGUAGE OverloadedStrings #-}

import Arbiter.Test.Config (getTestConnectionString)
import Test.Hspec

import Test.Arbiter.Servant.API qualified as API

main :: IO ()
main = do
  connStr <- getTestConnectionString
  hspec $ do
    describe "Arbiter.Servant.API" $
      API.spec connStr
