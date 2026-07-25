{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module Arbiter.Test.Fixtures
  ( TestPayload (..)
  , WorkerTestPayload (..)
  ) where

import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import GHC.Generics (Generic)

data TestPayload
  = TestMessage Text
  | TestCalculation Int Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

data WorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  | SlowTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)
