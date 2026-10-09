{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

-- | Shared payload types for the arbiter test suites.
module Arbiter.Test.Fixtures
  ( TestPayload (..)
  , WorkerTestPayload (..)
  ) where

import Arbiter.Core.Job.Kind (HasKind)
import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import GHC.Generics (Generic)

-- | A payload for the core operation suites.
data TestPayload
  = TestMessage Text
  | TestCalculation Int Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

instance HasKind TestPayload

-- | A payload for the worker pool suites, with a failing and a slow variant.
data WorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  | SlowTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

instance HasKind WorkerTestPayload
