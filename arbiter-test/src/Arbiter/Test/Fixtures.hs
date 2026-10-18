{-# LANGUAGE DeriveAnyClass #-}

-- | Shared payload types for the arbiter test suites.
module Arbiter.Test.Fixtures
  ( TestPayload (..)
  , WorkerTestPayload (..)
  , QueueAPayload (..)
  , QueueBPayload (..)
  ) where

import Arbiter.Core.Job.Kind (HasKind)
import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import GHC.Generics (Generic)

-- | A payload for the core operation suites.
newtype TestPayload = TestMessage Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

instance HasKind TestPayload

-- | A payload for the worker pool suites, with a failing variant.
data WorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

instance HasKind WorkerTestPayload

-- | The first queue's payload in the multi-queue listener suites.
newtype QueueAPayload = QueueAPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | The second queue's payload in the multi-queue listener suites.
newtype QueueBPayload = QueueBPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)
