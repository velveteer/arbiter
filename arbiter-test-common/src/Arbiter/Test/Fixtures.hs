{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeFamilies #-}

module Arbiter.Test.Fixtures
  ( TestPayload (..)
  , WorkerTestPayload (..)
  ) where

import Arbiter.Core.JobResult (HasJobResult (..))
import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import GHC.Generics (Generic)

data TestPayload
  = TestMessage Text
  | TestCalculation Int Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, HasJobResult, ToJSON)

data WorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  | SlowTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- The rollup tests collect @[Text]@ from children, so that is this queue's
-- result type. It is optional so a handler with nothing to store returns
-- 'mempty' and no result row is written.
instance HasJobResult WorkerTestPayload where
  type ResultOf WorkerTestPayload = Maybe [Text]
