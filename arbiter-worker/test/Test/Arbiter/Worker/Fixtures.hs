{-# LANGUAGE DeriveAnyClass #-}

module Test.Arbiter.Worker.Fixtures
  ( WorkerTestPayload (..)
  , mkTime
  ) where

import Arbiter.Core.Job.Kind (HasKind)
import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import Data.Time (UTCTime (..), fromGregorian, secondsToDiffTime)
import GHC.Generics (Generic)

-- | A payload for the worker suites.
newtype WorkerTestPayload = SimpleTask Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

instance HasKind WorkerTestPayload

-- | A UTCTime from calendar and clock components.
mkTime :: Integer -> Int -> Int -> Int -> Int -> Int -> UTCTime
mkTime year month day hour minute second =
  UTCTime (fromGregorian year month day) (secondsToDiffTime (fromIntegral (hour * 3600 + minute * 60 + second)))
