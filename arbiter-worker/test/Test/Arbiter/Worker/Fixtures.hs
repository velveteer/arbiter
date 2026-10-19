{-# LANGUAGE DeriveAnyClass #-}

module Test.Arbiter.Worker.Fixtures
  ( TaskPayload (..)
  , mkTime
  ) where

import Arbiter.Core.Job.Kind (HasKind)
import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import Data.Time (TimeOfDay (..), UTCTime (..), fromGregorian, timeOfDayToTime)
import GHC.Generics (Generic)

-- | A one-constructor payload for the worker unit tests.
newtype TaskPayload = SimpleTask Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

instance HasKind TaskPayload

-- | A UTCTime from calendar and clock components.
mkTime :: Integer -> Int -> Int -> Int -> Int -> Int -> UTCTime
mkTime year month day hour minute second =
  UTCTime
    { utctDay = fromGregorian year month day
    , utctDayTime = timeOfDayToTime TimeOfDay {todHour = hour, todMin = minute, todSec = fromIntegral second}
    }
