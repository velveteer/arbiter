-- | Configurable backoff strategies for job retries.
module Arbiter.Worker.BackoffStrategy
  ( BackoffStrategy (..)
  , Jitter (..)
  , ExponentialConfig (..)
  , LinearConfig (..)
  , calculateBackoff
  , applyJitter
  , exponentialBackoff
  , linearBackoff
  , constantBackoff
  ) where

import Data.Int (Int32)
import Data.Time (NominalDiffTime)
import System.Random (randomRIO)

-- | Exponential backoff configuration.
data ExponentialConfig = ExponentialConfig
  { exponentialBase :: Double
  , exponentialCap :: NominalDiffTime
  }
  deriving stock (Eq, Show)

-- | Linear backoff configuration.
data LinearConfig = LinearConfig
  { linearIncrement :: NominalDiffTime
  , linearCap :: NominalDiffTime
  }
  deriving stock (Eq, Show)

-- | Strategy for calculating retry delays based on attempt count.
data BackoffStrategy
  = -- | delay = base^attempts (e.g., 2s, 4s, 8s...)
    Exponential ExponentialConfig
  | -- | delay = increment * attempts (e.g., 30s, 60s, 90s...)
    Linear LinearConfig
  | -- | Same delay for all attempts
    Constant NominalDiffTime
  | -- | User-provided function (attempts -> delay)
    Custom (Int32 -> NominalDiffTime)

-- | Jitter strategy to randomize backoff delays.
--
-- Prevents thundering herd when many jobs fail simultaneously and retry at the same time.
data Jitter
  = -- | Use exact calculated delay
    NoJitter
  | -- | delay = random(0, calculated_delay)
    FullJitter
  | -- | delay = calculated_delay/2 + random(0, calculated_delay/2). Recommended.
    EqualJitter
  deriving stock (Eq, Show)

-- | Calculate backoff delay for given attempt count (1-indexed).
calculateBackoff :: BackoffStrategy -> Int32 -> NominalDiffTime
calculateBackoff strategy attempts = case strategy of
  Exponential (ExponentialConfig base cap) ->
    let delay = min (realToFrac cap) (base ^ attempts)
     in realToFrac (min (realToFrac cap :: Double) delay)
  Linear (LinearConfig increment cap) ->
    let delay = increment * fromIntegral attempts
     in min cap delay
  Constant delay ->
    delay
  Custom f ->
    f attempts

-- | Apply jitter to a calculated delay.
applyJitter :: Jitter -> NominalDiffTime -> IO NominalDiffTime
applyJitter jitter delay = case jitter of
  NoJitter -> pure delay
  FullJitter -> do
    -- random(0, delay) - convert to Double for randomness
    let delayD = realToFrac delay :: Double
    jitteredD <- randomRIO (0, delayD)
    pure (realToFrac jitteredD)
  EqualJitter -> do
    -- delay/2 + random(0, delay/2)
    let half = delay / 2
        halfD = realToFrac half :: Double
    jitterAmountD <- randomRIO (0, halfD)
    pure (half + realToFrac jitterAmountD)

exponentialBackoff :: Double -> NominalDiffTime -> BackoffStrategy
exponentialBackoff base cap = Exponential (ExponentialConfig base cap)

linearBackoff :: NominalDiffTime -> NominalDiffTime -> BackoffStrategy
linearBackoff increment cap = Linear (LinearConfig increment cap)

constantBackoff :: NominalDiffTime -> BackoffStrategy
constantBackoff = Constant
