-- | Repeat suppression for a failure a looping action keeps hitting.
module Arbiter.Core.FailureGate
  ( FailureGate
  , newFailureGate
  , holdFailure
  , clearFailure
  , defaultFailureRepeatInterval
  ) where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import GHC.Clock (getMonotonicTime)

-- | The failure a repeating action reported last and when, absent while healthy.
newtype FailureGate = FailureGate (IORef (Maybe (Text, Double)))

-- | A gate for one repeating action, starting healthy.
newFailureGate :: (MonadIO m) => m FailureGate
newFailureGate = FailureGate <$> liftIO (newIORef Nothing)

-- | Gap between repeats of a standing failure.
defaultFailureRepeatInterval :: NominalDiffTime
defaultFailureRepeatInterval = 60

-- | Take @failure@ as the one the gate holds. True when it is worth reporting.
holdFailure :: (MonadIO m) => FailureGate -> NominalDiffTime -> Text -> m Bool
holdFailure (FailureGate ref) repeatAfter failure = liftIO $ do
  now <- getMonotonicTime
  atomicModifyIORef' ref $ \held ->
    let worth = maybe True (\(prev, at) -> prev /= failure || now - at >= realToFrac repeatAfter) held
     in (if worth then Just (failure, now) else held, worth)

-- | Drop whatever the gate holds. True when it held a failure.
clearFailure :: (MonadIO m) => FailureGate -> m Bool
clearFailure (FailureGate ref) =
  liftIO $ atomicModifyIORef' ref (\held -> (Nothing, isJust held))
