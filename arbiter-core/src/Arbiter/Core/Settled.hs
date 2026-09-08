-- | The pool-level hook that runs in the transaction that took a job out of its queue.
module Arbiter.Core.Settled
  ( SettledJob (..)
  , SettledOutcome (..)
  , JobSettledHook
  , noJobSettled
  , fireJobSettled
  ) where

import Data.Aeson (Value)
import Data.Foldable (traverse_)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NE
import Data.Text (Text)

-- | How a job left its queue for good.
data SettledOutcome
  = -- | An ack deleted the row.
    JobAcked
  | -- | A failure moved the row to the dead-letter queue.
    JobDeadLettered
  deriving stock (Eq, Show)

-- | One job that left its queue, with the result it stored.
data SettledJob = SettledJob
  { settledQueue :: Text
  , settledJobId :: Int64
  , settledResult :: Maybe Value
  , settledOutcome :: SettledOutcome
  }
  deriving stock (Eq, Show)

-- | Called with the jobs one statement took out of a queue, inside that statement's
-- transaction. A job the ack suspended or another worker holds is absent. The hook is
-- payload-erased, so one value serves every pool.
type JobSettledHook m = NonEmpty SettledJob -> m ()

-- | The default hook.
noJobSettled :: (Applicative m) => JobSettledHook m
noJobSettled _ = pure ()

-- | Run the hook over the jobs one statement settled, skipping the call when none were.
fireJobSettled :: (Applicative m) => JobSettledHook m -> [SettledJob] -> m ()
fireJobSettled hook = traverse_ hook . NE.nonEmpty
