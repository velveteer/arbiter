-- | The W3C trace context captured on a job at enqueue.
module Arbiter.Core.Job.TraceContext
  ( TraceContext (..)
  , toTraceContext
  ) where

import Data.Text (Text)
import GHC.Generics (Generic)

-- | A job's W3C trace context.
data TraceContext = TraceContext
  { traceparent :: Text
  , tracestate :: Maybe Text
  }
  deriving stock (Eq, Generic, Show)

-- | A trace context from its two stored halves. An orphan @tracestate@ is dropped.
toTraceContext :: Maybe Text -> Maybe Text -> Maybe TraceContext
toTraceContext tp ts = flip TraceContext ts <$> tp
