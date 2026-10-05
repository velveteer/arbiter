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
  -- ^ The W3C @traceparent@ header value.
  , tracestate :: Maybe Text
  -- ^ The W3C @tracestate@ header value, if any.
  }
  deriving stock (Eq, Generic, Show)

-- | A trace context from its two stored halves. An orphan @tracestate@ is dropped.
toTraceContext :: Maybe Text -> Maybe Text -> Maybe TraceContext
toTraceContext parent state = flip TraceContext state <$> parent
