-- | W3C trace-context propagation for enqueued jobs.
module Arbiter.Core.Trace
  ( TraceContext
  , currentTraceContext
  , stampTraceContext
  ) where

import Control.Monad (guard)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString qualified as BS
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8)
import OpenTelemetry.Context (lookupSpan)
import OpenTelemetry.Context.ThreadLocal (getContext)
import OpenTelemetry.Propagator.W3CTraceContext (encodeSpanContext)
import OpenTelemetry.Trace.Core (getSpanContext, isValid)

import Arbiter.Core.Job.Types (Job (..), JobWrite)

-- | A job's @traceparent@ and @tracestate@.
type TraceContext = (Maybe Text, Maybe Text)

untraced :: TraceContext
untraced = (Nothing, Nothing)

-- | The ambient span's trace context, or 'untraced' when no span is active.
currentTraceContext :: (MonadIO m) => m TraceContext
currentTraceContext = liftIO $ maybe (pure untraced) encoded . lookupSpan =<< getContext
  where
    encoded sp = do
      valid <- isValid <$> getSpanContext sp
      if not valid
        then pure untraced
        else do
          (tp, ts) <- encodeSpanContext sp
          pure (Just (decodeUtf8 tp), decodeUtf8 ts <$ guard (not (BS.null ts)))

-- | Fill a job's trace context, unless it already carries one.
stampTraceContext :: TraceContext -> JobWrite payload -> JobWrite payload
stampTraceContext (tp, ts) job
  | isJust (traceparent job) = job
  | otherwise = job {traceparent = tp, tracestate = ts}
