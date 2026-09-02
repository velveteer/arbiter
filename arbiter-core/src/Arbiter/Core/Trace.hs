{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | W3C trace context carried on enqueued jobs, and the producer and consumer spans
-- around them. The OpenTelemetry API is inert until an SDK installs a provider, so
-- an untraced deployment pays for none of this.
module Arbiter.Core.Trace
  ( -- * Job trace context
    TraceContext (..)
  , stampTraceContext

    -- * Tracer
  , Tracer
  , resolveTracer

    -- * Producer
  , currentTraceContext
  , withPublishSpan

    -- * Consumer
  , ConsumeSpan
  , ConsumeShape (..)
  , consumeSpanFor
  , withConsumeSpan
  , withJobParent
  , capturingContext

    -- * Span enrichment
    -- $enrichment
  , markSpanError
  , recordJobFailure
  , recordJobCancelled
  ) where

import Control.Applicative ((<|>))
import Control.Exception (fromException)
import Control.Monad (guard)
import Control.Monad.IO.Class (MonadIO)
import Data.ByteString qualified as BS
import Data.HashMap.Strict qualified as HM
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.List.NonEmpty qualified as NE
import Data.Maybe (mapMaybe, maybeToList)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import Data.Text.Lazy (toStrict)
import Data.Text.Lazy.Builder (toLazyText)
import Data.Text.Lazy.Builder.Int (decimal)
import OpenTelemetry.Attributes (Attribute, toAttribute)
import OpenTelemetry.Attributes.Map (AttributeMap)
import OpenTelemetry.Context (Context, insertSpan, lookupSpan)
import OpenTelemetry.Context.ThreadLocal (attachContext, detachContext, getContext)
import OpenTelemetry.Propagator.W3CTraceContext (decodeSpanContext, encodeSpanContext)
import OpenTelemetry.Trace.Core
  ( ExceptionClassification (..)
  , ExceptionHandler
  , ExceptionResponse (..)
  , NewEvent (..)
  , NewLink (..)
  , SpanArguments (..)
  , SpanContext
  , SpanKind (..)
  , SpanStatus (..)
  , Tracer
  , TracerOptions (..)
  , addEvent
  , defaultSpanArguments
  , getActiveSpan
  , getGlobalTracerProvider
  , getSpanContext
  , inSpan''
  , isValid
  , makeTracer
  , setStatus
  , tracerIsEnabled
  , tracerOptions
  , withActiveSpan
  , wrapSpanContext
  )
import UnliftIO (MonadUnliftIO, bracket)
import UnliftIO.Async (AsyncCancelled (..))

import Arbiter.Core.Exceptions
  ( JobForceCancelled (..)
  , JobGoneException (..)
  , JobNackException (..)
  )
import Arbiter.Core.Job.Schema (TableName)
import Arbiter.Core.Job.Types (HasKind (..), Job, JobRead, JobWrite, TraceContext (..))
import Arbiter.Core.Job.Types qualified as JT

-- $enrichment
-- Reach for @hs-opentelemetry-api@ directly for custom spans and attributes.

-- | Fill a job's trace context, leaving a job that carries one of its own alone.
stampTraceContext :: Maybe TraceContext -> JobWrite payload -> JobWrite payload
stampTraceContext Nothing job = job
stampTraceContext ctx job = JT.setTraceContext (JT.traceContext job <|> ctx) job

-- | The ambient span's trace context, or 'Nothing' when no span is active. Read from
-- thread-local context, so it answers for the thread that enqueues.
currentTraceContext :: IO (Maybe TraceContext)
currentTraceContext = maybe (pure Nothing) encoded =<< getActiveSpan
  where
    encoded sp = do
      valid <- isValid <$> getSpanContext sp
      if not valid
        then pure Nothing
        else do
          (tp, ts) <- encodeSpanContext sp
          pure (Just (TraceContext (decodeUtf8 tp) (decodeUtf8 ts <$ guard (not (BS.null ts)))))

-- | Resolve the arbiter tracer once, or 'Nothing' when nothing is collecting.
resolveTracer :: (MonadIO m) => m (Maybe Tracer)
resolveTracer = do
  tp <- getGlobalTracerProvider
  let tracer = makeTracer tp "arbiter" arbiterTracerOptions
  pure (if tracerIsEnabled tracer then Just tracer else Nothing)

arbiterTracerOptions :: TracerOptions
arbiterTracerOptions = tracerOptions {tracerExceptionHandlerOptions = [routineControlFlow]}

-- | Record control-flow exceptions for nacks, reclaims, and job cancellations
-- without an error status. Ignore worker cancellation exceptions.
routineControlFlow :: ExceptionHandler
routineControlFlow e
  | Just JobNackException <- fromException e = recorded
  | Just (JobGoneException _ _) <- fromException e = recorded
  | Just (JobForceCancelled _ _) <- fromException e = recorded
  | Just AsyncCancelled <- fromException e = Just (ExceptionResponse IgnoredException mempty)
  | otherwise = Nothing
  where
    recorded = Just (ExceptionResponse RecordedException mempty)

-- | Run an action inside a named span, unwrapped when no tracer was resolved.
spanning :: (MonadUnliftIO m) => Maybe Tracer -> Text -> SpanArguments -> m a -> m a
spanning mTracer name args action =
  maybe action (\tracer -> inSpan'' tracer name args (const action)) mTracer

-- | Run an action inside a @publish \<queue\>@ producer span over @n@ jobs.
withPublishSpan :: (HasKind payload, MonadUnliftIO m) => TableName -> [JobWrite payload] -> m a -> m a
withPublishSpan queue jobs action =
  resolveTracer >>= \tracer -> spanning tracer ("publish " <> queue) (producerArgs queue jobs) action

-- | Mark the currently active span failed.
markSpanError :: (MonadIO m) => Text -> m ()
markSpanError msg = withActiveSpan (\sp -> setStatus sp (Error msg))

-- | Record one job's failure as an event on the active span, a no-op when none is active.
-- A batch span also covers the jobs that succeeded, so this leaves its status alone.
recordJobFailure :: (MonadIO m) => JobRead payload -> Text -> m ()
recordJobFailure = jobEvent "job.failed" "arbiter.job.failure_reason"

-- | Record one job's cancellation as an event on the active span.
recordJobCancelled :: (MonadIO m) => JobRead payload -> Text -> m ()
recordJobCancelled = jobEvent "job.cancelled" "arbiter.job.cancel_reason"

jobEvent :: (MonadIO m) => Text -> Text -> JobRead payload -> Text -> m ()
jobEvent name reasonKey job msg = withActiveSpan (`addEvent` event)
  where
    event =
      NewEvent
        { newEventName = name
        , newEventAttributes =
            HM.fromList
              [ ("messaging.message.id", messageId job)
              , (reasonKey, toAttribute msg)
              ]
        , newEventTimestamp = Nothing
        }

-- | Queue attributes for consumer spans. Resolve them one time for each pool
-- and reuse them for claimed jobs.
data ConsumeSpan = ConsumeSpan
  { consumeName :: Text
  , consumeAttrs :: AttributeMap
  , consumeShape :: ConsumeShape
  }

-- | What one consumer span covers.
data ConsumeShape = PerJob | PerBatch
  deriving stock (Eq, Show)

-- | The consumer-span shape for a queue.
consumeSpanFor :: TableName -> ConsumeShape -> ConsumeSpan
consumeSpanFor queue shape =
  ConsumeSpan
    { consumeName = "process " <> queue
    , consumeAttrs = messagingAttrs queue "process"
    , consumeShape = shape
    }

-- | Run a job handler inside a @process \<queue\>@ consumer span linked to its producer.
withConsumeSpan
  :: (MonadUnliftIO m) => Maybe Tracer -> ConsumeSpan -> NonEmpty (JobRead payload) -> m a -> m a
withConsumeSpan mTracer cs jobs =
  spanning mTracer (consumeName cs) args
  where
    (firstJob :| _) = jobs
    args = case consumeShape cs of
      PerBatch -> batchConsumerArgs cs jobs
      PerJob -> consumerArgs cs firstJob

-- | Capture the caller context for an action on a child thread. Return the
-- unchanged action if the context has no span.
capturingContext :: (MonadUnliftIO m) => m (m a -> m a)
capturingContext = (\ctx -> maybe id (const (withContext ctx)) (lookupSpan ctx)) <$> getContext

-- | Run an action under @ctx@, restoring the caller's own afterwards.
withContext :: (MonadUnliftIO m) => Context -> m a -> m a
withContext ctx action = bracket (attachContext ctx) detachContext (const action)

-- | Run an action with the job's stored trace context as the ambient parent, where the
-- consumer spans link to it instead.
withJobParent :: (MonadUnliftIO m) => JobRead payload -> m a -> m a
withJobParent job action = maybe action attached (spanContextForJob job)
  where
    attached sc = flip withContext action . insertSpan (wrapSpanContext sc) =<< getContext

-- | A span link reconstructed from a job's stored W3C trace context.
spanLinkForJob :: JobRead payload -> Maybe NewLink
spanLinkForJob job =
  (\sc -> NewLink {linkContext = sc, linkAttributes = mempty}) <$> spanContextForJob job

spanContextForJob :: JobRead payload -> Maybe SpanContext
spanContextForJob job = do
  ctx <- JT.traceContext job
  decodeSpanContext (Just (encodeUtf8 (traceparent ctx))) (encodeUtf8 <$> tracestate ctx)

-- | A lone job contributes its own attributes. A batch reports its size instead, per
-- the messaging conventions, since the jobs in it need not agree.
producerArgs :: (HasKind payload) => TableName -> [JobWrite payload] -> SpanArguments
producerArgs queue jobs =
  defaultSpanArguments {kind = Producer, attributes = messagingAttrs queue "publish" <> published}
  where
    published = case jobs of
      [job] -> writeAttrs job
      _ -> HM.fromList [("messaging.batch.message_count", toAttribute (length jobs))]

-- | What a job carries whichever end of the queue reads it.
jobShapeAttrs :: Job payload key q ins adm -> [(Text, Attribute)]
jobShapeAttrs job =
  ("arbiter.priority", toAttribute (fromIntegral (JT.priority job) :: Int))
    : foldMap (\g -> [("arbiter.group_key", toAttribute g)]) (JT.groupKey job)

writeAttrs :: (HasKind payload) => JobWrite payload -> AttributeMap
writeAttrs job = HM.fromList (kindAttr (kindOf (JT.payload job)) <> jobShapeAttrs job)

-- | The payload's variant label, absent for a payload that declares none.
kindAttr :: Maybe Text -> [(Text, Attribute)]
kindAttr k = [("arbiter.kind", toAttribute v) | v <- maybeToList k]

consumerArgs :: ConsumeSpan -> JobRead payload -> SpanArguments
consumerArgs cs job =
  defaultSpanArguments
    { kind = Consumer
    , links = maybeToList (spanLinkForJob job)
    , attributes = consumeAttrs cs <> jobAttrs job
    }

batchConsumerArgs :: ConsumeSpan -> NonEmpty (JobRead payload) -> SpanArguments
batchConsumerArgs cs jobs =
  defaultSpanArguments
    { kind = Consumer
    , links = kept
    , attributes = consumeAttrs cs <> HM.fromList (count : droppedAttr)
    }
  where
    (kept, over) = splitAt maxSpanLinks (mapMaybe spanLinkForJob (NE.toList jobs))
    count = ("messaging.batch.message_count", toAttribute (length jobs :: Int))
    droppedAttr = [("arbiter.batch.links_dropped", toAttribute (length over :: Int)) | not (null over)]

-- | The link count a span keeps under the SDK's default limits.
maxSpanLinks :: Int
maxSpanLinks = 128

messagingAttrs :: Text -> Text -> AttributeMap
messagingAttrs queue op =
  HM.fromList
    [ ("messaging.system", toAttribute ("arbiter" :: Text))
    , ("messaging.destination.name", toAttribute queue)
    , ("messaging.operation.type", toAttribute op)
    , ("messaging.operation.name", toAttribute op)
    ]

-- | Textual per the messaging semantic conventions, not the raw key.
messageId :: JobRead payload -> Attribute
messageId = toAttribute . toStrict . toLazyText . decimal . JT.primaryKey

jobAttrs :: JobRead payload -> AttributeMap
jobAttrs job =
  HM.fromList $
    [ ("messaging.message.id", messageId job)
    , ("messaging.message.retry.count", toAttribute (max 0 (fromIntegral (JT.attempts job) - 1) :: Int))
    ]
      <> kindAttr (JT.jobKind (JT.payloadKeys job))
      <> jobShapeAttrs job
