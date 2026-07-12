{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Native OpenTelemetry producer and consumer spans for arbiter jobs.
module Arbiter.Worker.Trace
  ( -- * Tracer
    Tracer
  , resolveTracer

    -- * Producer
  , withPublishSpan
  , currentTraceContext

    -- * Consumer
  , withConsumeSpan
  , withConsumeSpanBatch
  , spanLinkForJob

    -- * Custom spans
  , withSpan
  , SpanArguments (..)
  , SpanKind (..)
  , defaultSpanArguments

    -- * Span enrichment
  , addSpanAttributes
  , Attribute
  , toAttribute
  ) where

import Arbiter.Core.Job.Schema (TableName)
import Arbiter.Core.Job.Types (Job (..), JobRead)
import Arbiter.Core.Trace (currentTraceContext)
import Control.Monad.IO.Class (MonadIO)
import Data.Foldable (traverse_)
import Data.HashMap.Strict qualified as HM
import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NE
import Data.Maybe (mapMaybe, maybeToList)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import GHC.Stack (HasCallStack, withFrozenCallStack)
import OpenTelemetry.Attributes (Attribute, toAttribute)
import OpenTelemetry.Attributes.Map (AttributeMap)
import OpenTelemetry.Context (lookupSpan)
import OpenTelemetry.Context.ThreadLocal (getContext)
import OpenTelemetry.Propagator.W3CTraceContext (decodeSpanContext)
import OpenTelemetry.Trace.Core
  ( NewLink (..)
  , SpanArguments (..)
  , SpanKind (..)
  , Tracer
  , addAttributes
  , defaultSpanArguments
  , getGlobalTracerProvider
  , inSpan
  , makeTracer
  , tracerIsEnabled
  , tracerOptions
  )
import UnliftIO (MonadUnliftIO)

-- | Resolve the arbiter tracer once, or 'Nothing' when nothing is collecting.
-- Callers cache this at startup and pass it to the span helpers.
resolveTracer :: (MonadIO m) => m (Maybe Tracer)
resolveTracer = do
  tp <- getGlobalTracerProvider
  let tracer = makeTracer tp "arbiter" tracerOptions
  pure (if tracerIsEnabled tracer then Just tracer else Nothing)

-- | Open a span through the given tracer, unwrapped when none was resolved.
spanCore :: (HasCallStack, MonadUnliftIO m) => Maybe Tracer -> Text -> SpanArguments -> m a -> m a
spanCore mTracer name args action =
  maybe action (\tracer -> inSpan tracer name args action) mTracer

-- | Run an action inside a named span. The code attributes point at this call site.
withSpan :: (HasCallStack, MonadUnliftIO m) => Maybe Tracer -> Text -> SpanArguments -> m a -> m a
withSpan mTracer name args action = withFrozenCallStack (spanCore mTracer name args action)

-- | Run an action inside a @publish \<queue\>@ producer span.
withPublishSpan :: (HasCallStack, MonadUnliftIO m) => Maybe Tracer -> TableName -> m a -> m a
withPublishSpan mTracer queue action =
  withFrozenCallStack (spanCore mTracer ("publish " <> queue) (producerArgs queue) action)

-- | Add attributes to the currently active span, a no-op when none is active.
addSpanAttributes :: (MonadIO m) => [(Text, Attribute)] -> m ()
addSpanAttributes [] = pure ()
addSpanAttributes attrs = do
  mSpan <- lookupSpan <$> getContext
  traverse_ (\sp -> addAttributes sp (HM.fromList attrs)) mSpan

-- | Run a job handler inside a @process \<queue\>@ consumer span linked to its producer.
withConsumeSpan
  :: (HasCallStack, MonadUnliftIO m) => Maybe Tracer -> [(Text, Attribute)] -> JobRead payload -> m a -> m a
withConsumeSpan mTracer extra job action =
  withFrozenCallStack (spanCore mTracer ("process " <> queueName job) (consumerArgs extra job) action)

-- | As 'withConsumeSpan', for a batch, linking every job to the one consumer span. The
-- attributes describe the batch: the per-job ones ('jobAttrs') do not apply to N jobs.
withConsumeSpanBatch
  :: (HasCallStack, MonadUnliftIO m) => Maybe Tracer -> [(Text, Attribute)] -> NonEmpty (JobRead payload) -> m a -> m a
withConsumeSpanBatch mTracer extra jobs action =
  withFrozenCallStack (spanCore mTracer ("process " <> queueName (NE.head jobs)) (batchConsumerArgs extra jobs) action)

-- | A span link reconstructed from a job's stored W3C trace context.
spanLinkForJob :: JobRead payload -> Maybe NewLink
spanLinkForJob job = do
  tp <- traceparent job
  sc <- decodeSpanContext (Just (encodeUtf8 tp)) (encodeUtf8 <$> tracestate job)
  pure NewLink {linkContext = sc, linkAttributes = mempty}

producerArgs :: TableName -> SpanArguments
producerArgs queue =
  defaultSpanArguments {kind = Producer, attributes = messagingAttrs queue "publish"}

consumerArgs :: [(Text, Attribute)] -> JobRead payload -> SpanArguments
consumerArgs extra job =
  defaultSpanArguments
    { kind = Consumer
    , links = maybeToList (spanLinkForJob job)
    , attributes = messagingAttrs (queueName job) "process" <> jobAttrs job <> HM.fromList extra
    }

batchConsumerArgs :: [(Text, Attribute)] -> NonEmpty (JobRead payload) -> SpanArguments
batchConsumerArgs extra jobs =
  defaultSpanArguments
    { kind = Consumer
    , links = mapMaybe spanLinkForJob (NE.toList jobs)
    , attributes =
        messagingAttrs (queueName (NE.head jobs)) "process"
          <> HM.singleton "messaging.batch.message_count" (toAttribute (length jobs :: Int))
          <> HM.fromList extra
    }

messagingAttrs :: Text -> Text -> AttributeMap
messagingAttrs queue op =
  HM.fromList
    [ ("messaging.system", toAttribute ("arbiter" :: Text))
    , ("messaging.destination.name", toAttribute queue)
    , ("messaging.operation", toAttribute op)
    ]

jobAttrs :: JobRead payload -> AttributeMap
jobAttrs job =
  HM.fromList $
    [ ("messaging.message.id", toAttribute (T.pack (show (primaryKey job))))
    , ("messaging.message.retry.count", toAttribute (max 0 (fromIntegral (attempts job) - 1) :: Int))
    , ("arbiter.priority", toAttribute (fromIntegral (priority job) :: Int))
    ]
      <> foldMap (\g -> [("arbiter.group_key", toAttribute g)]) (groupKey job)
