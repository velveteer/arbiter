{-# LANGUAGE OverloadedStrings #-}

-- | Typed encoding and decoding for PostgreSQL queries.
--
--   * 'RowCodec' - free applicative for decoding result rows. Each backend
--     (postgresql-simple, hasql, orville) interprets it natively.
--
--   * 'Params' - typed parameter list for query execution.
--
-- Both use the same 'Col' GADT, keeping encode and decode types in sync.
module Arbiter.Core.Codec
  ( -- * Column types
    Col (..)
  , NullCol (..)

    -- * Row decoding
  , RowCodec
  , col
  , ncol
  , pureVal
  , runCodec
  , codecColumns

    -- * Parameter encoding
  , ParamType (..)
  , SomeParam (..)
  , Params
  , pval
  , pnul
  , parr
  , pnarr

    -- * Job codecs
  , jobRowCodec
  , dlqRowCodec
  , countCodec
  , rateLimitPolicyViewCodec
  , rateLimitBucketCodec
  , concurrencyPolicyViewCodec
  , concurrencyKeyViewCodec

    -- * Cron codecs
  , cronScheduleRowCodec

    -- * Worker codecs
  , workerRowCodec

    -- * Queue codecs
  , queueRowCodec
  ) where

import Control.Applicative.Free.Final (Ap, liftAp, runAp, runAp_)
import Data.Aeson (Value)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.Time (UTCTime)
import Data.UUID.Types (UUID)

import Arbiter.Core.Admission (splitPrefixedSuffix)
import Arbiter.Core.Concurrency.Spec (ConcurrencyKey (..))
import Arbiter.Core.Concurrency.Stats (ConcurrencyKeyView (..), ConcurrencyPolicyView (..))
import Arbiter.Core.CronSchedule (CronScheduleRow (..))
import Arbiter.Core.Job.Types (AdmissionKeys (..), DedupKey (..), Job (..), JobRead)
import Arbiter.Core.Queues (QueueRow (..))
import Arbiter.Core.RateLimit.Spec (RateLimitKey (..))
import Arbiter.Core.RateLimit.Stats (RateLimitBucketView (..), RateLimitPolicyView (..))
import Arbiter.Core.Worker (WorkerRow (..), workerHealthFromText)

-- | Scalar PostgreSQL column type. The GADT tag recovers the Haskell type.
data Col a where
  CInt4 :: Col Int32
  CInt8 :: Col Int64
  CText :: Col Text
  CBool :: Col Bool
  CTimestamptz :: Col UTCTime
  CJsonb :: Col Value
  CFloat8 :: Col Double
  CUuid :: Col UUID

-- | A named column with nullability. Carries the column name for
-- backends that use name-based decoding (e.g. orville).
data NullCol a where
  NotNull :: Text -> Col a -> NullCol a
  Nullable :: Text -> Col a -> NullCol (Maybe a)

-- | Free applicative over 'NullCol'. Backends interpret this into
-- their native row parser by pattern-matching on each 'NullCol'.
type RowCodec = Ap NullCol

-- | A non-nullable column.
col :: Text -> Col a -> RowCodec a
col name c = liftAp (NotNull name c)

-- | A nullable column.
ncol :: Text -> Col a -> RowCodec (Maybe a)
ncol name c = liftAp (Nullable name c)

-- | Inject a pure value (not read from the database).
pureVal :: a -> RowCodec a
pureVal = pure

-- | Interpret a 'RowCodec' by providing a natural transformation
-- from 'NullCol' to some 'Applicative'.
runCodec :: (Applicative f) => (forall x. NullCol x -> f x) -> RowCodec a -> f a
runCodec = runAp

-- | Extract the column names from a codec (in order).
codecColumns :: RowCodec a -> [Text]
codecColumns = runAp_ colName
  where
    colName :: NullCol a -> [Text]
    colName (NotNull name _) = [name]
    colName (Nullable name _) = [name]

-- | How a parameter is shaped: scalar, nullable, or array.
data ParamType a where
  PScalar :: Col a -> ParamType a
  PNullable :: Col a -> ParamType (Maybe a)
  PArray :: Col a -> ParamType [a]
  PNullArray :: Col a -> ParamType [Maybe a]

-- | An existentially-typed parameter: a 'ParamType' paired with its value.
data SomeParam where
  SomeParam :: ParamType a -> a -> SomeParam

-- | Positional query parameters.
type Params = [SomeParam]

pval :: Col a -> a -> SomeParam
pval c v = SomeParam (PScalar c) v

pnul :: Col a -> Maybe a -> SomeParam
pnul c v = SomeParam (PNullable c) v

parr :: Col a -> [a] -> SomeParam
parr c v = SomeParam (PArray c) v

pnarr :: Col a -> [Maybe a] -> SomeParam
pnarr c v = SomeParam (PNullArray c) v

-- ---------------------------------------------------------------------------
-- Job codecs
-- ---------------------------------------------------------------------------

jobRowCodec :: Text -> RowCodec (JobRead Value)
jobRowCodec queueName =
  Job
    <$> col "id" CInt8
    <*> col "payload" CJsonb
    <*> pureVal queueName
    <*> ncol "group_key" CText
    <*> col "inserted_at" CTimestamptz
    <*> ncol "updated_at" CTimestamptz
    <*> col "attempts" CInt4
    <*> ncol "last_error" CText
    <*> col "priority" CInt4
    <*> ncol "last_attempted_at" CTimestamptz
    <*> ncol "not_visible_until" CTimestamptz
    <*> dedupKeyCodec
    <*> ncol "max_attempts" CInt4
    <*> ncol "parent_id" CInt8
    <*> ncol "parent_state" CJsonb
    <*> ncol "traceparent" CText
    <*> ncol "tracestate" CText
    <*> col "suspended" CBool
    <*> ncol "claimed_by" CUuid
    <*> admissionKeysCodec

admissionKeysCodec :: RowCodec AdmissionKeys
admissionKeysCodec = AdmissionKeys <$> rateLimitCodec <*> concurrencyCodec

dedupKeyCodec :: RowCodec (Maybe DedupKey)
dedupKeyCodec = toDedupKey <$> ncol "dedup_key" CText <*> ncol "dedup_strategy" CText
  where
    toDedupKey Nothing _ = Nothing
    toDedupKey (Just k) (Just "replace") = Just (ReplaceDuplicate k)
    toDedupKey (Just k) _ = Just (IgnoreDuplicate k)

-- | Reconstruct a structured @prefix:suffix@ key from its stored full-key and
-- prefix columns, recovering the suffix by dropping the @prefix:@ part (so suffixes
-- containing @:@ survive).
prefixedKeyCodec :: Text -> Text -> (Text -> Text -> k) -> RowCodec (Maybe k)
prefixedKeyCodec keyCol prefixCol ctor = toKey <$> ncol keyCol CText <*> ncol prefixCol CText
  where
    toKey (Just key) (Just prefix) = Just (ctor prefix (splitPrefixedSuffix prefix key))
    toKey _ _ = Nothing

rateLimitCodec :: RowCodec (Maybe RateLimitKey)
rateLimitCodec = prefixedKeyCodec "rate_limit_key" "rate_limit_prefix" RateLimitKey

concurrencyCodec :: RowCodec (Maybe ConcurrencyKey)
concurrencyCodec = prefixedKeyCodec "concurrency_key" "concurrency_prefix" ConcurrencyKey

dlqRowCodec :: Text -> RowCodec (Int64, UTCTime, JobRead Value)
dlqRowCodec queueName =
  (,,)
    <$> col "id" CInt8
    <*> col "failed_at" CTimestamptz
    <*> jobRowCodecWithJobId queueName

jobRowCodecWithJobId :: Text -> RowCodec (JobRead Value)
jobRowCodecWithJobId queueName =
  Job
    <$> col "job_id" CInt8
    <*> col "payload" CJsonb
    <*> pureVal queueName
    <*> ncol "group_key" CText
    <*> col "inserted_at" CTimestamptz
    <*> ncol "updated_at" CTimestamptz
    <*> col "attempts" CInt4
    <*> ncol "last_error" CText
    <*> col "priority" CInt4
    <*> ncol "last_attempted_at" CTimestamptz
    <*> ncol "not_visible_until" CTimestamptz
    <*> dedupKeyCodec
    <*> ncol "max_attempts" CInt4
    <*> ncol "parent_id" CInt8
    <*> ncol "parent_state" CJsonb
    <*> ncol "traceparent" CText
    <*> ncol "tracestate" CText
    <*> col "suspended" CBool
    <*> ncol "claimed_by" CUuid
    <*> admissionKeysCodec

countCodec :: RowCodec Int64
countCodec = col "count" CInt8

-- | A policy row with bucket aggregates and live throttled count.
rateLimitPolicyViewCodec :: RowCodec RateLimitPolicyView
rateLimitPolicyViewCodec =
  RateLimitPolicyView
    <$> col "prefix_id" CText
    <*> col "default_max_tokens" CFloat8
    <*> col "default_refill_amount" CFloat8
    <*> col "default_interval" CFloat8
    <*> ncol "override_max_tokens" CFloat8
    <*> ncol "override_refill_amount" CFloat8
    <*> ncol "override_interval" CFloat8
    <*> col "bucket_count" CInt8
    <*> col "throttled_count" CInt8
    <*> ncol "min_tokens" CFloat8
    <*> ncol "avg_tokens" CFloat8

rateLimitBucketCodec :: RowCodec RateLimitBucketView
rateLimitBucketCodec =
  RateLimitBucketView
    <$> col "rate_limit_key" CText
    <*> col "policy_prefix" CText
    <*> col "tokens" CFloat8
    <*> col "max_tokens" CFloat8
    <*> ncol "fill_fraction" CFloat8
    <*> col "last_refill" CTimestamptz

concurrencyPolicyViewCodec :: RowCodec ConcurrencyPolicyView
concurrencyPolicyViewCodec =
  ConcurrencyPolicyView
    <$> col "prefix_id" CText
    <*> col "default_limit" CInt4
    <*> ncol "override_limit" CInt4
    <*> col "key_count" CInt8
    <*> col "total_in_flight" CInt8
    <*> ncol "max_in_flight" CInt4

concurrencyKeyViewCodec :: RowCodec ConcurrencyKeyView
concurrencyKeyViewCodec =
  ConcurrencyKeyView
    <$> col "concurrency_key" CText
    <*> col "concurrency_prefix" CText
    <*> col "in_flight" CInt4
    <*> col "effective_limit" CInt4
    <*> ncol "fill_fraction" CFloat8

-- ---------------------------------------------------------------------------
-- Cron codecs
-- ---------------------------------------------------------------------------

cronScheduleRowCodec :: RowCodec CronScheduleRow
cronScheduleRowCodec =
  CronScheduleRow
    <$> col "name" CText
    <*> col "queue_name" CText
    <*> col "default_expression" CText
    <*> col "default_overlap" CText
    <*> ncol "default_timezone" CText
    <*> ncol "override_expression" CText
    <*> ncol "override_overlap" CText
    <*> ncol "override_timezone" CText
    <*> col "enabled" CBool
    <*> ncol "last_fired_at" CTimestamptz
    <*> ncol "last_checked_at" CTimestamptz
    <*> col "created_at" CTimestamptz
    <*> col "updated_at" CTimestamptz

-- ---------------------------------------------------------------------------
-- Worker codecs
-- ---------------------------------------------------------------------------

workerRowCodec :: RowCodec WorkerRow
workerRowCodec =
  WorkerRow
    <$> col "worker_id" CUuid
    <*> col "queue_name" CText
    <*> ncol "host_name" CText
    <*> ncol "worker_count" CInt4
    <*> col "started_at" CTimestamptz
    <*> col "last_heartbeat" CTimestamptz
    <*> col "shutting_down" CBool
    <*> col "paused" CBool
    <*> col "stale_threshold_secs" CFloat8
    <*> ncol "metadata" CJsonb
    <*> (workerHealthFromText <$> col "health" CText)

-- ---------------------------------------------------------------------------
-- Queue codecs
-- ---------------------------------------------------------------------------

queueRowCodec :: RowCodec QueueRow
queueRowCodec =
  QueueRow
    <$> col "queue_name" CText
    <*> col "paused" CBool
    <*> ncol "paused_at" CTimestamptz
    <*> ncol "metadata" CJsonb
    <*> col "created_at" CTimestamptz
    <*> col "updated_at" CTimestamptz
