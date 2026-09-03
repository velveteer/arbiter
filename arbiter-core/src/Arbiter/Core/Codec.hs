{-# LANGUAGE OverloadedStrings #-}

-- | Typed encoding and decoding for PostgreSQL queries.
--
--   * 'RowCodec' decodes result rows. It is a free applicative that each backend
--     (postgresql-simple, hasql, orville) interprets natively.
--
--   * 'Params' is the typed parameter list for query execution.
--
-- Both use the same 'Col' GADT.
module Arbiter.Core.Codec
  ( -- * Column types
    Col (..)
  , NullCol (..)

    -- * Row decoding
  , RowCodec
  , col
  , ncol
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

    -- * Bidirectional job write codec
  , Codec
  , cDecode
  , cColumns
  , cScalar
  , cArray
  , jobCodec
  , JobWriteSource (..)
  , writeColumnNames

    -- * Job codecs
  , jobRowCodec
  , dlqRowCodec
  , archiveRowCodec
  , rateLimitPolicyViewCodec
  , rateLimitBucketCodec
  , concurrencyPolicyViewCodec
  , concurrencyKeyViewCodec

    -- * Cron codecs
  , cronScheduleRowCodec

    -- * Worker codecs
  , workerRowWithHealthCodec

    -- * Queue codecs
  , queueRowCodec
  ) where

import Control.Applicative.Free.Final (Ap, liftAp, runAp, runAp_)
import Data.Aeson (Value)
import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Time (UTCTime)
import Data.UUID.Types (UUID)

import Arbiter.Core.Admission (splitPrefixedSuffix)
import Arbiter.Core.Concurrency.Spec (ConcurrencyKey (..))
import Arbiter.Core.Concurrency.Stats (ConcurrencyKeyView (..), ConcurrencyPolicyView (..))
import Arbiter.Core.CronSchedule (CronScheduleRow (..))
import Arbiter.Core.Job.Types
  ( DedupKey (..)
  , JobRead
  , JobWrite
  , PayloadColumns (..)
  , PayloadKeys (..)
  , TraceContext (..)
  , dedupParts
  , defaultMaxAttempts
  , toTraceContext
  )
import Arbiter.Core.Job.Types qualified as JT
import Arbiter.Core.Job.Types.Internal (JobRecord (Job))
import Arbiter.Core.Queues (QueueRow (..))
import Arbiter.Core.RateLimit.Spec (RateLimitKey (..))
import Arbiter.Core.RateLimit.Stats (RateLimitBucketView (..), RateLimitPolicyView (..))
import Arbiter.Core.Worker (WorkerHealth (Live), WorkerRow (..))

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
col name colType = liftAp (NotNull name colType)

-- | A nullable column.
ncol :: Text -> Col a -> RowCodec (Maybe a)
ncol name colType = liftAp (Nullable name colType)

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

-- | An existentially-typed parameter. A 'ParamType' paired with its value.
data SomeParam where
  SomeParam :: ParamType a -> a -> SomeParam

-- | Positional query parameters.
type Params = [SomeParam]

-- | A non-null scalar parameter.
pval :: Col a -> a -> SomeParam
pval colType value = SomeParam (PScalar colType) value

-- | A nullable scalar parameter.
pnul :: Col a -> Maybe a -> SomeParam
pnul colType value = SomeParam (PNullable colType) value

-- | A non-null array parameter.
parr :: Col a -> [a] -> SomeParam
parr colType value = SomeParam (PArray colType) value

-- | An array parameter with nullable elements.
pnarr :: Col a -> [Maybe a] -> SomeParam
pnarr colType value = SomeParam (PNullArray colType) value

-- ---------------------------------------------------------------------------
-- Bidirectional (profunctor) codec
-- ---------------------------------------------------------------------------

-- | A profunctor codec. Writes source @s@ to INSERT columns and params, decodes value @a@ back.
data Codec s a = Codec
  { cDecode :: RowCodec a
  -- ^ The read side.
  , cWrite :: [WriteCol s]
  }

-- | One writable column: its name, 'Col', and accessor. Split by nullability.
data WriteCol s where
  WCol :: Text -> Col a -> (s -> a) -> WriteCol s
  WNCol :: Text -> Col a -> (s -> Maybe a) -> WriteCol s

-- | Writable (column name, PostgreSQL type) pairs, in order.
cColumns :: Codec s a -> [(Text, Text)]
cColumns codec = map nameType (cWrite codec)
  where
    nameType (WCol name colType _) = (name, pgType colType)
    nameType (WNCol name colType _) = (name, pgType colType)

-- | Single-row parameters, one per column.
cScalar :: Codec s a -> s -> Params
cScalar codec source = map param (cWrite codec)
  where
    param (WCol _ colType get) = pval colType (get source)
    param (WNCol _ colType get) = pnul colType (get source)

-- | Batch parameters, one array per column.
cArray :: Codec s a -> [s] -> Params
cArray codec rows = map param (cWrite codec)
  where
    param (WCol _ colType get) = parr colType (map get rows)
    param (WNCol _ colType get) = pnarr colType (map get rows)

-- | Retarget a codec's write source.
lmap :: (t -> s) -> Codec s a -> Codec t a
lmap project (Codec decode writes) = Codec decode (map retarget writes)
  where
    retarget (WCol name colType get) = WCol name colType (get . project)
    retarget (WNCol name colType get) = WNCol name colType (get . project)

instance Functor (Codec s) where
  fmap mapper (Codec decode writes) = Codec (fmap mapper decode) writes

instance Applicative (Codec s) where
  pure value = Codec (pure value) []
  Codec decodeFn writesFn <*> Codec decodeArg writesArg = Codec (decodeFn <*> decodeArg) (writesFn <> writesArg)

-- | A read-write column bound to its own value.
rw :: Text -> Col a -> Codec a a
rw name colType = Codec (col name colType) [WCol name colType id]

-- | A nullable read-write column bound to its own value.
rwN :: Text -> Col a -> Codec (Maybe a) (Maybe a)
rwN name colType = Codec (ncol name colType) [WNCol name colType id]

-- | A column that is only decoded.
ro :: RowCodec a -> Codec s a
ro decode = Codec decode []

-- | A column that is only written. Attach with '<*'.
wo :: Text -> Col a -> (s -> a) -> Codec s ()
wo name colType get = Codec (pure ()) [WCol name colType get]

-- | PostgreSQL type name for a scalar column, used for @unnest@ array casts.
pgType :: Col a -> Text
pgType = \case
  CInt4 -> "int"
  CInt8 -> "bigint"
  CText -> "text"
  CBool -> "boolean"
  CTimestamptz -> "timestamptz"
  CJsonb -> "jsonb"
  CFloat8 -> "float8"
  CUuid -> "uuid"

-- ---------------------------------------------------------------------------
-- Job codecs
-- ---------------------------------------------------------------------------

-- | The insert side of a job row. The payload encoding is built once by the caller.
data JobWriteSource payload = JobWriteSource
  { sourceJob :: JobWrite payload
  , sourceEncoded :: Value
  , sourceColumns :: PayloadColumns
  , sourceParentId :: Maybe Int64
  , sourceParentState :: Maybe Value
  , sourceSuspended :: Bool
  }

-- | A job codec pinned to a @Value@ payload, for the decode and column-list
-- projections that ignore the write source.
type JobCodec a = Codec (JobWriteSource Value) a

-- | Main-table codec. The write source contains public enqueue fields,
-- payload columns, parent id, rollup state, and suspension state.
jobCodec :: Text -> Codec (JobWriteSource payload) (JobRead Value)
jobCodec = jobCodecWith "id"

-- | 'jobCodec' with an explicit primary-key column.
jobCodecWith :: Text -> Text -> Codec (JobWriteSource payload) (JobRead Value)
jobCodecWith idColumn queueName =
  Job
    <$> ro (col idColumn CInt8)
    <*> lmap sourceEncoded (rw "payload" CJsonb)
    <*> pure queueName
    <*> lmap (JT.groupKey . sourceJob) (rwN "group_key" CText)
    <*> ro (col "inserted_at" CTimestamptz)
    <*> ro (ncol "updated_at" CTimestamptz)
    <*> lmap (const 0) (rw "attempts" CInt4)
    <*> lmap (const Nothing) (rwN "last_error" CText)
    <*> lmap (JT.priority . sourceJob) (rw "priority" CInt4)
    <*> ro (ncol "last_attempted_at" CTimestamptz)
    <*> lmap (JT.notVisibleUntil . sourceJob) (rwN "not_visible_until" CTimestamptz)
    <*> dedupCodec
    <*> lmap (Just . fromMaybe defaultMaxAttempts . JT.maxAttempts . sourceJob) (rwN "max_attempts" CInt4)
    <*> lmap sourceParentId (rwN "parent_id" CInt8)
    <*> lmap sourceParentState (rwN "parent_state" CJsonb)
    <*> traceCodec
    <*> lmap sourceSuspended (rw "suspended" CBool)
    <*> ro (ncol "claimed_by" CUuid)
    <*> ro (col "claim_seq" CInt8)
    <*> lmap (JT.archiveFor . sourceJob) (rwN "archive_for" CInt4)
    <*> lmap sourceColumns payloadCodec

-- | Decoder for a main-table job row.
jobRowCodec :: Text -> RowCodec (JobRead Value)
jobRowCodec queueName = cDecode (jobCodec queueName :: JobCodec (JobRead Value))

traceCodec :: Codec (JobWriteSource payload) (Maybe TraceContext)
traceCodec =
  toTraceContext
    <$> lmap (fmap JT.traceparent . JT.traceContext . sourceJob) (rwN "traceparent" CText)
    <*> lmap ((JT.tracestate =<<) . JT.traceContext . sourceJob) (rwN "tracestate" CText)

dedupCodec :: Codec (JobWriteSource payload) (Maybe DedupKey)
dedupCodec =
  toDedupKey
    <$> lmap (fst . dedupParts . JT.dedupKey . sourceJob) (rwN "dedup_key" CText)
    <*> lmap (snd . dedupParts . JT.dedupKey . sourceJob) (rwN "dedup_strategy" CText)
  where
    toDedupKey Nothing _ = Nothing
    toDedupKey (Just key) (Just "replace") = Just (ReplaceDuplicate key)
    toDedupKey (Just key) _ = Just (IgnoreDuplicate key)

payloadCodec :: Codec PayloadColumns PayloadKeys
payloadCodec =
  PayloadKeys
    <$> lmap JT.pcKind (rwN "kind" CText)
    <*> prefixedKeyCodec "rate_limit_key" "rate_limit_prefix" RateLimitKey JT.pcRateLimitKey JT.pcRateLimitPrefix
    <*> prefixedKeyCodec "concurrency_key" "concurrency_prefix" ConcurrencyKey JT.pcConcurrencyKey JT.pcConcurrencyPrefix
    <* wo "rate_limit_cost" CFloat8 JT.pcRateLimitCost

-- | Reconstruct a structured @prefix:suffix@ key from its stored full-key and
-- prefix columns. The suffix is the key with the @prefix:@ part dropped. @keyOf@
-- and @prefixOf@ project the two columns for writes.
prefixedKeyCodec :: Text -> Text -> (Text -> Text -> k) -> (s -> Maybe Text) -> (s -> Maybe Text) -> Codec s (Maybe k)
prefixedKeyCodec keyCol prefixCol ctor keyOf prefixOf =
  toKey
    <$> lmap keyOf (rwN keyCol CText)
    <*> lmap prefixOf (rwN prefixCol CText)
  where
    toKey (Just key) (Just prefix) = Just (ctor prefix (splitPrefixedSuffix prefix key))
    toKey _ _ = Nothing

-- | Writable job columns with PostgreSQL types, in insert order.
jobWriteColumns :: [(Text, Text)]
jobWriteColumns = cColumns (jobCodec "" :: JobCodec (JobRead Value))

-- | Writable job column names, in insert order.
writeColumnNames :: [Text]
writeColumnNames = map fst jobWriteColumns

-- | Envelope codec for the DLQ/archive tables: @id@, a timestamp column, and the job snapshot (@job_id@ for @id@).
jobEnvelopeCodec :: Text -> Text -> RowCodec (Int64, UTCTime, JobRead Value)
jobEnvelopeCodec tsColumn queueName =
  (,,)
    <$> col "id" CInt8
    <*> col tsColumn CTimestamptz
    <*> cDecode (jobCodecWith "job_id" queueName :: JobCodec (JobRead Value))

-- | DLQ envelope. The shared job snapshot plus its DLQ id and failure time.
dlqRowCodec :: Text -> RowCodec (Int64, UTCTime, JobRead Value)
dlqRowCodec = jobEnvelopeCodec "failed_at"

-- | Archive envelope. The shared job snapshot plus the @result@ a completed root job stored.
archiveRowCodec :: Text -> RowCodec (Int64, UTCTime, JobRead Value, Maybe Value)
archiveRowCodec queueName =
  (\(envelopeId, completed, job) result -> (envelopeId, completed, job, result))
    <$> jobEnvelopeCodec "completed_at" queueName
    <*> ncol "result" CJsonb

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

-- | A token-bucket row as the admin API reports it.
rateLimitBucketCodec :: RowCodec RateLimitBucketView
rateLimitBucketCodec =
  RateLimitBucketView
    <$> col "rate_limit_key" CText
    <*> col "policy_prefix" CText
    <*> col "tokens" CFloat8
    <*> col "max_tokens" CFloat8
    <*> ncol "fill_fraction" CFloat8
    <*> col "last_refill" CTimestamptz

-- | A pool policy row as the admin API reports it.
concurrencyPolicyViewCodec :: RowCodec ConcurrencyPolicyView
concurrencyPolicyViewCodec =
  ConcurrencyPolicyView
    <$> col "prefix_id" CText
    <*> col "default_limit" CInt4
    <*> ncol "override_limit" CInt4
    <*> col "key_count" CInt8
    <*> col "total_in_flight" CInt8
    <*> ncol "max_in_flight" CInt4

-- | A per-key in-flight count as the admin API reports it.
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

-- | A @cron_schedules@ row.
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
    <*> ncol "run_requested_at" CTimestamptz
    <*> ncol "last_manual_run_at" CTimestamptz
    <*> col "created_at" CTimestamptz
    <*> col "updated_at" CTimestamptz

-- ---------------------------------------------------------------------------
-- Worker codecs
-- ---------------------------------------------------------------------------

-- | Worker columns plus the raw derived health token. The operation layer
-- validates the token before returning a 'WorkerRow'.
workerRowWithHealthCodec :: RowCodec (WorkerRow, Text)
workerRowWithHealthCodec =
  toRow
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
    <*> col "health" CText
  where
    toRow wid queue host count started heartbeat shuttingDown paused stale metadata rawHealth =
      ( WorkerRow wid queue host count started heartbeat shuttingDown paused stale metadata Live
      , rawHealth
      )

-- ---------------------------------------------------------------------------
-- Queue codecs
-- ---------------------------------------------------------------------------

-- | An @arbiter_queues@ row.
queueRowCodec :: RowCodec QueueRow
queueRowCodec =
  QueueRow
    <$> col "queue_name" CText
    <*> col "paused" CBool
    <*> ncol "paused_at" CTimestamptz
    <*> ncol "metadata" CJsonb
    <*> col "created_at" CTimestamptz
    <*> col "updated_at" CTimestamptz
