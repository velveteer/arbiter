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
  , cScalar
  , cArray
  , jobCodec
  , writeColumnNames
  , insertColumns
  , insertValues
  , batchUnnest

    -- * Job codecs
  , jobRowCodec
  , dlqRowCodec
  , archiveRowCodec
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
import Data.Aeson (ToJSON (..), Value)
import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)
import Data.UUID.Types (UUID)

import Arbiter.Core.Admission (splitPrefixedSuffix)
import Arbiter.Core.Concurrency.Spec (ConcurrencyKey (..))
import Arbiter.Core.Concurrency.Stats (ConcurrencyKeyView (..), ConcurrencyPolicyView (..))
import Arbiter.Core.CronSchedule (CronScheduleRow (..))
import Arbiter.Core.Job.Types
  ( AdmissionColumns (..)
  , AdmissionKeys (..)
  , DedupKey (..)
  , Job (..)
  , JobRead
  , JobWrite
  , dedupParts
  , defaultMaxAttempts
  )
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
-- Bidirectional (profunctor) codec
-- ---------------------------------------------------------------------------

-- | A profunctor codec: write source @s@ to INSERT columns/params, decoded value @a@ back.
data Codec s a = Codec
  { cDecode :: RowCodec a
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
    nameType (WCol name c _) = (name, pgType c)
    nameType (WNCol name c _) = (name, pgType c)

-- | Single-row parameters, one per column.
cScalar :: Codec s a -> s -> Params
cScalar codec s = map param (cWrite codec)
  where
    param (WCol _ c get) = pval c (get s)
    param (WNCol _ c get) = pnul c (get s)

-- | Batch parameters, one array per column.
cArray :: Codec s a -> [s] -> Params
cArray codec rows = map param (cWrite codec)
  where
    param (WCol _ c get) = parr c (map get rows)
    param (WNCol _ c get) = pnarr c (map get rows)

-- | Retarget a codec's write source.
lmap :: (t -> s) -> Codec s a -> Codec t a
lmap f (Codec d w) = Codec d (map retarget w)
  where
    retarget (WCol name c get) = WCol name c (get . f)
    retarget (WNCol name c get) = WNCol name c (get . f)

instance Functor (Codec s) where
  fmap g (Codec d w) = Codec (fmap g d) w

instance Applicative (Codec s) where
  pure a = Codec (pure a) []
  Codec df w1 <*> Codec dx w2 = Codec (df <*> dx) (w1 <> w2)

-- | A read-write column bound to its own value.
rw :: Text -> Col a -> Codec a a
rw name c = Codec (col name c) [WCol name c id]

-- | A nullable read-write column bound to its own value.
rwN :: Text -> Col a -> Codec (Maybe a) (Maybe a)
rwN name c = Codec (ncol name c) [WNCol name c id]

-- | A read-only column: decoded, never written.
ro :: RowCodec a -> Codec s a
ro d = Codec d []

-- | A write-only column: emits a parameter, reads no column. Attach with '<*'.
wo :: Text -> Col a -> (s -> a) -> Codec s ()
wo name c f = Codec (pure ()) [WCol name c f]

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

-- | A job codec pinned to a @Value@ payload, for the decode and column-list
-- projections that ignore the write source.
type JobCodec a = Codec (JobWrite Value, AdmissionColumns) a

-- | The bidirectional main-table job codec. 'cDecode' is 'jobRowCodec'. The write
-- side turns a 'JobWrite' and its resolved 'AdmissionColumns' into the INSERT
-- column list and parameters.
jobCodec :: (ToJSON payload) => Text -> Codec (JobWrite payload, AdmissionColumns) (JobRead Value)
jobCodec = jobCodecWith "id"

-- | 'jobCodec' with the primary-key column named explicitly. The main table uses
-- @id@. The DLQ and archive snapshots store it as @job_id@.
jobCodecWith :: (ToJSON payload) => Text -> Text -> Codec (JobWrite payload, AdmissionColumns) (JobRead Value)
jobCodecWith idColumn queueName =
  Job
    <$> ro (col idColumn CInt8)
    <*> lmap (toJSON . payload . fst) (rw "payload" CJsonb)
    <*> pure queueName
    <*> lmap (groupKey . fst) (rwN "group_key" CText)
    <*> ro (col "inserted_at" CTimestamptz)
    <*> ro (ncol "updated_at" CTimestamptz)
    <*> lmap (attempts . fst) (rw "attempts" CInt4)
    <*> lmap (lastError . fst) (rwN "last_error" CText)
    <*> lmap (priority . fst) (rw "priority" CInt4)
    <*> ro (ncol "last_attempted_at" CTimestamptz)
    <*> lmap (notVisibleUntil . fst) (rwN "not_visible_until" CTimestamptz)
    <*> dedupCodec
    <*> lmap (Just . fromMaybe defaultMaxAttempts . maxAttempts . fst) (rwN "max_attempts" CInt4)
    <*> lmap (parentId . fst) (rwN "parent_id" CInt8)
    <*> lmap (parentState . fst) (rwN "parent_state" CJsonb)
    <*> lmap (suspended . fst) (rw "suspended" CBool)
    <*> ro (ncol "claimed_by" CUuid)
    <*> lmap (archiveFor . fst) (rwN "archive_for" CInt4)
    <*> lmap snd admissionCodec

-- | Decoder for a main-table job row.
jobRowCodec :: Text -> RowCodec (JobRead Value)
jobRowCodec queueName = cDecode (jobCodec queueName :: JobCodec (JobRead Value))

dedupCodec :: Codec (JobWrite payload, AdmissionColumns) (Maybe DedupKey)
dedupCodec =
  toDedupKey
    <$> lmap (fst . dedupParts . dedupKey . fst) (rwN "dedup_key" CText)
    <*> lmap (snd . dedupParts . dedupKey . fst) (rwN "dedup_strategy" CText)
  where
    toDedupKey Nothing _ = Nothing
    toDedupKey (Just k) (Just "replace") = Just (ReplaceDuplicate k)
    toDedupKey (Just k) _ = Just (IgnoreDuplicate k)

admissionCodec :: Codec AdmissionColumns AdmissionKeys
admissionCodec =
  AdmissionKeys
    <$> prefixedKeyCodec "rate_limit_key" "rate_limit_prefix" RateLimitKey acRateLimitKey acRateLimitPrefix
    <*> prefixedKeyCodec "concurrency_key" "concurrency_prefix" ConcurrencyKey acConcurrencyKey acConcurrencyPrefix
    <* wo "rate_limit_cost" CFloat8 acRateLimitCost

-- | Reconstruct a structured @prefix:suffix@ key from its stored full-key and
-- prefix columns, recovering the suffix by dropping the @prefix:@ part (so suffixes
-- containing @:@ survive). @keyOf@ and @prefixOf@ project the two columns for writes.
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

-- | Writable INSERT column list, shared by the single and batch inserts.
insertColumns :: Text
insertColumns = T.intercalate ", " writeColumnNames

-- | VALUES placeholders for a single-row insert, one per column.
insertValues :: Text
insertValues = T.intercalate ", " (map (const "?") writeColumnNames)

-- | @unnest@ source list for a batch insert, one array cast per column.
batchUnnest :: Text
batchUnnest =
  T.intercalate ", " ["unnest(?::" <> ty <> "[]) AS " <> name | (name, ty) <- jobWriteColumns]

-- | Envelope codec for the DLQ/archive tables: @id@, a timestamp column, and the job snapshot (@job_id@ for @id@).
jobEnvelopeCodec :: Text -> Text -> RowCodec (Int64, UTCTime, JobRead Value)
jobEnvelopeCodec tsColumn queueName =
  (,,)
    <$> col "id" CInt8
    <*> col tsColumn CTimestamptz
    <*> cDecode (jobCodecWith "job_id" queueName :: JobCodec (JobRead Value))

dlqRowCodec :: Text -> RowCodec (Int64, UTCTime, JobRead Value)
dlqRowCodec = jobEnvelopeCodec "failed_at"

-- | Archive envelope: the shared job snapshot plus the @result@ a completed root job stored.
archiveRowCodec :: Text -> RowCodec (Int64, UTCTime, JobRead Value, Maybe Value)
archiveRowCodec queueName =
  (\(i, t, j) r -> (i, t, j, r))
    <$> jobEnvelopeCodec "completed_at" queueName
    <*> ncol "result" CJsonb

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
    <*> ncol "run_requested_at" CTimestamptz
    <*> ncol "last_manual_run_at" CTimestamptz
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
