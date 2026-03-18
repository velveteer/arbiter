{-# LANGUAGE OverloadedStrings #-}

-- | Typed encoding and decoding for PostgreSQL queries.
--
--   * 'RowCodec' — a free applicative that describes how to decode result rows.
--     Each backend interprets it natively (postgresql-simple positional fields,
--     hasql typed decoders, orville named marshallers).
--
--   * 'Params' — a list of typed parameters for query execution.
--     Each 'SomeParam' pairs a 'ParamType' with its value, letting backends
--     encode without an untyped intermediary.
--
-- Both sides are built from the same 'Col' GADT, ensuring type consistency
-- between what we send and what we read back.
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
  , statsRowCodec
  ) where

import Control.Applicative.Free.Final (Ap, liftAp, runAp, runAp_)
import Data.Aeson (Value)
import Data.Int (Int32, Int64)
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Time (UTCTime)

import Arbiter.Core.Job.Types (DedupKey (..), Job (..))

-- | Scalar PostgreSQL column type. The GADT tag recovers the Haskell type.
data Col a where
  CInt4 :: Col Int32
  CInt8 :: Col Int64
  CText :: Col Text
  CBool :: Col Bool
  CTimestamptz :: Col UTCTime
  CJsonb :: Col Value
  CFloat8 :: Col Double

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

jobRowCodec :: Text -> RowCodec (Job Value Int64 Text UTCTime)
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
    <*> (isJust <$> ncol "parent_state" CJsonb)
    <*> col "suspended" CBool

dedupKeyCodec :: RowCodec (Maybe DedupKey)
dedupKeyCodec = toDedupKey <$> ncol "dedup_key" CText <*> ncol "dedup_strategy" CText
  where
    toDedupKey Nothing _ = Nothing
    toDedupKey (Just k) (Just "replace") = Just (ReplaceDuplicate k)
    toDedupKey (Just k) _ = Just (IgnoreDuplicate k)

dlqRowCodec :: Text -> RowCodec (Int64, UTCTime, Job Value Int64 Text UTCTime)
dlqRowCodec queueName =
  (,,)
    <$> col "id" CInt8
    <*> col "failed_at" CTimestamptz
    <*> jobRowCodecWithJobId queueName

jobRowCodecWithJobId :: Text -> RowCodec (Job Value Int64 Text UTCTime)
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
    <*> (isJust <$> ncol "parent_state" CJsonb)
    <*> col "suspended" CBool

countCodec :: RowCodec Int64
countCodec = col "count" CInt8

statsRowCodec :: RowCodec (Int64, Int64, Maybe Double)
statsRowCodec =
  (,,)
    <$> col "total_jobs" CInt8
    <*> col "visible_jobs" CInt8
    <*> ncol "oldest_job_age_seconds" CFloat8
