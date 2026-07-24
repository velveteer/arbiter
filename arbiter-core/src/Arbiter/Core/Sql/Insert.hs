{-# LANGUAGE OverloadedStrings #-}

-- | INSERT fragments derived from a profunctor 'Codec', so the column list, the
-- placeholders, and the parameters all come from one value and cannot drift.
module Arbiter.Core.Sql.Insert
  ( insertFrag
  , batchFrag
  ) where

import Data.Text (Text)
import Data.Text qualified as T

import Arbiter.Core.Codec (Codec, cArray, cColumns, cScalar)
import Arbiter.Core.Sql.Query (Query, param, raw, sepBy)

-- | A single row: @(c1, c2, ...) VALUES (?, ?, ...)@ with one scalar parameter
-- per column, from 'cScalar'.
insertFrag :: Codec s a -> s -> Query ()
insertFrag codec s =
  raw ("(" <> columnList codec <> ") VALUES (")
    <> sepBy ", " (map param (cScalar codec s))
    <> raw ")"

-- | A batch source: @(c1, ...) SELECT c1, ... FROM (SELECT unnest(?::t1[]) AS c1,
-- ...) src@ with one array parameter per column, from 'cArray'.
batchFrag :: Codec s a -> [s] -> Query ()
batchFrag codec rows =
  raw ("(" <> cols <> ") SELECT " <> cols <> " FROM (SELECT ")
    <> sepBy ", " (zipWith unnestCol (cColumns codec) (cArray codec rows))
    <> raw ") src"
  where
    cols = columnList codec
    unnestCol (name, ty) p = raw "unnest(" <> param p <> raw ("::" <> ty <> "[]) AS " <> name)

columnList :: Codec s a -> Text
columnList = T.intercalate ", " . map fst . cColumns
