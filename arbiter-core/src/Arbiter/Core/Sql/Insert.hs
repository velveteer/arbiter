{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | INSERT fragments derived from a profunctor 'Codec'. The column list, the
-- placeholders, and the parameters all come from one value.
module Arbiter.Core.Sql.Insert
  ( insertFrag
  , batchFrag
  , RowEdit (..)
  , rowEdit
  , editJoin
  ) where

import Data.Text (Text)

import Arbiter.Core.Codec (Codec, cArray, cColumns, cScalar, joinColumns)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, param, sepBy)

-- | A single row: @(c1, c2, ...) VALUES (?, ?, ...)@ with one scalar parameter
-- per column, from 'cScalar'.
insertFrag :: Codec s a -> s -> Query ()
insertFrag codec value =
  let columns = columnList codec
      values = sepBy ", " (map param (cScalar codec value))
   in [sql|(${columns}) VALUES (${values})|]

-- | A batch source: @(c1, ...) SELECT c1, ... FROM (SELECT unnest(?::t1[]) AS c1,
-- ...) src@ with one array parameter per column, from 'cArray'.
batchFrag :: Codec s a -> [s] -> Query ()
batchFrag codec rows =
  let columns = columnList codec
      unnested = sepBy ", " (zipWith unnestCol (cColumns codec) (cArray codec rows))
   in [sql|(${columns}) SELECT ${columns} FROM (SELECT ${unnested}) src|]
  where
    unnestCol (name, sqlType) value =
      let arrayParam = param value
       in [sql|unnest(${arrayParam}::${sqlType}[]) AS ${name}|]

columnList :: Codec s a -> Text
columnList = joinColumns . map fst . cColumns

-- | Columns a statement reads from a one-row source aliased @edit@ in place of its own.
data RowEdit = RowEdit
  { editColumns :: [Text]
  , editSource :: Query ()
  }

-- | A codec's written columns as a 'RowEdit': @(SELECT ?::t1 AS c1, ...) edit@.
rowEdit :: Codec s a -> s -> RowEdit
rowEdit codec value =
  let selected = sepBy ", " (zipWith castAs (cColumns codec) (cScalar codec value))
   in RowEdit {editColumns = map fst (cColumns codec), editSource = [sql|(SELECT ${selected}) edit|]}
  where
    castAs (name, sqlType) columnValue =
      let hole = param columnValue
       in [sql|${hole}::${sqlType} AS ${name}|]

-- | @CROSS JOIN@ an edit's source, or nothing without an edit.
editJoin :: Maybe RowEdit -> Query ()
editJoin = foldMap (\RowEdit {editSource = source} -> [sql|CROSS JOIN ${source}|])
