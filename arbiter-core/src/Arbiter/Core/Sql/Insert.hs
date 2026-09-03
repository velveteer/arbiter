{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | INSERT fragments derived from a profunctor 'Codec'. The column list, the
-- placeholders, and the parameters all come from one value.
module Arbiter.Core.Sql.Insert
  ( insertFrag
  , batchFrag
  ) where

import Data.Text (Text)
import Data.Text qualified as T

import Arbiter.Core.Codec (Codec, cArray, cColumns, cScalar)
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
columnList = T.intercalate ", " . map fst . cColumns
