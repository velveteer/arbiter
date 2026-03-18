{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Core.Array
  ( -- * Array Formatting
    fmtArray
  , fmtNullableArray
  ) where

import Data.ByteString (ByteString)
import Database.PostgreSQL.Simple.Arrays qualified as PSA

-- | Format a list of values as a PostgreSQL array literal.
fmtArray :: [ByteString] -> ByteString
fmtArray = PSA.fmt ',' . PSA.Array . map PSA.Quoted

-- | Format a nullable list as a PostgreSQL array literal (Nothing becomes NULL).
fmtNullableArray :: [Maybe ByteString] -> ByteString
fmtNullableArray = PSA.fmt ',' . PSA.Array . map toElement
  where
    toElement Nothing = PSA.Plain "NULL"
    toElement (Just bs) = PSA.Quoted bs
