{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Haskell values rendered as inline SQL literals.
module Arbiter.Core.SqlLiterals
  ( textLiteral
  , quoteIdentifier
  , doubleLiteral
  , intLiteral
  , defaultMaxAttemptsSQL
  , attemptsLeftSQL
  , minMaxAttemptsSQL
  , maxIdentifierBytes
  , sharedTable
  , lockGroupKeyOnConflictSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T

import Arbiter.Core.Job.Types (defaultMaxAttempts, minMaxAttempts)

-- | A single-quoted SQL text literal, escaping embedded quotes.
textLiteral :: Text -> Text
textLiteral text = "'" <> T.replace "'" "''" text <> "'"

-- | A double-quoted SQL identifier, doubling embedded quotes.
quoteIdentifier :: Text -> Text
quoteIdentifier ident = "\"" <> T.replace "\"" "\"\"" ident <> "\""

-- | A @double precision@ literal. Non-finite values are emitted quoted and cast.
doubleLiteral :: Double -> Text
doubleLiteral value
  | isNaN value || isInfinite value = "'" <> T.pack (show value) <> "'::double precision"
  | otherwise = T.pack (show value)

-- | An integer literal.
intLiteral :: (Integral a) => a -> Text
intLiteral value = T.pack (show (toInteger value))

-- | 'defaultMaxAttempts' as a SQL literal.
defaultMaxAttemptsSQL :: Text
defaultMaxAttemptsSQL = intLiteral defaultMaxAttempts

-- | Whether a row has attempts left. @col@ prefixes each column.
attemptsLeftSQL :: Text -> Text
attemptsLeftSQL col = col <> "attempts < COALESCE(" <> col <> "max_attempts, " <> defaultMaxAttemptsSQL <> ")"

-- | 'minMaxAttempts' as a SQL literal.
minMaxAttemptsSQL :: Text
minMaxAttemptsSQL = intLiteral minMaxAttempts

-- | The PostgreSQL identifier limit in bytes (@NAMEDATALEN - 1@).
maxIdentifierBytes :: Int
maxIdentifierBytes = 63

-- | A schema-qualified shared table. The table name stays unquoted.
sharedTable :: Text -> Text -> Text
sharedTable schemaName tableName = quoteIdentifier schemaName <> "." <> tableName

-- | An ON CONFLICT clause that row-locks an existing group key and writes nothing.
-- @DO UPDATE@ takes the lock. @WHERE FALSE@ skips the write.
lockGroupKeyOnConflictSQL :: Text
lockGroupKeyOnConflictSQL = "ON CONFLICT (group_key) DO UPDATE SET group_key = EXCLUDED.group_key WHERE FALSE"
