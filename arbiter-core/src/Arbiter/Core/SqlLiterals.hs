{-# LANGUAGE OverloadedStrings #-}

-- | Rendering Haskell values as inline SQL literals for statements that cannot
-- use parameter binding, such as migrations and seed upserts.
module Arbiter.Core.SqlLiterals
  ( textLiteral
  , quoteIdentifier
  , doubleLiteral
  , intLiteral
  ) where

import Data.Text (Text)
import Data.Text qualified as T

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
