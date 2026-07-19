{-# LANGUAGE OverloadedStrings #-}

-- | Rendering Haskell values as inline SQL literals, for statements built by
-- string interpolation rather than parameter binding (migrations, seed upserts).
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
textLiteral t = "'" <> T.replace "'" "''" t <> "'"

-- | A double-quoted SQL identifier, doubling embedded quotes.
quoteIdentifier :: Text -> Text
quoteIdentifier ident = "\"" <> T.replace "\"" "\"\"" ident <> "\""

-- | A @double precision@ literal. Non-finite values are emitted quoted-and-cast so
-- @Infinity@\/@NaN@ parse instead of breaking the statement.
doubleLiteral :: Double -> Text
doubleLiteral d
  | isNaN d || isInfinite d = "'" <> T.pack (show d) <> "'::double precision"
  | otherwise = T.pack (show d)

-- | An integer literal.
intLiteral :: (Integral a) => a -> Text
intLiteral n = T.pack (show (toInteger n))
