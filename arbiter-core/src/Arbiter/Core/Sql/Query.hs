{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A SQL query bundling its text, its positional parameters, and its row
-- decoder in one value. Built by the @sql@
-- quasiquoter in "Arbiter.Core.Sql.QQ". The parameters and decoder use the
-- same 'Arbiter.Core.Codec.Col'-driven vocabulary as the profunctor codec in
-- "Arbiter.Core.Codec".
module Arbiter.Core.Sql.Query
  ( Query (..)
  , raw
  , param
  , rows
  , rawRows
  , sepBy
  , mwhen
  , numberPlaceholders
  , ToFragment (..)
  ) where

import Data.Text (Text)
import Data.Text qualified as T

import Arbiter.Core.Codec (Params, RowCodec, SomeParam)

-- | A parameterized query paired with the decoder for its result rows.
data Query a = Query
  { qSql :: !Text
  -- ^ SQL text with @?@ placeholders, one per entry of 'qParams'.
  , qParams :: Params
  -- ^ Positional parameters, in placeholder order.
  , qDecode :: RowCodec a
  -- ^ Decoder for the result rows.
  }

instance Functor Query where
  fmap fn (Query text params decoder) = Query text params (fmap fn decoder)

-- | Concatenation is defined for @Query ()@, a fragment with text and parameters and no output columns.
instance Semigroup (Query ()) where
  Query text1 params1 _ <> Query text2 params2 _ = Query (text1 <> text2) (params1 <> params2) (pure ())

instance Monoid (Query ()) where
  mempty = Query "" [] (pure ())
  mconcat queries = Query (T.concat (map qSql queries)) (concatMap qParams queries) (pure ())

-- | Literal SQL with no parameters (table names, static clauses).
raw :: Text -> Query ()
raw text = Query text [] (pure ())

-- | One @?@ placeholder bound to a single parameter.
param :: SomeParam -> Query ()
param value = Query "?" [value] (pure ())

-- | Rewrite the @?@ placeholders in a query's text to PostgreSQL positional
-- placeholders (@$1@, @$2@, ...) for libpq-based backends. The Nth @?@ maps to
-- the Nth entry of 'qParams'.
numberPlaceholders :: Text -> Text
numberPlaceholders text =
  case T.splitOn "?" text of
    [] -> ""
    (first : rest) ->
      first <> mconcat (zipWith (\index part -> "$" <> T.pack (show (index :: Int)) <> part) [1 ..] rest)

-- | Attach a handwritten row decoder to a parameterized fragment.
rows :: RowCodec a -> Query () -> Query a
rows decoder (Query text params _) = Query text params decoder

-- | Attach a decoder to literal, parameter-free SQL rendered as plain 'Text'.
rawRows :: RowCodec a -> Text -> Query a
rawRows decoder = rows decoder . raw

-- | Join fragments with a separator, concatenating their text and parameters
-- in order. Used for @WHERE ... AND ...@ and runtime-sized @VALUES@ lists.
sepBy :: Text -> [Query ()] -> Query ()
sepBy sep queries =
  Query
    (T.intercalate sep (map qSql queries))
    (concatMap qParams queries)
    (pure ())

-- | The fragment under a true flag. @mempty@ under a false one.
mwhen :: (Monoid m) => Bool -> m -> m
mwhen True fragment = fragment
mwhen False _ = mempty

-- | Values a @${...}@ splice accepts: raw 'Text' (a bare clause or table name)
-- or a 'Query' fragment (whose parameters interleave at the splice site).
class ToFragment a where
  toFragment :: a -> Query ()

instance ToFragment Text where
  toFragment = raw

instance ToFragment (Query ()) where
  toFragment = id
