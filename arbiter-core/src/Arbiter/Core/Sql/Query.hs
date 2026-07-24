{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A SQL query bundling its text, its positional parameters, and its row
-- decoder in one value, so the three cannot drift. Built by the @sql@
-- quasiquoter in "Arbiter.Core.Sql.QQ". The parameters and decoder use the
-- same 'Col'-driven vocabulary as the profunctor codec in
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
  fmap f (Query s p d) = Query s p (fmap f d)

-- | Concatenation carries no decoder, so it is defined only for @Query ()@:
-- fragments that contribute text and parameters but no output columns.
instance Semigroup (Query ()) where
  Query s1 p1 _ <> Query s2 p2 _ = Query (s1 <> s2) (p1 <> p2) (pure ())

instance Monoid (Query ()) where
  mempty = Query "" [] (pure ())
  mconcat qs = Query (T.concat (map qSql qs)) (concatMap qParams qs) (pure ())

-- | Literal SQL with no parameters (table names, static clauses).
raw :: Text -> Query ()
raw t = Query t [] (pure ())

-- | One @?@ placeholder bound to a single parameter.
param :: SomeParam -> Query ()
param p = Query "?" [p] (pure ())

-- | Rewrite the @?@ placeholders in a query's text to PostgreSQL positional
-- placeholders (@$1@, @$2@, ...) for libpq-based backends. The Nth @?@ maps to
-- the Nth entry of 'qParams'.
numberPlaceholders :: Text -> Text
numberPlaceholders t =
  case T.splitOn "?" t of
    [] -> ""
    (first : rest) ->
      first <> mconcat (zipWith (\i part -> "$" <> T.pack (show (i :: Int)) <> part) [1 ..] rest)

-- | Attach a handwritten row decoder to a parameterized fragment. Takes a
-- @Query ()@ so a fragment that already declares its own output columns cannot
-- have a second decoder bolted on.
rows :: RowCodec a -> Query () -> Query a
rows d (Query s p _) = Query s p d

-- | Attach a decoder to literal, parameter-free SQL rendered as plain 'Text'.
rawRows :: RowCodec a -> Text -> Query a
rawRows d = rows d . raw

-- | Join fragments with a separator, concatenating their text and parameters
-- in order. Used for @WHERE ... AND ...@ and runtime-sized @VALUES@ lists.
sepBy :: Text -> [Query ()] -> Query ()
sepBy sep qs =
  Query
    (T.intercalate sep (map qSql qs))
    (concatMap qParams qs)
    (pure ())

-- | A fragment present only when the flag holds, else the empty monoid.
mwhen :: (Monoid m) => Bool -> m -> m
mwhen True m = m
mwhen False _ = mempty

-- | Values a @${...}@ splice accepts: raw 'Text' (a bare clause or table name)
-- or a 'Query' fragment (whose parameters interleave at the splice site).
class ToFragment a where
  toFragment :: a -> Query ()

instance ToFragment Text where
  toFragment = raw

instance ToFragment (Query ()) where
  toFragment = id
