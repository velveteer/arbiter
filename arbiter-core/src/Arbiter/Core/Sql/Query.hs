{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A SQL query bundling its text, its positional parameters, and its row
-- decoder in one value. The text is a list of literals and parameter holes, rendered
-- in the placeholder form each backend needs. Built by the @sql@
-- quasiquoter in "Arbiter.Core.Sql.QQ". The parameters and decoder use the
-- same 'Arbiter.Core.Codec.Col'-driven vocabulary as the profunctor codec in
-- "Arbiter.Core.Codec".
module Arbiter.Core.Sql.Query
  ( Query (..)
  , mkQuery
  , raw
  , param
  , rows
  , rawRows
  , sepBy
  , mwhen
  , Piece (..)
  , ToFragment (..)
  ) where

import Data.List (intercalate, mapAccumL)
import Data.Text (Text)
import Data.Text qualified as T

import Arbiter.Core.Codec (Params, RowCodec, SomeParam)

-- | One run of literal SQL or one parameter hole.
data Piece = Lit !Text | Hole
  deriving stock (Eq, Show)

-- | A parameterized query paired with the decoder for its result rows.
data Query a = Query
  { qPieces :: [Piece]
  -- ^ The text as literals and holes, one hole per entry of 'qParams'.
  , qParams :: Params
  -- ^ Positional parameters, in hole order.
  , qSql :: Text
  -- ^ The text with @?@ placeholders.
  , qPositional :: Text
  -- ^ The text with @$n@ placeholders.
  , qDecode :: RowCodec a
  -- ^ Decoder for the result rows.
  }

-- | A query from its pieces, parameters and decoder.
mkQuery :: [Piece] -> Params -> RowCodec a -> Query a
mkQuery pieces params = Query pieces params (render (const "?") pieces) (render positional pieces)
  where
    positional index = "$" <> T.pack (show index)

-- | Render the pieces, numbering the holes from one.
render :: (Int -> Text) -> [Piece] -> Text
render hole = T.concat . snd . mapAccumL step 1
  where
    step !index (Lit literal) = (index, literal)
    step !index Hole = (index + 1, hole index)

instance Functor Query where
  fmap fn query = query {qDecode = fmap fn (qDecode query)}

-- | Concatenation is defined for @Query ()@, a fragment with text and parameters and no output columns.
instance Semigroup (Query ()) where
  query1 <> query2 = mkQuery (qPieces query1 <> qPieces query2) (qParams query1 <> qParams query2) (pure ())

instance Monoid (Query ()) where
  mempty = mkQuery [] [] (pure ())
  mconcat queries = mkQuery (concatMap qPieces queries) (concatMap qParams queries) (pure ())

-- | Literal SQL with no parameters (table names, static clauses).
raw :: Text -> Query ()
raw literal = mkQuery [Lit literal] [] (pure ())

-- | One hole bound to a single parameter.
param :: SomeParam -> Query ()
param value = mkQuery [Hole] [value] (pure ())

-- | Attach a handwritten row decoder to a parameterized fragment.
rows :: RowCodec a -> Query () -> Query a
rows decoder query = query {qDecode = decoder}

-- | Attach a decoder to literal, parameter-free SQL rendered as plain 'Text'.
rawRows :: RowCodec a -> Text -> Query a
rawRows decoder = rows decoder . raw

-- | Join fragments with a separator, concatenating their text and parameters
-- in order. Used for @WHERE ... AND ...@ and runtime-sized @VALUES@ lists.
sepBy :: Text -> [Query ()] -> Query ()
sepBy sep queries =
  mkQuery
    (intercalate [Lit sep] (map qPieces queries))
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
