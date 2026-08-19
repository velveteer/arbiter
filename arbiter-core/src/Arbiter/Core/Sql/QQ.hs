{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskellQuotes #-}

-- | The @sql@ quasiquoter. It builds a 'Arbiter.Core.Sql.Query.Query' whose text, parameters, and row
-- decoder all come from one template, so they cannot drift.
--
-- Holes reference in-scope identifiers, like @NeatInterpolation@'s @${var}@:
--
--   * @${x}@ splices a fragment: 'Text' (raw clause or table name) or a
--     @Query ()@ (its parameters interleave at the splice site), via @ToFragment@.
--   * @#{ident :: CInt8}@ emits one @?@ and binds in-scope @ident@ as a
--     parameter. @Maybe CInt8@, @[CInt8]@, and @[Maybe CInt8]@ pick the
--     nullable, array, and nullable-array encoders.
--   * @\@{name :: CInt8}@ emits the identifier @name@ and adds @col \"name\"
--     CInt8@ to the decoder (@Maybe CInt8@ uses @ncol@). The quote's result type
--     is @Query@ of the tuple of these holes, or @Query ()@ when there are none.
--
-- A bare @?@ is rejected so every placeholder comes from a hole. A literal @?@
-- is unsupported. Use @jsonb_exists@ and friends for jsonb key-existence.
module Arbiter.Core.Sql.QQ
  ( sql
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import Language.Haskell.TH (Exp, Name, Q)
import Language.Haskell.TH qualified as TH
import Language.Haskell.TH.Quote (QuasiQuoter (..))

import Arbiter.Core.Codec
  ( Col (..)
  , col
  , ncol
  , parr
  , pnarr
  , pnul
  , pval
  )
import Arbiter.Core.Sql.Query (param, raw, rows, toFragment)

-- | The @sql@ quasiquoter, valid in expression position only.
sql :: QuasiQuoter
sql =
  QuasiQuoter
    { quoteExp = \s -> either fail compile (tokenize (T.unpack (normalizeIndent (T.pack s))))
    , quotePat = badContext
    , quoteType = badContext
    , quoteDec = badContext
    }
  where
    badContext _ = fail "sql: only valid in expression position"

-- ---------------------------------------------------------------------------
-- Parsing
-- ---------------------------------------------------------------------------

-- | How a parameter column is shaped.
data Kind = KScalar | KNullable | KArray | KNullArray

data Piece
  = Lit Text
  | -- | @${ident}@
    Splice Text
  | -- | @#{ident :: coltype}@: identifier, shape, column constructor.
    InHole Text Kind Text
  | -- | @\@{name :: coltype}@: name, nullable, column constructor.
    OutHole Text Bool Text

-- | Scan a template into pieces. A bare @?@ is an error.
tokenize :: String -> Either String [Piece]
tokenize = go ""
  where
    go buf s = case s of
      [] -> Right (flush buf)
      ('$' : '{' : rest) -> hole '$' buf rest
      ('#' : '{' : rest) -> hole '#' buf rest
      ('@' : '{' : rest) -> hole '@' buf rest
      ('?' : _) ->
        Left
          "sql: bare '?' placeholder. Use a #{} hole. For jsonb key-existence use jsonb_exists/jsonb_exists_any/jsonb_exists_all"
      (c : rest) -> go (c : buf) rest

    hole sig buf rest = do
      (inside, rest') <- takeBrace rest
      piece <- mkPiece sig inside
      rest'' <- go "" rest'
      Right (flush buf ++ [piece] ++ rest'')

    flush buf = [Lit (T.pack (reverse buf)) | not (null buf)]

takeBrace :: String -> Either String (String, String)
takeBrace s = case break (== '}') s of
  (inside, '}' : rest) -> Right (inside, rest)
  _ -> Left "sql: unterminated hole (missing '}')"

mkPiece :: Char -> String -> Either String Piece
mkPiece '$' inside =
  let name = T.strip (T.pack inside)
   in if T.null name then Left "sql: empty ${} splice" else Right (Splice name)
mkPiece '#' inside = do
  (ident, colType) <- splitAnn inside
  (kind, colName) <- parseColType colType
  Right (InHole ident kind colName)
mkPiece '@' inside = do
  (name, colType) <- splitAnn inside
  (kind, colName) <- parseColType colType
  case kind of
    KScalar -> Right (OutHole name False colName)
    KNullable -> Right (OutHole name True colName)
    _ -> Left "sql: @{} output holes cannot be array-typed"
mkPiece c _ = Left ("sql: unknown hole sigil " ++ [c])

-- | Split @expr :: coltype@ on the @::@.
splitAnn :: String -> Either String (Text, Text)
splitAnn s = case T.breakOn "::" (T.pack s) of
  (lhs, rhs)
    | T.null rhs -> Left ("sql: hole needs a ':: ColType' annotation: " ++ s)
    | otherwise -> Right (T.strip lhs, T.strip (T.drop 2 rhs))

-- | Parse a column-type annotation into its shape and 'Col' constructor name.
parseColType :: Text -> Either String (Kind, Text)
parseColType s0 =
  let s = T.strip s0
   in case bracketed s of
        Just inner -> case maybePrefixed inner of
          Just c -> Right (KNullArray, c)
          Nothing -> Right (KArray, inner)
        Nothing -> case maybePrefixed s of
          Just c -> Right (KNullable, c)
          Nothing -> Right (KScalar, s)
  where
    bracketed t = do
      a <- T.stripPrefix "[" (T.strip t)
      T.strip <$> T.stripSuffix "]" (T.strip a)
    maybePrefixed t = T.strip <$> T.stripPrefix "Maybe " (T.strip t)

-- ---------------------------------------------------------------------------
-- Codegen
-- ---------------------------------------------------------------------------

compile :: [Piece] -> Q Exp
compile pieces = do
  let textExpr = [|mconcat $(TH.listE (map pieceExp pieces))|]
      outHoles = [(n, nul, c) | OutHole n nul c <- pieces]
  case outHoles of
    [] -> textExpr
    _ -> do
      dec <- mkDecoder outHoles
      [|rows $(pure dec) $textExpr|]

-- | A single piece as an expression of type @Query ()@.
pieceExp :: Piece -> Q Exp
pieceExp (Lit t) = [|raw (T.pack $(TH.stringE (T.unpack t)))|]
pieceExp (Splice name) = [|toFragment $(TH.varE (TH.mkName (T.unpack name)))|]
pieceExp (OutHole name _ _) = [|raw (T.pack $(TH.stringE (T.unpack name)))|]
pieceExp (InHole ident kind colType) = do
  colN <- colConName colType
  let encoder = case kind of
        KScalar -> 'pval
        KNullable -> 'pnul
        KArray -> 'parr
        KNullArray -> 'pnarr
  [|param ($(TH.varE encoder) $(TH.conE colN) $(TH.varE (TH.mkName (T.unpack ident))))|]

-- | Applicative decoder for the output holes, as a tuple for arity >= 2.
mkDecoder :: [(Text, Bool, Text)] -> Q Exp
mkDecoder holes
  | length holes > 8 =
      fail "sql: more than 8 @{} output holes. Attach a handwritten codec with `rows` instead"
  | otherwise = case map colDecoder holes of
      [] -> fail "sql: mkDecoder called with no holes"
      [c] -> c
      cs -> foldl' (\acc c -> [|$acc <*> $c|]) [|pure $(TH.conE (TH.tupleDataName (length cs)))|] cs
  where
    colDecoder (name, nullable, colType) = do
      colN <- colConName colType
      let dec = if nullable then 'ncol else 'col
      [|$(TH.varE dec) (T.pack $(TH.stringE (T.unpack name))) $(TH.conE colN)|]

-- | The 'Col' constructor 'Name' for a column-type token.
colConName :: Text -> Q Name
colConName t = case t of
  "CInt4" -> pure 'CInt4
  "CInt8" -> pure 'CInt8
  "CText" -> pure 'CText
  "CBool" -> pure 'CBool
  "CTimestamptz" -> pure 'CTimestamptz
  "CJsonb" -> pure 'CJsonb
  "CFloat8" -> pure 'CFloat8
  "CUuid" -> pure 'CUuid
  other -> fail ("sql: unknown column type '" <> T.unpack other <> "'")

-- ---------------------------------------------------------------------------
-- Indentation
-- ---------------------------------------------------------------------------

-- | Strip the common leading indentation and surrounding blank lines, matching
-- how @NeatInterpolation@ normalizes a multi-line quote.
normalizeIndent :: Text -> Text
normalizeIndent t =
  let ls = dropTrailingBlank (dropWhile isBlank (T.lines t))
      indent = minimum (maxBound : [leading l | l <- ls, not (isBlank l)])
   in T.intercalate "\n" (map (T.drop indent) ls)
  where
    isBlank = T.null . T.strip
    leading = T.length . T.takeWhile (== ' ')
    dropTrailingBlank = reverse . dropWhile isBlank . reverse
