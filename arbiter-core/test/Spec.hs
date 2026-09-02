{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Main (main) where

import Control.Exception (SomeException, someExceptionContext, try)
import Control.Exception.Context (displayExceptionContext)
import Data.Aeson (ToJSON (..), Value (String), object, (.=))
import Data.Int (Int32, Int64)
import Data.List (isInfixOf)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime (..), fromGregorian)
import Data.UUID.Types qualified as UUID
import GHC.Generics (Generic)
import Test.Hspec

import Arbiter.Core.Codec
  ( Col (..)
  , JobWriteSource (..)
  , ParamType (..)
  , SomeParam (..)
  , cScalar
  , codecColumns
  , jobCodec
  , jobRowCodec
  , writeColumnNames
  )
import Arbiter.Core.Exceptions (displayEx, throwInternal, throwNack)
import Arbiter.Core.Job.Kind (HasKind (..), constructorKind, constructorKinds, kindFromField)
import Arbiter.Core.Job.Status (JobStatus (Ready), jobStatusFromText)
import Arbiter.Core.Job.Types (PayloadColumns (..), defaultJob)
import Arbiter.Core.Operations (QueueStats, buildWhereClause, statsRowCodec)
import Arbiter.Core.Sql.Claim (ClaimAdmission (..), claimJobsBatchedSQL)
import Arbiter.Core.Sql.Jobs (JobFilter (..))
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query (..), sepBy)
import Arbiter.Core.Sql.Stats (getQueueStatsSQL)
import Arbiter.Core.Sql.Tree (lockJobTreesFromRootSQL)
import Arbiter.Core.Worker (WorkerHealth (Live), workerHealthFromText)

-- | A short tag per parameter, so tests can assert the encoders and their order.
paramTags :: [SomeParam] -> [Text]
paramTags = map tag
  where
    tag (SomeParam pt _) = case pt of
      PScalar c -> colTag c
      PNullable c -> colTag c <> "?"
      PArray c -> colTag c <> "[]"
      PNullArray c -> colTag c <> "?[]"

colTag :: Col a -> Text
colTag CInt4 = "int4"
colTag CInt8 = "int8"
colTag CText = "text"
colTag CBool = "bool"
colTag CTimestamptz = "ts"
colTag CJsonb = "jsonb"
colTag CFloat8 = "float8"
colTag CUuid = "uuid"

-- | The JSONB values a set of parameters carries.
jsonParams :: [SomeParam] -> [Value]
jsonParams params = [v | SomeParam (PScalar CJsonb) v <- params]

-- | An insert source whose stored encoding is not the payload's own.
sentinelSource :: JobWriteSource KindPayload
sentinelSource =
  JobWriteSource
    { sourceJob = defaultJob (SendWelcome "alice")
    , sourceEncoded = String "sentinel"
    , sourceColumns = PayloadColumns Nothing Nothing Nothing 1 Nothing Nothing
    , sourceParentId = Nothing
    , sourceParentState = Nothing
    , sourceSuspended = False
    }

-- | The text a set of parameters carries, for asserting on an escaped search term.
textParams :: [SomeParam] -> [Text]
textParams params = [t | SomeParam (PScalar CText) t <- params]

-- | One of every filter, in the order 'buildWhereClause' renders them.
allFilters :: [JobFilter]
allFilters =
  [ FilterClaimedBy UUID.nil
  , FilterKind "SendWelcome"
  , FilterPayloadText "term"
  , FilterRateLimitPrefix "smtp"
  , FilterConcurrencyPrefix "tenant"
  , FilterInsertedAfter epoch
  , FilterInsertedBefore epoch
  , FilterCompletedAfter epoch
  , FilterCompletedBefore epoch
  ]
  where
    epoch = UTCTime (fromGregorian 2026 8 30) 0

-- | A tagged sum taking the generic default.
data KindPayload
  = SendWelcome Text
  | SendReceipt Int32
  deriving stock (Eq, Generic, Show)

instance ToJSON KindPayload

instance HasKind KindPayload

-- | A payload with no instance, taking the empty default.
newtype PlainPayload = PlainPayload Text
  deriving stock (Eq, Show)

instance ToJSON PlainPayload where
  toJSON (PlainPayload t) = toJSON t

-- | A payload labelling its constructors by something other than their names.
data RenamedPayload
  = RenamedA
  | RenamedB
  deriving stock (Bounded, Enum, Eq, Generic, Show)

renamedTag :: RenamedPayload -> Text
renamedTag RenamedA = "a_lower"
renamedTag RenamedB = "b_lower"

instance HasKind RenamedPayload where
  kindOf = Just . renamedTag
  kindsFor = map renamedTag [minBound .. maxBound]

-- | An envelope labelled from the sum it wraps.
newtype Envelope = Envelope KindPayload
  deriving stock (Eq, Show)

lowerFirst :: Text -> Text
lowerFirst t = maybe t (\(c, rest) -> T.toLower (T.singleton c) <> rest) (T.uncons t)

instance HasKind Envelope where
  kindOf (Envelope b) = Just (lowerFirst (constructorKind b))
  kindsFor = map lowerFirst (constructorKinds @KindPayload)

-- | A payload with no constructors to name, labelled from the data itself.
newtype RuntimePayload = RuntimePayload Value
  deriving stock (Eq, Show)

instance HasKind RuntimePayload where
  kindOf (RuntimePayload v) = kindFromField "type" v
  kindsFor = []

-- | The per-queue stats query over a declared label set.
statsSQL :: [Text] -> Query QueueStats
statsSQL = getQueueStatsSQL statsRowCodec "arbiter" "jobs"

sqlOf :: Query a -> Text
sqlOf = T.strip . qSql

squished :: Query a -> Text
squished = squish . qSql

squish :: Text -> Text
squish = T.unwords . T.words

main :: IO ()
main = hspec $ do
  describe "job status decoding" $ do
    it "accepts a known status" $
      jobStatusFromText "ready" `shouldBe` Right Ready

    it "rejects an unknown SQL status" $
      jobStatusFromText "new_status" `shouldBe` Left "unknown job status: new_status"

  describe "exception rendering" $ do
    it "renders a caught exception without the backtrace base attaches" $ do
      caught <- try (throwInternal "arbiter listener: consumeInput failed") :: IO (Either SomeException ())
      either displayEx (const "") caught `shouldBe` "arbiter listener: consumeInput failed"

    it "collects no backtrace for a control-flow exception" $ do
      caught <- try throwNack :: IO (Either SomeException ())
      either (displayExceptionContext . someExceptionContext) (const "") caught
        `shouldSatisfy` (not . isInfixOf "backtrace")

  describe "worker health decoding" $ do
    it "accepts known health text" $
      workerHealthFromText "live" `shouldBe` Right Live

    it "rejects unknown health text" $
      workerHealthFromText "new_health" `shouldBe` Left "unknown worker health: new_health"

  describe "payload kinds" $ do
    it "labels a job with its constructor name" $ do
      kindOf (SendWelcome "alice") `shouldBe` Just "SendWelcome"
      kindOf (SendReceipt 1) `shouldBe` Just "SendReceipt"

    it "collects every constructor, in declaration order" $
      kindsFor @KindPayload `shouldBe` ["SendWelcome", "SendReceipt"]

    it "leaves a payload with no instance unlabelled" $ do
      kindOf (PlainPayload "x") `shouldBe` Nothing
      kindsFor @PlainPayload `shouldBe` []

    it "takes both the label and the set from a hand-written instance" $ do
      kindOf RenamedA `shouldBe` Just "a_lower"
      map Just (kindsFor @RenamedPayload) `shouldBe` map kindOf [minBound .. maxBound :: RenamedPayload]

    it "labels an envelope by the constructor of the body it wraps" $ do
      kindOf (Envelope (SendWelcome "alice")) `shouldBe` Just "sendWelcome"
      map Just (kindsFor @Envelope) `shouldBe` map (kindOf . Envelope) [SendWelcome "a", SendReceipt 1]

    it "labels a payload that has no constructors to name from its data" $ do
      kindOf (RuntimePayload (object ["type" .= ("from_data" :: Text)])) `shouldBe` Just "from_data"
      kindOf (RuntimePayload (object ["other" .= ("x" :: Text)])) `shouldBe` Nothing
      kindsFor @RuntimePayload `shouldBe` []

    it "writes the payload column from the encoding the caller built" $
      jsonParams (cScalar (jobCodec "jobs") sentinelSource) `shouldBe` [String "sentinel"]

    it "stores the label as a written column that reads back" $ do
      writeColumnNames `shouldSatisfy` elem "kind"
      codecColumns (jobRowCodec "jobs") `shouldSatisfy` elem "kind"

  describe "sql quasiquoter" $ do
    it "emits one placeholder per input hole, in order" $ do
      let jobId = 7 :: Int64
          att = 2 :: Int32
          q = [sql|UPDATE t SET a = #{att :: CInt4} WHERE id = #{jobId :: CInt8}|] :: Query ()
      sqlOf q `shouldBe` "UPDATE t SET a = ? WHERE id = ?"
      paramTags (qParams q) `shouldBe` ["int4", "int8"]

    it "reuses an in-scope identifier for each occurrence" $ do
      let jobId = 9 :: Int64
          att = 1 :: Int32
          q = [sql|WHERE id = #{jobId :: CInt8} AND attempts = #{att :: CInt4} AND parent_id = #{jobId :: CInt8}|] :: Query ()
      sqlOf q `shouldBe` "WHERE id = ? AND attempts = ? AND parent_id = ?"
      paramTags (qParams q) `shouldBe` ["int8", "int4", "int8"]

    it "picks nullable, array, and nullable-array encoders" $ do
      let a = Just (1 :: Int64)
          b = [2, 3] :: [Int32]
          c = [Nothing] :: [Maybe Text]
          q = [sql|#{a :: Maybe CInt8} #{b :: [CInt4]} #{c :: [Maybe CText]}|] :: Query ()
      paramTags (qParams q) `shouldBe` ["int8?", "int4[]", "text?[]"]

    it "interleaves the parameters of a ${} Query splice at its position" $ do
      let tbl = "jobs" :: Text
          a = 1 :: Int32
          b = 2 :: Int64
          inner = [sql|x = #{a :: CInt4}|] :: Query ()
          q = [sql|SELECT ${tbl} WHERE ${inner} AND y = #{b :: CInt8}|] :: Query ()
      sqlOf q `shouldBe` "SELECT jobs WHERE x = ? AND y = ?"
      paramTags (qParams q) `shouldBe` ["int4", "int8"]

    it "emits @{} alias names and builds a matching decoder" $ do
      let q = [sql|SELECT @{parent_id :: Maybe CInt8}, @{n :: CInt8} FROM t|] :: Query (Maybe Int64, Int64)
      sqlOf q `shouldBe` "SELECT parent_id, n FROM t"
      codecColumns (qDecode q) `shouldBe` ["parent_id", "n"]

    it "sepBy joins fragments and concatenates their parameters" $ do
      let g = "grp" :: Text
          p = 3 :: Int64
          f1 = [sql|group_key = #{g :: CText}|]
          f2 = [sql|parent_id = #{p :: CInt8}|]
          w = sepBy " AND " [f1, f2]
      sqlOf w `shouldBe` "group_key = ? AND parent_id = ?"
      paramTags (qParams w) `shouldBe` ["text", "int8"]

  describe "statement shapes" $ do
    it "locks a named job's own subtree as well as its root's" $ do
      let rendered = squished (lockJobTreesFromRootSQL "arbiter" "jobs" [1, 2])
      rendered `shouldSatisfy` T.isInfixOf "WHERE id = ANY(?) OR id IN (SELECT id FROM roots)"

    it "moves the claim token on a rate-limited defer as well as an admit" $ do
      let rendered = squish (claimJobsBatchedSQL "arbiter" "jobs" (ClaimAdmission True False) 10 1 60)
      rendered `shouldSatisfy` T.isInfixOf "claim_seq = j.claim_seq + 1,"
      rendered `shouldSatisfy` (not . T.isInfixOf "WHEN dc._admit THEN j.claim_seq + 1")

    it "reads the gated group head as two ordered runs, not one sort" $ do
      let rendered = squish (claimJobsBatchedSQL "arbiter" "jobs" (ClaimAdmission False True) 10 1 60)
      rendered `shouldSatisfy` T.isInfixOf "AND t.attempts > 0 ORDER BY t.attempts DESC, t.priority ASC, t.id ASC LIMIT 10"
      rendered `shouldSatisfy` T.isInfixOf "AND t.attempts = 0 ORDER BY t.priority ASC, t.id ASC LIMIT 10"

    it "binds the claimant, so one statement serves every claimer" $ do
      let rendered = squish (claimJobsBatchedSQL "arbiter" "jobs" (ClaimAdmission False False) 1 1 60)
      rendered `shouldSatisfy` T.isInfixOf "claimed_by = ?::uuid"

    it "measures queue ages from clock_timestamp, so none of them can go negative" $ do
      let rendered = squished (statsSQL [])
      rendered `shouldSatisfy` T.isInfixOf "clock_timestamp() - MIN(last_attempted_at)"
      rendered `shouldSatisfy` T.isInfixOf "clock_timestamp() - MIN(inserted_at)"
      rendered `shouldSatisfy` (not . T.isInfixOf "NOW() - MIN(")

    it "renders each job filter against the column its table names" $ do
      let rendered = squished (buildWhereClause allFilters)
      rendered
        `shouldBe` "WHERE claimed_by = ? AND kind = ? AND payload::text ILIKE ? ESCAPE '\\' \
                   \AND rate_limit_prefix = ? AND concurrency_prefix = ? \
                   \AND inserted_at >= ? AND inserted_at < ? \
                   \AND completed_at >= ? AND completed_at < ?"
      paramTags (qParams (buildWhereClause allFilters))
        `shouldBe` ["uuid", "text", "text", "text", "text", "ts", "ts", "ts", "ts"]

    it "matches a payload search literally, so its wildcards are not pattern syntax" $ do
      let param = qParams (buildWhereClause [FilterPayloadText "50%_off"])
      textParams param `shouldBe` ["%50\\%\\_off%"]

    it "narrows nothing when no filter is given" $
      squished (buildWhereClause []) `shouldBe` ""

    it "rolls up depth by label over the rows the stats query already reads" $ do
      let rendered = squished (statsSQL (kindsFor @KindPayload))
      rendered `shouldSatisfy` T.isInfixOf "GROUP BY GROUPING SETS ((), (kind))"
      rendered `shouldSatisfy` T.isInfixOf "jsonb_object_agg"
      T.count "FROM \"arbiter\".\"jobs\"" rendered `shouldBe` 1

    it "rolls up only the labels the payload declares" $
      squished (statsSQL (kindsFor @KindPayload))
        `shouldSatisfy` T.isInfixOf "CASE WHEN kind IN ('SendWelcome', 'SendReceipt') THEN kind END AS kind"

    it "rolls up nothing for a payload that declares no label" $
      squished (statsSQL (kindsFor @PlainPayload)) `shouldSatisfy` T.isInfixOf "NULL::text AS kind"

    it "selects stats columns in the decoder's own order" $ do
      let cols = codecColumns statsRowCodec
          rendered = squished (statsSQL (kindsFor @KindPayload))
          aliasAt c = T.length (fst (T.breakOn (" AS " <> c) rendered))
          positions = map aliasAt cols
      cols `shouldSatisfy` all (\c -> T.isInfixOf (" AS " <> c) rendered)
      positions `shouldSatisfy` \ps -> and (zipWith (<) ps (drop 1 ps))
