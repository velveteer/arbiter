{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Main (main) where

import Control.Exception (SomeException, someExceptionContext, try)
import Control.Exception.Context (displayExceptionContext)
import Data.Int (Int32, Int64)
import Data.List (isInfixOf)
import Data.Text (Text)
import Data.Text qualified as T
import Test.Hspec

import Arbiter.Core.Codec (Col (..), ParamType (..), SomeParam (..), codecColumns)
import Arbiter.Core.Exceptions (displayEx, throwInternal, throwNack)
import Arbiter.Core.Job.Status (JobStatus (Ready), jobStatusFromText)
import Arbiter.Core.Sql.Claim (ClaimAdmission (..), claimJobsBatchedSQL)
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
      let rendered = squish (claimJobsBatchedSQL "arbiter" "jobs" (ClaimAdmission True False) 10 1 60 Nothing)
      rendered `shouldSatisfy` T.isInfixOf "claim_seq = j.claim_seq + 1,"
      rendered `shouldSatisfy` (not . T.isInfixOf "WHEN dc._admit THEN j.claim_seq + 1")

    it "measures queue ages from clock_timestamp, so none of them can go negative" $ do
      let rendered = squish (getQueueStatsSQL "arbiter" "jobs")
      rendered `shouldSatisfy` T.isInfixOf "clock_timestamp() - MIN(last_attempted_at)"
      rendered `shouldSatisfy` T.isInfixOf "clock_timestamp() - MIN(inserted_at)"
      rendered `shouldSatisfy` (not . T.isInfixOf "NOW() - MIN(")
