{-# LANGUAGE OverloadedStrings #-}

-- | Job row edits from a side connection, as another worker or an operator makes them.
module Arbiter.Worker.TestKit.Rows
  ( updateJob
  , reclaimJob
  , flagCancelled
  , releaseRow
  , takeClaimHolder
  , holdRowLock
  , rowCount
  ) where

import Arbiter.Core.Job.Schema (jobQueueTable)
import Arbiter.Test.Setup (execute_, withConn)
import Control.Concurrent (threadDelay)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple qualified as PG

-- | Run one UPDATE on a job row over a fresh connection.
updateJob :: ByteString -> Text -> Text -> Text -> Int64 -> IO ()
updateJob connStr schema table setClause jobId = withConn connStr $ \conn ->
  void $
    PG.execute
      conn
      (fromString (T.unpack ("UPDATE " <> jobQueueTable schema table <> " SET " <> setClause <> " WHERE id = ?")))
      (PG.Only jobId)

-- | Take the claim under a new token, as another worker's claim does.
reclaimJob :: ByteString -> Text -> Text -> Int64 -> IO ()
reclaimJob connStr schema table = updateJob connStr schema table "attempts = attempts + 1, claim_seq = claim_seq + 1"

-- | Flag a job cancelled under its lease, as a force-cancel does, without the NOTIFY.
flagCancelled :: ByteString -> Text -> Text -> Int64 -> IO ()
flagCancelled connStr schema table = updateJob connStr schema table "cancel_requested_at = NOW(), claim_seq = claim_seq + 1"

-- | Release a claim and make the row claimable now.
releaseRow :: ByteString -> Text -> Text -> Int64 -> IO ()
releaseRow connStr schema table = updateJob connStr schema table "claimed_by = NULL, not_visible_until = NULL"

-- | Take every open claim without bumping its token. The extend then reports 'Arbiter.Core.HighLevel.VisibilityUnchanged'.
takeClaimHolder :: ByteString -> Text -> Text -> IO ()
takeClaimHolder connStr schema table = withConn connStr $ \conn ->
  execute_
    conn
    ( "UPDATE "
        <> jobQueueTable schema table
        <> " SET claimed_by = '00000000-0000-0000-0000-000000000009'::uuid WHERE claimed_by IS NOT NULL"
    )

-- | Hold a row lock on one job for @micros@, as a transaction touching it would.
holdRowLock :: ByteString -> Text -> Text -> Int64 -> Int -> IO ()
holdRowLock connStr schema table jobId micros = withConn connStr $ \conn -> do
  PG.begin conn
  _ <-
    PG.query
      conn
      (fromString (T.unpack ("SELECT id FROM " <> jobQueueTable schema table <> " WHERE id = ? FOR UPDATE")))
      (PG.Only jobId)
      :: IO [PG.Only Int64]
  threadDelay micros
  PG.commit conn

-- | How many rows carry the job id.
rowCount :: ByteString -> Text -> Text -> Int64 -> IO Int
rowCount connStr schema table jobId = withConn connStr $ \conn -> do
  [PG.Only count] <-
    PG.query
      conn
      (fromString (T.unpack ("SELECT count(*)::int FROM " <> jobQueueTable schema table <> " WHERE id = ?")))
      (PG.Only jobId)
  pure count
