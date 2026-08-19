{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Cross-pool gates for coordinated maintenance and shared query results.
module Arbiter.Core.Operations.Gates
  ( runGated
  , runGatedBounded
  , runGatedShared
  , runGatedState
  , runGatedStateBounded
  , setLocalStatementTimeout
  , gateNameFor
  , Shared (..)
  ) where

import Control.Exception qualified as E
import Control.Monad (void)
import Data.Aeson (FromJSON, ToJSON, Value, parseJSON, toJSON)
import Data.Aeson.Types (parseEither, parseMaybe)
import Data.Bifunctor (second)
import Data.List (sort)
import Data.Maybe (fromMaybe, listToMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import UnliftIO (tryAny, withRunInIO)
import UnliftIO qualified as UIO

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.MonadArbiter (MonadArbiter, withDbTransaction)
import Arbiter.Core.MonadArbiter qualified as MA
import Arbiter.Core.Sql.Gates qualified as Sql
import Arbiter.Core.Sql.QQ qualified as QQ

-- ---------------------------------------------------------------------------
-- Global Gate Operations
-- ---------------------------------------------------------------------------

-- | Bound the current transaction's statements to a wall-clock limit, so a stuck op aborts at the DB rather than hanging the caller.
setLocalStatementTimeout :: (MonadArbiter m) => NominalDiffTime -> m ()
setLocalStatementTimeout limit =
  let ms = ceiling (realToFrac limit * 1000 :: Double) :: Int
      msTxt = T.pack (show ms)
   in void $
        MA.executeQuery
          [QQ.sql|SELECT set_config('statement_timeout', '${msTxt}', true) AS @{set_config :: CText}|]

-- | 'runGated' with each statement of @work@ bounded by @limit@. The bound is
-- transaction-local, which the gate transaction @work@ runs in makes effective.
runGatedBounded :: (MonadArbiter m) => SchemaName -> Text -> NominalDiffTime -> NominalDiffTime -> m a -> m (Maybe a)
runGatedBounded schemaName task interval limit work =
  runGated schemaName task interval (setLocalStatementTimeout limit >> work)

-- | Run @work@ at most once per @interval@ across every worker pool sharing
-- the same schema, keyed by @task@. Uses a watermark row in @arbiter_gates@
-- claimed via @SELECT FOR UPDATE SKIP LOCKED@, so the interval check and the
-- mutual exclusion happen in one statement. Returns @Just@ the work's result
-- if it ran, @Nothing@ if either the gate said "too recent" or another pool
-- was already running the task.
runGated
  :: (MonadArbiter m)
  => SchemaName
  -> Text
  -- ^ Task identifier (used as the gate row key).
  -> NominalDiffTime
  -- ^ Minimum interval between runs, in seconds.
  -> m a
  -- ^ Work to perform when this caller wins the gate.
  -> m (Maybe a)
runGated schemaName task interval work =
  runGatedInner schemaName task interval (const ((,Nothing) <$> work))

-- | 'runGated' where the task resumes from the state its last run left in the gate row,
-- read under the claim and written with the watermark. A payload that no longer parses
-- reads as no state.
runGatedState
  :: (FromJSON s, MonadArbiter m, ToJSON s)
  => SchemaName
  -> Text
  -> NominalDiffTime
  -> (Maybe s -> m (a, s))
  -> m (Maybe a)
runGatedState schemaName task interval work =
  runGatedInner schemaName task interval (fmap (second (Just . toJSON)) . work . (>>= parseMaybe parseJSON))

-- | 'runGatedState' with each statement of @work@ bounded by @limit@, as
-- 'runGatedBounded' bounds 'runGated'.
runGatedStateBounded
  :: (FromJSON s, MonadArbiter m, ToJSON s)
  => SchemaName
  -> Text
  -> NominalDiffTime
  -> NominalDiffTime
  -> (Maybe s -> m (a, s))
  -> m (Maybe a)
runGatedStateBounded schemaName task interval limit work =
  runGatedState schemaName task interval (\state -> setLocalStatementTimeout limit >> work state)

-- | The gate body both forms share. 'Nothing' back from @work@ leaves the row's state
-- as it was.
runGatedInner
  :: (MonadArbiter m)
  => SchemaName
  -> Text
  -> NominalDiffTime
  -> (Maybe Value -> m (a, Maybe Value))
  -> m (Maybe a)
runGatedInner schemaName task interval work = do
  _ <-
    MA.executeStatement
      (Sql.ensureGateRowSQL schemaName task)
  gateOpen <- checkGateOuter
  if not gateOpen
    then pure Nothing
    else withDbTransaction $ tryClaimGate >>= traverse ran
  where
    intervalSecs = realToFrac interval :: Double

    checkGateOuter = do
      rows <- MA.executeQuery (Sql.checkGateSQL schemaName intervalSecs task)
      pure $ fromMaybe True (listToMaybe rows)

    tryClaimGate = listToMaybe <$> MA.executeQuery (Sql.tryClaimGateSQL schemaName task intervalSecs)

    ran state = do
      (r, next) <- work state
      r <$ MA.executeStatement (maybe (Sql.bumpGateSQL schemaName task) (Sql.bumpGateStateSQL schemaName task) next)

-- | A gate name for a set of parts: the sorted set itself while it fits the gate's
-- key, an md5 digest of it beyond that.
gateNameFor :: (MonadArbiter m) => Text -> [Text] -> m Text
gateNameFor prefix parts
  | T.length joined <= maxGateNameLength = pure (prefix <> ":" <> joined)
  | otherwise = do
      rows <- MA.executeQuery (Sql.gateNameDigestSQL joined)
      pure (prefix <> ":#" <> fromMaybe joined (listToMaybe rows))
  where
    joined = T.intercalate "," (sort parts)

-- | Well under the btree index-row limit the gates table's primary key sits on.
maxGateNameLength :: Int
maxGateNameLength = 200

-- | Where a shared result came from.
data Shared a
  = -- | This caller won the gate and ran the work itself.
    Ran a
  | -- | Read from the gate, with its age in seconds.
    Published Double a
  | -- | A published result this caller could not decode, with the parse error.
    Unreadable Text
  deriving stock (Eq, Functor, Show)

-- | 'runGated' where the callers that lost the gate read the winner's published
-- result. 'Nothing' once none is fresh within @maxAge@. The winner runs @work@
-- after the gate transaction commits, so a slow scan holds neither the gate row
-- nor a read snapshot. Exclusion is by interval rather than by lock, and the interval
-- restarts from the publish. A run or publish that throws puts the watermark back, so a
-- winner that keeps failing does not keep every other caller from running. That
-- compensation is bounded by @interval@.
runGatedShared
  :: (FromJSON a, MonadArbiter m, ToJSON a)
  => SchemaName
  -> Text
  -> NominalDiffTime
  -- ^ Minimum interval between runs.
  -> NominalDiffTime
  -- ^ How long a published result stands.
  -> m a
  -> m (Maybe (Shared a))
runGatedShared schemaName task interval maxAge work =
  MA.executeQuery claimOrRead >>= maybe (pure Nothing) shared . listToMaybe
  where
    claimOrRead =
      Sql.claimOrReadGateSQL schemaName task (realToFrac interval) (realToFrac maxAge)
    shared (mClaimedAt, mPrevious, mPayload, mAge) = case (mClaimedAt, mPrevious) of
      (Just at, Just previous) -> Just . Ran <$> publish at previous
      _ -> pure (decoded <$> mPayload <*> mAge)
    -- Base onException: UnliftIO's masks the handler uninterruptibly.
    publish at previous = withRunInIO $ \run ->
      run (work >>= \a -> a <$ MA.executeStatement (Sql.setGateMetadataSQL schemaName (toJSON a) at task))
        `E.onException` run (reopen at previous)
    reopen at previous =
      void (tryAny (UIO.timeout (micros interval) (MA.executeStatement (Sql.releaseGateSQL schemaName task at previous))))
    decoded v age = either (Unreadable . (\e -> task <> " gate payload: " <> T.pack e)) (Published age) (parseEither parseJSON v)

micros :: NominalDiffTime -> Int
micros seconds = round (realToFrac seconds * 1_000_000 :: Double)
