{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The job engine operations the state-machine suites generate, their
-- postconditions, and the fixtures they share.
module Arbiter.Test.StateMachine.Actions
  ( -- * Environment
    ArbiterC
  , Env (..)
  , withinSecs

    -- * Payload and fixtures
  , SMPayload (..)
  , Extras (..)
  , smWorker
  , smPayload
  , smJob
  , smChildren
  , applyExtras
  , resetSeeded

    -- * Generated operations
  , Act (..)
  , JobOp (..)
  , BatchOp (..)
  , ParentOp (..)
  , DlqOp (..)
  , ActMode (..)
  , Check
  , genAct
  , interpret

    -- * Engine helpers
  , mkInsert
  , mkInsertTree
  , mkBatchInsert
  , mkDedup
  , mkRetry
  , claimToDLQ
  , claimSpawnCount
  , runReaper
  , lapseLeases
  , isLiveInFlight
  , leaseState
  , lookupDlqId
  , withRetry
  , isRetryableError
  , isDeadlock
  ) where

import Arbiter.Core.Concurrency.Spec
  ( HasConcurrency (..)
  , concurrencyBy
  , concurrencyByCase
  , concurrencyPolicy
  , noConcurrency
  )
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema (jobQueueDLQTable, jobQueueTable)
import Arbiter.Core.Job.Types
  ( DedupKey (..)
  , JobRead
  , JobWrite
  , attempts
  , defaultGroupedJob
  , defaultJob
  , notVisibleUntil
  , primaryKey
  , setDedupKey
  , setMaxAttempts
  , setNotVisibleUntil
  , setPayload
  , setPriority
  , suspended
  )
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables, TableForPayload)
import Arbiter.Core.RateLimit.Schema (upsertRateLimitPolicyRowSQL)
import Arbiter.Core.RateLimit.Spec
  ( HasRateLimit (..)
  , RateLimitPolicy
  , limitBy
  , limitByCase
  , noLimit
  , tokenBucket
  )
import Control.Exception (SomeException, throwIO)
import Control.Monad (filterM, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import Data.List (isInfixOf)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (catMaybes, isNothing, listToMaybe)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock (NominalDiffTime, UTCTime, addUTCTime, getCurrentTime)
import Data.UUID.Types qualified as UUID
import Database.PostgreSQL.Simple (Only (..))
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.FromRow (FromRow (..), field)
import GHC.Generics (Generic)
import GHC.TypeLits (KnownSymbol)
import Hedgehog (Gen, MonadGen)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import System.Timeout (timeout)
import Test.Hspec (expectationFailure)
import UnliftIO (MonadUnliftIO, tryAny)

import Arbiter.Test.Setup (execute_, seedConcurrencyPolicySQL, toQuery)
import Arbiter.Test.StateMachine.Oracle (WithConn)

-- | Constraints the runner of the state-machine suites must satisfy.
type ArbiterC m =
  ( KnownSymbol (TableForPayload SMPayload (RegistryOf m))
  , MonadArbiter m
  , MonadUnliftIO m
  , RegistryTables (RegistryOf m)
  )

-- | The schema, the queue table, and a raw-connection accessor.
data Env = Env
  { envSchema :: Text
  , envTable :: Text
  , envWithConn :: WithConn
  }

-- | The postconditions of one operation, run after it returns. Each message is a violation.
type Check = IO [String]

-- | An id-targeted operation on one live job.
data JobOp
  = OpAck
  | OpCancel
  | OpSuspend
  | OpResume
  | OpPromote
  | OpRetry NominalDiffTime
  | OpExtend
  | OpRelease
  | OpToDLQ
  deriving stock (Eq, Show)

-- | A multi-row operation on a set of live jobs.
data BatchOp = BatchCancel | BatchToDLQ
  deriving stock (Eq, Show)

-- | An operation on a rollup finalizer.
data ParentOp = ParentCancelCascade | ParentPauseChildren | ParentResumeChildren
  deriving stock (Eq, Show)

-- | An operation on a DLQ row.
data DlqOp = DlqRetry | DlqDelete
  deriving stock (Eq, Show)

-- | One engine operation with its parameters. A target index picks a row by its
-- position in id order, modulo the rows present. A resume targets suspended non-rollup rows,
-- and a retry, extend or release targets leased rows.
data Act
  = AInsert (Maybe Text) (Maybe Int) Int (Maybe Int) Extras
  | ABatchInsert [(Maybe Text, Int)]
  | AInsertTree (Maybe Text) Int
  | ADedup Text Bool (Maybe Text) Int
  | AClaim
  | AClaimAck
  | AClaimRetry
  | AClaimCancel
  | AClaimForceCancel
  | AClaimExtend
  | AClaimRelease
  | AClaimToDLQ
  | AClaimSpawn (Maybe Text) Int
  | AOnJob JobOp Int
  | AOnJobs BatchOp [Int]
  | AOnParent ParentOp Int
  | AOnDlq DlqOp Int
  | ADeleteDLQBatch [Int]
  | AReaper
  | ACrash
  | ASuspendLapsed
  deriving stock (Eq, Show)

-- | Which suite draws the operations.
data ActMode = SequentialMode | ConcurrentMode
  deriving stock (Eq, Show)

-- | Groups for the generated jobs.
smGroups :: [Text]
smGroups = ["g" <> T.pack (show index) | index <- [1 .. 3 :: Int]]

-- | Dedup keys for the generated jobs.
smDedupKeys :: [Text]
smDedupKeys = ["d" <> T.pack (show index) | index <- [1 .. 6 :: Int]]

-- | One generated operation. Only the sequential suite holds claims across steps
-- and lapses every lease.
genAct :: ActMode -> Gen Act
genAct mode = Gen.frequency (shared <> modeOnly mode)
  where
    shared =
      [ (3, AInsert <$> genGroup <*> genDelay <*> genPrio <*> genMaxAtts <*> genExtras)
      , (1, ABatchInsert <$> Gen.list (Range.linear 2 4) ((,) <$> genGroup <*> genPrio))
      , (1, AInsertTree <$> genGroup <*> Gen.int (Range.linear 1 3))
      , (2, ADedup <$> Gen.element smDedupKeys <*> Gen.bool <*> genGroup <*> genPrio)
      , (3, pure AClaimAck)
      , (2, pure AClaimRetry)
      , (2, pure AClaimCancel)
      , (2, pure AClaimForceCancel)
      , (1, pure AClaimExtend)
      , (1, pure AClaimRelease)
      , (2, pure AClaimToDLQ)
      , (2, AClaimSpawn <$> genGroup <*> Gen.int (Range.linear 1 2))
      , (6, AOnJob <$> genJobOp <*> genPick)
      , (2, AOnJobs <$> Gen.element [BatchCancel, BatchToDLQ] <*> Gen.list (Range.linear 1 4) genPick)
      , (3, AOnParent <$> Gen.element [ParentCancelCascade, ParentPauseChildren, ParentResumeChildren] <*> genPick)
      , (2, AOnDlq <$> Gen.element [DlqRetry, DlqDelete] <*> genPick)
      , (1, ADeleteDLQBatch <$> Gen.list (Range.linear 1 3) genPick)
      , (1, pure AReaper)
      ]
    modeOnly SequentialMode =
      [(3, pure AClaim), (6, AOnJob <$> genJobOp <*> genPick), (1, pure ACrash), (1, pure ASuspendLapsed)]
    modeOnly ConcurrentMode = []
    genGroup = Gen.maybe (Gen.element smGroups)
    genDelay = Gen.maybe (Gen.int (Range.linear 30 120))
    genPrio = Gen.int (Range.linear 0 5)
    genMaxAtts = Gen.maybe (Gen.int (Range.linear 1 3))
    genPick = Gen.int (Range.linear 0 15)
    genJobOp =
      Gen.element
        [OpAck, OpCancel, OpSuspend, OpResume, OpPromote, OpRetry 0, OpRetry 30, OpExtend, OpRelease, OpToDLQ]

-- | Run an operation, returning its postconditions.
interpret :: forall sm. (ArbiterC sm) => Env -> Act -> sm Check
interpret env act = case act of
  AInsert group delay prio maxAtts extras -> unchecked (mkInsert @sm (applyExtras extras) group delay prio maxAtts)
  ABatchInsert specs -> unchecked (mkBatchInsert @sm specs)
  AInsertTree group childCount -> unchecked (mkInsertTree @sm group childCount)
  ADedup key replace group prio -> unchecked (mkDedup @sm key replace group prio)
  AClaim -> claimChecked @sm env
  AClaimAck -> unchecked (claimAck @sm)
  AClaimRetry -> unchecked (claimRetry @sm)
  AClaimCancel -> unchecked (claimCancel @sm)
  AClaimForceCancel -> unchecked (claimForceCancel @sm)
  AClaimExtend -> unchecked (claimExtend @sm)
  AClaimRelease -> unchecked (claimRelease @sm)
  AClaimToDLQ -> unchecked (claimToDLQ @sm)
  AClaimSpawn group childCount -> unchecked (claimSpawn @sm group childCount)
  AOnJob op pick -> picked (targetSql op) [pick] >>= maybe noCheck (onJob @sm env op) . listToMaybe
  AOnJobs op picks -> picked liveSql picks >>= onJobs @sm env op
  AOnParent op pick -> picked parentSql [pick] >>= maybe noCheck (onParent @sm env op) . listToMaybe
  AOnDlq op pick -> picked dlqSql [pick] >>= maybe noCheck (onDlq @sm env op) . listToMaybe
  ADeleteDLQBatch picks -> picked dlqSql picks >>= unchecked . HL.deleteDLQJobsBatch @SMPayload
  AReaper -> unchecked (runReaper @sm (envSchema env) (envTable env))
  ACrash -> unchecked (liftIO (lapseLeases (envSchema env) (envTable env) (envWithConn env)))
  ASuspendLapsed -> do
    ids <- mkClaim @sm
    liftIO (lapseLeases (envSchema env) (envTable env) (envWithConn env))
    unchecked (traverse_ (void . HL.suspendJob @SMPayload) ids)
  where
    picked sql picks = liftIO (pickRows env sql picks)
    tbl = jobQueueTable (envSchema env) (envTable env)
    liveSql = "SELECT id FROM " <> tbl <> " ORDER BY id"
    targetSql = \case
      OpResume -> "SELECT id FROM " <> tbl <> " WHERE suspended AND parent_state IS NULL ORDER BY id"
      OpRetry _ -> leasedSql
      OpExtend -> leasedSql
      OpRelease -> leasedSql
      _ -> liveSql
    leasedSql =
      "SELECT id FROM " <> tbl <> " WHERE claimed_by IS NOT NULL AND NOT suspended AND not_visible_until > NOW() ORDER BY id"
    parentSql = "SELECT id FROM " <> tbl <> " WHERE parent_state IS NOT NULL ORDER BY id"
    dlqSql = "SELECT id FROM " <> jobQueueDLQTable (envSchema env) (envTable env) <> " ORDER BY id"

noCheck :: (Applicative f) => f Check
noCheck = pure (pure [])

unchecked :: (Functor f) => f a -> f Check
unchecked = (pure [] <$)

-- | The distinct ids the indexes pick from the query's rows, in id order.
pickRows :: Env -> Text -> [Int] -> IO [Int64]
pickRows env sql picks = envWithConn env $ \conn -> do
  ids <- map fromOnly <$> PG.query_ conn (toQuery sql)
  let total = length ids
  pure (Set.toAscList (Set.fromList [jid | total > 0, pick <- picks, jid <- take 1 (drop (pick `mod` total) ids)]))

-- | The state of one main-queue row the postconditions read.
data RowState = RowState
  { rowSuspended :: Bool
  , rowLeased :: Bool
  , rowVisible :: Bool
  , rowParked :: Bool
  , rowFarOut :: Bool
  , rowRollup :: Bool
  , rowFlagged :: Bool
  , rowNotVisibleUntil :: Maybe UTCTime
  }
  deriving stock (Eq, Show)

instance FromRow RowState where
  fromRow = RowState <$> field <*> field <*> field <*> field <*> field <*> field <*> field <*> field

-- | A main-queue row, or Nothing when it is gone.
readRow :: Env -> Int64 -> IO (Maybe RowState)
readRow env jid = envWithConn env $ \conn -> listToMaybe <$> PG.query conn (toQuery sql) (Only jid)
  where
    sql =
      "SELECT suspended"
        <> ", claimed_by IS NOT NULL AND NOT suspended AND COALESCE(not_visible_until > NOW(), FALSE)"
        <> ", not_visible_until IS NULL OR not_visible_until <= NOW()"
        <> ", COALESCE(not_visible_until > NOW(), FALSE)"
        <> ", COALESCE(not_visible_until > NOW() + interval '60 seconds', FALSE)"
        <> ", parent_state IS NOT NULL"
        <> ", cancel_requested_at IS NOT NULL"
        <> ", not_visible_until FROM "
        <> jobQueueTable (envSchema env) (envTable env)
        <> " WHERE id = ?"

-- | Whether a DLQ row holds the original job id.
dlqHoldsJob :: Env -> Int64 -> IO Bool
dlqHoldsJob env jid = envWithConn env $ \conn -> do
  rows <- PG.query conn (toQuery sql) (Only jid)
  pure (rows /= ([] :: [Only Int]))
  where
    sql = "SELECT 1 FROM " <> jobQueueDLQTable (envSchema env) (envTable env) <> " WHERE job_id = ? LIMIT 1"

-- | Whether the DLQ row is present.
dlqRowExists :: Env -> Int64 -> IO Bool
dlqRowExists env dlqId = envWithConn env $ \conn -> do
  rows <- PG.query conn (toQuery sql) (Only dlqId)
  pure (rows /= ([] :: [Only Int]))
  where
    sql = "SELECT 1 FROM " <> jobQueueDLQTable (envSchema env) (envTable env) <> " WHERE id = ?"

-- | A failure message for an operation that broke its postcondition.
violation :: (Show after, Show before, Show op) => op -> [Int64] -> Int64 -> before -> after -> String
violation op ids reported before after =
  show op <> " on " <> show ids <> " returned " <> show reported <> ", before " <> show before <> ", after " <> show after

-- | Claim and hold a batch. Each claimed job is a live in-flight row and the batch fits the size.
claimChecked :: forall sm. (ArbiterC sm) => Env -> sm Check
claimChecked env = do
  ids <- mkClaim @sm
  pure $ do
    live <- traverse (isLiveInFlight (envSchema env) (envTable env) (envWithConn env)) ids
    pure $
      ["claimed " <> show ids <> ", over the batch size " <> show claimBatchSize | length ids > claimBatchSize]
        <> ["claimed job " <> show jid <> " is not in flight" | (jid, False) <- zip ids live]

-- | Run an id-targeted operation. A reported success holds in the row it leaves.
onJob :: forall sm. (ArbiterC sm) => Env -> JobOp -> Int64 -> sm Check
onJob env op jid = do
  before <- liftIO (readRow env jid)
  reported <- runJobOp
  pure $ do
    after <- readRow env jid
    inDlq <- dlqHoldsJob env jid
    pure [violation op [jid] reported before after | not (holds before reported after inDlq)]
  where
    runJobOp = case op of
      OpAck -> fetched HL.ackJob
      OpCancel -> HL.cancelJob @SMPayload jid
      OpSuspend -> HL.suspendJob @SMPayload jid
      OpResume -> HL.resumeJob @SMPayload jid
      OpPromote -> HL.promoteJob @SMPayload jid
      OpRetry backoff -> leased (HL.updateJobForRetry backoff "sm retry")
      OpExtend -> leased (HL.setVisibilityTimeout 90)
      OpRelease -> leased (HL.setVisibilityTimeout 0)
      OpToDLQ -> fetched (HL.moveToDLQ "sm dlq")
    fetched act = fetchJob @sm jid >>= maybe (pure 0) act
    leased act = fetched (\job -> isLeased job >>= \live -> if live then act job else pure 0)
    holds before reported after inDlq =
      let succeeded = reported > 0
          present check = maybe False check after
       in case op of
            OpAck -> not succeeded || isNothing after || (maybe False rowRollup before && present rowSuspended)
            OpCancel -> not succeeded || isNothing after
            OpSuspend -> not succeeded || present rowSuspended
            OpResume -> not succeeded || present (not . rowSuspended)
            OpPromote
              | maybe False (\row -> rowLeased row || rowSuspended row) before ->
                  reported == 0 && fmap rowNotVisibleUntil after == fmap rowNotVisibleUntil before
              | otherwise -> not succeeded || present rowVisible
            OpRetry backoff -> not succeeded || present (if backoff > 0 then rowParked else rowVisible)
            OpExtend -> not succeeded || present rowFarOut
            OpRelease -> not succeeded || present rowVisible
            OpToDLQ -> not succeeded || (isNothing after && inDlq)

-- | Run a multi-row operation. A batch cancel removes as many rows as it reports. A
-- batch dead-letter move leaves no row gone without its DLQ entry.
onJobs :: forall sm. (ArbiterC sm) => Env -> BatchOp -> [Int64] -> sm Check
onJobs env op ids = case op of
  BatchCancel -> do
    reported <- HL.cancelJobsBatch @SMPayload ids
    pure $ do
      gone <- filterM (fmap isNothing . readRow env) ids
      pure [violation op ids reported ids gone | fromIntegral (length gone) /= reported]
  BatchToDLQ -> do
    jobs <- catMaybes <$> traverse (fetchJob @sm) ids
    reported <- HL.moveToDLQBatch (map (,"sm batch dlq") jobs)
    pure $ do
      lost <- filterM (\jid -> (&&) <$> (isNothing <$> readRow env jid) <*> (not <$> dlqHoldsJob env jid)) ids
      pure [violation op ids reported ids lost | not (null lost)]

-- | Run an operation on a rollup finalizer. A reported cascade cancel removes or flags it.
onParent :: forall sm. (ArbiterC sm) => Env -> ParentOp -> Int64 -> sm Check
onParent env op parentId = case op of
  ParentCancelCascade -> do
    reported <- HL.cancelJobCascade @SMPayload parentId
    pure $ do
      after <- readRow env parentId
      pure [violation op [parentId] reported () after | reported > 0, maybe False (not . rowFlagged) after]
  ParentPauseChildren -> unchecked (HL.pauseChildren @SMPayload parentId)
  ParentResumeChildren -> unchecked (HL.resumeChildren @SMPayload parentId)

-- | Run an operation on a DLQ row. A restore or delete reported done removes the
-- DLQ row. A restored job is live.
onDlq :: forall sm. (ArbiterC sm) => Env -> DlqOp -> Int64 -> sm Check
onDlq env op dlqId = case op of
  DlqRetry -> do
    restored <- HL.retryFromDLQ dlqId :: sm (Maybe (JobRead SMPayload))
    pure $ case restored of
      Nothing -> pure []
      Just job -> do
        live <- readRow env (primaryKey job)
        still <- dlqRowExists env dlqId
        pure [violation op [dlqId] (primaryKey job) still live | still || isNothing live]
  DlqDelete -> do
    reported <- HL.deleteDLQJob @SMPayload dlqId
    pure $ do
      still <- dlqRowExists env dlqId
      pure [violation op [dlqId] reported () still | reported > 0, still]

-- | Whether a job is in flight at its read.
isLeased :: (MonadIO m) => JobRead SMPayload -> m Bool
isLeased job = do
  now <- liftIO getCurrentTime
  pure (attempts job > 0 && not (suspended job) && maybe False (> now) (notVisibleUntil job))

-- | Park a leased job for @backoff@ seconds.
mkRetry :: forall sm. (ArbiterC sm) => Int64 -> NominalDiffTime -> sm ()
mkRetry jid backoff =
  fetchJob @sm jid
    >>= traverse_ (\job -> isLeased job >>= \live -> when live (void (HL.updateJobForRetry backoff "sm retry" job)))

-- ---------------------------------------------------------------------------
-- Fixtures and engine helpers
-- ---------------------------------------------------------------------------

-- | Worker id for the generated claim paths.
smWorker :: UUID.UUID
smWorker = UUID.fromWords 0 0 0 7

-- | Per-job rate-limit key and concurrency policy a generated insert can carry.
data Extras = Extras (Maybe Text) (Maybe Text)
  deriving stock (Eq, Show)

-- | Seeded policies with a fixed limit.
smConcSlots :: [(Text, Int32)]
smConcSlots = [("cap-a", 1), ("cap-b", 2), ("cap-c", 3)]

-- | One suffix per policy. Each policy drives a single count-row key.
smConcSuffix :: Text
smConcSuffix = "s"

smRateKeys :: [Text]
smRateKeys = ["rk-1", "rk-2"]

genExtras :: (MonadGen g) => g Extras
genExtras = Extras <$> Gen.maybe (Gen.element (map fst smConcSlots)) <*> Gen.maybe (Gen.element smRateKeys)

applyExtras :: Extras -> JobWrite SMPayload -> JobWrite SMPayload
applyExtras (Extras concSlot rateKey) job = setPayload ((Job.payload job) {smConcSlot = concSlot, smRateKey = rateKey}) job

-- | A payload carrying optional concurrency and rate-limit keys.
data SMPayload = SMPayload
  { smMessage :: Text
  -- ^ Free text.
  , smConcSlot :: Maybe Text
  -- ^ Concurrency policy: @cap-a@ (limit 1), @cap-b@ (limit 2) or @cap-c@ (limit 3). Other values take no slot.
  , smRateKey :: Maybe Text
  -- ^ Rate-limit key: @rk-1@ or @rk-2@ on the @smrl@ bucket. Other values take no token.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

smPayload :: Text -> SMPayload
smPayload text = SMPayload text Nothing Nothing

data SMSlot = SlotNone | SlotA | SlotB | SlotC
  deriving stock (Bounded, Enum, Eq)

instance HasConcurrency SMPayload where
  concurrencyFor = concurrencyByCase slotTag slotSel
    where
      slotTag payload = case smConcSlot payload of
        Just "cap-a" -> SlotA
        Just "cap-b" -> SlotB
        Just "cap-c" -> SlotC
        _ -> SlotNone
      slotSel SlotNone = noConcurrency
      slotSel SlotA = concurrencyBy (concurrencyPolicy "cap-a" 1) (const smConcSuffix)
      slotSel SlotB = concurrencyBy (concurrencyPolicy "cap-b" 2) (const smConcSuffix)
      slotSel SlotC = concurrencyBy (concurrencyPolicy "cap-c" 3) (const smConcSuffix)

data SMRate = RateNone | RateK1 | RateK2
  deriving stock (Bounded, Enum, Eq)

-- | A binding bucket. Capacity 3 with negligible refill over a test run.
smBucket :: RateLimitPolicy
smBucket = tokenBucket "smrl" 3 60

instance HasRateLimit SMPayload where
  rateLimitFor = limitByCase rateTag rateSel
    where
      rateTag payload = case smRateKey payload of
        Just "rk-1" -> RateK1
        Just "rk-2" -> RateK2
        _ -> RateNone
      rateSel RateNone = noLimit
      rateSel RateK1 = limitBy smBucket (const "rk-1")
      rateSel RateK2 = limitBy smBucket (const "rk-2")

-- | Seed the concurrency policies. Idempotent.
seedConcurrencyPolicies :: Text -> WithConn -> IO ()
seedConcurrencyPolicies schema withConn = withConn $ \conn ->
  traverse_
    (\(policy, limit) -> traverse_ (execute_ conn) (seedConcurrencyPolicySQL schema policy limit))
    smConcSlots

-- | Seed the rate-limit policy. Idempotent.
seedRateLimitPolicies :: Text -> WithConn -> IO ()
seedRateLimitPolicies schema withConn = withConn $ \conn ->
  execute_ conn (upsertRateLimitPolicyRowSQL schema smBucket)

-- | Reset the tables, then re-seed both admission policies.
resetSeeded :: IO () -> Text -> WithConn -> IO ()
resetSeeded reset schema withConn = do
  reset
  seedConcurrencyPolicies schema withConn
  seedRateLimitPolicies schema withConn

mkInsert
  :: forall sm
   . (ArbiterC sm)
  => (JobWrite SMPayload -> JobWrite SMPayload)
  -> Maybe Text
  -> Maybe Int
  -> Int
  -> Maybe Int
  -> sm Int64
mkInsert deco group delay prio maxAtts = do
  nvu <- traverse (\secs -> liftIO (addUTCTime (fromIntegral secs) <$> getCurrentTime)) delay
  let job =
        setMaxAttempts (fromIntegral <$> maxAtts)
          $ setPriority (fromIntegral prio)
          $ setNotVisibleUntil nvu
          $ maybe (defaultJob payload) (`defaultGroupedJob` payload) group
  inserted <- HL.insertJob (deco job)
  maybe (liftIO (throwIO (userError "insert returned no row"))) (pure . primaryKey) inserted
  where
    payload = smPayload "sm"

-- | Backdate every live lease, leaving each row's holder in place.
lapseLeases :: Text -> Text -> WithConn -> IO ()
lapseLeases schema table withConn = withConn $ \conn ->
  execute_ conn $
    "UPDATE "
      <> jobQueueTable schema table
      <> " SET not_visible_until = NOW() - interval '1 second'"

claimBatchSize :: Int
claimBatchSize = 3

mkClaim :: forall sm. (ArbiterC sm) => sm [Int64]
mkClaim = do
  jobs <- HL.claimNextVisibleJobsAs claimBatchSize 60 smWorker :: sm [JobRead SMPayload]
  pure (map primaryKey jobs)

-- | True if the id names a live row that is currently in flight.
isLiveInFlight :: Text -> Text -> WithConn -> Int64 -> IO Bool
isLiveInFlight schema table withConn jid = withConn $ \conn -> do
  rows <-
    PG.query conn (toQuery sql) (Only jid)
  pure $ case rows of
    Only count : _ -> (count :: Int64) > 0
    _ -> False
  where
    sql =
      "SELECT count(*) FROM "
        <> jobQueueTable schema table
        <> " WHERE id = ? AND not_visible_until > NOW() AND NOT suspended AND attempts > 0"

-- | @(leased, suspended, nvu)@ for one row.
leaseState
  :: Text
  -> Text
  -> WithConn
  -> Int64
  -> IO (Bool, Bool, Maybe UTCTime)
leaseState schema table withConn jid = withConn $ \conn -> do
  rows <- PG.query conn (toQuery sql) (Only jid)
  pure $ case rows of
    row : _ -> row
    _ -> (False, False, Nothing)
  where
    sql =
      "SELECT claimed_by IS NOT NULL AND NOT suspended AND not_visible_until IS NOT NULL AND not_visible_until > NOW(), suspended, not_visible_until FROM "
        <> jobQueueTable schema table
        <> " WHERE id = ?"

-- | Read a job by id at this test's payload type.
fetchJob :: forall sm. (ArbiterC sm) => Int64 -> sm (Maybe (JobRead SMPayload))
fetchJob = HL.getJobById

-- | The id of the DLQ row holding the given original job id.
lookupDlqId :: Text -> Text -> WithConn -> Int64 -> IO Int64
lookupDlqId schema table withConn jid = withConn $ \conn -> do
  rows <- PG.query conn (toQuery sql) (Only jid)
  pure $ case rows of
    Only dlqId : _ -> dlqId
    _ -> jid
  where
    sql =
      "SELECT id FROM "
        <> jobQueueDLQTable schema table
        <> " WHERE job_id = ? ORDER BY id DESC LIMIT 1"

mkBatchInsert
  :: forall sm
   . (ArbiterC sm)
  => [(Maybe Text, Int)]
  -> sm ()
mkBatchInsert specs = void (HL.insertJobsBatch_ (map toJob specs))
  where
    toJob (group, prio) =
      setPriority (fromIntegral prio) $ maybe (defaultJob payload) (`defaultGroupedJob` payload) group
    payload = smPayload "sm batch"

mkInsertTree
  :: forall sm
   . (ArbiterC sm)
  => Maybe Text
  -> Int
  -> sm ()
mkInsertTree group childCount =
  void (HL.insertJobTree @SMPayload (smJob group "sm tree parent" <~~ smChildren group "sm tree child" childCount))

-- | A labelled job, grouped when the caller names a group.
smJob :: Maybe Text -> Text -> JobWrite SMPayload
smJob group lbl = maybe (defaultJob (smPayload lbl)) (`defaultGroupedJob` smPayload lbl) group

-- | @childCount@ jobs labelled from the prefix, numbered from zero.
smChildren :: Maybe Text -> Text -> Int -> NonEmpty (JobWrite SMPayload)
smChildren group prefix childCount =
  smJob group (childLabel 0) :| [smJob group (childLabel index) | index <- [1 .. childCount - 1]]
  where
    childLabel index = prefix <> " " <> T.pack (show index)

mkDedup
  :: forall sm
   . (ArbiterC sm)
  => Text
  -> Bool
  -> Maybe Text
  -> Int
  -> sm ()
mkDedup key replace group prio = void (HL.insertJob job)
  where
    dedupKey = if replace then ReplaceDuplicate key else IgnoreDuplicate key
    job =
      setPriority (fromIntegral prio)
        $ setDedupKey (Just dedupKey)
        $ maybe (defaultJob payload) (`defaultGroupedJob` payload) group
    payload = smPayload "sm dedup"

-- | A reaper tick. Drift-correct the groups summary, sweep exhausted jobs to the
-- DLQ and sweep lapsed force-cancelled jobs.
runReaper
  :: forall sm
   . (MonadArbiter sm, RegistryTables (RegistryOf sm))
  => Text
  -> Text
  -> sm ()
runReaper schema table = do
  void (HL.refreshAllGroupsFully @sm)
  void (Ops.sweepExhaustedJobs schema [table])
  void (Ops.sweepCancelledJobs schema [table])

claimThenWith :: forall sm a. (ArbiterC sm) => (JobRead SMPayload -> sm a) -> sm [a]
claimThenWith handle = do
  jobs <- HL.claimNextVisibleJobsAs 3 30 smWorker :: sm [JobRead SMPayload]
  traverse handle jobs

claimThen :: forall sm. (ArbiterC sm) => (JobRead SMPayload -> sm ()) -> sm ()
claimThen = void . claimThenWith @sm

claimAck
  , claimRetry
  , claimCancel
  , claimForceCancel
  , claimExtend
  , claimRelease
  , claimToDLQ
    :: forall sm. (ArbiterC sm) => sm ()
claimAck = claimThen @sm (void . HL.ackJob)
claimRetry = claimThen @sm (void . HL.updateJobForRetry 30 "conc retry")
claimCancel = claimThen @sm (void . HL.cancelJob @SMPayload . primaryKey)
-- Force-cancel while still leased flags the job in place. It stays present and keeps blocking its group head.
claimForceCancel = claimThen @sm (void . HL.forceCancelJob @SMPayload . primaryKey)
claimExtend = claimThen @sm (void . HL.setVisibilityTimeout 60)
claimRelease = claimThen @sm (void . HL.setVisibilityTimeout 0)
claimToDLQ = claimThen @sm (void . HL.moveToDLQ "conc dlq")

-- | Attach children to a claimed job and settle it suspended, as a spawn callback does.
claimSpawn :: forall sm. (ArbiterC sm) => Maybe Text -> Int -> sm ()
claimSpawn group childCount = void (claimSpawnCount @sm group childCount)

-- | 'claimSpawn', counting the spawns that committed.
claimSpawnCount :: forall sm. (ArbiterC sm) => Maybe Text -> Int -> sm Int
claimSpawnCount group childCount = length . filter id <$> claimThenWith @sm spawnOne
  where
    children = smChildren group "sm spawn child" childCount
    spawnOne job =
      tryAny
        ( withDbTransaction $ do
            void (HL.spawnChildren job children)
            void (HL.ackJob job)
        )
        >>= either (\err -> False <$ rethrowAbort err) (const (pure True))

-- | Let a deadlock or serialization abort through, swallowing the rest.
rethrowAbort :: (MonadIO m) => SomeException -> m ()
rethrowAbort err = when (isRetryableError err || isDeadlock err) (liftIO (throwIO err))

-- | Run an action, retrying transient serialization aborts.
withRetry :: IO () -> IO ()
withRetry act = go (5 :: Int)
  where
    go remaining = do
      outcome <- tryAny act
      case outcome of
        Right () -> pure ()
        Left err
          | remaining > 0 && isRetryableError err -> go (remaining - 1)
          | otherwise -> throwIO err

-- | Detect a transient serialization abort by the SQLSTATE in the rendered message.
-- A deadlock is a lock-order defect, so it fails the test.
isRetryableError :: SomeException -> Bool
isRetryableError err = "40001" `isInfixOf` show err

-- | Detect a deadlock abort by the SQLSTATE in the rendered message.
isDeadlock :: SomeException -> Bool
isDeadlock err = "40P01" `isInfixOf` show err

-- | Set a wall-clock time limit for a concurrent test.
withinSecs :: Int -> IO () -> IO ()
withinSecs secs act =
  timeout (secs * 1_000_000) act
    >>= maybe (expectationFailure ("timed out after " <> show secs <> "s")) pure
