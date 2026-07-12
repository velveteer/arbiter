{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | The pull-consumer routes (claim, ack, nack, extend, fail) over a runtime queue:
-- one named at boot with a raw-JSON payload rather than declared in a compile-time
-- registry, whose admission the claim reads off the job's own columns.
module Test.Arbiter.Servant.Consumer (spec) where

import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.RateLimit.Schema (PolicyRow (..))
import Arbiter.Core.Sql.Claim (ClaimAdmission (..))
import Arbiter.Migrations
  ( AdmissionSeeds (..)
  , allTableAdmission
  , defaultMigrationConfig
  , runMigrationsTrackedForTables
  )
import Arbiter.RateLimit (Durability (Unlogged))
import Arbiter.Test.Setup (cleanupData, createSharedPool)
import Control.Monad (replicateM_, void)
import Data.Aeson (Value, decode, object, (.=))
import Data.Aeson.Types (Pair)
import Data.ByteString (ByteString)
import Data.Map.Strict qualified as Map
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.UUID.Types qualified as UUID
import Database.PostgreSQL.Simple qualified as PG
import Network.HTTP.Types (statusCode)
import Network.Wai (Application)
import Test.Hspec
import Test.Hspec.Wai

import Arbiter.Servant (ArbiterServerConfig (..), arbiterApp, initArbiterServer, runtimeQueue)
import Arbiter.Servant.Types (ApiJob (..), ClaimResponse (..))
import Test.Arbiter.Servant.API (postJSON)

-- | The schema these examples migrate into, and the one queue they serve.
consumerSchema :: Text
consumerSchema = "arbiter_servant_consumer"

consumerQueue :: Text
consumerQueue = "t"

-- | A claimant that never held the lease, so every finalizer must reject it.
forgedClaimant :: Text
forgedClaimant = "00000000-0000-0000-0000-000000000000"

-- | An id no job row has, so a finalizer reports it as missing rather than stale.
missingJobId :: Int
missingJobId = 999999

-- | A rate-limit policy (2 tokens / 10s) so the admission examples have a burst to hit.
seeds :: AdmissionSeeds
seeds = AdmissionSeeds [PolicyRow "rl" 2 2 10] [] Unlogged

-- ---------------------------------------------------------------------------
-- Request helpers
-- ---------------------------------------------------------------------------

status :: SResponse -> Int
status = statusCode . simpleStatus

queuePath :: String -> ByteString
queuePath suffix = fromString ("/api/v1/queues/" <> T.unpack consumerQueue <> suffix)

-- | @\/jobs\/\<id\>\/\<verb\>@, the route every finalizer hangs off.
jobPath :: Int -> String -> ByteString
jobPath jobId verb = queuePath ("/jobs/" <> show jobId <> "/" <> verb)

-- | The lease a claim hands back: what a consumer must quote to finalize the job.
data Lease = Lease {leaseId :: Int, leaseAtt :: Int, leaseBy :: Text}

leaseOf :: ApiJob Value -> Lease
leaseOf (ApiJob job) =
  Lease
    { leaseId = fromIntegral (Job.primaryKey job)
    , leaseAtt = fromIntegral (Job.attempts job)
    , leaseBy = foldMap UUID.toText (Job.claimedBy job)
    }

enqueue :: Value -> WaiSession st SResponse
enqueue p = postJSON (queuePath "/jobs") (object ["payload" .= p])

claim :: Int -> WaiSession st [Lease]
claim n =
  map leaseOf . foldMap claimedJobs . decode . simpleBody
    <$> postJSON (queuePath "/claim") (object ["maxJobs" .= n])

claimOne :: WaiSession st Lease
claimOne =
  claim 1 >>= \case
    lease : _ -> pure lease
    [] -> liftIO (fail "claim returned no jobs")

enqueueAndClaim :: Value -> WaiSession st Lease
enqueueAndClaim p = enqueue p >> claimOne

-- | Finalize a lease with an explicit @(attempts, claimedBy)@, yielding the status code.
finalize :: String -> [Pair] -> Lease -> Int -> Text -> WaiSession st Int
finalize verb extra lease att by =
  status <$> postJSON (jobPath (leaseId lease) verb) (object (["attempts" .= att, "claimedBy" .= by] <> extra))

ack, nack :: Lease -> Int -> Text -> WaiSession st Int
ack = finalize "ack" []
nack = finalize "nack" []

extend :: Lease -> Int -> Text -> WaiSession st Int
extend = finalize "extend" ["visibilitySecs" .= (120 :: Int)]

-- | Ack a lease, storing @result@ for the job's parent to roll up.
ackWithResult :: Lease -> Value -> WaiSession st Int
ackWithResult lease result = finalize "ack" ["result" .= result] lease (leaseAtt lease) (leaseBy lease)

-- | Report a lease as failed: retryable by default, dead-lettered when permanent.
failWith :: Lease -> Text -> Bool -> Text -> WaiSession st Int
failWith lease err permanent by =
  status
    <$> postJSON
      (jobPath (leaseId lease) "fail")
      ( object
          [ "attempts" .= leaseAtt lease
          , "claimedBy" .= by
          , "error" .= err
          , "permanent" .= permanent
          , "retryDelaySecs" .= (0 :: Int)
          ]
      )

-- ---------------------------------------------------------------------------
-- Spec
-- ---------------------------------------------------------------------------

spec :: ByteString -> Spec
spec connStr = do
  app <- runIO (consumerApp connStr (ClaimAdmission True False))
  ungated <- runIO (consumerApp connStr (ClaimAdmission False False))
  pool <- runIO (createSharedPool connStr)
  let clean = withResource pool (cleanupData consumerSchema consumerQueue)

  describe "Consumer API (runtime queue)" $ with (clean >> pure app) $ do
    describe "lease" $ do
      it "ack accepts only the real claimant, once" $ do
        lease <- enqueueAndClaim (object ["n" .= (1 :: Int)])
        ack lease (leaseAtt lease) forgedClaimant `shouldRespondCode` 409
        ack lease (leaseAtt lease) (leaseBy lease) `shouldRespondCode` 204
        ack lease (leaseAtt lease) (leaseBy lease) `shouldRespondCode` 404

      it "ack rejects a stale attempt even with the right claimant" $ do
        lease <- enqueueAndClaim (object ["n" .= (2 :: Int)])
        ack lease (leaseAtt lease + 1) (leaseBy lease) `shouldRespondCode` 409

    describe "runtime queue" $
      it "a queue named only at boot drains after produce, claim, and ack" $ do
        lease <- enqueueAndClaim (object ["hello" .= ("runtime" :: Text)])
        ack lease (leaseAtt lease) (leaseBy lease) `shouldRespondCode` 204
        claim 5 >>= \remaining -> liftIO (length remaining `shouldBe` 0)

    describe "pause" $ do
      it "a paused queue leases nothing until it resumes" $ do
        _ <- enqueue (object ["n" .= (3 :: Int)])
        _ <- postJSON (queuePath "/pause") (object [])
        claim 5 >>= \paused -> liftIO (length paused `shouldBe` 0)
        _ <- postJSON (queuePath "/resume") (object [])
        claim 5 >>= \resumed -> liftIO (length resumed `shouldBe` 1)

      -- Paused out of band, as another replica would: the claim itself has to refuse.
      it "the claim refuses a pause this server never saw" $ do
        _ <- enqueue (object ["n" .= (4 :: Int)])
        liftIO $ withResource pool (`setPausedDirectly` True)
        claim 5 >>= \paused -> liftIO (length paused `shouldBe` 0)
        liftIO $ withResource pool (`setPausedDirectly` False)
        claim 5 >>= \resumed -> liftIO (length resumed `shouldBe` 1)

    describe "nack" $ do
      it "hands a job back at once, whatever lease the claimant took" $ do
        _ <- enqueue (object ["n" .= (6 :: Int)])
        held <-
          map leaseOf . foldMap claimedJobs . decode . simpleBody
            <$> postJSON (queuePath "/claim") (object ["maxJobs" .= (1 :: Int), "visibilitySecs" .= (3600 :: Int)])
        lease <- case held of
          l : _ -> pure l
          [] -> liftIO (fail "claim returned no jobs")
        nack lease (leaseAtt lease) (leaseBy lease) `shouldRespondCode` 204
        again <- claimOne
        liftIO $ do
          leaseId again `shouldBe` leaseId lease
          leaseAtt again `shouldBe` leaseAtt lease

      -- The write carries the lease guard, so a rejection still has to say which it was.
      it "reports a forged claimant apart from a job that is gone" $ do
        lease <- enqueueAndClaim (object ["n" .= (7 :: Int)])
        nack lease (leaseAtt lease) forgedClaimant `shouldRespondCode` 409
        nack lease (leaseAtt lease + 1) (leaseBy lease) `shouldRespondCode` 409
        nack (missingLease lease) 1 (leaseBy lease) `shouldRespondCode` 404

    describe "extend" $
      it "extends only for the real claimant, at the claimed attempt" $ do
        lease <- enqueueAndClaim (object ["n" .= (8 :: Int)])
        extend lease (leaseAtt lease) forgedClaimant `shouldRespondCode` 409
        extend lease (leaseAtt lease + 1) (leaseBy lease) `shouldRespondCode` 409
        extend (missingLease lease) 1 (leaseBy lease) `shouldRespondCode` 404
        extend lease (leaseAtt lease) (leaseBy lease) `shouldRespondCode` 204

    describe "fail" $ do
      it "retries a failed job, then dead-letters it when told it is permanent" $ do
        lease <- enqueueAndClaim (object ["n" .= (4 :: Int)])
        failWith lease "downstream 503" False (leaseBy lease) `shouldRespondCode` 200
        again <- claimOne
        failWith again "malformed" True (leaseBy again) `shouldRespondCode` 200
        claim 5 >>= \remaining -> liftIO (length remaining `shouldBe` 0)

      it "rejects a failure reported by anyone but the claimant" $ do
        lease <- enqueueAndClaim (object ["n" .= (5 :: Int)])
        failWith lease "forged" False forgedClaimant `shouldRespondCode` 409

    describe "rollup" $
      it "acks a child, rolls its result up, and wakes the parent" $ do
        (parent, child) <- liftIO (withResource pool insertRollup)
        lease <- claimOne
        liftIO (leaseId lease `shouldBe` child)
        ackWithResult lease (object ["ok" .= True]) `shouldRespondCode` 204
        liftIO $ withResource pool $ \conn -> do
          rolledUpResults conn parent `shouldReturn` [object ["ok" .= True]]
          isSuspended conn parent `shouldReturn` False

    describe "admission" $ do
      it "a rate-limited claim yields at most the bucket's burst" $ do
        replicateM_ 5 . postJSON (queuePath "/jobs") $
          object ["payload" .= object [], "rateLimit" .= rateLimitRef "adm" Nothing]
        claim 10 >>= \admitted -> liftIO (length admitted `shouldBe` 2)

      -- A cost the claim clamps to 0 admits every job and debits nothing.
      it "rejects a non-positive rate-limit cost rather than exempting the job" $ do
        let enqueueCost c =
              status
                <$> postJSON
                  (queuePath "/jobs")
                  (object ["payload" .= object [], "rateLimit" .= rateLimitRef "cost" (Just c)])
        enqueueCost 0 `shouldRespondCode` 400
        enqueueCost (-1) `shouldRespondCode` 400
        enqueueCost 1 `shouldRespondCode` 200

  -- An admission read before the policy existed renders no gate, so it must leave those jobs alone.
  describe "Consumer API (admission predating the policy)" $
    with (clean >> pure ungated) $
      it "leaves a governed job alone and still claims an ungoverned one" $ do
        _ <-
          postJSON (queuePath "/jobs") $
            object ["payload" .= object [], "rateLimit" .= rateLimitRef "drift" Nothing]
        claim 10 >>= \governed -> liftIO (length governed `shouldBe` 0)
        _ <- enqueue (object ["ungoverned" .= True])
        claim 10 >>= \admitted -> liftIO (length admitted `shouldBe` 1)
  where
    shouldRespondCode act expected = act >>= \actual -> liftIO (actual `shouldBe` expected)
    missingLease lease = lease {leaseId = missingJobId}
    rateLimitRef suffix mCost =
      object $
        ["prefix" .= ("rl" :: Text), "suffix" .= (suffix :: Text)]
          <> foldMap (\c -> ["cost" .= (c :: Double)]) mCost

-- | Pause the queue behind the server's back, as another replica's admin call would.
setPausedDirectly :: PG.Connection -> Bool -> IO ()
setPausedDirectly conn paused =
  void $
    PG.execute
      conn
      ( fromString
          ( "INSERT INTO "
              <> T.unpack consumerSchema
              <> ".arbiter_queues (queue_name, paused) VALUES (?, ?) ON CONFLICT (queue_name) DO UPDATE SET paused = EXCLUDED.paused"
          )
      )
      (consumerQueue, paused)

-- | A suspended parent and the one child it is waiting on, which the HTTP producer route
-- cannot express: it never sets @parent_id@.
insertRollup :: PG.Connection -> IO (Int, Int)
insertRollup conn = do
  [PG.Only parent] <-
    PG.query_ conn (fromString ("INSERT INTO " <> qualified <> " (payload, suspended) VALUES ('{}', TRUE) RETURNING id"))
  [PG.Only child] <-
    PG.query
      conn
      (fromString ("INSERT INTO " <> qualified <> " (payload, parent_id) VALUES ('{}', ?) RETURNING id"))
      (PG.Only parent)
  pure (parent, child)
  where
    qualified = T.unpack consumerSchema <> "." <> T.unpack consumerQueue

-- | Is the job still waiting on its children?
isSuspended :: PG.Connection -> Int -> IO Bool
isSuspended conn jobId = do
  rows <-
    PG.query
      conn
      (fromString ("SELECT suspended FROM " <> T.unpack consumerSchema <> "." <> T.unpack consumerQueue <> " WHERE id = ?"))
      (PG.Only jobId)
  pure (any PG.fromOnly rows)

-- | The results a parent has rolled up so far.
rolledUpResults :: PG.Connection -> Int -> IO [Value]
rolledUpResults conn parent =
  map PG.fromOnly
    <$> PG.query
      conn
      ( fromString
          ("SELECT result FROM " <> T.unpack consumerSchema <> "." <> T.unpack consumerQueue <> "_results WHERE parent_id = ?")
      )
      (PG.Only parent)

-- | The app under test: one runtime queue, claiming under the admission it is given.
consumerApp :: ByteString -> ClaimAdmission -> IO Application
consumerApp connStr admission = do
  _ <-
    runMigrationsTrackedForTables
      connStr
      consumerSchema
      [(consumerQueue, allTableAdmission)]
      defaultMigrationConfig
      seeds
  cfg <- initArbiterServer (Proxy @'[]) connStr consumerSchema
  pure $ arbiterApp cfg {serverQueues = Map.singleton consumerQueue (runtimeQueue admission)}
