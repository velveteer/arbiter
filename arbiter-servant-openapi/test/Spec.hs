{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

import Arbiter.Core.Concurrency.Stats (ConcurrencyPolicyUpdate (..))
import Arbiter.Core.CronSchedule (CronScheduleRow, CronScheduleUpdate (..))
import Arbiter.Core.Job.Status (JobStatus (InFlight))
import Arbiter.Core.Job.Types (JobRead, PayloadKeys (..), Stored, defaultJob)
import Arbiter.Core.Job.Types.Internal (JobRecord (..))
import Arbiter.Core.RateLimit.Stats (RateLimitPolicyUpdate (..))
import Arbiter.Servant.Types (ApiJobWithStatus (..), GroupsResponse (..))
import Data.Aeson (FromJSON, ToJSON, Value (Null, Object, String), toJSON)
import Data.Aeson.Key (toText)
import Data.Aeson.KeyMap (keys, toList)
import Data.HashMap.Strict.InsOrd qualified as InsOrd
import Data.List ((\\))
import Data.Maybe (fromMaybe, isJust)
import Data.OpenApi
  ( Reference (Reference)
  , Referenced (Inline, Ref)
  , Schema (..)
  , ToSchema (..)
  , schemaName
  , toSchema
  )
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Time (UTCTime (..), fromGregorian)
import GHC.Generics (Generic)
import Test.Tasty (defaultMain, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

import Arbiter.Servant.OpenApi ()

main :: IO ()
main =
  defaultMain $
    testGroup
      "schemas describe the encodings"
      [ testCase "generic optional fields accept null" $ nullRefused @CronScheduleRow @?= []
      , testCase "a stored optional payload accepts null" $
          acceptsNull <$> InsOrd.lookup "payload" (_schemaProperties (toSchema (Proxy @(ApiJobWithStatus (Stored (Maybe Int))))))
            @?= Just True
      , testCase "a payload that wraps an optional value accepts null" $
          ( toJSON (Wrapped Nothing)
          , acceptsNull <$> InsOrd.lookup "payload" (_schemaProperties (toSchema (Proxy @(ApiJobWithStatus (Stored Wrapped)))))
          )
            @?= (Null, Just True)
      , testCase "a stored payload that never encodes null refuses it" $
          acceptsNull <$> InsOrd.lookup "payload" (_schemaProperties (toSchema (Proxy @(ApiJobWithStatus (Stored Int)))))
            @?= Just False
      , testCase "handwritten schemas accept the nulls the encoding sends" $
          nullDrift ApiJobWithStatus {ajwsJob = job, ajwsStatus = InFlight} @?= []
      , testGroup
          "handwritten schemas name the keys the encoding sends"
          [ testCase "JobWithStatus" $ keyDrift ApiJobWithStatus {ajwsJob = job, ajwsStatus = InFlight} @?= ([], [])
          , testCase "GroupsResponse" $
              keyDrift GroupsResponse {groups = [], groupsTotal = 0, groupsOffset = 0, groupsLimit = 0} @?= ([], [])
          , testCase "RateLimitPolicyUpdate" $
              keyDrift
                RateLimitPolicyUpdate
                  { overrideMaxTokens = Just (Just 1)
                  , overrideRefillAmount = Just (Just 1)
                  , overrideInterval = Just (Just 1)
                  }
                @?= ([], [])
          , testCase "ConcurrencyPolicyUpdate" $ keyDrift ConcurrencyPolicyUpdate {overrideLimit = Just (Just 1)} @?= ([], [])
          , testCase "CronScheduleUpdate" $
              keyDrift
                CronScheduleUpdate
                  { overrideExpression = Just (Just "x")
                  , overrideOverlap = Just (Just "x")
                  , overrideTimezone = Just (Just "x")
                  , enabled = Just True
                  }
                @?= ([], [])
          ]
      ]

-- | Optional properties whose schema refuses null. Generic JSON encodes 'Nothing' as null.
nullRefused :: forall a. (ToSchema a) => [Text]
nullRefused =
  [ name
  | (name, prop) <- InsOrd.toList (_schemaProperties schema)
  , name `notElem` _schemaRequired schema
  , not (acceptsNull prop)
  ]
  where
    schema = toSchema (Proxy @a)

-- | Keys the schema declares that the encoding omits, then keys the encoding sends that the schema omits.
keyDrift :: forall a. (ToJSON a, ToSchema a) => a -> ([Text], [Text])
keyDrift sample = (declared \\ sent, sent \\ declared)
  where
    declared = InsOrd.keys (_schemaProperties (toSchema (Proxy @a)))
    sent = case toJSON sample of
      Object o -> map toText (keys o)
      _ -> []

-- | Keys the encoding sends as null whose schema refuses null.
nullDrift :: forall a. (ToJSON a, ToSchema a) => a -> [Text]
nullDrift sample = case toJSON sample of
  Object o ->
    [ toText key
    | (key, Null) <- toList o
    , not (maybe False acceptsNull (InsOrd.lookup (toText key) (_schemaProperties (toSchema (Proxy @a)))))
    ]
  _ -> []

-- | OpenAPI 3.0.3 applies @nullable@ only beside a @type@, so a reference takes a null branch.
acceptsNull :: Referenced Schema -> Bool
acceptsNull (Inline s) =
  (_schemaNullable s == Just True && isJust (_schemaType s)) || any acceptsNull (fromMaybe [] (_schemaAnyOf s))
acceptsNull (Ref (Reference name)) = Just name == schemaName (Proxy @Value)

epoch :: UTCTime
epoch = UTCTime (fromGregorian 2026 1 1) 0

-- | A payload whose encoding is null when its value is absent.
newtype Wrapped = Wrapped (Maybe Int)
  deriving stock (Generic)
  deriving anyclass (FromJSON, ToJSON, ToSchema)

job :: JobRead Value
job =
  (defaultJob (String "x"))
    { primaryKey = 1
    , queueName = "q"
    , insertedAt = epoch
    , payloadKeys = PayloadKeys {jobKind = Nothing, jobRateLimitKey = Nothing, jobConcurrencyKey = Nothing}
    }
