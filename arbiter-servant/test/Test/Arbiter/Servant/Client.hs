{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Servant.Client (spec, apiClient) where

import Arbiter.Core.QueueRegistry (Queue)
import Data.Proxy (Proxy (..))
import Servant.API ((:<|>) (..))
import Servant.Client.Core (Client, RunClient, clientIn)
import Servant.Links (allLinks, linkURI)
import Test.Hspec

import Arbiter.Servant.API (ArbiterAPI, JobsAPI (..), TableAPI (..))

type Registry = '[Queue "q" Int]

-- | Compiles only when every route has a client instance.
apiClient :: forall m. (RunClient m) => Client m (ArbiterAPI Registry)
apiClient = clientIn (Proxy @(ArbiterAPI Registry)) (Proxy @m)

spec :: Spec
spec =
  describe "servant interpreters" $
    it "link a lease route" $ do
      let queueLinks :<|> _ = allLinks (Proxy @(ArbiterAPI Registry))
      show (linkURI (ackClaimedJob (jobs queueLinks) 7)) `shouldBe` "api/v1/queues/q/jobs/7/ack"
