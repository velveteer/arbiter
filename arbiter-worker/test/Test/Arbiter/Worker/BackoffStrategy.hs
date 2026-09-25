module Test.Arbiter.Worker.BackoffStrategy (spec) where

import Test.Hspec (Spec, describe, it, shouldBe)

import Arbiter.Worker.BackoffStrategy (calculateBackoff, exponentialBackoff)

spec :: Spec
spec =
  describe "exponential backoff" $ do
    it "caps large attempt counts before converting to a time interval" $
      calculateBackoff (exponentialBackoff 2 300) 10_000 `shouldBe` 300
    it "returns a fractional cap exactly" $
      calculateBackoff (exponentialBackoff 2 0.3) 5 `shouldBe` 0.3
