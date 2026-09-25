module Test.Arbiter.Worker.BackoffStrategy (spec) where

import Test.Hspec (Spec, describe, it, shouldBe)

import Arbiter.Worker.BackoffStrategy (calculateBackoff, exponentialBackoff)

spec :: Spec
spec =
  describe "exponential backoff" $
    it "caps large attempt counts before converting to a time interval" $
      calculateBackoff (exponentialBackoff 2 300) 10_000 `shouldBe` 300
