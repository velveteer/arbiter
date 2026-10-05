{-# LANGUAGE DataKinds #-}

-- Pins the dashboard's OpenAPI document. Regenerate with --accept, then npm run types.
import Arbiter.Core.QueueRegistry (QueueWithResult)
import Arbiter.Servant.OpenApi (openApiSpec)
import Data.Aeson (Value)
import Data.Aeson.Encode.Pretty (Config (..), defConfig, encodePretty')
import Test.Tasty (defaultMain)
import Test.Tasty.Golden (goldenVsString)

-- | One queue with free-form payload and result, which is how the dashboard sees every queue.
type DashboardRegistry = '[QueueWithResult "queue" Value Value]

main :: IO ()
main =
  defaultMain
    $ goldenVsString "openapi.json" "test/golden/openapi.json"
    $ pure (encodePretty' defConfig {confCompare = compare} (openApiSpec @DashboardRegistry))
