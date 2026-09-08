{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QualifiedDo #-}
{-# LANGUAGE TypeFamilies #-}

-- | The worked example from the guide, kept compiling.
module Test.Arbiter.Workflow.Example
  ( spec
  , fulfilOrder
  , notifyCustomer
  , definitions
  , paymentHandler
  , runPayments
  , placeOrder
  , carrierWebhook
  ) where

import Arbiter.Core.Job.Types (JobWrite, defaultJob, payload, setGroupKey, setMaxAttempts)
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.QueueRegistry (QueueWithResult)
import Arbiter.Simple (SimpleDb, SimpleEnv, runSimpleDb)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.Config (transactionalWorkerConfig)
import Data.Aeson (FromJSON, ToJSON, toJSON)
import Data.Text (Text)
import Data.Text qualified as T
import GHC.Generics (Generic)
import Test.Hspec

import Arbiter.Workflow
import Arbiter.Workflow.Graph (Node (..))
import Arbiter.Workflow.Graph qualified as Graph
import Arbiter.Workflow.Interpret (Materialized (..), materialize)

data Order = Order
  { orderRef :: Text
  , customer :: Text
  , orderLines :: [Line]
  , shipTo :: Text
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

data Line = Line {sku :: Text, quantity :: Int}
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

data Reservation = Reservation {holdRef :: Text, heldSku :: Text}
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

data Payment = Charge Text Int | Refund Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype Stock = Hold Line
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

data Dispatch = Ship Text [Text] Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

data Notice = Email Text Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type Fulfilment =
  '[ QueueWithResult "payment_jobs" Payment Text
   , QueueWithResult "stock_jobs" Stock Reservation
   , QueueWithResult "dispatch_jobs" Dispatch Text
   , QueueWithResult "notice_jobs" Notice Text
   ]

-- | Tell the customer something. Both arms of the branch below embed it.
notifyCustomer :: Workflow Fulfilment (Text, Text) Text
notifyCustomer = Workflow "notify-customer" 1 $ \request ->
  step "send" (uncurry Email) (use request)

fulfilOrder :: Workflow Fulfilment Order Text
fulfilOrder = Workflow "fulfil-order" 1 $ \order -> Graph.do
  paid <- stepWith "charge" chargeFor (use order)

  held <- forEach (orderLines <$> use order) $ \line ->
    step "hold" Hold (use line)

  branch
    (shippable <$> use order <*> use held)
    ( \ready -> Graph.do
        tracking <- step "ship" shipmentFor (use ready)
        arrival <- signal "carrier-delivered" (72 * 3600)
        embed "say-it-shipped" notifyCustomer $ do
          shipment <- use ready
          reference <- use tracking
          landed <- use arrival
          pure (buyerOf shipment, "Order shipped as " <> reference <> ", " <> landed)
    )
    ( \short -> Graph.do
        refunded <- step "refund" (Refund . snd) (use short)
        embed "say-we-refunded" notifyCustomer $ do
          shortfall <- use short
          reference <- use refunded
          charged <- use paid
          pure (fst shortfall, "Out of stock, refunded " <> charged <> " as " <> reference)
    )

-- | One charge at a time per customer, and money is not retried forever.
chargeFor :: Order -> JobWrite Payment
chargeFor order =
  setGroupKey (Just (customer order))
    . setMaxAttempts (Just 3)
    $ defaultJob (Charge (customer order) (sum (map quantity (orderLines order))))

buyerOf :: (Text, Text, [Reservation]) -> Text
buyerOf (who, _, _) = who

shipmentFor :: (Text, Text, [Reservation]) -> Dispatch
shipmentFor (_, reference, holds) = Ship reference (map holdRef holds) "as ordered"

-- | Everything held, or not.
shippable :: Order -> [Reservation] -> Either (Text, Text, [Reservation]) (Text, Text)
shippable order holds
  | length holds == length (orderLines order) = Left (customer order, orderRef order, holds)
  | otherwise = Right (customer order, orderRef order)

definitions :: WorkflowRegistry Fulfilment
definitions = workflows [workflow fulfilOrder, workflow notifyCustomer]

type App = SimpleDb Fulfilment IO

paymentHandler :: JobHandler App Payment Text
paymentHandler _conn job = case payload job of
  Charge who amount -> pure ("ch_" <> who <> "_" <> T.pack (show amount))
  Refund reference -> pure ("re_" <> reference)

runPayments :: SimpleEnv Fulfilment -> IO ()
runPayments env = do
  config <- transactionalWorkerConfig 4 paymentHandler
  runSimpleDb env (runWorkerPool (withWorkflows definitions config))

placeOrder :: Order -> App (Either Text RunId)
placeOrder = startWorkflow definitions fulfilOrder

carrierWebhook :: RunId -> Text -> App (Either Text SettleOutcome)
carrierWebhook runId note = sendWorkflowSignal definitions runId "carrier-delivered" (toJSON note)

spec :: Spec
spec = describe "the guide's worked example" $ do
  it "renders both arms and one node per continuation" $ do
    rendered <- either (fail . T.unpack) pure (render fulfilOrder)
    map (stepNameText . fst) (renderedNodes rendered) `shouldBe` exampleNodes

  it "gives every step of a run its queue" $ do
    built <- either (fail . T.unpack) pure (materialize notifyCustomer (toJSON ("a" :: Text, "b" :: Text)) mempty)
    map nodeQueue (materializedNodes built) `shouldBe` [Nothing, Just "notice_jobs"]

exampleNodes :: [Text]
exampleNodes =
  [ "input"
  , "charge"
  , "expand@0"
  , "expand@0.merge"
  , "branch@0"
  , "branch@0.left"
  , "branch@0.left.ship"
  , "branch@0.left.signal@0"
  , "branch@0.left.say-it-shipped"
  , "branch@0.left.say-it-shipped.send"
  , "branch@0.right"
  , "branch@0.right.refund"
  , "branch@0.right.say-we-refunded"
  , "branch@0.right.say-we-refunded.send"
  ]
