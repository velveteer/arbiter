{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A step's input, built from the outputs of the steps before it. Applicative, not
-- monadic: a graph's shape cannot depend on a value.
module Arbiter.Workflow.Expr
  ( Ref
  , toRef
  , refName
  , Expr (..)
  , use
  , both
  , refsOf
  , evalExpr
  ) where

import Data.Aeson (FromJSON, Result (..), Value, fromJSON)
import Data.Kind (Type)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as T

import Arbiter.Workflow.Types (StepName (..))

-- | A handle to a step's output. It is a name, never a value. @s@ is the graph it
-- belongs to, so a ref cannot be read outside the definition that made it.
newtype Ref (s :: Type) a = Ref StepName
  deriving stock (Eq, Ord, Show)

-- | A handle to the output of the step with this name.
toRef :: StepName -> Ref s a
toRef = Ref

-- | The step a handle names.
refName :: Ref s a -> StepName
refName (Ref name) = name

-- | A step input over prior outputs. A free applicative over 'Ref'.
data Expr (s :: Type) a where
  Pure :: a -> Expr s a
  Var :: (FromJSON a) => Ref s a -> Expr s a
  Ap :: Expr s (x -> a) -> Expr s x -> Expr s a

instance Functor (Expr s) where
  fmap fn = Ap (Pure fn)

instance Applicative (Expr s) where
  pure = Pure
  (<*>) = Ap

-- | The output of a step, as a step input. The spelling to reach for.
use :: (FromJSON a) => Ref s a -> Expr s a
use = Var

-- | The outputs of two steps, as one input.
both :: (FromJSON a, FromJSON b) => Ref s a -> Ref s b -> Expr s (a, b)
both left right = (,) <$> use left <*> use right

-- | Every step an expression reads, in the order it reads them.
refsOf :: Expr s a -> [StepName]
refsOf (Pure _) = []
refsOf (Var ref) = [refName ref]
refsOf (Ap fn value) = refsOf fn <> refsOf value

-- | Fold an expression over the outputs the steps have stored.
evalExpr :: Map StepName Value -> Expr s a -> Either Text a
evalExpr _ (Pure value) = Right value
evalExpr stored (Var ref) =
  case Map.lookup (refName ref) stored of
    Nothing -> Left ("no output stored for step " <> stepNameText (refName ref))
    Just value -> case fromJSON value of
      Success decoded -> Right decoded
      Error err -> Left ("the output of step " <> stepNameText (refName ref) <> " does not decode: " <> T.pack err)
evalExpr stored (Ap fn value) = evalExpr stored fn <*> evalExpr stored value
