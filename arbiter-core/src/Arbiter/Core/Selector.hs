-- | A selector reads job fields and applies policies. Evaluation returns the
-- selected result. Static inspection returns all reachable policies for
-- migration initialization.
module Arbiter.Core.Selector
  ( Selector
  , field
  , usePolicy
  , runSelector
  , collectPolicies
  , usesAnyPolicy
  , chooseWhen
  , selectByCase
  ) where

import Control.Selective (bindS, ifS)
import Control.Selective.Free (Select, getEffects, liftSelect, runSelect)
import Data.Maybe (mapMaybe)
import Data.Set (Set)
import Data.Set qualified as Set

-- | A primitive in a selector: read a field of the job, or use a policy (which
-- records it for collection and yields it for building a key).
data Prim policy payload a
  = ReadField (payload -> a)
  | UsePolicy policy a
  deriving stock (Functor)

-- | A selective description over a payload. Evaluation for a job returns an
-- @a@. Static inspection returns the reachable policies.
type Selector policy payload = Select (Prim policy payload)

-- | Read a field of the job (for predicates or key suffixes).
field :: (payload -> a) -> Selector policy payload a
field = liftSelect . ReadField

-- | Record a policy for initialization and return it for key construction.
usePolicy :: policy -> Selector policy payload policy
usePolicy p = liftSelect (UsePolicy p p)

-- | Run a selector against a concrete job to get its result.
runSelector :: forall policy payload a. payload -> Selector policy payload a -> a
runSelector job program = runSelect nt program job
  where
    nt :: forall x. Prim policy payload x -> (payload -> x)
    nt (ReadField f) = f
    nt (UsePolicy _ x) = const x

-- | All policies that a selector can reach across all branches.
collectPolicies :: (Ord policy) => Selector policy payload a -> Set policy
collectPolicies = Set.fromList . mapMaybe used . getEffects
  where
    used (UsePolicy p _) = Just p
    used (ReadField _) = Nothing

-- | Test for a reachable policy. Stop at the first match.
usesAnyPolicy :: Selector policy payload a -> Bool
usesAnyPolicy = any isUse . getEffects
  where
    isUse (UsePolicy _ _) = True
    isUse (ReadField _) = False

-- | Select between two selectors with a job predicate. Policy collection
-- inspects both branches.
chooseWhen
  :: (payload -> Bool)
  -> Selector policy payload a
  -> Selector policy payload a
  -> Selector policy payload a
chooseWhen p = ifS (field p)

-- | N-way 'chooseWhen': map the job to a finite tag, then each tag to its selector.
-- Policy collection evaluates every tag in @[minBound..maxBound]@, so the tag's
-- 'Bounded'\/'Enum' and the selector must be total over @k@.
selectByCase
  :: (Bounded k, Enum k, Eq k)
  => (payload -> k)
  -> (k -> Selector policy payload a)
  -> Selector policy payload a
selectByCase tag = bindS (field tag)
