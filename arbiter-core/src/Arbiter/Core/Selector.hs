-- | A selector reads fields of a job and uses policies. Running it against a
-- job picks a result, while statically inspecting it collects every policy it
-- could reach (so a migration seeds exactly what a registry references).
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

-- | A selective description over a payload, yielding an @a@. Run it against a job
-- to pick the @a@. Inspect it statically to collect reachable policies.
type Selector policy payload = Select (Prim policy payload)

-- | Read a field of the job (for predicates or key suffixes).
field :: (payload -> a) -> Selector policy payload a
field = liftSelect . ReadField

-- | Use a policy: records it for seeding and yields it for building a key.
usePolicy :: policy -> Selector policy payload policy
usePolicy p = liftSelect (UsePolicy p p)

-- | Run a selector against a concrete job to get its result.
runSelector :: forall policy payload a. payload -> Selector policy payload a -> a
runSelector job program = runSelect nt program job
  where
    nt :: forall x. Prim policy payload x -> (payload -> x)
    nt (ReadField f) = f
    nt (UsePolicy _ x) = const x

-- | Every policy a selector could reach, over-approximating across both sides of
-- each choice (so seeding covers every branch).
collectPolicies :: (Ord policy) => Selector policy payload a -> Set policy
collectPolicies = Set.fromList . mapMaybe used . getEffects
  where
    used (UsePolicy p _) = Just p
    used (ReadField _) = Nothing

-- | Whether a selector could reach any policy, short-circuiting on the first one
-- without building the full policy set.
usesAnyPolicy :: Selector policy payload a -> Bool
usesAnyPolicy = any isUse . getEffects
  where
    isUse (UsePolicy _ _) = True
    isUse (ReadField _) = False

-- | Choose between two selectors by a predicate on the job. Both sides are
-- visible to policy collection, so every policy either branch could use is seeded.
chooseWhen
  :: (payload -> Bool)
  -> Selector policy payload a
  -> Selector policy payload a
  -> Selector policy payload a
chooseWhen p whenTrue whenFalse = ifS (field p) whenTrue whenFalse

-- | N-way 'chooseWhen': map the job to a finite tag, then each tag to its selector.
-- Policy collection evaluates every tag in @[minBound..maxBound]@, so the tag's
-- 'Bounded'\/'Enum' and the selector must be total over @k@.
selectByCase
  :: (Bounded k, Enum k, Eq k)
  => (payload -> k)
  -> (k -> Selector policy payload a)
  -> Selector policy payload a
selectByCase tag = bindS (field tag)
