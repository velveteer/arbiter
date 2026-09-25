{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE UndecidableInstances #-}

-- | A payload's variant label, stored on the job row for filtering and grouping.
module Arbiter.Core.Job.Kind
  ( HasKind (..)
  , constructorKind
  , constructorKinds
  , GKind (..)
  ) where

import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import GHC.Generics (C1, D1, Generic (..), M1 (..), Meta (MetaCons), Rep, V1, (:+:) (..))
import GHC.TypeLits (KnownSymbol, symbolVal)

-- | A payload's per-job variant label. Defaults to unlabelled.
class HasKind payload where
  -- | The label stored for a job.
  kindOf :: payload -> Maybe Text
  default kindOf :: (GKind (Rep payload), Generic payload) => payload -> Maybe Text
  kindOf = Just . constructorKind

  -- | Every label 'kindOf' can return. Empty when the set is not known.
  kindsFor :: [Text]
  default kindsFor :: (GKind (Rep payload)) => [Text]
  kindsFor = constructorKinds @payload

instance {-# OVERLAPPABLE #-} HasKind payload where
  kindOf _ = Nothing
  kindsFor = []

-- | Constructor name of a wrapped sum. Requires @Generic@ on the wrapped type.
--
-- @
-- instance HasKind Envelope where
--   kindOf = Just . constructorKind . envelopePayload
--   kindsFor = constructorKinds \@EmailPayload
-- @
constructorKind :: (GKind (Rep a), Generic a) => a -> Text
constructorKind = gKindOf . from

-- | Every constructor name of a type, in declaration order.
constructorKinds :: forall a. (GKind (Rep a)) => [Text]
constructorKinds = gKindsOf @(Rep a)

-- | Constructor names of a generic representation, in declaration order.
class GKind f where
  gKindOf :: f a -> Text
  gKindsOf :: [Text]

instance (GKind f) => GKind (D1 d f) where
  gKindOf (M1 inner) = gKindOf inner
  gKindsOf = gKindsOf @f

instance (GKind f, GKind g) => GKind (f :+: g) where
  gKindOf (L1 inner) = gKindOf inner
  gKindOf (R1 inner) = gKindOf inner
  gKindsOf = gKindsOf @f <> gKindsOf @g

instance (KnownSymbol n) => GKind (C1 (MetaCons n fx s) f) where
  gKindOf _ = T.pack (symbolVal (Proxy @n))
  gKindsOf = [T.pack (symbolVal (Proxy @n))]

instance GKind V1 where
  gKindOf empty = case empty of {}
  gKindsOf = []
