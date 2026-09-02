{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE UndecidableInstances #-}

-- | A payload's variant label, stored on the job row for filtering and grouping.
module Arbiter.Core.Job.Kind
  ( HasKind (..)
  , kindFromField
  , constructorKind
  , constructorKinds
  , GKindOf (..)
  , GKindsOf (..)
  ) where

import Data.Aeson (Value (Object, String))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import GHC.Generics (C1, D1, Generic (..), M1 (..), Meta (MetaCons), Rep, V1, (:+:) (..))
import GHC.TypeLits (KnownSymbol, symbolVal)

-- | A payload's per-job variant label. Defaults to unlabelled.
class HasKind payload where
  -- | The label stored for a job.
  kindOf :: payload -> Maybe Text
  default kindOf :: (GKindOf (Rep payload), Generic payload) => payload -> Maybe Text
  kindOf = Just . constructorKind

  -- | Every label 'kindOf' can return. Empty when the set is not known.
  kindsFor :: [Text]
  default kindsFor :: (GKindsOf (Rep payload)) => [Text]
  kindsFor = constructorKinds @payload

instance {-# OVERLAPPABLE #-} HasKind payload where
  kindOf _ = Nothing
  kindsFor = []

-- | The string at a field of a JSON payload, for a payload with no constructors
-- to name. Wrap the payload in a newtype: an instance on @Value@ itself is an
-- orphan, and a call site that does not import it labels the job NULL.
--
-- @
-- newtype RuntimeJob = RuntimeJob Value
--
-- instance HasKind RuntimeJob where
--   kindOf (RuntimeJob v) = kindFromField "type" v
--   kindsFor = []
-- @
kindFromField :: Text -> Value -> Maybe Text
kindFromField field = \case
  Object o -> case KeyMap.lookup (Key.fromText field) o of
    Just (String t) -> Just t
    _ -> Nothing
  _ -> Nothing

-- | The constructor name of a value, for a payload that wraps the sum it wants
-- labelled. Needs @Generic@ on the wrapped type, not a 'HasKind' instance.
--
-- @
-- instance HasKind Envelope where
--   kindOf = Just . constructorKind . envelopePayload
--   kindsFor = constructorKinds \@EmailPayload
-- @
constructorKind :: (GKindOf (Rep a), Generic a) => a -> Text
constructorKind = gKindOf . from

-- | Every constructor name of a type, in declaration order.
constructorKinds :: forall a. (GKindsOf (Rep a)) => [Text]
constructorKinds = gKindsOf @(Rep a)

-- | The constructor name of a generic value.
class GKindOf f where
  gKindOf :: f a -> Text

instance (GKindOf f) => GKindOf (D1 d f) where
  gKindOf (M1 x) = gKindOf x

instance (GKindOf f, GKindOf g) => GKindOf (f :+: g) where
  gKindOf (L1 x) = gKindOf x
  gKindOf (R1 x) = gKindOf x

instance (KnownSymbol n) => GKindOf (C1 (MetaCons n fx s) f) where
  gKindOf _ = T.pack (symbolVal (Proxy @n))

instance GKindOf V1 where
  gKindOf v = case v of {}

-- | Every constructor name of a generic representation, in declaration order.
class GKindsOf f where
  gKindsOf :: [Text]

instance (GKindsOf f) => GKindsOf (D1 d f) where
  gKindsOf = gKindsOf @f

instance (GKindsOf f, GKindsOf g) => GKindsOf (f :+: g) where
  gKindsOf = gKindsOf @f <> gKindsOf @g

instance (KnownSymbol n) => GKindsOf (C1 (MetaCons n fx s) f) where
  gKindsOf = [T.pack (symbolVal (Proxy @n))]

instance GKindsOf V1 where
  gKindsOf = []
