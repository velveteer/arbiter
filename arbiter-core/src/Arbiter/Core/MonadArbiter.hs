{-# LANGUAGE TypeFamilies #-}

module Arbiter.Core.MonadArbiter
  ( MonadArbiter (..)
  , Params
  , SomeParam (..)
  , ParamType (..)
  , JobHandler
  , BatchedJobHandler
  ) where

import Control.Monad.IO.Class (MonadIO)
import Data.Int (Int64)
import Data.Kind (Type)
import Data.List.NonEmpty (NonEmpty)
import Data.Text (Text)

import Arbiter.Core.Codec (ParamType (..), Params, RowCodec, SomeParam (..))
import Arbiter.Core.Job.Types (JobRead)

class (Monad m, MonadIO m) => MonadArbiter m where
  type Handler m jobs result :: Type

  executeQuery :: Text -> Params -> RowCodec a -> m [a]

  executeStatement :: Text -> Params -> m Int64

  withDbTransaction :: m a -> m a

  runHandlerWithConnection :: Handler m jobs result -> jobs -> m result

type JobHandler m payload result = Handler m (JobRead payload) result
type BatchedJobHandler m payload result = Handler m (NonEmpty (JobRead payload)) result
