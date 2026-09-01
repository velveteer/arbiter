{-# LANGUAGE OverloadedStrings #-}

-- | RTS thread labels, so an eventlog, ThreadScope session or @ghc-debug@ dump
-- assigns descriptive names to Arbiter threads.
module Arbiter.Core.Threads
  ( labelArbiterThread
  ) where

import Control.Concurrent (myThreadId)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Foldable (toList)
import Data.Text (Text)
import Data.Text qualified as T
import GHC.Conc.Sync (labelThread)

-- | Label the calling thread @arbiter:role[:queue]@. Long-lived threads only, a
-- per-job label costing an allocation on the claim path.
labelArbiterThread :: (MonadIO m) => Text -> Maybe Text -> m ()
labelArbiterThread role mQueue = liftIO $ do
  tid <- myThreadId
  labelThread tid (T.unpack (T.intercalate ":" ("arbiter" : slug role : toList mQueue)))
  where
    slug = T.replace " " "-" . T.toLower
