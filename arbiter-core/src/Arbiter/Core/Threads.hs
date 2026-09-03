{-# LANGUAGE OverloadedStrings #-}

-- | RTS thread labels for an eventlog, ThreadScope session or @ghc-debug@ dump.
module Arbiter.Core.Threads
  ( labelArbiterThread
  ) where

import Control.Concurrent (myThreadId)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Foldable (toList)
import Data.Text (Text)
import Data.Text qualified as T
import GHC.Conc.Sync (labelThread)

-- | Label the calling thread @arbiter:role[:queue]@. For long-lived threads only.
labelArbiterThread :: (MonadIO m) => Text -> Maybe Text -> m ()
labelArbiterThread role mQueue = liftIO $ do
  tid <- myThreadId
  labelThread tid (T.unpack (T.intercalate ":" ("arbiter" : slug role : toList mQueue)))
  where
    slug = T.replace " " "-" . T.toLower
