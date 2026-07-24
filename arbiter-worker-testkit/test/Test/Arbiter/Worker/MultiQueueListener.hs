{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Multi-queue shared-listener tests on the postgresql-simple backend.
module Test.Arbiter.Worker.MultiQueueListener (spec) where

import Arbiter.Core.JobResult (HasJobResult)
import Arbiter.Simple (createSimpleEnv, destroySimpleEnv, runSimpleDb)
import Arbiter.Test.Setup (addQueueTable, cleanupData, setupOnce)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import GHC.Generics (Generic)
import Test.Hspec (Spec, beforeAll)

import Arbiter.Worker.TestKit (multiQueueListenerSpec)

newtype QueueAPayload = QueueAPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, HasJobResult, ToJSON)

newtype QueueBPayload = QueueBPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, HasJobResult, ToJSON)

type MultiQRegistry =
  '[ '("mq_listen_a", QueueAPayload)
   , '("mq_listen_b", QueueBPayload)
   ]

schemaName :: Text
schemaName = "mq_listen_test"

tableA :: Text
tableA = "mq_listen_a"

tableB :: Text
tableB = "mq_listen_b"

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr schemaName tableA True >> addQueueTable connStr schemaName tableB True) $
    multiQueueListenerSpec @QueueAPayload @QueueBPayload @MultiQRegistry
      tableA
      tableB
      connStr
      QueueAPayload
      QueueBPayload
      mkEnv
      destroySimpleEnv
      (\f _conn job -> f job)
      runSimpleDb
  where
    mkEnv = do
      conn <- connectPostgreSQL connStr
      cleanupData schemaName tableA conn
      cleanupData schemaName tableB conn
      close conn
      createSimpleEnv (Proxy @MultiQRegistry) connStr schemaName
