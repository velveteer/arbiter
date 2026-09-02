{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Worker.Logging (spec) where

import Arbiter.Core.Listen (HubLog (..))
import Arbiter.Core.Listen qualified as Listen
import Control.Exception (ErrorCall (..), throwIO)
import Control.Monad (void)
import Data.Foldable (traverse_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Text (Text)
import System.Timeout (timeout)
import Test.Hspec
import UnliftIO.STM (atomically, checkSTM, modifyTVar', newTVarIO, readTVar)

import Arbiter.Worker.Logger
  ( LogConfig (..)
  , LogDestination (LogCallback)
  , LogLevel (..)
  , defaultLogConfig
  , hubLogFor
  , newFailureGate
  , newFailureGates
  , sharedHubLogFor
  , tryReported
  , tryReportedOn
  , (.=)
  )

spec :: Spec
spec = do
  listenerSpec
  gateSpec

-- | How long a hub failure has to reach the registrant.
reportWaitMicros :: Int
reportWaitMicros = 2_000_000

listenerSpec :: Spec
listenerSpec = describe "Listener logging" $
  it "reports a connection failure to every registrant when the listener has no logger" $ do
    reported <- newTVarIO ([] :: [Text])
    listener <- Listen.newPoolListener (const (throwIO (ErrorCall "connect failed")))
    let lg =
          HubLog
            { hubInfo = const (pure ())
            , hubWarn = const (pure ())
            , hubError = \msg -> atomically (modifyTVar' reported (msg :))
            }
    Listen.withChannels listener lg [("arbiter_logging_test", const (pure ()))] $ \_ ->
      timeout reportWaitMicros (atomically (readTVar reported >>= checkSTM . not . null))
        `shouldNotReturn` Nothing

gateSpec :: Spec
gateSpec = describe "Failure gates" $ do
  it "logs the first failure and stays quiet while it persists" $ do
    (cfg, readLines) <- capturingConfig
    gate <- newFailureGate
    traverse_ (const (attempt cfg gate (throwIO (ErrorCall "boom")))) [1 :: Int .. 5]
    readLines `shouldReturn` [(Error, "Claim failed: boom")]

  it "logs the recovery, then re-arms for the next outage" $ do
    (cfg, readLines) <- capturingConfig
    gate <- newFailureGate
    attempt cfg gate (throwIO (ErrorCall "boom"))
    attempt cfg gate (pure ())
    attempt cfg gate (pure ())
    attempt cfg gate (throwIO (ErrorCall "again"))
    readLines
      `shouldReturn` [ (Error, "Claim failed: boom")
                     , (Info, "Claim recovered")
                     , (Error, "Claim failed: again")
                     ]

  it "reports the recovery to a log that only takes warnings" $ do
    (cfg, readLines) <- capturingConfig
    gate <- newFailureGate
    let quiet = cfg {minLogLevel = Warning}
    attempt quiet gate (throwIO (ErrorCall "boom"))
    attempt quiet gate (pure ())
    readLines
      `shouldReturn` [ (Error, "Claim failed: boom")
                     , (Warning, "Claim recovered")
                     ]

  it "logs a failure that reads differently from the one it holds" $ do
    (cfg, readLines) <- capturingConfig
    gate <- newFailureGate
    attempt cfg gate (throwIO (ErrorCall "boom"))
    attempt cfg gate (throwIO (ErrorCall "boom"))
    attempt cfg gate (throwIO (ErrorCall "bang"))
    attempt cfg gate (throwIO (ErrorCall "bang"))
    attempt cfg gate (pure ())
    readLines
      `shouldReturn` [ (Error, "Claim failed: boom")
                     , (Error, "Claim failed: bang")
                     , (Info, "Claim recovered")
                     ]

  it "says nothing while the action keeps succeeding" $ do
    (cfg, readLines) <- capturingConfig
    gate <- newFailureGate
    traverse_ (const (attempt cfg gate (pure ()))) [1 :: Int .. 3]
    readLines `shouldReturn` []
  it "says so again once the repeat interval has passed" $ do
    (cfg, readLines) <- capturingConfig
    gate <- newFailureGate
    let insistent = cfg {failureRepeatInterval = 0}
    traverse_ (const (attempt insistent gate (throwIO (ErrorCall "boom")))) [1 :: Int .. 3]
    attempt insistent gate (pure ())
    readLines
      `shouldReturn` [ (Error, "Claim failed: boom")
                     , (Error, "Claim failed: boom")
                     , (Error, "Claim failed: boom")
                     , (Info, "Claim recovered")
                     ]

  it "drops the pool's context on hub-wide messages and keeps it on the pool's own" $ do
    ref <- newIORef []
    let cfg =
          defaultLogConfig
            { minLogLevel = Debug
            , logDestination = LogCallback $ \_ _ ctx -> append ref (map fst ctx)
            , additionalContext = pure ["service" .= ("checkout" :: Text)]
            , identityContext = ["pool" .= ("email_queue" :: Text)]
            }
    hubError (sharedHubLogFor cfg) "arbiter listener: connect failed"
    hubWarn (hubLogFor cfg) "channel handler exception: boom"
    (reverse <$> readIORef ref) `shouldReturn` [["service"], ["pool", "service"]]

  it "gates each subject on its own" $ do
    (cfg, readLines) <- capturingConfig
    gates <- newFailureGates
    let try_ subject action = void (tryReportedOn cfg Error gates subject action)
    try_ "Scan" (throwIO (ErrorCall "boom"))
    try_ "Scan" (throwIO (ErrorCall "boom"))
    try_ "Tick" (throwIO (ErrorCall "bang"))
    try_ "Scan" (pure ())
    readLines
      `shouldReturn` [ (Error, "Scan failed: boom")
                     , (Error, "Tick failed: bang")
                     , (Info, "Scan recovered")
                     ]
  where
    attempt cfg gate action = void (tryReported cfg Error gate "Claim" action)

-- | A config that appends every emitted line to a list, and the reader for it.
capturingConfig :: IO (LogConfig, IO [(LogLevel, Text)])
capturingConfig = do
  ref <- newIORef []
  let cfg =
        defaultLogConfig
          { minLogLevel = Debug
          , logDestination = LogCallback $ \level msg _ -> append ref (level, msg)
          }
  pure (cfg, reverse <$> readIORef ref)

append :: IORef [a] -> a -> IO ()
append ref x = atomicModifyIORef' ref $ \xs -> (x : xs, ())
