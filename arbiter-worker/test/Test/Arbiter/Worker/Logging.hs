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
import UnliftIO.STM (atomically, checkSTM, modifyTVar', newTVarIO, readTVar, readTVarIO)

import Arbiter.Worker.Logger
  ( LogConfig (..)
  , LogDestination (LogCallback)
  , LogLevel (..)
  , defaultLogConfig
  , hubLogFor
  , newFailureGate
  , newFailureGates
  , tryReported
  , tryReportedOn
  , (.=)
  )

spec :: Spec
spec = do
  listenerSpec
  gateSpec

-- | How long a hub failure has to reach a registrant.
reportWaitMicros :: Int
reportWaitMicros = 5_000_000

listenerSpec :: Spec
listenerSpec = describe "Listener logging" $
  it "reports a connection failure once, not once per registrant" $ do
    listener <- Listen.newPoolListener (const (throwIO (ErrorCall "connect failed")))
    first <- newTVarIO ([] :: [Text])
    second <- newTVarIO ([] :: [Text])
    Listen.withChannels listener (reportingTo first) [("arbiter_logging_test_a", const (pure ()))] $ \_ ->
      Listen.withChannels listener (reportingTo second) [("arbiter_logging_test_b", const (pure ()))] $ \_ -> do
        heard first
        readTVarIO second `shouldReturn` []
  where
    -- Every attempt reports, so a registrant that hears the hub hears repeats.
    heard seen =
      timeout reportWaitMicros (atomically (readTVar seen >>= checkSTM . (>= 2) . length))
        `shouldNotReturn` Nothing
    reportingTo seen =
      HubLog
        { hubRecovered = const (pure ())
        , hubWarn = const (pure ())
        , hubError = \msg -> atomically (modifyTVar' seen (msg :))
        , hubRepeatInterval = 0
        }

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
        lg = hubLogFor cfg
    hubError lg "arbiter listener: connect failed"
    hubRecovered lg "arbiter listener: reconnected"
    hubWarn lg "channel handler exception: boom"
    (reverse <$> readIORef ref)
      `shouldReturn` [["service"], ["service"], ["pool", "service"]]

  it "takes the hub's repeat cadence from the pool's own setting" $
    hubRepeatInterval (hubLogFor defaultLogConfig {failureRepeatInterval = 300})
      `shouldBe` 300

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
