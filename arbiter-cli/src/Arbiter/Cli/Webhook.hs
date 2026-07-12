{-# LANGUAGE OverloadedStrings #-}

-- | Forwards each claimed job to an external HTTP endpoint, whose status decides its fate.
module Arbiter.Cli.Webhook
  ( WebhookConfig (..)
  , defaultWebhookConfig
  , newWebhookHandler
  , jobEnvelope
  ) where

import Arbiter.Core.Exceptions (throwPermanent, throwRetryable)
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Worker.Trace qualified as Trace
import Control.Exception (throwIO, try)
import Crypto.Hash.Algorithms (SHA256)
import Crypto.MAC.HMAC (HMAC, hmac)
import Data.Aeson (Value, (.=))
import Data.Aeson qualified as Aeson
import Data.ByteArray.Encoding (Base (Base16), convertToBase)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
import Data.Maybe (catMaybes)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time.Clock.POSIX (getPOSIXTime)
import Network.HTTP.Client
  ( BodyReader
  , HttpException (..)
  , Manager
  , RequestBody (RequestBodyBS, RequestBodyLBS)
  , Response
  , brReadSome
  , host
  , method
  , path
  , redirectCount
  , requestBody
  , requestHeaders
  , responseBody
  , responseStatus
  , responseTimeout
  , responseTimeoutMicro
  , withResponse
  )
import Network.HTTP.Types.Header (Header)
import Network.HTTP.Types.Status (Status, statusCode)

import Arbiter.Cli.Config (parseWebhookUrl)
import Arbiter.Cli.GenericWorker (GenericHandler)

-- | How to reach an external handler.
data WebhookConfig = WebhookConfig
  { whUrl :: String
  -- ^ Endpoint the job is POSTed to.
  , whSecret :: Maybe ByteString
  -- ^ HMAC-SHA256 signing key. See 'signHeaders'.
  , whTimeoutSecs :: Int
  -- ^ Per-request timeout. A timeout is a retryable failure.
  , whExtraHeaders :: [Header]
  -- ^ Extra headers sent on every request (e.g. an auth token).
  }

-- | A config with no signing, no extra headers, and a 30s timeout.
defaultWebhookConfig :: String -> WebhookConfig
defaultWebhookConfig url = WebhookConfig url Nothing 30 []

-- | The JSON body POSTed for a job.
jobEnvelope :: Job.JobRead Value -> Value
jobEnvelope job =
  Aeson.object
    [ "job_id" .= Job.primaryKey job
    , "queue" .= Job.queueName job
    , "attempts" .= Job.attempts job
    , "max_attempts" .= Job.maxAttempts job
    , "group_key" .= Job.groupKey job
    , "parent_id" .= Job.parentId job
    , "priority" .= Job.priority job
    , "payload" .= Job.payload job
    ]

-- | Build a handler backed by a shared 'Manager', parsing the request template once.
newWebhookHandler :: Manager -> WebhookConfig -> IO GenericHandler
newWebhookHandler mgr cfg = do
  parsed <- maybe (throwIO badUrl) pure (parseWebhookUrl (whUrl cfg))
  let baseReq =
        parsed
          { redirectCount = 0
          , method = "POST"
          , responseTimeout = responseTimeoutMicro (whTimeoutSecs cfg * 1_000_000)
          , requestHeaders =
              requestHeaders parsed
                <> [("Content-Type", "application/json")]
                <> whExtraHeaders cfg
          }
  pure $ \job -> do
    trace <- traceHeaders job
    let encoded = Aeson.encode (jobEnvelope job)
    (body, signature) <- case whSecret cfg of
      Nothing -> pure (RequestBodyLBS encoded, [])
      Just secret -> do
        let bytes = BL.toStrict encoded
        (,) (RequestBodyBS bytes) <$> signHeaders secret bytes
    let req =
          baseReq
            { requestBody = body
            , requestHeaders =
                requestHeaders baseReq
                  <> [ ("X-Arbiter-Job-Id", BC.pack (show (Job.primaryKey job)))
                     , ("X-Arbiter-Queue", TE.encodeUtf8 (Job.queueName job))
                     ]
                  <> trace
                  <> signature
            }
    either transportFailed (dispatchStatus . statusCode)
      =<< try @HttpException (withResponse req mgr drainStatus)
  where
    badUrl = InvalidUrlException (whUrl cfg) "not a valid absolute http(s) url"
    transportFailed err = throwRetryable ("webhook transport error: " <> redactedError err)

-- | The response status, draining a bounded prefix of the body so the connection is reused.
drainStatus :: Response BodyReader -> IO Status
drainStatus resp = responseStatus resp <$ brReadSome (responseBody resp) maxDrainBytes
  where
    maxDrainBytes = 64 * 1024

-- | A transport failure, without the request: its @show@ renders every header.
redactedError :: HttpException -> Text
redactedError = \case
  HttpExceptionRequest req content ->
    TE.decodeUtf8Lenient (host req <> path req) <> ": " <> T.pack (show content)
  InvalidUrlException _ reason -> "invalid webhook url: " <> T.pack reason

-- | The job's fate, from the response status.
dispatchStatus :: Int -> IO ()
dispatchStatus code
  | code >= 200 && code < 300 = pure ()
  | code == 408 || code == 429 = throwRetryable msg
  | code >= 300 && code < 400 = throwPermanent (msg <> " (redirects are not followed)")
  | code >= 400 && code < 500 = throwPermanent msg
  | otherwise = throwRetryable msg
  where
    msg = "webhook returned HTTP " <> T.pack (show code)

-- | The live consume span's W3C trace context, falling back to the job's stored one.
traceHeaders :: Job.JobRead Value -> IO [Header]
traceHeaders job = do
  ctx <- Trace.currentTraceContext
  pure $ headersFor $ case ctx of
    (Nothing, _) -> (Job.traceparent job, Job.tracestate job)
    _ -> ctx
  where
    headersFor (tp, tsv) =
      catMaybes
        [ (\v -> ("traceparent", TE.encodeUtf8 v)) <$> tp
        , (\v -> ("tracestate", TE.encodeUtf8 v)) <$> tsv
        ]

-- | Timestamp and signature headers for a signed request.
signHeaders :: ByteString -> ByteString -> IO [Header]
signHeaders secret body = do
  now <- getPOSIXTime
  let tsBytes = BC.pack (show (floor now :: Integer))
      h = hmac secret (tsBytes <> "." <> body) :: HMAC SHA256
      hex = convertToBase Base16 h :: ByteString
  pure
    [ ("X-Arbiter-Timestamp", tsBytes)
    , ("X-Arbiter-Signature", "v1=" <> hex)
    ]
