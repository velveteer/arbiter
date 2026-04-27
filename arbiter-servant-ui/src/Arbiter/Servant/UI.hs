{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeOperators #-}

-- | Embedded admin dashboard (Bootstrap 5 + Alpine.js, compiled-in static files).
--
-- __Security:__ No built-in authentication. All queue management operations
-- (view, delete, retry) are publicly accessible. Add auth middleware before
-- exposing to untrusted networks.
--
-- = Quick Start
--
-- @
-- run port $ arbiterAppWithAdmin \@MyRegistry config
-- @
--
-- = Custom Composition
--
-- Mount the API and admin UI under a shared prefix:
--
-- @
-- type MyApp = "arbiter" :> (ArbiterAPI MyRegistry :\<|\> AdminUI) :\<|\> MyRoutes
-- run port $ serve (Proxy \@MyApp) ((arbiterServer config :\<|\> adminUIServer) :\<|\> myHandler)
-- @
--
-- The admin UI auto-discovers the API path from its own URL.
-- If it loads at @\/arbiter\/@ it finds the API at @\/arbiter\/api\/v1\/@.
module Arbiter.Servant.UI
  ( -- * Servant integration
    AdminUI
  , adminUIServer
  , adminUIServerHoisted
  , adminUIServerDev
  , adminUIServerDevHoisted

    -- * Standalone WAI app
  , adminApplication
  , devAdminApplication

    -- * Combined app helper
  , arbiterAppWithAdmin
  , arbiterAppWithAdminDev
  ) where

import Arbiter.Servant.API (ArbiterAPI)
import Arbiter.Servant.Server (ArbiterServerConfig, BuildServer, arbiterServer)
import Control.Exception (IOException, catch)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as LBS
import Data.FileEmbed (embedDir)
import Data.List (isSuffixOf)
import Data.Text qualified as T
import Network.HTTP.Types (HeaderName, status200, status301, status404)
import Network.Wai (pathInfo, rawPathInfo, responseLBS)
import Servant
import System.FilePath ((</>))

-- | All static files embedded at compile time
staticFiles :: [(FilePath, ByteString)]
staticFiles = $(embedDir "static")

-- | Servant API type for the admin UI (catch-all behind API routes)
type AdminUI = Raw

-- | Servant server for 'AdminUI'
adminUIServer :: Server AdminUI
adminUIServer = Tagged adminApplication

-- | Hoisted variant for integration into a route tree using a custom monad.
adminUIServerHoisted :: forall m. (forall x. Handler x -> m x) -> ServerT AdminUI m
adminUIServerHoisted nt = hoistServer (Proxy @AdminUI) nt adminUIServer

-- | Standalone WAI Application serving embedded static files.
--
-- Serves @index.html@ for @\/@ and other files by relative path.
adminApplication :: Application
adminApplication = serveStaticApp $ \fp -> pure (lookup fp staticFiles)

-- | Dev-mode WAI Application serving static files from disk.
--
-- Reads files on every request - no recompile needed for HTML\/JS\/CSS changes.
devAdminApplication :: FilePath -> Application
devAdminApplication dir = serveStaticApp $ \fp ->
  (Just <$> BS.readFile (dir </> fp)) `catch` (\(_ :: IOException) -> pure Nothing)

-- | Shared implementation for both embedded and dev-mode applications.
--
-- Takes a file resolver and returns a WAI 'Application' that serves
-- @index.html@ for @\/@ and looks up other files by relative path.
--
-- When the root is requested without a trailing slash, sends a 301 redirect
-- to add one. This ensures relative asset paths (e.g. @css\/dashboard.css@)
-- resolve correctly when the UI is mounted under a prefix like @\/arbiter@.
serveStaticApp :: (FilePath -> IO (Maybe ByteString)) -> Application
serveStaticApp resolveFile req sendResponse = do
  let segments = filter (not . T.null) (pathInfo req)
      path = T.intercalate "/" segments
  if T.null path && not ("/" `BS.isSuffixOf` rawPathInfo req)
    then
      sendResponse $
        responseLBS status301 [("Location", rawPathInfo req <> "/")] ""
    else do
      let filePath =
            if T.null path || path == "index.html"
              then "index.html"
              else T.unpack path
      mContent <- resolveFile filePath
      case mContent of
        Just content ->
          sendResponse $ responseLBS status200 (securityHeaders ++ [contentTypeHeader filePath]) (LBS.fromStrict content)
        Nothing ->
          sendResponse $ responseLBS status404 [("Content-Type", "text/plain")] "Not found"

-- | Security headers included on all static responses.
securityHeaders :: [(HeaderName, ByteString)]
securityHeaders =
  [ ("X-Content-Type-Options", "nosniff")
  , ("X-Frame-Options", "DENY")
  ]

-- | Infer Content-Type from file extension
contentTypeHeader :: FilePath -> (HeaderName, ByteString)
contentTypeHeader path
  | ".html" `isSuffixOf` path = ("Content-Type", "text/html; charset=utf-8")
  | ".css" `isSuffixOf` path = ("Content-Type", "text/css; charset=utf-8")
  | ".js" `isSuffixOf` path = ("Content-Type", "application/javascript; charset=utf-8")
  | ".json" `isSuffixOf` path = ("Content-Type", "application/json")
  | ".png" `isSuffixOf` path = ("Content-Type", "image/png")
  | ".svg" `isSuffixOf` path = ("Content-Type", "image/svg+xml")
  | ".ico" `isSuffixOf` path = ("Content-Type", "image/x-icon")
  | otherwise = ("Content-Type", "application/octet-stream")

-- | Dev-mode Servant server for 'AdminUI' - serves from disk.
adminUIServerDev :: FilePath -> Server AdminUI
adminUIServerDev dir = Tagged (devAdminApplication dir)

-- | Hoisted dev-mode variant for integration into a route tree using a custom monad.
adminUIServerDevHoisted
  :: forall m. (forall x. Handler x -> m x) -> FilePath -> ServerT AdminUI m
adminUIServerDevHoisted nt dir = hoistServer (Proxy @AdminUI) nt (adminUIServerDev dir)

-- | Combine arbiterApp with admin UI
--
-- Serves the API at @\/api\/v1\/...@ and admin UI at @\/@
arbiterAppWithAdmin
  :: forall registry
   . ( BuildServer registry registry
     , HasServer (ArbiterAPI registry) '[]
     )
  => ArbiterServerConfig registry
  -> Application
arbiterAppWithAdmin config =
  serve
    (Proxy @(ArbiterAPI registry :<|> AdminUI))
    (arbiterServer config :<|> adminUIServer)

-- | Like 'arbiterAppWithAdmin' but serves static files from disk for development.
arbiterAppWithAdminDev
  :: forall registry
   . ( BuildServer registry registry
     , HasServer (ArbiterAPI registry) '[]
     )
  => FilePath
  -> ArbiterServerConfig registry
  -> Application
arbiterAppWithAdminDev dir config =
  serve
    (Proxy @(ArbiterAPI registry :<|> AdminUI))
    (arbiterServer config :<|> adminUIServerDev dir)
