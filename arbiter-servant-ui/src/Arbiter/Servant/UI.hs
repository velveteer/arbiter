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

-- | The dashboard's static files, embedded at compile time.
staticFiles :: [(FilePath, ByteString)]
staticFiles = $(embedDir "static")

-- | The admin UI route, a catch-all sitting behind the API routes.
type AdminUI = Raw

-- | Serve 'AdminUI' from the embedded files.
adminUIServer :: Server AdminUI
adminUIServer = Tagged adminApplication

-- | Hoisted variant for integration into a route tree using a custom monad.
adminUIServerHoisted :: forall m. (forall x. Handler x -> m x) -> ServerT AdminUI m
adminUIServerHoisted nt = hoistServer (Proxy @AdminUI) nt adminUIServer

-- | The dashboard as a standalone WAI application over the embedded files.
adminApplication :: Application
adminApplication = serveStaticApp $ \fp -> pure (lookup fp staticFiles)

-- | The dashboard served from disk, read per request, so an edit needs no rebuild.
devAdminApplication :: FilePath -> Application
devAdminApplication dir = serveStaticApp $ \fp ->
  (Just <$> BS.readFile (dir </> fp)) `catch` (\(_ :: IOException) -> pure Nothing)

-- | The application both forms share, over a resolver for the files: @index.html@ at the
-- root, everything else by relative path. A root request without its trailing slash is
-- redirected to one, so the page's relative asset paths still resolve when the UI is
-- mounted under a prefix.
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

-- | A file's content type, from its extension.
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

-- | Serve 'AdminUI' from disk.
adminUIServerDev :: FilePath -> Server AdminUI
adminUIServerDev dir = Tagged (devAdminApplication dir)

-- | Hoisted dev-mode variant for integration into a route tree using a custom monad.
adminUIServerDevHoisted
  :: forall m. (forall x. Handler x -> m x) -> FilePath -> ServerT AdminUI m
adminUIServerDevHoisted nt dir = hoistServer (Proxy @AdminUI) nt (adminUIServerDev dir)

-- | The API at @\/api\/v1@ and the admin UI at the root, in one application.
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

-- | 'arbiterAppWithAdmin' serving the UI from disk.
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
