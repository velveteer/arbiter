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
import Data.ByteString.Char8 qualified as BS8
import Data.ByteString.Lazy qualified as LBS
import Data.FileEmbed (embedDir)
import Data.Hashable (hash)
import Data.List (isSuffixOf)
import Data.Text (Text)
import Data.Text qualified as T
import Network.HTTP.Types (HeaderName, status200, status301, status404)
import Network.Wai (pathInfo, rawPathInfo, responseLBS)
import Numeric (showHex)
import Servant
import System.FilePath ((</>))

-- | The dashboard's static files, embedded at compile time.
staticFiles :: [(FilePath, ByteString)]
staticFiles = $(embedDir "static")

-- | Embedded files with the build version in each asset URL. Versioned assets
-- can use an immutable cache. A new build produces new URLs.
--
-- Replace a file path only when it is a complete attribute value. Bound
-- attributes contain expressions and do not match these paths.
versionedFiles :: [(FilePath, ByteString)]
versionedFiles = map stampPage staticFiles
  where
    stampPage (path, content)
      | path == indexPath = (path, foldr (stamp . fst) content staticFiles)
      | otherwise = (path, content)
    stamp path = replaceAll (attribute path) (attribute (versionedPath path))
    attribute value = BS8.pack (value <> "\"")

-- | Put an asset below a path segment that identifies the build. Path segments
-- give reverse proxies and build tools stable cache keys.
versionedPath :: FilePath -> FilePath
versionedPath path = T.unpack versionPrefix <> "/" <> BS8.unpack buildVersion <> "/" <> path

-- | The segment that introduces a version.
versionPrefix :: Text
versionPrefix = "v"

-- | Remove a version prefix and report if one was present. The resolver ignores
-- the version value.
stripVersion :: [Text] -> (Bool, [Text])
stripVersion (prefix : _version : rest) | prefix == versionPrefix = (True, rest)
stripVersion segments = (False, segments)

-- | Dashboard entry point. It has no version and is fetched again on each load.
indexPath :: FilePath
indexPath = "index.html"

-- | One version for all files embedded in the binary.
buildVersion :: ByteString
buildVersion = BS8.pack (showHex (fromIntegral (hash (map snd staticFiles)) :: Word) "")

replaceAll :: ByteString -> ByteString -> ByteString -> ByteString
replaceAll needle new haystack
  | BS.null found = before
  | otherwise = before <> new <> replaceAll needle new (BS.drop (BS.length needle) found)
  where
    (before, found) = BS.breakSubstring needle haystack

-- | The admin UI route, a catch-all sitting behind the API routes.
type AdminUI = Raw

-- | Serve 'AdminUI' from the embedded files.
adminUIServer :: Server AdminUI
adminUIServer = Tagged adminApplication

-- | Hoisted variant for integration into a route tree using a custom monad.
adminUIServerHoisted :: forall m. (forall x. Handler x -> m x) -> ServerT AdminUI m
adminUIServerHoisted natTrans = hoistServer (Proxy @AdminUI) natTrans adminUIServer

-- | The dashboard as a standalone WAI application over the embedded files.
adminApplication :: Application
adminApplication = serveStaticApp Versioned $ \filePath -> pure (lookup filePath versionedFiles)

-- | The dashboard served from disk, read per request.
devAdminApplication :: FilePath -> Application
devAdminApplication dir = serveStaticApp AlwaysFresh $ \filePath ->
  (Just <$> BS.readFile (dir </> filePath)) `catch` (\(_ :: IOException) -> pure Nothing)

-- | Serve files through a resolver. The root returns @index.html@ and other
-- paths return the named file. Redirect a root path without a trailing slash.
--
-- Cache versioned assets as immutable. Do not cache @index.html@. A version
-- prefix is valid for asset paths.
serveStaticApp :: Caching -> (FilePath -> IO (Maybe ByteString)) -> Application
serveStaticApp caching resolveFile req sendResponse = sendResponse =<< reply
  where
    (versioned, segments) = stripVersion (filter (not . T.null) (pathInfo req))
    path = T.intercalate "/" segments
    isIndex = T.null path || path == T.pack indexPath
    filePath = if isIndex then indexPath else T.unpack path
    reply
      | isIndex && versioned = pure notFound
      | T.null path && not ("/" `BS.isSuffixOf` rawPathInfo req) =
          pure $ responseLBS status301 [("Location", rawPathInfo req <> "/")] ""
      | otherwise = maybe notFound found <$> resolveFile filePath
    found content =
      responseLBS
        status200
        (securityHeaders ++ cacheHeaders caching versioned ++ [contentTypeHeader filePath])
        (LBS.fromStrict content)
    notFound = responseLBS status404 [("Content-Type", "text/plain")] "Not found"

-- | Response caching mode. Embedded, versioned files are immutable while the
-- server runs. Files read from disk can change between requests.
data Caching = Versioned | AlwaysFresh

-- | Cache versioned asset responses as immutable. Require revalidation for
-- unversioned responses. Relative URLs in a stylesheet inherit its versioned path.
cacheHeaders :: Caching -> Bool -> [(HeaderName, ByteString)]
cacheHeaders Versioned True =
  [cacheControl ("public, max-age=" <> BS8.pack (show immutableMaxAge) <> ", immutable")]
cacheHeaders _ _ = [cacheControl "no-cache"]

cacheControl :: ByteString -> (HeaderName, ByteString)
cacheControl = (,) "Cache-Control"

-- | One-year cache duration for a versioned asset.
immutableMaxAge :: Int
immutableMaxAge = 31536000

-- | Security headers for all static responses. The dashboard uses resources
-- from its own origin.
securityHeaders :: [(HeaderName, ByteString)]
securityHeaders =
  [ ("X-Content-Type-Options", "nosniff")
  , ("X-Frame-Options", "DENY")
  , ("Referrer-Policy", "same-origin")
  , ("Content-Security-Policy", contentSecurityPolicy)
  ]

-- | Content security policy for bundled dashboard assets. @connect-src@ permits
-- same-origin API and event-stream requests. Alpine requires @unsafe-eval@ for
-- attribute expressions. Style bindings require @unsafe-inline@.
contentSecurityPolicy :: ByteString
contentSecurityPolicy =
  BS.intercalate
    "; "
    [ "default-src 'none'"
    , "script-src 'self' 'unsafe-eval'"
    , "style-src 'self' 'unsafe-inline'"
    , "img-src 'self' data:"
    , "font-src 'self'"
    , "connect-src 'self'"
    , "form-action 'none'"
    , "base-uri 'none'"
    , "frame-ancestors 'none'"
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
  | ".woff2" `isSuffixOf` path = ("Content-Type", "font/woff2")
  | ".woff" `isSuffixOf` path = ("Content-Type", "font/woff")
  | otherwise = ("Content-Type", "application/octet-stream")

-- | Serve 'AdminUI' from disk.
adminUIServerDev :: FilePath -> Server AdminUI
adminUIServerDev dir = Tagged (devAdminApplication dir)

-- | Hoisted dev-mode variant for integration into a route tree using a custom monad.
adminUIServerDevHoisted
  :: forall m. (forall x. Handler x -> m x) -> FilePath -> ServerT AdminUI m
adminUIServerDevHoisted natTrans dir = hoistServer (Proxy @AdminUI) natTrans (adminUIServerDev dir)

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
