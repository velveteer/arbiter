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

-- | The embedded files, with every asset the page links to carrying this build's
-- version, so an asset is cached for good and a new build reaches clients by naming
-- new urls rather than by anything expiring.
--
-- Each file's own path is substituted where it appears as a complete attribute value.
-- Nothing else can match: a bound attribute holds an expression, not a path.
versionedFiles :: [(FilePath, ByteString)]
versionedFiles = map stampPage staticFiles
  where
    stampPage (path, content)
      | path == indexPath = (path, foldr (stamp . fst) content staticFiles)
      | otherwise = (path, content)
    stamp path = replaceAll (attribute path) (attribute (versionedPath path))
    attribute value = BS8.pack (value <> "\"")

-- | Where a stamped url puts a file: under a segment naming this build. A path segment
-- rather than a query, since a proxy between here and the reader is likelier to cache
-- one, and it is what a build tool would emit.
versionedPath :: FilePath -> FilePath
versionedPath path = T.unpack versionPrefix <> "/" <> BS8.unpack buildVersion <> "/" <> path

-- | The segment that introduces a version.
versionPrefix :: Text
versionPrefix = "v"

-- | Drop the version a stamped url carries, reporting whether one was there. The
-- version selects nothing: it only makes the url new, so an old one still resolves and
-- a reader holding a stale page is served rather than broken.
stripVersion :: [Text] -> (Bool, [Text])
stripVersion (prefix : _version : rest) | prefix == versionPrefix = (True, rest)
stripVersion segments = (False, segments)

-- | The dashboard's entry point, the one file that cannot carry a version of its own:
-- it is what names the others, so it is fetched afresh to learn their current versions.
indexPath :: FilePath
indexPath = "index.html"

-- | One version for the whole bundle. The files ship together inside one binary, so
-- they change together, and a version per file would only add ways to be wrong.
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
adminUIServerHoisted nt = hoistServer (Proxy @AdminUI) nt adminUIServer

-- | The dashboard as a standalone WAI application over the embedded files.
adminApplication :: Application
adminApplication = serveStaticApp Versioned $ \fp -> pure (lookup fp versionedFiles)

-- | The dashboard served from disk, read per request, so an edit needs no rebuild.
devAdminApplication :: FilePath -> Application
devAdminApplication dir = serveStaticApp AlwaysFresh $ \fp ->
  (Just <$> BS.readFile (dir </> fp)) `catch` (\(_ :: IOException) -> pure Nothing)

-- | The application both forms share, over a resolver for the files: @index.html@ at the
-- root, everything else by relative path. A root request without its trailing slash is
-- redirected to one, so the page's relative asset paths still resolve when the UI is
-- mounted under a prefix.
--
-- Assets are reached through the versioned URLs @index.html@ names, and are cached for
-- good: a new build stamps new URLs rather than expiring the old ones. @index.html@
-- itself is never cached, so a new build is picked up on the next load. A version prefix
-- names an asset, so it never reaches @index.html@.
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

-- | Whether a response may be held at all. The embedded files are stamped with this
-- build's version and cannot change under a running server; files read from disk are
-- edited while it runs, which is the whole point of serving them that way.
data Caching = Versioned | AlwaysFresh

-- | Caching headers for a response. A url carrying a version came from an @index.html@
-- this build stamped, so its bytes cannot change under it. Everything else is the page
-- itself, or a url nothing links to, and is fetched afresh. A stylesheet's own urls
-- resolve against its versioned path, so what it reaches is versioned too.
cacheHeaders :: Caching -> Bool -> [(HeaderName, ByteString)]
cacheHeaders Versioned True =
  [cacheControl ("public, max-age=" <> BS8.pack (show immutableMaxAge) <> ", immutable")]
cacheHeaders _ _ = [cacheControl "no-cache"]

cacheControl :: ByteString -> (HeaderName, ByteString)
cacheControl = (,) "Cache-Control"

-- | How long a versioned asset stands: a year, the longest value the spec defines.
immutableMaxAge :: Int
immutableMaxAge = 31536000

-- | Security headers included on all static responses. The dashboard loads no third-party
-- code and talks only to its own origin, so the policy names exactly that.
securityHeaders :: [(HeaderName, ByteString)]
securityHeaders =
  [ ("X-Content-Type-Options", "nosniff")
  , ("X-Frame-Options", "DENY")
  , ("Referrer-Policy", "same-origin")
  , ("Content-Security-Policy", contentSecurityPolicy)
  ]

-- | The dashboard's own bundled assets, and nothing else. @connect-src@ covers the API
-- fetches and the event stream, both same-origin. Alpine compiles its attribute
-- expressions with the Function constructor, so @script-src@ has to allow eval.
-- Style bindings write the style attribute, which is what @unsafe-inline@ permits here.
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
