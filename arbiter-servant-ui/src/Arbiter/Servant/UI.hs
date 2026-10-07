{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeOperators #-}

-- | Embedded admin dashboard. Bootstrap 5 CSS and Vue 3, compiled in.
--
-- __Security:__ No built-in authentication. The dashboard calls the arbiter-servant
-- API, which has no authentication. Add auth middleware before you expose either to
-- untrusted networks.
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
-- If it loads at @\/arbiter\/@ it finds the API at @\/arbiter\/api\/@.
module Arbiter.Servant.UI
  ( -- * Servant integration
    AdminUI
  , adminUIServer
  , adminUIServerHoisted
  , adminUIServerDev
  , adminUIServerDevHoisted

    -- * Standalone WAI app
  , adminApp
  , adminAppDev

    -- * Combined app helper
  , arbiterAppWithAdmin
  , arbiterAppWithAdminDev
  ) where

import Arbiter.Core.MonadArbiter (HasRegistry)
import Arbiter.Servant.API (ArbiterAPI)
import Arbiter.Servant.Server (ArbiterServerConfig, BuildServer, arbiterServer)
import Codec.Compression.GZip (compress)
import Control.Exception (IOException, catch)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.ByteString.Lazy qualified as LBS
import Data.Char (toLower)
import Data.FileEmbed (embedDir)
import Data.Hashable (hash)
import Data.List (isSuffixOf)
import Data.Maybe (fromMaybe, isJust)
import Data.Text (Text)
import Data.Text qualified as T
import Network.HTTP.Media (Encoding, matchAccept)
import Network.HTTP.Types (HeaderName, status200, status301, status404)
import Network.Wai (Request, pathInfo, rawPathInfo, rawQueryString, requestHeaders, responseLBS)
import Numeric (showHex)
import Servant
import System.FilePath (isAbsolute, takeExtension, (</>))

-- | The dashboard's static files, embedded at compile time.
staticFiles :: [(FilePath, ByteString)]
staticFiles = filter (isServed . fst) $(embedDir "static")

-- | A file the dashboard serves. Type declarations are for the checker only.
isServed :: FilePath -> Bool
isServed = not . isSuffixOf ".d.ts" . map toLower

-- | Embedded files with the build version in each asset URL. Versioned assets
-- can use an immutable cache. A new build produces new URLs.
--
-- Replace a file path in index.html only where it is a whole attribute value.
versionedFiles :: [(FilePath, ByteString)]
versionedFiles = map stampPage staticFiles
  where
    stampPage (path, content)
      | path == indexPath = (path, foldl' (flip (stamp . fst)) content staticFiles)
      | otherwise = (path, content)
    stamp path = replaceAll (attribute path) (attribute (versionedPath path))
    attribute value = BS8.pack ("\"" <> value <> "\"")

-- | Put an asset below a path segment that identifies the build. Path segments
-- give reverse proxies and build tools stable cache keys.
versionedPath :: FilePath -> FilePath
versionedPath path = T.unpack versionPrefix <> "/" <> BS8.unpack buildVersion <> "/" <> path

-- | The segment that introduces a version.
versionPrefix :: Text
versionPrefix = "v"

-- | Remove a version prefix and return its version, if one was present.
stripVersion :: [Text] -> (Maybe Text, [Text])
stripVersion (prefix : version : rest) | prefix == versionPrefix = (Just version, rest)
stripVersion segments = (Nothing, segments)

-- | Dashboard entry point. It has no version and is fetched again on each load.
indexPath :: FilePath
indexPath = "index.html"

-- | A served file and, for a text format, its gzip encoding.
data Asset = Asset
  { assetBody :: ByteString
  , assetGzip :: Maybe ByteString
  }

-- | A file served as it is.
plainAsset :: ByteString -> Asset
plainAsset content = Asset {assetBody = content, assetGzip = Nothing}

-- | The versioned files. Each text format is compressed once, on its first request.
embeddedAssets :: [(FilePath, Asset)]
embeddedAssets = [(path, Asset {assetBody = content, assetGzip = gzipFor path content}) | (path, content) <- versionedFiles]

gzipFor :: FilePath -> ByteString -> Maybe ByteString
gzipFor path content
  | compressible (fileType path) = Just (LBS.toStrict (compress (LBS.fromStrict content)))
  | otherwise = Nothing

-- | How a file is served.
data FileType = FileType
  { mediaType :: ByteString
  , compressible :: Bool
  }

-- | A file's type, from its extension.
fileType :: FilePath -> FileType
fileType path = fromMaybe (binary "application/octet-stream") (lookup (takeExtension path) fileTypes)

-- | Served types by extension. Images and fonts other than SVG are compressed already.
fileTypes :: [(String, FileType)]
fileTypes =
  [ (".html", text "text/html; charset=utf-8")
  , (".css", text "text/css; charset=utf-8")
  , (".js", text "application/javascript; charset=utf-8")
  , (".json", text "application/json")
  , (".svg", text "image/svg+xml")
  , (".txt", text "text/plain; charset=utf-8")
  , (".png", binary "image/png")
  , (".ico", binary "image/x-icon")
  , (".woff2", binary "font/woff2")
  , (".woff", binary "font/woff")
  ]
  where
    text media = FileType {mediaType = media, compressible = True}

binary :: ByteString -> FileType
binary media = FileType {mediaType = media, compressible = False}

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
adminUIServer = Tagged adminApp

-- | Hoisted variant for integration into a route tree using a custom monad.
adminUIServerHoisted :: forall m. (forall x. Handler x -> m x) -> ServerT AdminUI m
adminUIServerHoisted natTrans = hoistServer (Proxy @AdminUI) natTrans adminUIServer

-- | The dashboard as a standalone WAI application over the embedded files.
adminApp :: Application
adminApp = serveStaticApp Versioned $ \filePath -> pure (lookup filePath embeddedAssets)

-- | The dashboard served from disk, read per request.
adminAppDev
  :: FilePath
  -- ^ The static directory.
  -> Application
adminAppDev dir = serveStaticApp AlwaysFresh $ \filePath ->
  if isServed filePath
    then (Just . plainAsset <$> BS.readFile (dir </> filePath)) `catch` (\(_ :: IOException) -> pure Nothing)
    else pure Nothing

-- | Serve files through a resolver. The root returns @index.html@ and other
-- paths return the named file. Redirect a root path without a trailing slash.
-- Refuse a path with a segment that could leave the served directory.
--
-- Cache an asset under the current build's version as immutable. Make other
-- responses revalidate. A version prefix is valid for asset paths. Send the gzip
-- encoding to a client that accepts it.
serveStaticApp :: Caching -> (FilePath -> IO (Maybe Asset)) -> Application
serveStaticApp caching resolveFile req sendResponse = sendResponse =<< reply
  where
    (version, segments) = stripVersion (filter (not . T.null) (pathInfo req))
    path = T.intercalate "/" segments
    isIndex = T.null path || path == T.pack indexPath
    filePath = if isIndex then indexPath else T.unpack path
    reply
      | isIndex && isJust version = pure notFound
      | any unsafeSegment segments = pure notFound
      | T.null path && not ("/" `BS.isSuffixOf` rawPathInfo req) =
          pure $ responseLBS status301 [("Location", rawPathInfo req <> "/" <> rawQueryString req)] ""
      | otherwise = maybe notFound found <$> resolveFile filePath
    found asset =
      let (body, encodingHeaders) = negotiate req asset
       in responseLBS
            status200
            ( securityHeaders
                ++ cacheHeaders (currentBuild caching version)
                ++ [("Content-Type", mediaType (fileType filePath))]
                ++ encodingHeaders
            )
            (LBS.fromStrict body)
    notFound = responseLBS status404 [("Content-Type", "text/plain")] "Not found"

-- | The body for a request and the headers that describe its encoding.
negotiate :: Request -> Asset -> (ByteString, [(HeaderName, ByteString)])
negotiate req asset = case assetGzip asset of
  Just gzipped | acceptsGzip req -> (gzipped, [("Content-Encoding", "gzip"), varyEncoding])
  Just _ -> (assetBody asset, [varyEncoding])
  Nothing -> (assetBody asset, [])
  where
    varyEncoding = ("Vary", "Accept-Encoding")

-- | The client accepts gzip, by name or through @*@, with a nonzero quality.
acceptsGzip :: Request -> Bool
acceptsGzip req = isJust (matchAccept [gzip] =<< lookup "Accept-Encoding" (requestHeaders req))
  where
    gzip = "gzip" :: Encoding

-- | A decoded path segment that could name a file outside the served directory.
unsafeSegment :: Text -> Bool
unsafeSegment segment =
  segment `elem` [".", ".."] || T.any (`elem` ['/', '\\']) segment || isAbsolute (T.unpack segment)

-- | A version that names the embedded build.
currentBuild :: Caching -> Maybe Text -> Bool
currentBuild Versioned (Just version) = BS8.pack (T.unpack version) == buildVersion
currentBuild _ _ = False

-- | Response caching mode. Embedded, versioned files are immutable while the
-- server runs. Files read from disk can change between requests.
data Caching = Versioned | AlwaysFresh

-- | Cache a response as immutable, or require revalidation. Relative URLs in a
-- stylesheet inherit its versioned path.
cacheHeaders :: Bool -> [(HeaderName, ByteString)]
cacheHeaders True = [cacheControl ("public, max-age=" <> BS8.pack (show immutableMaxAge) <> ", immutable")]
cacheHeaders False = [cacheControl "no-cache"]

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
-- same-origin API and event-stream requests. Vue compiles its templates at
-- runtime, which requires @unsafe-eval@. Style bindings require @unsafe-inline@.
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

-- | Serve 'AdminUI' from disk.
adminUIServerDev
  :: FilePath
  -- ^ The static directory.
  -> Server AdminUI
adminUIServerDev dir = Tagged (adminAppDev dir)

-- | Hoisted dev-mode variant for integration into a route tree using a custom monad.
adminUIServerDevHoisted
  :: forall m
   . (forall x. Handler x -> m x)
  -> FilePath
  -- ^ The static directory.
  -> ServerT AdminUI m
adminUIServerDevHoisted natTrans dir = hoistServer (Proxy @AdminUI) natTrans (adminUIServerDev dir)

-- | The API at @\/api@ and the admin UI at the root, in one application.
arbiterAppWithAdmin
  :: forall registry m
   . ( BuildServer registry registry
     , HasRegistry m registry
     , HasServer (ArbiterAPI registry) '[]
     )
  => ArbiterServerConfig m registry
  -> Application
arbiterAppWithAdmin config =
  serve
    (Proxy @(ArbiterAPI registry :<|> AdminUI))
    (arbiterServer config :<|> adminUIServer)

-- | 'arbiterAppWithAdmin' serving the UI from disk.
arbiterAppWithAdminDev
  :: forall registry m
   . ( BuildServer registry registry
     , HasRegistry m registry
     , HasServer (ArbiterAPI registry) '[]
     )
  => FilePath
  -- ^ The static directory.
  -> ArbiterServerConfig m registry
  -> Application
arbiterAppWithAdminDev dir config =
  serve
    (Proxy @(ArbiterAPI registry :<|> AdminUI))
    (arbiterServer config :<|> adminUIServerDev dir)
