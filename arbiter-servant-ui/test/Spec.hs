{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

import Codec.Compression.GZip (decompress)
import Data.ByteString (ByteString)
import Network.HTTP.Types (methodGet)
import Servant (Proxy (..), serve, (:>))
import System.Directory (createDirectory)
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import Test.Hspec
import Test.Hspec.Wai
import Test.Hspec.Wai.Internal (runWaiSession)

import Arbiter.Servant.UI (AdminUI, adminUIServer, devAdminApplication)

main :: IO ()
main = hspec $ do
  describe "devAdminApplication" $
    around withStaticDir $ do
      it "serves a file inside the directory" $ \dir ->
        served dir $
          get "/app.js" `shouldRespondWith` 200
      it "refuses an encoded parent segment" $ \dir ->
        served dir $
          get "/..%2Fsecret.txt" `shouldRespondWith` 404
      it "refuses a bare parent segment" $ \dir ->
        served dir $
          get "/%2E%2E/secret.txt" `shouldRespondWith` 404
      it "refuses an encoded absolute path" $ \dir ->
        served dir $
          get "/%2Fetc%2Fhosts" `shouldRespondWith` 404
      it "refuses a backslash in a segment" $ \dir ->
        served dir $
          get "/..%5Csecret.txt" `shouldRespondWith` 404
      it "does not serve the type checker's declarations" $ \dir ->
        served dir $
          get "/app.d.ts" `shouldRespondWith` 404
  describe "adminUIServer" $
    with (pure (serve (Proxy @("arbiter" :> AdminUI)) adminUIServer)) $
      do
        it "keeps the query string on the trailing-slash redirect" $
          get "/arbiter?queue=emails"
            `shouldRespondWith` 301 {matchHeaders = ["Location" <:> "/arbiter/?queue=emails"]}
        it "does not serve the type checker's declarations" $
          get "/arbiter/vendor/vue.esm-browser.prod.d.ts" `shouldRespondWith` 404
        it "sends a script gzipped to a client that accepts gzip" $ do
          plain <- get scriptPath
          gzipped <- request methodGet scriptPath [("Accept-Encoding", "deflate, GZIP;q=1.0, br")] ""
          liftIO $ do
            lookup "Content-Encoding" (simpleHeaders gzipped) `shouldBe` Just "gzip"
            lookup "Vary" (simpleHeaders gzipped) `shouldBe` Just "Accept-Encoding"
            decompress (simpleBody gzipped) `shouldBe` simpleBody plain
        it "sends a script plain to a client that does not accept gzip" $ do
          plain <- get scriptPath
          liftIO $ do
            lookup "Content-Encoding" (simpleHeaders plain) `shouldBe` Nothing
            lookup "Vary" (simpleHeaders plain) `shouldBe` Just "Accept-Encoding"
        it "sends a script plain to a client that refuses gzip" $ do
          refused <- request methodGet scriptPath [("Accept-Encoding", "gzip;q=0, identity")] ""
          liftIO $ lookup "Content-Encoding" (simpleHeaders refused) `shouldBe` Nothing
        it "sends a script gzipped to a client that accepts any coding" $ do
          anyCoding <- request methodGet scriptPath [("Accept-Encoding", "*")] ""
          liftIO $ lookup "Content-Encoding" (simpleHeaders anyCoding) `shouldBe` Just "gzip"
        it "sends a script plain to a client that accepts any coding but gzip" $ do
          refused <- request methodGet scriptPath [("Accept-Encoding", "*, gzip;q=0")] ""
          liftIO $ lookup "Content-Encoding" (simpleHeaders refused) `shouldBe` Nothing
        it "sends an image plain to a client that accepts gzip" $ do
          image <- request methodGet "/arbiter/apple-touch-icon.png" [("Accept-Encoding", "gzip")] ""
          liftIO $ lookup "Content-Encoding" (simpleHeaders image) `shouldBe` Nothing

-- | An embedded script.
scriptPath :: ByteString
scriptPath = "/arbiter/js/main.js"

-- | Run a session against the dashboard served from @dir@.
served :: FilePath -> WaiSession () a -> IO a
served dir session = runWaiSession session (devAdminApplication dir)

-- | A dashboard directory with a sibling file outside it.
withStaticDir :: (FilePath -> IO ()) -> IO ()
withStaticDir action =
  withSystemTempDirectory "arbiter-servant-ui-test" $ \root -> do
    let dir = root </> "static"
    createDirectory dir
    writeFile (dir </> "app.js") "ok"
    writeFile (dir </> "app.d.ts") "declare const ok: string"
    writeFile (root </> "secret.txt") "secret"
    action dir
