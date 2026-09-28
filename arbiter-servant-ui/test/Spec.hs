{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

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
  describe "adminUIServer"
    $ with (pure (serve (Proxy @("arbiter" :> AdminUI)) adminUIServer))
    $ it "keeps the query string on the trailing-slash redirect"
    $ get "/arbiter?queue=emails"
      `shouldRespondWith` 301 {matchHeaders = ["Location" <:> "/arbiter/?queue=emails"]}

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
    writeFile (root </> "secret.txt") "secret"
    action dir
