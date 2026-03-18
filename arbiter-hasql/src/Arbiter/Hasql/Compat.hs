{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Compatibility layer for hasql API differences.
--
-- All version-specific code lives here. The rest of arbiter-hasql
-- imports from this module and never uses CPP directly.
module Arbiter.Hasql.Compat
  ( runSQL
  , hasqlSettings
  , HasqlSettings
  ) where

import Arbiter.Core.Exceptions (throwInternal)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Text.Encoding.Error qualified as TE
import Hasql.Connection qualified as Hasql
import Hasql.Session qualified as Session
import UnliftIO (MonadUnliftIO)

#if MIN_VERSION_hasql(1,10,0)
import Hasql.Connection.Settings qualified as Settings
#else
import Hasql.Connection.Setting qualified as Setting
import Hasql.Connection.Setting.Connection qualified as ConnSetting
#endif

-- | Run a simple SQL command on a hasql connection (e.g., BEGIN, COMMIT).
runSQL :: (MonadUnliftIO m) => Hasql.Connection -> ByteString -> m ()
runSQL conn sql = do
  result <- liftIO $ Hasql.use conn (runScript (TE.decodeUtf8With TE.lenientDecode sql))
  case result of
    Right () -> pure ()
    Left err -> throwInternal $ "hasql runSQL error: " <> T.pack (show err)

#if MIN_VERSION_hasql(1,10,0)
runScript :: T.Text -> Session.Session ()
runScript = Session.script
#else
runScript :: T.Text -> Session.Session ()
runScript = Session.sql
#endif

-- | Convert a connection string ByteString to hasql settings.
hasqlSettings :: ByteString -> HasqlSettings
hasqlSettings = hasqlSettingsFromConnStr

#if MIN_VERSION_hasql(1,10,0)
type HasqlSettings = Settings.Settings
hasqlSettingsFromConnStr :: ByteString -> Settings.Settings
hasqlSettingsFromConnStr = Settings.connectionString . TE.decodeUtf8With TE.lenientDecode
#else
type HasqlSettings = [Setting.Setting]
hasqlSettingsFromConnStr :: ByteString -> [Setting.Setting]
hasqlSettingsFromConnStr connStr = [Setting.connection (ConnSetting.string (TE.decodeUtf8With TE.lenientDecode connStr))]
#endif
