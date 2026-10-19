-- | postgresql-simple primitives for writing a custom
-- 'Arbiter.Core.MonadArbiter.MonadArbiter' instance.
--
-- Handlers receive a @Database.PostgreSQL.Simple.Connection@ for running queries
-- inside the worker transaction:
--
-- @
-- import Arbiter.Core qualified as Arb
-- import Arbiter.Simple qualified as ArbS
-- import Database.PostgreSQL.Simple (Connection)
--
-- instance Arb.MonadArbiter MyApp where
--   type RegistryOf MyApp = MyRegistry
--   type Handler MyApp job result = Connection -> job -> MyApp result
--   getSchema                = asks appSchema
--   executeQuery             = ArbS.simpleExecuteQuery
--   executeStatement         = ArbS.simpleExecuteStatement
--   withDbTransaction        = ArbS.simpleWithDbTransaction
--   runHandlerWithConnection = ArbS.simpleRunHandlerWithConnection
--   getListener              = asks appListener
-- @
--
-- The primitives also need a @HasPoolState Connection MyApp@ instance. See
-- 'Arbiter.Simple.SimpleDb.HasPoolState'.
--
-- Write a handler's own signature as @JobHandler MyApp MyPayload MyResult@, which is
-- 'Arbiter.Core.MonadArbiter.Handler' at that queue's job and declared result types.
module Arbiter.Simple.MonadArbiter
  ( -- * MonadArbiter implementation
    simpleExecuteQuery
  , simpleExecuteStatement
  , simpleWithDbTransaction
  , simpleRunHandlerWithConnection

    -- * Connection
  , simpleWithConnection
  ) where

import Arbiter.Core.Backend (HasPoolState, pinConnection, withConn, withSavepointTransaction)
import Arbiter.Core.MonadArbiter
  ( Col (..)
  , NullCol (..)
  , ParamType (..)
  , SomeParam (..)
  , Stored (..)
  , runCodec
  , storedBytes
  )
import Arbiter.Core.MonadArbiter qualified as MA
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.Text.Encoding qualified as TE
import Database.PostgreSQL.Simple (Connection)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.FromField (FromField (..), ResultError (..), returnError, typeOid)
import Database.PostgreSQL.Simple.FromRow (RowParser, field)
import Database.PostgreSQL.Simple.ToField (Action (..), ToField (..), toField)
import Database.PostgreSQL.Simple.TypeInfo.Static qualified as TI
import Database.PostgreSQL.Simple.Types (PGArray (..), Query (..))
import UnliftIO (MonadUnliftIO)

-- | Pin one pooled connection for the action.
simpleWithConnection :: (HasPoolState Connection m, MonadUnliftIO m) => m a -> m a
simpleWithConnection = pinConnection

-- | Run a query, decoding rows.
simpleExecuteQuery
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => MA.Query a
  -> m [a]
simpleExecuteQuery query =
  runSplit (PG.queryWith_ parser) (PG.queryWith parser) query
  where
    parser = runCodec interpretNullCol (MA.qDecode query)

-- | Run a statement, returning rows affected.
simpleExecuteStatement
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => MA.Query a
  -> m Int64
simpleExecuteStatement = runSplit PG.execute_ PG.execute

-- | Run a query on a pooled connection. The first runner takes a query with no
-- parameters. The second takes the encoded parameters.
runSplit
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => (Connection -> Query -> IO r)
  -> (Connection -> Query -> [Action] -> IO r)
  -> MA.Query a
  -> m r
runSplit bare withParams query =
  withConn $ \conn -> liftIO $ case MA.qParams query of
    [] -> bare conn sql
    params -> withParams conn sql (map someParamToAction params)
  where
    sql = Query (TE.encodeUtf8 (MA.qSql query))

interpretNullCol :: NullCol a -> RowParser a
interpretNullCol (NotNull _ col) = withColField col (\from _ -> from <$> field)
interpretNullCol (Nullable _ col) = withColField col (\from _ -> fmap from <$> field)

-- | Transaction bracket. Nests via savepoints.
simpleWithDbTransaction
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => m a
  -> m a
simpleWithDbTransaction =
  withSavepointTransaction (\conn sql -> void (PG.execute_ conn (Query sql))) PG.withTransaction

-- | Run a handler on the pinned connection, or on a borrowed pool connection when none is pinned.
simpleRunHandlerWithConnection
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => (Connection -> job -> m result)
  -> job
  -> m result
simpleRunHandlerWithConnection handler job =
  withConn $ \conn -> handler conn job

someParamToAction :: SomeParam -> Action
someParamToAction (SomeParam (PScalar col) value) = withColField col (\_ to -> toField (to value))
someParamToAction (SomeParam (PNullable col) value) = withColField col (\_ to -> toField (to <$> value))
someParamToAction (SomeParam (PArray col) value) = withColField col (\_ to -> toField (PGArray (map to value)))
someParamToAction (SomeParam (PNullArray col) value) = withColField col (\_ to -> toField (PGArray (map (fmap to) value)))

-- | The field instances for a column, with raw JSON carried as 'RawJson'.
withColField :: Col a -> (forall b. (FromField b, ToField b) => (b -> a) -> (a -> b) -> r) -> r
withColField CInt4 k = k id id
withColField CInt8 k = k id id
withColField CText k = k id id
withColField CBool k = k id id
withColField CTimestamptz k = k id id
withColField CJsonb k = k id id
withColField CStored k = k (\(RawJson bytes) -> Stored bytes) (RawJson . storedBytes)
withColField CFloat8 k = k id id
withColField CUuid k = k id id

-- | JSON bytes passed through a @json@ or @jsonb@ column without parsing.
newtype RawJson = RawJson ByteString

instance FromField RawJson where
  fromField f mdata
    | typeOid f /= TI.jsonbOid && typeOid f /= TI.jsonOid = returnError Incompatible f ""
    | otherwise = maybe (returnError UnexpectedNull f "") (pure . RawJson) mdata

instance ToField RawJson where
  toField (RawJson bytes) = Escape bytes
