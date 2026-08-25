# arbiter-hasql (hasql)

Built on `hasql` with `resource-pool`. Handlers receive a `Hasql.Connection`
for typed hasql queries inside the worker transaction.

```haskell
env <- ArbH.createHasqlEnv (Proxy @AppRegistry) connStr "arbiter"
ArbH.runHasqlDb env $ Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")
```

Share a transaction with external hasql work:

```haskell
-- Session.script (hasql >= 1.10) or Session.sql (hasql < 1.10)
_ <- Hasql.use conn (Session.script "BEGIN")
ArbH.inTransaction @AppRegistry conn "arbiter" $
  Arb.insertJob (Arb.defaultJob (ProcessOrder orderId))
_ <- Hasql.use conn (Session.script "COMMIT")
```

See the [arbiter-hasql haddocks](https://arbiterq.dev/arbiter-hasql/Arbiter-Hasql.html) for the env and pool constructors.
