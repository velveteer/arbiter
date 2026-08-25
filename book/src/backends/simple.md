# arbiter-simple (postgresql-simple)

Built on `postgresql-simple` with `resource-pool`. Handlers receive a raw
`Connection`. Nested transactions use savepoints automatically.

```haskell
env <- ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
ArbS.runSimpleDb env $ Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")
```

Share a transaction with external database work:

```haskell
PG.withTransaction conn $ do
  PG.execute conn "INSERT INTO orders (id) VALUES (?)" (PG.Only orderId)
  ArbS.inTransaction @AppRegistry conn "arbiter" $
    Arb.insertJob (Arb.defaultJob (ProcessOrder orderId))
```

See the [arbiter-simple haddocks](https://arbiterq.dev/arbiter-simple/Arbiter-Simple.html) for the env and pool constructors.
