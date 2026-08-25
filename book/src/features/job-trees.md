# Job Trees (Fan-out/Fan-in)

Children run in parallel. Parents run when all of their children are acked or
DLQ'd.

```haskell
import Arbiter.Core.JobTree (leaf, rollup, (<~~))
import Data.List.NonEmpty (NonEmpty ((:|)))

data PipelinePayload
  = ProcessChunk Text
  | AggregateSection Text
  | Aggregate
  deriving stock (Generic)
  deriving anyclass (ToJSON, FromJSON)

-- One entry for the whole tree: children and parents share the queue.
type PipelineRegistry = '[ QueueWithResult "pipeline_queue" PipelinePayload [Text] ]

myTree = Arb.defaultJob Aggregate <~~
  ( Arb.defaultJob (ProcessChunk "chunk-1")
      :| [ Arb.defaultJob (ProcessChunk "chunk-2")
         , Arb.defaultJob (ProcessChunk "chunk-3")
         ]
  )
Right _ <- Arb.insertJobTree myTree
```

Multi-level trees use `rollup` and `leaf`:

```haskell
myTree = rollup (Arb.defaultJob Aggregate)
  ( rollup (Arb.defaultJob (AggregateSection "section-1"))
      ( leaf (Arb.defaultJob (ProcessChunk "leaf-1a"))
          :| [leaf (Arb.defaultJob (ProcessChunk "leaf-1b"))]
      )
      :| [ rollup (Arb.defaultJob (AggregateSection "section-2"))
             (leaf (Arb.defaultJob (ProcessChunk "leaf-2a")) :| [])
         ]
  )
```

A nested rollup does not merge upward on its own. An intermediate finalizer has
to return the merged value for it to reach the level above.

A parent reads its children with `Worker.mergedChildResults`, which merges the
results of its immediate children and reports the ones that dead-lettered,
keyed for `retryFromDLQ`. Intermediate results are cleaned up when the parent
is acked.

```haskell
handler :: Arb.JobHandler (ArbS.SimpleDb PipelineRegistry IO) PipelinePayload [Text]
handler _conn job =
  case Arb.payload job of
    ProcessChunk name -> pure ["processed: " <> name]
    AggregateSection name -> do
      (childResults, dlqFailures) <- Worker.mergedChildResults job
      if not (null dlqFailures)
        then Arb.throwPermanent $ name <> ": has failed children"
        else processSection childResults
    Aggregate -> do
      (childResults, _) <- Worker.mergedChildResults job
      sendToS3 childResults
      pure childResults

config <- Worker.transactionalWorkerConfig 4 handler
```

Tree-scoped cancellation:

- `throwTreeCancel` - cancels the entire tree (root and all descendants).
- `throwBranchCancel` - deletes this job's parent and everything under it,
  which takes this job and its siblings with it.

## Recipe: Chunked Data Migration

Migrate a large table in chunks. Each child job carries its own chunk of row
ids, and the parent runs once every chunk is done:

```haskell
import Data.List.NonEmpty qualified as NE

data MigrationJob
  = MigrateChunk [Int64]
  | MigrationComplete
  deriving stock (Generic)
  deriving anyclass (ToJSON, FromJSON)

type MigrationRegistry =
  '[ QueueWithResult "migration_queue" MigrationJob (Sum Int) ]

rowIds <- findRowsToMigrate  -- SELECT id FROM orders WHERE needs_migration
case NE.nonEmpty (chunksOf 1000 rowIds) of  -- chunksOf is from the split package
  Nothing -> reportComplete 0
  Just chunks -> do
    let tree = Arb.defaultJob MigrationComplete
          <~~ fmap (Arb.defaultJob . MigrateChunk) chunks
    Right _ <- Arb.insertJobTree tree
    pure ()
```

```haskell
handler conn job = case Arb.payload job of
  MigrateChunk ids -> do
    rowCount <- migrateRows conn ids
    pure (Sum rowCount)

  MigrationComplete -> do
    (Sum totalRows, _) <- Worker.mergedChildResults job
    reportComplete totalRows
    pure (Sum totalRows)
```

See the [`Arbiter.Core.JobTree` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-JobTree.html) for the tree builders.
