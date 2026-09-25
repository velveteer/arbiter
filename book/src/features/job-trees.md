# Job Trees (Fan-out/Fan-in)

Children run in parallel. A parent runs when every child is acked or in the
DLQ.

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

Each intermediate finalizer returns the value for the level above.

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

| Exception | Deletes |
| --- | --- |
| `throwTreeCancel` | the root and every descendant |
| `throwBranchCancel` | the current job's parent and every descendant of that parent |

## Spawning Children at Runtime

`spawn` inserts children under the running job and suspends it, in one
transaction. The job wakes when every child has left the main queue.

```haskell
batchHandler jobs cbs = for_ jobs $ \job -> do
  (results, _) <- Worker.mergedChildResults job
  chunks <- discoverChunks (Arb.payload job) results
  case NE.nonEmpty chunks of
    Nothing -> do
      sendToS3 results
      Worker.ack cbs job
    Just cs -> Worker.spawn cbs job (fmap (Arb.defaultJob . ProcessChunk) cs)
```

With `transactionalWorkerConfig`, call `Arb.spawnChildren` in the handler. The
worker's ack suspends the job in the same transaction. Every round stores the
handler's return value. Return `mempty` from a round that spawns.

- **Hooks.** A spawn round fires `onJobSuccess`.
- **Attempts.** A spawn refunds the attempt its claim spent. Rounds are
  unbounded.
- **Results.** Each spawn deletes the previous round's results.
- **Crash before commit.** The job is reprocessed and spawns again. Give the
  children dedup keys when a repeat insert is wrong.
- **Child in the DLQ.** It wakes the finalizer. Its error is in that round's
  `mergedChildResults`. The next spawn detaches it. A DLQ retry restores it as
  a root.
- **Dedup conflict on a child.** The handler aborts and commits nothing. The
  abort names the spawning job. Batch siblings are nacked.
- **Callback after a spawn.** Ignored, with a warning.

## Chunked Data Migration

Each child migrates one set of row ids:

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

Tree builders: [`Arbiter.Core.JobTree` Haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-JobTree.html).
