# Payload Kinds

Each job stores a `kind`: a label that Arbiter derives from the payload at
enqueue. Use it to filter jobs by variant and to break queue depth down by
variant.

Define a `HasKind` instance for the payload. The generic default labels each job
with its constructor name and declares the whole constructor set.

```haskell
data EmailPayload
  = SendWelcome UserId
  | SendReceipt OrderId
  deriving stock (Eq, Generic, Show)

instance HasKind EmailPayload
```

A job of `SendReceipt 7` stores `kind = "SendReceipt"`, and the queue declares
`["SendWelcome", "SendReceipt"]`.

A payload without an instance stores no label.

## Other label sources

`kindOf` gives one job's label. `kindsFor` gives every label that `kindOf` can
return. Take both from the same function, so they cannot drift.

| Payload | `kindOf` | `kindsFor` |
|---------|----------|------------|
| Tagged sum | generic default | generic default |
| Constructors under other names | `Just . emailTag` | `map emailTag [minBound .. maxBound]` |
| Sum inside a wrapper | `Just . constructorKind . envelopePayload` | `constructorKinds @EmailPayload` |
| Runtime `Value` | `kindFromField "type"` | `[]` |

```haskell
data Envelope = Envelope
  { envelopeTraceId :: Text
  , envelopePayload :: EmailPayload
  }

instance HasKind Envelope where
  kindOf = Just . constructorKind . envelopePayload
  kindsFor = constructorKinds @EmailPayload

newtype RuntimeJob = RuntimeJob Value

instance HasKind RuntimeJob where
  kindOf (RuntimeJob v) = kindFromField "type" v
  kindsFor = []
```

`constructorKind` and `constructorKinds` read the constructors of the wrapped
type. They need `Generic` on it, not a `HasKind` instance, so they also label a
sum from a library you do not own, with no orphan instance.

## Where the label appears

| Surface | Uses |
|---------|------|
| `GET /api/v1/:queue/jobs?kind=` (also `dlq` and `archive`) | stored label |
| `GET /api/v1/:queue/kinds` | `kindsFor` |
| Admin UI Kind column and filter | both |
| `GET /api/v1/:queue/stats` `kindCounts` | stored label, filtered by `kindsFor` |
| `arbiter.queue.depth_by_kind` | `kindsFor` |
| `arbiter.jobs.*` and the handler histogram | stored label, filtered by `kindsFor` |
| `arbiter.kind` on the producer and consumer spans | `kindOf` and the stored label |

A metric and the `kindCounts` rollup use only the declared labels, so their
size is bounded by the payload type. A queue whose payload declares no labels
exports no `kind` and reports no counts, whatever its rows carry. Spans have no
such bound and always carry the label.

See the [`Arbiter.Core.Job.Kind` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Kind.html)
for the class and its helpers.
