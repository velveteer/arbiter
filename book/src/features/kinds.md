# Payload Kinds

A job stores a `kind`. Arbiter derives it from the payload at enqueue. Filter
jobs by it, and break queue depth down by it.

`HasKind` gives the label. No instance means no label.

| Member | Gives |
|--------|-------|
| `kindOf` | the label of one job |
| `kindsFor` | every label `kindOf` can return |

## Write the two functions

```haskell
data EmailKind = Welcome | Receipt | PasswordReset
  deriving stock (Bounded, Enum, Show)

emailKindText :: EmailKind -> Text
emailKindText = T.toLower . T.pack . show

data EmailPayload = EmailPayload
  { emailKind :: EmailKind
  , emailTo :: Text
  }

instance HasKind EmailPayload where
  kindOf = Just . emailKindText . emailKind
  kindsFor = map emailKindText [minBound .. maxBound]
```

`EmailPayload Receipt "a@b.c"` stores `kind = "receipt"`. The queue declares
`["welcome", "receipt", "passwordreset"]`.

> Build `kindOf` and `kindsFor` from the same function. Arbiter stores a label
> that `kindsFor` omits, but counts it nowhere.

## Or take the generic default

The default uses the constructor names.

```haskell
data EmailPayload
  = SendWelcome UserId
  | SendReceipt OrderId
  deriving stock (Generic)

instance HasKind EmailPayload
```

`SendReceipt 7` stores `kind = "SendReceipt"`. The queue declares
`["SendWelcome", "SendReceipt"]`.

## Or read the constructors of another type

`constructorKind` and `constructorKinds` need `Generic` on that type, not
`HasKind`. Use them for a wrapper, or for a sum from a library you do not own.

```haskell
data Envelope = Envelope
  { envelopeTraceId :: Text
  , envelopePayload :: EmailPayload
  }

instance HasKind Envelope where
  kindOf = Just . constructorKind . envelopePayload
  kindsFor = constructorKinds @EmailPayload
```

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

`kindsFor` bounds the metrics and the `kindCounts` rollup. Spans have no bound
and always carry the label.

See the [`Arbiter.Core.Job.Kind` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Kind.html).
