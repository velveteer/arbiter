# Payload Kinds

A payload kind is an optional `Text` label stored with each job at insert.
[Label Use](#label-use) lists where it appears.

`HasKind` has two members:

| Member | Type | Description |
|--------|------|-------------|
| `kindOf` | `payload -> Maybe Text` | The label for a payload. |
| `kindsFor` | `[Text]` | Every label `kindOf` can return. |

With no instance for the payload, `kindOf` returns `Nothing` and `kindsFor`
is `[]`.

## Constructor Labels

The default methods use constructor names. Derive `Generic` and declare an
empty instance:

```haskell
data EmailPayload
  = SendWelcome UserId
  | SendReceipt OrderId
  deriving stock (Generic)

instance HasKind EmailPayload
```

`kindOf (SendReceipt 7)` is `Just "SendReceipt"`. `kindsFor @EmailPayload` is
`["SendWelcome", "SendReceipt"]`.

## Custom Labels

Implement both members for other labels:

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

`kindOf (EmailPayload Receipt "a@b.c")` is `Just "receipt"`.
`kindsFor @EmailPayload` is `["welcome", "receipt", "passwordreset"]`.

`kindsFor` must list every label `kindOf` can return. Kind metrics and
`kindCounts` skip undeclared labels.

## Labels from a Nested Type

`constructorKind` and `constructorKinds` read the constructors of any `Generic`
type:

```haskell
data Envelope = Envelope
  { envelopeTraceId :: Text
  , envelopePayload :: EmailPayload
  }

instance HasKind Envelope where
  kindOf = Just . constructorKind . envelopePayload
  kindsFor = constructorKinds @EmailPayload
```

## Label Use

| Interface | Label source |
|-----------|--------------|
| `GET /api/v1/queues/:queue/jobs?kind=` and the DLQ and archive filters | Stored job label |
| `GET /api/v1/queues/:queue/kinds` | `kindsFor` |
| Admin UI kind column | Stored job label |
| Admin UI kind filter | `kindsFor` |
| `GET /api/v1/queues/:queue/stats` field `kindCounts` | Stored labels declared by `kindsFor` |
| `arbiter.queue.depth_by_kind` | Stored labels declared by `kindsFor` |
| `arbiter.jobs.*` metrics and the handler histogram | Stored labels declared by `kindsFor` |
| Producer span attribute `arbiter.kind` | `kindOf` |
| Consumer span attribute `arbiter.kind` | Stored job label |

API details: [`Arbiter.Core.Job.Kind`](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Kind.html).
