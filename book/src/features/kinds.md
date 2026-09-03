# Payload Kinds

A payload kind is an optional `Text` label stored with each job. Arbiter calls
`kindOf` during insertion. Kind labels support job filters, queue statistics,
metrics, traces, and the admin UI.

`HasKind` defines the label for one payload and the finite set of labels for its
payload type:

| Member | Type | Description |
|--------|------|-------------|
| `kindOf` | `payload -> Maybe Text` | Returns the label for a payload. |
| `kindsFor` | `[Text]` | Lists all labels returned by `kindOf`. |

The fallback instance for a payload type returns `Nothing` and an empty list.
Declare a payload-specific instance to enable kind labels.

## Constructor Labels

The generic implementation uses data-constructor names. Derive `Generic` and
declare an empty instance:

```haskell
data EmailPayload
  = SendWelcome UserId
  | SendReceipt OrderId
  deriving stock (Generic)

instance HasKind EmailPayload
```

For this instance, `kindOf (SendReceipt 7)` returns `Just "SendReceipt"` and
`kindsFor @EmailPayload` returns `["SendWelcome", "SendReceipt"]`.

## Custom Labels

Implement both members when constructor names are not suitable:

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

For this instance, `kindOf (EmailPayload Receipt "a@b.c")` returns
`Just "receipt"`. `kindsFor @EmailPayload` returns
`["welcome", "receipt", "passwordreset"]`.

`kindsFor` must contain each non-`Nothing` value that `kindOf` can return.
Arbiter excludes undeclared labels from kind metrics and the `kindCounts`
statistics field.

## Labels from a Nested Type

`constructorKind` and `constructorKinds` read the constructors of any
`Generic` type. They do not require a `HasKind` instance on that type.

```haskell
data Envelope = Envelope
  { envelopeTraceId :: Text
  , envelopePayload :: EmailPayload
  }

instance HasKind Envelope where
  kindOf = Just . constructorKind . envelopePayload
  kindsFor = constructorKinds @EmailPayload
```

This form supports wrapper payloads and external sum types without an orphan
`HasKind` instance.

## Label Use

| Interface | Label source |
|-----------|--------------|
| `GET /api/v1/:queue/jobs?kind=` and the equivalent DLQ and archive filters | Stored job label |
| `GET /api/v1/:queue/kinds` | `kindsFor` |
| Admin UI kind column | Stored job label |
| Admin UI kind filter | `kindsFor` |
| `GET /api/v1/:queue/stats` field `kindCounts` | Stored labels declared by `kindsFor` |
| `arbiter.queue.depth_by_kind` | Stored labels declared by `kindsFor` |
| `arbiter.jobs.*` metrics and the handler histogram | Stored labels declared by `kindsFor` |
| Producer span attribute `arbiter.kind` | `kindOf` |
| Consumer span attribute `arbiter.kind` | Stored job label |

The finite `kindsFor` set limits metric cardinality. Span attributes can include
an undeclared label.

API details: [`Arbiter.Core.Job.Kind`](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Kind.html).
