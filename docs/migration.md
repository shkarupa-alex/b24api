# Migrating within b24api 2.x

This 2.x release keeps the established defaults but makes wire representation, traversal completion, and failure
evidence explicit. Existing JSON requests and the `Transport.send()` protocol remain supported.

## Important semantic corrections

- Unstructured status 408 and 5xx responses are ambiguous for `UNSAFE` and `UNKNOWN` requests;
  they are never replayed automatically. Structured Bitrix errors remain conclusive.
- API error text preserves the portal's wire spelling and shows the normalized spelling only when
  it differs. Match `.normalized_code`, not rendered text.
- Boolean values in form and physical-batch bracket encoding are now `1` and `0`; the old `False`
  text was truthy to PHP.
- `None` values are omitted from form and physical-batch bracket encoding, matching PHP query conventions;
  JSON encoding continues to send them as `null`.
- A 2xx response missing the canonical result envelope raises `EnvelopeContractError`, still a
  subclass of `HTTPGatewayError`. Malformed non-empty JSON remains `ProtocolError`.
- Counted identity is optional. Without it, matching a qualified total yields count-only assurance;
  with it, the report records identity-and-count assurance.
- Final 1xx and 3xx responses are classified as `HTTPGatewayError` with their actual status before
  envelope decoding.
- `RequestSummary.to_dict()` now includes bounded `encoding` and normalized `header_names` fields.
- `WireResponse.__repr__` is intentionally value-free; do not parse or snapshot its former body text.

Two construction/identity details can affect callers that used public values as low-level building blocks:

- `AmbiguousExecutionError` construction now requires explicit `reason` and `declared_unsafe` keywords;
- `Request` equality and hashing are structural and type-sensitive, so equivalent immutable request trees
  compare equal and can be used safely as dictionary or set keys.
- Canonical JSON nesting deeper than 256 levels is rejected at construction with `ValueError` for request,
  response, parameter-update, batch, and reference values; an over-deep server response surfaces as
  `ProtocolError`. This rejects the value before equality or hashing can encounter the recursion limit.

Use `UnknownRequestCollector` as the client's `unknown_request_audit` hook to inventory requests
that still rely on the default replay classification. The hook receives value-free summaries only.

Physical batches cannot represent per-command form bodies or scoped headers. Dispatch those requests
directly. Use `call_bytes()` explicitly for binary success responses. Fixed-step, split-order,
mapping-shape, and composite-identity configurations are shown in [endpoint recipes](recipes.md).

This release retains the compatibility removals already made in 2.x. Migrate by capability rather than by
preserving names or return-shaping flags.

| Earlier capability | Current 2.x operation | Important difference |
|---|---|---|
| Decoded `call()` | `call()` | Returns detached decoded JSON. |
| Raw/envelope call | `call_response()` | Always returns the immutable `Response` type. |
| Disable retries | `ExecutionPolicy(max_attempts_per_request=1)` | Replay safety is separately declared on `Request`. |
| Finite batch | `batch(Command(...))` | The logical source may be arbitrarily long; physical chunks remain bounded. |
| Payload tuples / `with_payload` | `Command.correlation` | Correlation stays off-wire and is present on typed outcomes. |
| Tolerant batch | `batch_outcomes()` | Handle the closed success/failure/not-executed/unknown union. |
| Sequential offset list | `iter_list()` | Conservative default; follows server continuation sequentially. |
| Counted batched list | `iter_list_counted()` | Direct head plus physically batched tail; requires an exact total; identity is optional but strengthens assurance. |
| No-count/keyset list | `iter_list_keyset()` | Sequential remains the default; opt into range, partitioned, or auto only for a caller-asserted stable unique integer key and writable strict bounds/order controls. |
| Cursor wrappers | `iter_list_cursor()` | Requires a strict unique monotonic cursor. |
| Independent request wrappers | `fan_out()` / `fan_out_outcomes()` | Explicit direct or batch dispatch and delivery order. |
| Per-parent/reference wrappers | `Binding` + `iter_references()` / `iter_reference_outcomes()` | Parent correlation and traversal state are explicit and isolated. |

## Removed intentionally

- return-type-changing Python flags;
- tuple-shaped payload conventions;
- automatic endpoint profiles and method-name inference;
- permissive cursor de-duplication that could hide missing rows;
- automatic unsafe direct fallback;
- public low-level execution plans and compatibility data models.

There is no assumption-free fast no-count shortcut. Existing calls stay sequential. For a verified
integer keyset, pass an explicit execution contract and account for its pre-emission planning cost;
otherwise keep exact sequential keyset/cursor traversal. Use `iter_list_counted()` only when an
endpoint supplies an exact filtered total. Fast keyset totals remain advisory, and the application
still owns mutation and business-filter reconciliation.

## Practical migration order

1. Replace direct calls and choose `call()` versus `call_response()` by required return type.
2. Mark replay safety explicitly for operations whose semantics are known.
3. Replace payload tuples with `Command.correlation` or `Binding.correlation`.
4. Choose list traversal from the table in README based on evidence the endpoint actually exposes.
5. Consume terminal reports and tolerant outcome unions exhaustively.
6. Add application checks for business filters and composite identities; the generic client cannot
   infer them from method names.

Signed-upload support is not exported because its acceptance gate was not established by the
available evidence. Counted-tail direct recovery is likewise not enabled: the observed failure was
an identity-contract defect, not a proven batch-only transport failure. Both omissions are deliberate
release-gate outcomes rather than silent fallbacks.
