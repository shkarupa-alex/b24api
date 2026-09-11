# Migrating within b24api 2.x

This release keeps the established wire representation and transport compatibility, but changes
the default execution of direct no-count keyset traversal from sequential to auto. Existing JSON
requests and the `Transport.send()` protocol remain supported.

## Keyset verification, cursor fan-out, and page adaptation

Normal `iter_list_keyset()` no longer sends the five diagnostic canary commands. Range and
partitioned reports use `KeysetAssuranceSource.CALLER_ASSERTED_BOUNDS`; the legacy
`CANARY_VERIFIED_BOUNDS`, `KeysetPhase.CANARY`, and canary report counters remain readable for
compatibility, but normal traversal never produces that assurance and its canary counters are zero.
This reduces every bounded runtime estimate by the former canary waves, but a broken endpoint can
now emit a partial prefix before later bound validation raises typed `IncompleteTraversalError`.

Qualify the exact portal, credentials, request/filter, identity, ordering representation and page
cap on a stable development or staging fixture. Keep the guard immediately beside the production
call; verification does not switch runtime mode and is not cached:

```python
import os

if not os.environ.get("PROD"):
    await api.verify_keyset_capability(
        request,
        selector=selector,
        identity=identity,
        keyset=keyset,
    )

stream = api.iter_list_keyset(
    request,
    selector=selector,
    identity=identity,
    keyset=keyset,
)
```

`UNSUPPORTED` proves a shape, order, cap, or out-of-interval defect. `INCONCLUSIVE` is also
fail-closed, but means the fixture is sparse or changed during verification. Re-run after portal,
permission, request shape, filter, or server changes. Do not use fast keyset for
`crm.contact.userfield.list` or deprecated `crm.productsection.list`, which were observed to ignore
strict ID bounds; use `iter_list()` / `iter_list_counted()` instead. Prefer
`catalog.section.list` over the deprecated product-section method. These observations are guidance,
not a built-in endpoint registry or a guarantee for other portals.

For one or many independent parents, replace manual cursor batching or the verbose
`iter_references(..., traversal=CursorTraversal(...))` form with `iter_cursors()`:

```python
bindings = [
    Binding(
        "chat 42",
        (ParameterUpdate(ParameterPath(("DIALOG_ID",)), "chat42"),),
        correlation={"chat_id": 42},
        start_cursor=7300,
    ),
]

async with api.iter_cursors(
    request,
    bindings,
    selector=ResultSelector(("messages",)),
    cursor=cursor,
) as stream:
    async for event in stream:
        consume(event)
```

A one-element binding source is the canonical single-parent correlated form. The existing
`iter_list_cursor()` is not removed or renamed: it remains the simpler raw-row API for one request.
`iter_cursors()` is finite and fail-fast; tolerant processing remains
`iter_reference_outcomes(..., traversal=CursorTraversal(...))`. The application owns the sync/async
binding source and any database/session lifetime; `b24api` never owns or imports storage machinery.

`Binding.start_cursor` is an application-owned checkpoint. Persist the identity of the last item
successfully committed downstream, then recreate the binding after restart. Delivery is
at-least-once across the gap between stream delivery and durable commit, so downstream writes must
be idempotent. Each parent advances independently and still requires an empty confirmation page.

All list APIs now accept a synchronous object strategy `page_adapter`. It sees one immutable
`PageView` containing the full frozen result and the selected frozen items, and returns an
`AdaptedPage` with the same cardinality, order and configured identity/cursor values. For example,
an application-owned adapter may enrich `messages` from sibling `users` and `files` nodes:

```python
class ImMessagePageAdapter:
    def adapt(self, page: PageView, /) -> AdaptedPage:
        users = page.result["users"]
        return AdaptedPage(
            {**message, "author": users[str(message["author_id"])]}
            for message in page.items
        )
```

Adapters must be pure, reentrant and synchronous: no I/O, `await`, filtering, fan-out, aggregation
or reordering. Violations raise `PageAdaptationError`; inspect its closed
`PageAdaptationViolation`, while raw rows, sibling metadata, correlation and application exception
text remain absent from safe diagnostics.

`BatchDispatch` now defaults to `coalesce_wait=0.020`: an underfilled physical wave may wait up to
20 ms for another capacity-eligible producer. A full batch or zero producer potential is sent
immediately, and the absolute deadline is per wave, not per operation. Set `coalesce_wait=0` for
latency-oriented workloads. The dispatcher shutdown remains cancellation-based; the old unreachable
`None` queue sentinel has been removed.

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
| No-count/keyset list | `iter_list_keyset()` | Auto is now the default and may select boundary-only, sequential, range, or partitioned execution after a planning barrier. |
| Cursor wrappers | `iter_list_cursor()` / `iter_cursors()` | Raw single traversal, or correlated one/many-parent scheduling with optional per-binding seed. |
| Independent request wrappers | `fan_out()` / `fan_out_outcomes()` | Explicit direct or batch dispatch and delivery order. |
| Per-parent/reference wrappers | `Binding` + `iter_references()` / `iter_reference_outcomes()` | Parent correlation and traversal state are explicit and isolated. |

## Removed intentionally

- return-type-changing Python flags;
- tuple-shaped payload conventions;
- automatic endpoint profiles and method-name inference;
- permissive cursor de-duplication that could hide missing rows;
- automatic unsafe direct fallback;
- public low-level execution plans and compatibility data models.

There is no assumption-free fast no-count shortcut. Direct `Bitrix24.iter_list_keyset()` calls and
CLI keyset contracts that omit `execution` now assert the default `StableIntegerKeysetContract` and
use auto planning, so verify that the endpoint honors a unique integer identity, strict numeric
bounds, ordering, and empty-confirmation completion. `KeysetTraversal` used by reference traversal
remains explicitly sequential because reference keysets do not support fast execution. Account for
the pre-emission planning cost. Fast keyset totals remain advisory, and the application still owns
mutation and business-filter reconciliation.

Static ineligibility raises `CapabilityError` synchronously from the `iter_list_keyset(...)` call,
before iteration starts. Typical causes include a non-integer identity, caller-supplied order,
start, or strict-bound controls, incompatible consistency requirements, and insufficient policy
capacity. If the endpoint accepts the controls but violates the declared ordering or bounds, the
traversal fails closed with `IncompleteTraversalError` (for example, `range_contradiction`), possibly
after a partial prefix. Auto never restarts such a failed traversal as sequential.

If auto is ineligible for an endpoint, exposes a portal incompatibility, or the old request-by-request
behavior is required, opt out per call. This performs the original sequential keyset traversal and
does not run the auto planning barrier:

```python
from b24api import SequentialKeysetExecution

stream = client.iter_list_keyset(
    request,
    selector=selector,
    identity=identity,
    keyset=keyset,
    execution=SequentialKeysetExecution(),
)
```

For CLI keyset contracts, use `"execution": {"kind": "sequential"}`. Omitting `execution` (or
passing an empty execution object) selects auto. Use `iter_list_counted()` only when an endpoint
supplies an exact filtered total. CLI reports now include a `keyset_execution` object for default
keyset traversal; report consumers should treat that additive field as part of the selected-plan
evidence.

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
