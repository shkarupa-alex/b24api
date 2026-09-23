# Migrating to the issues architecture

The route is now a required part of every `Request` and closed request mapping. Existing callers
must choose `RouteKind.BARE`, `RouteKind.JSON` or `RouteKind.API_V3`; `with_parameters()` preserves
that choice. The CLI likewise requires `--route bare|json|api_v3` for `call`, `list` and
`verify-keyset`. A classic webhook base must have the form `/rest/<user>/<token>/`. The transport
resolves bare and `.json` suffixes or `/rest/api/<user>/<token>/` at dispatch. Physical batch
inner commands accept only `BARE`; use explicit direct dispatch for other routes. V3 accepts a JSON
body and reports object-valued API errors with typed validation details.

```python
from b24api import Request, RouteKind

request = Request("profile", route=RouteKind.BARE)
v3_request = Request("tasks.task.result.list", route=RouteKind.API_V3)
```

HTTPX INFO records for requests owned by `HttpxTransport` have their registered webhook URL
redacted before logging handlers format them. This applies to an injected `httpx.AsyncClient` while
it is used through that transport, including its redirect hops and any request its auth flow
substitutes. Any other request through the same client, even one sent from its event hook or auth
flow while an owned request is in flight, is logged unchanged.
Direct use of a caller-owned client after the transport closes is outside that shield. An application enabling the separate `httpcore` DEBUG logger needs its own
logging policy and test; this guarantee covers the emitting `httpx` INFO logger.
The supported HTTPX range is `>=0.28.1,<0.29`; raising that upper bound requires rerunning the
positive logger controls against the newly admitted version.

Direct access to `RateCoordinator.acquire()` now requires a non-empty `methods` frozenset. A
physical batch passes every inner method as one admission unit. `Retry-After` pauses the portal
host; `OPERATION_TIME_LIMIT` pauses only its method, with a configurable 120-second default.
The coordinator uses one portal host per `HttpxTransport` and rejects attempts to share it across
different hosts. An unsafe request or batch failure records the throttle without automatically
replaying the request. Callers can pass an absolute monotonic `DeadlineBudget` to bound permit
waits and handle typed budget, closed, and capacity errors.

Qualified PHP methods that take positional arguments now use `PositionalArguments` with an
explicit `PositionalLayout`. Pass that value as the second argument to `Request`. Slots are
represented by `Present`, `EmptyObject`, `EmptyArray`, `Null`, or a trailing `Omitted`. A layout
declares exact arity, each slot's shape, fixed slots, and case-sensitive writable control paths.
`write_control()` returns a new value and rejects an undeclared or missing parent path. A declared
final mapping leaf may be created when every parent container already exists. Positional requests
use a top-level JSON array; form encoding and physical batch reject them before I/O.

`OperationReport.exhausted` now records whether every binding reached qualified full-source
closure. A successful page-boundary stop therefore has `state=COMPLETED`, `exhausted=false`, and
`partial=true`. Failures, unknown outcomes, early close, and cleanup failure also cannot claim
exhaustion. Applications that previously treated successful state as a complete checkpoint must
gate that checkpoint on `report.exhausted`.

The public `page_stop` callback runs after the validated page has been delivered. Return only after
the page and its checkpoint are durably committed; the client emits `PageAcknowledged` after the
callback succeeds and before scheduling the next page. A callback exception is a traversal failure.
Counted physical-batch tails reject page-stop construction because already scheduled sibling pages
cannot be withdrawn safely.

Bounded keyset execution now needs a qualified admitted upper boundary, an enforced fence, and the
declared method contract in `BoundedIdentityRange`. `SequentialKeysetExecution` consumes that
boundary, closes only after the exact admitted upper ID is witnessed, and reports
`BOUNDED_RANGE_OBSERVED`; it does not need a trailing empty confirmation. Fast
`RangeKeysetExecution`, `PartitionedKeysetExecution`, and auto execution reject a boundary before
I/O, and so does a reference `KeysetTraversal`, because one filter-bound range cannot be shared by
bindings that rewrite parameters. `CALLER_ASSERTED_BOUNDS` describes the source of fast-execution bounds and does not assert a
stable snapshot of a mutating source.

Logical-batch `CommandFailure` and `CommandOutcomeUnknown`, reference failures and unknown outcomes,
blocking violations, and `IncompleteTraversalError` now expose the kernel's
`replay_disposition`. Retry only when it is `ReplayDisposition.ELIGIBLE`; replay safety and
retryability remain inputs to that closed decision.

For one-based page controls, pass `OffsetSpec(parameter_path=path, page_index=PageIndex(path,
initial=1, increment=1, max_rows=10))` to `iter_list(..., page_size=10)`. The wire control
advances by one even when a page selects fewer than ten rows. An empty page is the terminal
witness. `PageStride` records a method's qualified server offset granularity separately from its
decoded row cap and rejects increments that would alias a rounded server page. For a sparse
selected result, `SparseRawBound` adds an exact raw total path, fixed stride, finite page budget,
and stable-order contract; empty selected pages remain traversable until the raw range is covered.
This is structural coverage, so its report uses `RAW_RANGE_COVERED` rather than claiming a
snapshot of a mutable source.

`OffsetContinuation.FIXED_STEP` without `TotalTermination.EXACT_QUALIFIED` no longer accepts an
empty page after a short page as closure. Such a traversal now raises `IncompleteTraversalError`
(cause `PaginationError`) and reports `exhausted=False`; full pages followed by an empty page still
complete with `mechanics_only` assurance. Qualify an exact total, switch to `OBSERVED_COUNT` or
`PageIndex` when the endpoint supports them, or use `SparseRawBound`; see
[endpoint recipes](recipes.md#fixed-server-stride).

`SparseRawBound.total_path` also accepts `RawTotalSource.ENVELOPE` when the qualified raw extent is
the response envelope `total` rather than a field inside `result`; a missing or negative envelope
total fails closed like a missing result field.

Exact traversal keeps its in-memory `ExecutionPolicy.max_identity_keys` budget. `iter_list` and
`iter_list_counted` accept `identity_store=`, a caller-owned `IdentityStore` whose
`add_if_absent(identity_store_key(value)) -> bool` records each identity after the rest of the page
validates; `False` is a duplicate, and any exception rejects the page. The client does not close the
store. Under `DuplicatePolicy.REPORT`, an observed duplicate now withdraws identity strength: the
report carries `mechanics_only` instead of `identity_exact` or `identity_and_count_matched`.

When a stride's decoded row cap differs from its wire increment and the request owns a limit
control, set `requested_wire_limit` explicitly to at least the wire increment. Construction now
rejects an omitted or smaller value before I/O instead of leaving part of each wire window
unrequested. For ordinary traversal, `max_decoded_rows` must equal the wire increment, which
prevents both skipped subwindows and overlapping windows from claiming completion. Sparse raw-bound
traversal keeps its separate raw-range closure contract.

The earlier 2.x keyset migration notes below remain as historical guidance for that API.

## Keyset verification, cursor fan-out, and page adaptation

Normal `iter_list_keyset()` no longer sends the five diagnostic canary commands. Range and
partitioned reports use `KeysetAssuranceSource.CALLER_ASSERTED_BOUNDS`; the legacy
`CANARY_VERIFIED_BOUNDS`, `KeysetPhase.CANARY`, and canary report counters remain readable for
compatibility, but normal traversal never produces that assurance and its canary counters are zero.
This reduces every bounded runtime estimate by the former canary waves, but a broken endpoint can
now emit a partial prefix before later bound validation raises typed `IncompleteTraversalError`.

Capability-report identity evidence is recursively immutable (`FrozenJson`) in Python. Consumers
that need mutable or directly serializable containers should use `KeysetCapabilityReport.to_dict()`,
which returns a detached ordinary JSON tree. Fast-keyset adapter failures are identified in page
traces by the additive `PageRejectionCode.PAGE_ADAPTATION` enum member.

Qualify the exact portal, credentials, request/filter, identity, ordering representation and page
cap on a stable development or staging fixture. Keep the guard immediately beside the production
call; verification does not switch runtime mode and is not cached:

```python
import os

if os.environ.get("ENV") != "PROD":
    # Accepting an ID filter does not prove strict bounds or ordering.
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
When `CursorSpec.allow_create_controls=False`, the request must already contain the complete cursor
control path including its leaf. `start_cursor` may replace that leaf, but never creates it.

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
import os

from b24api import SequentialKeysetExecution

if os.environ.get("ENV") != "PROD":
    # Accepting an ID filter does not prove strict bounds or ordering.
    await client.verify_keyset_capability(
        request,
        selector=selector,
        identity=identity,
        keyset=keyset,
    )

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
