# Migration guide

## Upgrading from 2.3 to 3.0

3.0.0 changes only what the list below names. Work through it in order; each item links to the
details.

1. **Root imports.** The `b24api` root exports 51 names; 115 others moved to `b24api.contracts`,
   `b24api.errors`, `b24api.transport` or `b24api.completion`. Old imports still work in 3.x with a
   `DeprecationWarning`, and type checkers flag them. Run `python -m b24api.migration src/ tests/`
   to list them; see [Root imports in 3.0](#root-imports-in-30).
2. **Removed report vocabulary.** Values no code path produced are gone from `KeysetExecutionReport`,
   `KeysetAssuranceSource`, `ReplayDisposition`, `CompletionAssurance`, `SnapshotState` and
   `NotExecutedReason`, and `PageValidated` has no `identity_digest`. Drop those arms from exhaustive
   matches; see [Removed intentionally](#removed-intentionally).
3. **`EnvelopeContractError` is a `ProtocolError`.** `except ProtocolError` now also catches a 2xx
   response without the result envelope; see
   [Important semantic corrections](#important-semantic-corrections). When such a response answers
   the physical batch of a fast keyset wave, `page_trace` records `PageRejectionCode.BATCH_ENVELOPE`
   for its pages (previously `COMMAND_FAILURE`). A 2xx body with a top-level `error` that is not
   strict JSON (invalid UTF-8, `NaN` or `Infinity`, a duplicate batch correlation key) is now an
   `EnvelopeContractError` too, where 2.3 reported the embedded error as `ApiResponseError`.
4. **A logical batch closed early says so.** Closing `batch()` or `batch_outcomes()` before the input
   is exhausted gives an `EARLY_CLOSED` report whose `terminal_reason` is
   `"stream closed before exhaustion"` (previously `"GeneratorExit"`).
5. **Reports name the public failure.** A fail-fast batch reports `terminal_reason="BatchCommandError"`
   with the violation `batch_command_failure`, instead of a private carrier class and
   `internal_failure`. Code that matched the old strings must match the new ones.
6. **Fixed step refuses at once.** `OffsetContinuation.FIXED_STEP` without an exact qualified total
   raises right after a short page instead of first requesting an unusable confirmation page, so such
   a traversal sends one request fewer.
7. **Stream lifecycle.** Every stream family now terminates through one lifecycle owner:
   - reading a stream after `aclose()` ends the iteration with `StopAsyncIteration` and sends
     nothing (previously `RuntimeError("stream was closed before exhaustion")`);
   - `aclose()` during an in-flight read reports `EARLY_CLOSED` (previously `CANCELLED`): the close
     owns termination, even though it cancels the read;
   - a report is final once published, after cleanup; a later `aclose()` never raises a cleanup error
     or changes it;
   - when cleanup fails after the source was exhausted, the report records a `cleanup_failure`
     violation and `CleanupState.FAILURE`, and the cleanup error is raised;
   - a failure during cleanup that follows another failure is kept as a secondary `cleanup_failure`
     violation (for example `reference cleanup also failed (RuntimeError)`), so a report can carry
     more than one.
   - a stream closed before its first read, by `aclose()` or on leaving `async with`, still closes
     the iterator it took from your command or reference source, once, without reading from it; a
     failing close is raised and recorded as a `cleanup_failure` on an `EARLY_CLOSED` report;
   - a stream raises its terminal failure once: a later read, before or after `aclose()`, ends the
     iteration with `StopAsyncIteration` and sends nothing (previously the same exception was raised
     again). Keep the exception from the read that raised it if you need it later; it carries the
     published report;
   - a cancellation during the cleanup on leaving `async with` no longer replaces the exception raised
     in its body: that exception propagates and the cancellation is raised at the next `await`.

8. **Compressed responses.** Only identity, `gzip` and `deflate` bodies are decoded; `br`, `zstd`,
   stacked and unknown codings are refused as a transport failure. Library-owned requests send
   `Accept-Encoding: gzip, deflate`. If you inject an HTTPX client that asks for `br` or `zstd`,
   remove that header.
9. **Batch replay is decided per command.** A failed physical batch is not retried or abandoned as
   a whole. Each `SAFE` command is sent again, in a smaller physical batch, after any transient
   failure. An `UNSAFE` or `UNKNOWN` command is sent again only when the failure proves it did not
   run: a transport failure before dispatch, an unstructured 423, 425 or 429, or a
   `QUERY_LIMIT_EXCEEDED` / `OPERATION_TIME_LIMIT` refusal of the batch or of that command in
   `result_error`. When the batch may have run (a transport failure after dispatch, or a 408 or 5xx
   status from `AmbiguityPolicy.ambiguous_unstructured_statuses`), its `UNSAFE` and `UNKNOWN`
   commands arrive as `CommandOutcomeUnknown`; reconcile them as you would an ambiguous direct call.
   In 2.3 the whole batch was retried or not by its least safe command. Replays share the request's
   attempt and time budgets. A command keeps its last outcome when the budget stops a replay before
   it is sent; a replay that was sent reports the command as possibly executed, unless its last
   answer was a listed refusal. A direct `UNSAFE`
   or `UNKNOWN` request is now also retried after such a proven refusal. The refusals come from
   `AmbiguityPolicy.refusal_http_statuses` and `AmbiguityPolicy.refusal_api_codes`. A code or status
   you add to `RetryPolicy` alone retries `SAFE` work only; add it to the refusal sets too if it
   proves that Bitrix did not run the method. A fail-fast `batch()` is not split.
10. **Error text.** Error descriptions show field names from your request as `field#N`, known V3
    codes verbatim, and distinct hidden mapping keys as `[REDACTED#1]`, `[REDACTED#2]`, … Code that
    parses error strings must accept these forms.
11. **Mid-collection start with an exact total.** An offset traversal whose initial `start` differs
    from the plan's first offset raises `CapabilityError` before any request when
    `TotalTermination.EXACT_QUALIFIED` closes it. Start from the beginning, or use
    `TotalTermination.DISABLED` to walk a suffix.
12. **Keyset verification.** When a boundary read is answered wrongly, `verify_keyset_capability()`
    raises `KeysetCapabilityError` with an `UNSUPPORTED` report (`error.report`) instead of a raw
    `PaginationError` (order, page cap) or `CapabilityError` (identity shape). Code that caught
    `PaginationError` there must catch `KeysetCapabilityError` or `CapabilityError`.
13. **Keyset selection reasons.** `KeysetSelectionReason` gains `EXPLICIT_SEQUENTIAL` and
    `PAGE_STOP`; handle them in exhaustive matches.
14. **HTTP/2 and hpack logging.** While a library HTTPX client is open, `hpack.hpack` and
    `hpack.table` records are dropped. Removing that filter makes the next HTTP/2 send raise
    `CapabilityError` before I/O.
15. **Permanent transport refusals.** A `TransportError` with `retryable=False` is raised after one
    send instead of being retried until `BudgetExceededError`. Code that caught
    `BudgetExceededError` for such a failure must catch `TransportError`.
16. **Exceptions from an injected transport.** An arbitrary exception raised inside a custom
    `Transport.send` or `send_wire` becomes `TransportError(phase=DISPATCH_STARTED, retryable=False)`
    with your exception as `__cause__`. A direct `SAFE` request raises that `TransportError` after one
    send; a direct `UNKNOWN` or `UNSAFE` request raises `AmbiguousExecutionError`; every admitted
    command of a physical batch arrives as `CommandOutcomeUnknown`. Catch these instead of your own
    exception class, and read `__cause__` for the original. A closed `HttpxTransport` raises
    `TransportError(phase=NOT_DISPATCHED, retryable=False)` instead of `RuntimeError`: nothing was
    sent, so no command is reported as possibly executed.
17. **Oversized responses.** A response larger than `ExecutionPolicy.max_response_bytes` is refused
    before decoding. An injected transport is now held to the same ceiling as the bundled one: a
    direct `SAFE` request raises `ResponseTooLargeError`, and a direct `UNKNOWN` or `UNSAFE` request
    raises `AmbiguousExecutionError`. For a physical batch the change applies to every transport,
    the bundled `HttpxTransport` included: each command arrives as `CommandOutcomeUnknown` with the
    `ResponseTooLargeError` as the cause, where 2.3 gave a `CommandFailure`, and a keyset page trace
    records the page as an ambiguous execution rather than a command failure. Code that retried
    such commands with a smaller `select` must now treat them as possibly executed.

Additions that need no change: `Request.bare()` and `Request.v3()`, `Bitrix24.from_webhook()`,
`HttpxTransport` in the root, `ExecutionPolicy.from_settings()` and
`OperationReport.keyset_selection`.

The sections below describe the full current contract, including the 2.3 changes (a required
`route=`, positional arguments, page stops) for code upgrading from earlier releases.

## Migrating to the issues architecture (2.3)

The route is now a required part of every `Request` and closed request mapping. Existing callers
must choose `RouteKind.BARE`, `RouteKind.JSON` or `RouteKind.API_V3`; `with_parameters()` preserves
that choice. The CLI likewise requires `--route bare|json|api_v3` for `call`, `list` and
`verify-keyset`. A classic webhook base must have the form `/rest/<user>/<token>/`. The transport
resolves bare and `.json` suffixes or `/rest/api/<user>/<token>/` at dispatch. Physical batch
inner commands accept only `BARE`; use explicit direct dispatch for other routes. V3 accepts a JSON
body and reports object-valued API errors with typed validation details. A transport serves only the
routes it declares in `TransportCapabilities.routes`, which defaults to `BARE`; `HttpxTransport`
declares all three. A custom send-only transport, which cannot declare routes, serves only `BARE`.
An undeclared route is refused with `CapabilityError` before any request, instead of silently
reaching the classic handler.

```python
from b24api import Request, RouteKind

request = Request("profile", route=RouteKind.BARE)
v3_request = Request("tasks.task.result.list", route=RouteKind.API_V3)
```

HTTPX INFO records for requests owned by `HttpxTransport` have their registered webhook URL
redacted before logging handlers format them. This applies to an injected `httpx.AsyncClient` while
it is used through that transport, including its redirect hops. Any other request through the same
client, even one sent from its event hook or auth flow while an owned request is in flight, is logged
unchanged except that the registered webhook secret itself is never logged.
An injected client's `auth` still runs on owned requests when it authorizes the request in place, as
`httpx.BasicAuth`, `httpx.DigestAuth` and header-setting flows do. A flow that yields any other
`Request` (a clone, a credential rotation or an unrelated request) is refused before that request is
sent, with a non-retryable `TransportError` whose phase records whether the owned request had
already been answered: a substitute would make it undecidable which records carry the webhook.
Direct use of a caller-owned client after the transport closes is outside that shield. An
application enabling the separate `httpcore` DEBUG logger needs its own logging policy and test;
this guarantee covers the emitting `httpx` INFO logger.
The supported HTTPX range is `>=0.28.1,<0.29`, with `h2>=4.3.0,<4.5` and `hpack>=4.1.0,<4.3` as
direct requirements, because the shield filters the `hpack` logger names verified on those lines. An
environment pinned below them must upgrade; raising any of these upper bounds requires rerunning the
positive logger controls against the newly admitted version.

Direct access to `RateCoordinator.acquire()` now requires a non-empty `methods` frozenset. A
physical batch passes every inner method as one admission unit. `Retry-After` pauses the portal
host; `OPERATION_TIME_LIMIT` pauses only its method, with a configurable 120-second default.
The coordinator binds the portal host of every transport it serves, `HttpxTransport` or custom,
through the `Transport.host` property, and rejects attempts to share it across different hosts; a
custom transport without `host` is refused with `TypeError` when the `Executor` is built. An unsafe
request or batch failure records the throttle without automatically replaying the request. Callers
can pass an absolute monotonic `DeadlineBudget` to bound permit waits and handle typed budget,
closed, and capacity errors.

Qualified PHP methods that take positional arguments now use `PositionalArguments` with an
explicit `PositionalLayout`. Pass that value as the second argument to `Request`. Slots are
represented by `Present`, `EmptyObject`, `EmptyArray`, `Null`, or a trailing `Omitted`. A layout
declares exact arity, each slot's shape, fixed slots, and case-sensitive writable control paths.
`write_control()` returns a new value and rejects an undeclared or missing parent path. A declared
final mapping leaf may be created when every parent container already exists. Positional requests
use a top-level JSON array; form encoding and physical batch reject them before I/O. A traversal
control the slots cannot accept raises `CapabilityError` before I/O; its message names a value-free
reason and its cause is the positional control error carrying a
`b24api.contracts.positional.PositionalControlFault`.

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

A per-reference `IncompleteTraversalError` (the `error` of a `ReferenceFailure`, including those in
`ReferenceFailed.outcomes`) now carries `report=None`: only the whole stream has an operation report,
available as `stream.report` and `ReferenceFailed.report`. Read the binding's typed cause from the
error's `error` attribute and its delivered prefix from `ReferenceFailure.partial_rows`.

For one-based page controls, pass `OffsetSpec(parameter_path=path, page_index=PageIndex(path,
initial=1, increment=1, max_rows=10))` to `iter_list(..., page_size=10)`. The wire control
advances by one even when a page selects fewer than ten rows. An empty page is the terminal
witness. `PageStride` records a method's qualified server offset granularity separately from its
decoded row cap and rejects increments that would alias a rounded server page. For a sparse
selected result, `SparseRawBound` adds an exact raw total path, fixed stride, finite page budget,
and stable-order contract; empty selected pages remain traversable until the raw range is covered.
This is structural coverage, so its report uses `RAW_RANGE_COVERED` rather than claiming a
snapshot of a mutable source. A sparse traversal starts at offset zero and is available only
through `iter_list`: a resumed nonzero start, an offset off the server granularity, and a reference
traversal with `SparseRawBound` are each refused with `CapabilityError` before any request.

`OffsetContinuation.FIXED_STEP` without `TotalTermination.EXACT_QUALIFIED` no longer accepts an
empty page after a short page as closure. Such a traversal now raises `IncompleteTraversalError`
(cause `PaginationError`) and reports `exhausted=False`; full pages followed by an empty page still
complete with `mechanics_only` assurance. Qualify an exact total, switch to `OBSERVED_COUNT` or
`PageIndex` when the endpoint supports them, or use `SparseRawBound`; see
[endpoint recipes](recipes.md#fixed-server-stride). Assurance names what a completed traversal
proved: a report that did not complete carries at most `mechanics_only`, whatever its plan declared
and even when a caller stopped one of its bindings; `bounded_prefix` appears only on a completed
report. `completed_with_failures` keeps the declared assurance, which describes the bindings that
completed.

`SparseRawBound.total_path` also accepts `RawTotalSource.ENVELOPE` when the qualified raw extent is
the response envelope `total` rather than a field inside `result`; a missing or negative envelope
total fails closed like a missing result field.

Exact traversal now has an in-memory `ExecutionPolicy.max_identity_keys` budget (100,000 keys by
default); exceeding it fails the operation closed. `iter_list` and
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

## Root imports in 3.0

The `b24api` root now exports only the names a typical application needs: the client and its
settings, requests and routes, policies, traversal and dispatch specifications, the report and the
main errors, and the transports, including `HttpxTransport`. The other 115 names moved to one of the
public packages `b24api.contracts`, `b24api.errors`, `b24api.transport` and `b24api.completion`,
which export the same objects. No object was removed or renamed.

For all of 3.x the old root paths still work: `from b24api import CommandSuccess` returns the same
class and emits `DeprecationWarning: b24api.CommandSuccess moved to b24api.contracts.CommandSuccess`.
Type checkers do not see these aliases, so mypy and pyright report each old root import as
`attr-defined`; that is intentional and points at every line to change. Leaf modules such as
`b24api.contracts.request` or `b24api.transport.base` are not public paths; import from the packages
above.

To list every old root import in your code, run the scanner. It rewrites nothing and exits with 1
when it finds any:

```console
$ python -m b24api.migration src/ tests/
src/app.py:3 b24api.CommandSuccess -> b24api.contracts.CommandSuccess
```

<!-- ROOT_MOVES table: generated by b24api.migration.migration_table(); do not edit by hand -->
| Old import | New import |
|---|---|
| `from b24api import CompletionGate` | `from b24api.completion import CompletionGate` |
| `from b24api import AdaptedPage` | `from b24api.contracts import AdaptedPage` |
| `from b24api import AmbiguityReason` | `from b24api.contracts import AmbiguityReason` |
| `from b24api import BinaryEvidence` | `from b24api.contracts import BinaryEvidence` |
| `from b24api import BinaryResponse` | `from b24api.contracts import BinaryResponse` |
| `from b24api import BindingAdmitted` | `from b24api.contracts import BindingAdmitted` |
| `from b24api import BindingClosure` | `from b24api.contracts import BindingClosure` |
| `from b24api import BindingTerminal` | `from b24api.contracts import BindingTerminal` |
| `from b24api import BodyEncoding` | `from b24api.contracts import BodyEncoding` |
| `from b24api import CallerStop` | `from b24api.contracts import CallerStop` |
| `from b24api import CleanupOutcome` | `from b24api.contracts import CleanupOutcome` |
| `from b24api import CleanupState` | `from b24api.contracts import CleanupState` |
| `from b24api import ClosureWitness` | `from b24api.contracts import ClosureWitness` |
| `from b24api import Command` | `from b24api.contracts import Command` |
| `from b24api import CommandFailure` | `from b24api.contracts import CommandFailure` |
| `from b24api import CommandNotExecuted` | `from b24api.contracts import CommandNotExecuted` |
| `from b24api import CommandOutcome` | `from b24api.contracts import CommandOutcome` |
| `from b24api import CommandOutcomeUnknown` | `from b24api.contracts import CommandOutcomeUnknown` |
| `from b24api import CommandSettlement` | `from b24api.contracts import CommandSettlement` |
| `from b24api import CommandSuccess` | `from b24api.contracts import CommandSuccess` |
| `from b24api import CompletionEvent` | `from b24api.contracts import CompletionEvent` |
| `from b24api import CompositeIdentitySpec` | `from b24api.contracts import CompositeIdentitySpec` |
| `from b24api import ConsistencyPolicy` | `from b24api.contracts import ConsistencyPolicy` |
| `from b24api import ContinuePage` | `from b24api.contracts import ContinuePage` |
| `from b24api import CursorDomain` | `from b24api.contracts import CursorDomain` |
| `from b24api import DeliveryOrder` | `from b24api.contracts import DeliveryOrder` |
| `from b24api import DuplicatePolicy` | `from b24api.contracts import DuplicatePolicy` |
| `from b24api import EmptyArray` | `from b24api.contracts import EmptyArray` |
| `from b24api import EmptyObject` | `from b24api.contracts import EmptyObject` |
| `from b24api import FrozenJson` | `from b24api.contracts import FrozenJson` |
| `from b24api import FrozenMapping` | `from b24api.contracts import FrozenMapping` |
| `from b24api import identity_store_key` | `from b24api.contracts import identity_store_key` |
| `from b24api import IdentityComponent` | `from b24api.contracts import IdentityComponent` |
| `from b24api import IdentityPageAdapter` | `from b24api.contracts import IdentityPageAdapter` |
| `from b24api import IdentityStore` | `from b24api.contracts import IdentityStore` |
| `from b24api import KeysetAssuranceSource` | `from b24api.contracts import KeysetAssuranceSource` |
| `from b24api import KeysetCapabilityCheckName` | `from b24api.contracts import KeysetCapabilityCheckName` |
| `from b24api import KeysetCapabilityCheckOutcome` | `from b24api.contracts import KeysetCapabilityCheckOutcome` |
| `from b24api import KeysetCapabilityCheckResult` | `from b24api.contracts import KeysetCapabilityCheckResult` |
| `from b24api import KeysetCapabilityReport` | `from b24api.contracts import KeysetCapabilityReport` |
| `from b24api import KeysetCapabilityVerdict` | `from b24api.contracts import KeysetCapabilityVerdict` |
| `from b24api import KeysetExecution` | `from b24api.contracts import KeysetExecution` |
| `from b24api import KeysetExecutionKind` | `from b24api.contracts import KeysetExecutionKind` |
| `from b24api import KeysetExecutionReport` | `from b24api.contracts import KeysetExecutionReport` |
| `from b24api import KeysetInconclusiveReason` | `from b24api.contracts import KeysetInconclusiveReason` |
| `from b24api import KeysetPageCompletion` | `from b24api.contracts import KeysetPageCompletion` |
| `from b24api import KeysetPhase` | `from b24api.contracts import KeysetPhase` |
| `from b24api import KeysetSelectionReason` | `from b24api.contracts import KeysetSelectionReason` |
| `from b24api import MembershipRecheck` | `from b24api.contracts import MembershipRecheck` |
| `from b24api import NotExecutedReason` | `from b24api.contracts import NotExecutedReason` |
| `from b24api import Null` | `from b24api.contracts import Null` |
| `from b24api import Omitted` | `from b24api.contracts import Omitted` |
| `from b24api import OperationStream` | `from b24api.contracts import OperationStream` |
| `from b24api import PageAcknowledged` | `from b24api.contracts import PageAcknowledged` |
| `from b24api import PageBoundary` | `from b24api.contracts import PageBoundary` |
| `from b24api import PageCommandOutcome` | `from b24api.contracts import PageCommandOutcome` |
| `from b24api import PageDelivered` | `from b24api.contracts import PageDelivered` |
| `from b24api import PageDispatch` | `from b24api.contracts import PageDispatch` |
| `from b24api import PageIndex` | `from b24api.contracts import PageIndex` |
| `from b24api import PageOutcome` | `from b24api.contracts import PageOutcome` |
| `from b24api import PageRecord` | `from b24api.contracts import PageRecord` |
| `from b24api import PageRejected` | `from b24api.contracts import PageRejected` |
| `from b24api import PageRejectionCode` | `from b24api.contracts import PageRejectionCode` |
| `from b24api import PageScheduled` | `from b24api.contracts import PageScheduled` |
| `from b24api import PageStride` | `from b24api.contracts import PageStride` |
| `from b24api import PageValidated` | `from b24api.contracts import PageValidated` |
| `from b24api import PageView` | `from b24api.contracts import PageView` |
| `from b24api import PartialResult` | `from b24api.contracts import PartialResult` |
| `from b24api import partition_command_outcomes` | `from b24api.contracts import partition_command_outcomes` |
| `from b24api import partition_reference_outcomes` | `from b24api.contracts import partition_reference_outcomes` |
| `from b24api import PositionalArguments` | `from b24api.contracts import PositionalArguments` |
| `from b24api import PositionalLayout` | `from b24api.contracts import PositionalLayout` |
| `from b24api import Present` | `from b24api.contracts import Present` |
| `from b24api import RawTotalSource` | `from b24api.contracts import RawTotalSource` |
| `from b24api import ReferenceComplete` | `from b24api.contracts import ReferenceComplete` |
| `from b24api import ReferenceEvent` | `from b24api.contracts import ReferenceEvent` |
| `from b24api import ReferenceFailure` | `from b24api.contracts import ReferenceFailure` |
| `from b24api import ReferenceItem` | `from b24api.contracts import ReferenceItem` |
| `from b24api import ReferenceNotExecuted` | `from b24api.contracts import ReferenceNotExecuted` |
| `from b24api import ReferenceOutcome` | `from b24api.contracts import ReferenceOutcome` |
| `from b24api import ReferenceOutcomeUnknown` | `from b24api.contracts import ReferenceOutcomeUnknown` |
| `from b24api import ReplayDisposition` | `from b24api.contracts import ReplayDisposition` |
| `from b24api import RequestHeaders` | `from b24api.contracts import RequestHeaders` |
| `from b24api import RequestSummary` | `from b24api.contracts import RequestSummary` |
| `from b24api import Response` | `from b24api.contracts import Response` |
| `from b24api import ResultCollectionShape` | `from b24api.contracts import ResultCollectionShape` |
| `from b24api import ResultErrorShape` | `from b24api.contracts import ResultErrorShape` |
| `from b24api import ResultErrorSpec` | `from b24api.contracts import ResultErrorSpec` |
| `from b24api import SlotContract` | `from b24api.contracts import SlotContract` |
| `from b24api import SlotShape` | `from b24api.contracts import SlotShape` |
| `from b24api import SparseRawBound` | `from b24api.contracts import SparseRawBound` |
| `from b24api import SplitOrderSpec` | `from b24api.contracts import SplitOrderSpec` |
| `from b24api import StreamClosure` | `from b24api.contracts import StreamClosure` |
| `from b24api import StreamTerminal` | `from b24api.contracts import StreamTerminal` |
| `from b24api import TotalHintMode` | `from b24api.contracts import TotalHintMode` |
| `from b24api import TraceClass` | `from b24api.contracts import TraceClass` |
| `from b24api import traversal_control_paths` | `from b24api.contracts import traversal_control_paths` |
| `from b24api import TraversalIdentity` | `from b24api.contracts import TraversalIdentity` |
| `from b24api import UnknownRequestAudit` | `from b24api.contracts import UnknownRequestAudit` |
| `from b24api import UnknownRequestCollector` | `from b24api.contracts import UnknownRequestCollector` |
| `from b24api import Violation` | `from b24api.contracts import Violation` |
| `from b24api import ViolationSeverity` | `from b24api.contracts import ViolationSeverity` |
| `from b24api import BatchCommandError` | `from b24api.errors import BatchCommandError` |
| `from b24api import EnvelopeContractError` | `from b24api.errors import EnvelopeContractError` |
| `from b24api import HTTPGatewayError` | `from b24api.errors import HTTPGatewayError` |
| `from b24api import IdentityContractError` | `from b24api.errors import IdentityContractError` |
| `from b24api import InputSourceError` | `from b24api.errors import InputSourceError` |
| `from b24api import PageAdaptationError` | `from b24api.errors import PageAdaptationError` |
| `from b24api import PageAdaptationViolation` | `from b24api.errors import PageAdaptationViolation` |
| `from b24api import ResponseTooLargeError` | `from b24api.errors import ResponseTooLargeError` |
| `from b24api import ResultShapeError` | `from b24api.errors import ResultShapeError` |
| `from b24api import ValidationIssue` | `from b24api.errors import ValidationIssue` |
| `from b24api import TransportCapabilities` | `from b24api.transport import TransportCapabilities` |
| `from b24api import WireRequest` | `from b24api.transport import WireRequest` |
| `from b24api import WireResponse` | `from b24api.transport import WireResponse` |
<!-- end of ROOT_MOVES table -->

## Keyset verification, cursor fan-out, and page adaptation

Normal `iter_list_keyset()` no longer sends the five diagnostic canary commands. Range and
partitioned reports use `KeysetAssuranceSource.CALLER_ASSERTED_BOUNDS`. 3.0.0 removes the legacy
`KeysetAssuranceSource.CANARY_VERIFIED_BOUNDS` and the always-zero `canary_requests`,
`canary_commands` and `canary_rows` fields of `KeysetExecutionReport`; `KeysetPhase.CANARY` remains
because `verify_keyset_capability()` still records its canary wave under that phase.
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
- A 2xx response missing the canonical result envelope raises `EnvelopeContractError`. It is a
  subclass of both `HTTPGatewayError` and `ProtocolError`, so `except ProtocolError` now catches it
  too; its origin stays `http_gateway`, it is never retried, and reports still classify it as
  `envelope_contract`. Malformed non-empty JSON remains a plain `ProtocolError`.
- `ExecutionPolicy.from_settings(settings)` returns the client's default policy (the library
  defaults with `max_retry_elapsed_per_request = settings.http_timeout`). A `policy=` argument still
  replaces that default wholesale; derive it with `dataclasses.replace` to keep the timeout.
- Counted identity is optional. Without it, matching a qualified total yields count-only assurance;
  with it, the report records identity-and-count assurance.
- `iter_list_counted()` no longer fails when the first page has no rows, no `next`, and no usable
  `total` (missing, `null`, or `-1`). It completes and is exhausted after that one request, with
  `mechanics_only` assurance (`identity_exact` with an identity) and the terminal reason
  `empty source observed without a total`. Callers that must see an exact total set
  `ConsistencyPolicy.confirmation_policy` to `b24api.contracts.policy.ConfirmationPolicy.QUALIFIED_TOTAL`,
  which keeps such a page incomplete.
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
- report values no code path produced (3.0.0): `ReplayDisposition.REPLAYED_DIRECT` and
  `DIRECT_REPLAY_FAILED` (a physical batch is never replayed as direct calls),
  `CompletionAssurance.ORACLE_VERIFIED`, `SnapshotState.VERIFIED` and `SnapshotState.CHANGED`
  (no snapshot oracle exists; a required snapshot reports `UNVERIFIED`), and
  `NotExecutedReason.SCHEDULER_STOPPED`. Drop those arms from exhaustive matches; the enum inputs
  `SnapshotRequirement` and `ConfirmationPolicy` keep every member.
- `PageValidated.identity_digest` (3.0.0). The gate only ever checked that the digest was non-empty,
  so every recorder paid a SHA-256 per page for no guarantee. The gate still checks event order and
  `row_count`; a custom recorder or test that builds `PageValidated` drops the argument.

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
supplies an exact filtered total. CLI reports now include a `keyset_selection` object
(`requested_kind`, `selected_kind`, `reason`) for every keyset traversal, and a detailed
`keyset_execution` object only when the fast path ran; a `page_stop` traversal with the default auto
execution reports `auto`, `sequential`, `page_stop`. Report consumers should treat both additive
fields as selected-plan evidence.

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
