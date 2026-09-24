# Release notes

## Unreleased (3.0.0)

- **Breaking (3.0.0):** the `b24api` root exports 51 names instead of 165 (C7). The other 115 moved
  to `b24api.contracts`, `b24api.errors`, `b24api.transport` or `b24api.completion`, which export
  the same objects; nothing was removed or renamed. The old root paths keep working for all of 3.x
  with a `DeprecationWarning` naming the new path, but type checkers report them as `attr-defined`.
  `python -m b24api.migration PATH...` lists every old root import, and the migration guide has the
  full table, generated from `b24api.migration.ROOT_MOVES`. `KeysetSelectionSummary` is exported
  only by `b24api.contracts`.
- `HttpxTransport` is exported from the root.
- **Breaking (3.0.0):** response bodies are decompressed inside a bounded decoder. Only identity or
  one `gzip`/`deflate` coding is accepted; stacked, unknown, `br` and `zstd` codings are refused
  before decompression as a body-read transport failure. Library-owned requests send
  `Accept-Encoding: gzip, deflate`; a header set by the caller or an injected client is kept (A2).
- **Breaking (3.0.0):** a physical batch that may have been accepted when its transport failed is
  never replayed, even when every command is `SAFE`; each admitted command becomes
  `CommandOutcomeUnknown` with its own `AmbiguousExecutionError` (A3). A physical batch answered with
  an HTTP error status and no Bitrix envelope is not replayed either, even when `SAFE`: its commands
  become unknown after 408 or 5xx and fail after 423, 425 or 429 (in 2.3 a `SAFE` batch was replayed).
  A transport failure marked `retryable=False` is raised once instead of exhausting the attempt budget.
  An arbitrary exception from an injected transport becomes
  `TransportError(phase=DISPATCH_STARTED, retryable=False)` with the original as its cause, and a
  closed `HttpxTransport` refuses with `TransportError(phase=NOT_DISPATCHED, retryable=False)`
  instead of `RuntimeError` (A13). A response over `max_response_bytes` from an injected transport is refused before decoding, and on
  any transport, the bundled one included, every command of a physical batch whose response is
  refused becomes `CommandOutcomeUnknown` instead of a `CommandFailure` (B29).
- **Breaking (3.0.0):** error rendering is contextual. Known V3 error codes are shown verbatim, field
  names taken from the request render as `field#N` aliases, the request's own sensitive values are
  exact secrets, and distinct hidden mapping keys become `[REDACTED#1]`, `[REDACTED#2]`, … instead
  of merging into one key. A registered secret of any length is hidden; one shorter than six
  characters is replaced where it stands as a whole token. `str()`, `repr()`, `to_safe_dict()`,
  reports and the CLI show the same text (A7).
- **Breaking (3.0.0):** an offset traversal that starts mid-collection while a caller-qualified exact
  total (`TotalTermination.EXACT_QUALIFIED`) closes it raises `CapabilityError` before any request,
  instead of ending incomplete; with `TotalTermination.DISABLED` the suffix traversal still works
  (A9).
- **Breaking (3.0.0):** when the portal answers a boundary read wrongly, `verify_keyset_capability()`
  raises `KeysetCapabilityError` carrying an `UNSUPPORTED` report, instead of a raw `PaginationError`
  (order, page cap) or `CapabilityError` (identity shape). The report marks the failed boundary
  check (`TWO_ROW_ASC` and/or `TWO_ROW_DESC`) as `ORDER_INVALID`, `CAP_EXCEEDED` or `SHAPE_INVALID`
  and the other checks as `NOT_EXECUTED`. `KeysetCapabilityError` is a `CapabilityError`, not a
  `PaginationError`. The CLI exits with 6 (A10).
- **Breaking (3.0.0):** closing a logical batch early reports `EARLY_CLOSED` with the reason
  `"stream closed before exhaustion"` (A12). A fail-fast batch reports `BatchCommandError` and the
  violation `batch_command_failure` instead of a private carrier class (A14). A failed source close
  after an early close publishes exactly one report, and a stream closed before its first read closes
  the caller's source iterator once (A11).
- **Breaking (3.0.0):** `OffsetContinuation.FIXED_STEP` without an exact qualified total fails right
  after a short page, without the unusable confirmation request (B10).
- **Breaking (3.0.0):** `EnvelopeContractError` is also a `ProtocolError` with gateway origin (B20).
  A fast keyset wave whose physical batch gets such a response records `BATCH_ENVELOPE` in
  `page_trace` instead of `COMMAND_FAILURE`. A 2xx body with a top-level `error` that only the strict
  parse rejects (invalid UTF-8, a non-finite number, a duplicate correlation key) is an
  `EnvelopeContractError` instead of an `ApiResponseError` (B8).
- The HTTPX log shield drops every `hpack.hpack` and `hpack.table` record while a library HTTPX
  client is open. An HTTP/2 send is refused before I/O with `CapabilityError` when that filter was
  removed or an injected client cannot be registered (A20). The `httpx` logger filter that hides a
  registered webhook secret stays installed while an injected client outlives its transport.
- Every keyset traversal reports `OperationReport.keyset_selection`;
  `KeysetSelectionReason` gains `EXPLICIT_SEQUENTIAL` and `PAGE_STOP`, which exhaustive matches must
  handle (B9).
- `ExecutionPolicy.from_settings(settings)` returns the client's default policy; a `policy=` argument
  replaces it wholesale (B18).
- `X-Bitrix-RateLimit-Reset` accepts epoch seconds (≥ 1e9, against the wall clock) besides delta
  seconds and HTTP-dates; the delay stays capped, and an unusable header never starts a cooldown (A19).
- The client closes the rate coordinator it owns, and permits release synchronously (A5, B0). An
  admission audit retains at most 128 violations, like a report (A16).
- The wheel ships `py.typed` (A6), and the minimum pydantic is 2.12.0 (A4).
- Development: `pytest-httpx` is no longer a dev dependency. Tests share one offline responder
  transport and build clients through the public constructor, and the README, recipe and migration
  examples are bound against the real `Bitrix24` signatures (B13, B15).
- `Request.bare(...)` and `Request.v3(...)` build a request with the route fixed; they take the
  constructor's other arguments unchanged, and `v3` keeps the V3 contract (a mapping, JSON only). The
  route stays explicit and `Request(...)` is unchanged (C1, C13).
- `Bitrix24.from_webhook(url, *, http_timeout=None, policy=None)` validates the URL through
  `Settings` and owns the transport and coordinator it creates; `http_timeout=None` keeps the
  `Settings` default.
- Examples: scenario 14 (`requisite_links`) now uses the camelCase `select`, `filter` and `order` keys
  of `crm.requisitelink.list` and reads its `requisiteLinks` collection. The live method silently
  ignores the uppercase keys the recipe used, so the recipe's selector never matched a real response
  (found by live probe L3).
- The README's list traversal animation is replayed from executed traces: a test runs all three
  lanes against one scripted portal and checks the drawn HTTP counts, batch sizes and the final
  keyset call (§3.13). Its caption now says that an unbounded keyset ends with an empty confirmation
  call and that a `BoundedIdentityRange` stops at its upper ID without one.
- The README opens with a quickstart on `crm.deal.list` and `user.get` that needs four concepts:
  the client, a request, `iter_list()` and the report. It runs in a doc-test on the real client (B26).
- **Breaking (3.0.0):** removed never-produced report vocabulary: the `canary_requests`,
  `canary_commands` and `canary_rows` fields of `KeysetExecutionReport`,
  `KeysetAssuranceSource.CANARY_VERIFIED_BOUNDS`, `ReplayDisposition.REPLAYED_DIRECT` and
  `DIRECT_REPLAY_FAILED`, `CompletionAssurance.ORACLE_VERIFIED`, `SnapshotState.VERIFIED` and
  `CHANGED`, and `NotExecutedReason.SCHEDULER_STOPPED` (B11). See the migration guide.
- **Breaking (3.0.0):** `PageValidated` no longer carries `identity_digest`, and the recorders no
  longer hash identities per page; the gate checks event order and `row_count` as before (B12).
- A JSON success body is parsed once, strictly, instead of once by the error codec and again by the
  envelope decoder. Structured errors and malformed bodies keep their previous classification, apart
  from the strict-only defects listed under B20; a 16 MiB call decodes about 15% faster (B8).
- A public operation stream's report now always carries its own cleanup result: a cleanup failure
  that the underlying kernel did not record is added as a `cleanup_failure` violation, by the same
  §3.1 rules as the kernel reports. Each of the five stream families has a barrier-driven test for
  every row of the §3.1 transition table.
- The rate coordinator and the execution ledger no longer take locks; every state change is
  synchronous between awaits, which a test pins (C8).
- Internal kernel copies of public values carry a `Kernel` prefix (`KernelReferenceItem`,
  `KernelReferenceFailure`, `KernelBatchDispatch`, `KernelDirectDispatch`), so every public class name
  has one definition; an architecture test keeps it that way (D30).
- The PyPI release workflow accepts only canonical stable tags `MAJOR.MINOR.PATCH` (for example
  `2.2.1`, with no `fix-` or `v` prefix) and rejects any other tag before the build backend runs. It
  then requires exactly one sdist and one wheel whose filenames and metadata carry that version, and
  passes `twine check --strict`, before upload; publication still depends on the build job. The
  checkout, setup-python, upload-artifact, and download-artifact actions move to their Node 24 `v7`
  majors.
- A positional request whose slots cannot accept a declared traversal control still raises
  `CapabilityError` with the original positional error as its cause; the message now appends a
  value-free reason (undeclared path, absent slot, missing parent, or near-match casing), and the
  cause is a `ValueError` subclass carrying the closed
  `b24api.contracts.positional.PositionalControlFault`.
- Documented the five-slot positional `task.elapseditem.getlist` route and the optional,
  offline-tested sequential positional keyset profile.
- `iter_list_counted()` accepts a first page with no rows, no `next`, and a missing, `null`, or `-1`
  `total` as an observed empty source. The report completes and is exhausted with `mechanics_only`
  assurance (`identity_exact` with an identity) and never claims a count; `total: 0` keeps its
  count-matched result, and an explicit `ConsistencyPolicy` confirmation of `QUALIFIED_TOTAL` keeps such a page strict.

## 2.3.0 — 2026-09-23

- **Breaking:** `Request` requires `route=`, a `RouteKind` (`BARE`, `JSON` or `API_V3`). There is no
  default route, so a V3 method can no longer reach the classic endpoint by accident. A transport
  serves only the routes its `TransportCapabilities.routes` declare.
- Added positional arguments (`PositionalArguments`, `PositionalLayout` and the slot markers) for
  methods whose parameters are positional.
- Added whole-page stops (`PageStopPolicy`, `CallerStop`, `ContinuePage`, `PageBoundary`), page index
  and stride offsets (`PageIndex`, `PageStride`), sparse raw bounds (`SparseRawBound`,
  `RawTotalSource`), `CursorDomain`, `BoundedIdentityRange`, `DuplicatePolicy` and the external
  `IdentityStore`.
- The completion gate and its events are public, and every report carries its cleanup outcome
  (`CleanupState`, `CleanupOutcome`) and `ReplayDisposition`.
- HTTPX log records of the client's own requests, including auth retries and redirects, are
  attributed to that request and scrubbed of the webhook secret; other loggers' records are left alone.

## 2.2.0 — 2026-09-12

- Added `Bitrix24.iter_cursors()` with lazy one/many-parent scheduling, per-binding
  `Binding.start_cursor`, strict seed progression, shared physical batching and existing
  fail-fast/tolerant reference semantics. When cursor-control creation is disabled, the complete
  control path including its leaf must already exist; a seed authorizes replacement, not creation.
- Added immutable `PageView` / `AdaptedPage` and the synchronous `PageAdapter` strategy to every
  list traversal. Contract violations now raise value-free `PageAdaptationError`; rejected fast
  keyset pages use the additive `PageRejectionCode.PAGE_ADAPTATION` trace category.
- Added the strict, standalone `verify_keyset_capability()` Python API and `b24api verify-keyset`
  CLI. Unsupported and inconclusive reports use distinct CLI exit codes 6 and 7. Capability
  evidence is stored as deeply immutable `FrozenJson`; `to_dict()` returns detached ordinary JSON.
- Normal range and partitioned `iter_list_keyset()` execution no longer sends runtime canaries.
  It reports the new `KeysetAssuranceSource.CALLER_ASSERTED_BOUNDS`; consumers using exhaustive enum
  matching must handle that additive member. A broken endpoint may now yield a partial prefix before
  active page validation raises a typed incomplete traversal.
- AUTO estimates no longer include canary waves, and the canary-prefix-only small-selection branch
  was removed. `KeysetSelectionReason` is unchanged and `SMALL_SELECTION` remains a cost result.
- `BatchDispatch.coalesce_wait` defaults to an absolute, capacity-aware 20 ms deadline for each
  underfilled physical wave. Set it to zero for low latency. Parallel workers preserve aggregate
  work but do not promise an exact split. The unreachable `None` queue sentinel was removed; live
  shutdown still cancels workers and pending futures.
- Selection, shape checks, identity validation and fingerprints now use the response's existing
  frozen JSON tree; only published rows are thawed.
- Legacy `KeysetAssuranceSource.CANARY_VERIFIED_BOUNDS`, `KeysetPhase.CANARY`, and canary report
  fields remain source/deserialization compatible. Normal runtime leaves their counters at zero.
