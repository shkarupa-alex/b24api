# Specification: Bitrix24 client 2.0 and evidence harness

Status: final approved design after three council review rounds and the normative round-3 resolution in section 27. No fourth review round is permitted.

Scope: `/Users/alex/Develop/b24api/client`.

The later migration of `/Users/alex/Develop/b24api/skills/bitrix24` is out of scope. Its source is read-only compatibility and use-case evidence.

## 1. Objective

Deliver a correctness-first Bitrix24 client 2.0 that:

- preserves committed APIs through compatibility wrappers;
- uses one plan-driven engine for calls, batching, pagination, and reference fan-out;
- distinguishes execution completion from the strength of evidence supporting completeness;
- makes retry, pagination, mutation, redaction, and performance assumptions explicit;
- streams with bounded memory and prompt cancellation;
- preserves useful concurrency where endpoint semantics permit independent work;
- includes deterministic and opt-in live evidence for admitting endpoint profiles and optimizations.

The correctness core is independently releasable. Optional optimizations ship only after their own correctness and benefit gates pass.

## 2. Requirements

| ID | Requirement |
|---|---|
| R1 | Preserve committed public methods, positional arguments, root exports, and documented valid behavior unless an authorized correction applies. |
| R2 | A terminal report must distinguish operational completion, evidence assurance, snapshot consistency, cancellation, and failure. |
| R3 | Runtime probes may invalidate a capability but may not authorize or strengthen one. |
| R4 | Requests and plans are immutable; injected controls never mutate caller-owned objects. |
| R5 | Every operation has bounded physical requests, logical pages, retries, elapsed time, and buffered rows. |
| R6 | Retry decisions use replay safety and failure phase. |
| R7 | Default exceptions, reports, logs, and artifacts are redacted and bounded. |
| R8 | All compatibility wrappers delegate to the shared execution and traversal engines. |
| R9 | Batch and reference execution preserve total command/reference correlation. |
| R10 | Correctness gates precede performance gates. |
| R11 | Streams support backpressure, cancellation, deterministic cleanup, and observable terminal reports. |
| R12 | Endpoint profiles are versioned, query-shape-specific, and explicitly selected. |
| R13 | Optimized traversal must prove the same result contract as its baseline before timing comparisons. |
| R14 | The deterministic harness covers dense, sparse, clustered, deleted-ID, mutation, and hostile protocol cases. |
| R15 | Live writes are separately planned, explicitly enabled, manifest-owned, and resumably cleaned up. |
| R16 | The user’s dirty working tree is preserved and not used as an implicit implementation baseline. |

## 3. Conservative assumptions

Until stronger reviewed evidence exists:

1. Endpoint behavior may vary by portal, build, tariff, scope, and query shape.
2. Totals, continuations, limits, offsets, filters, and ordering are untrusted.
3. A caller-selected plan authorizes execution under caller assertions; it does not produce profile-verified assurance.
4. A mutating selection is not a stable snapshot without a frozen manifest or independent pre/post oracle.
5. A cursor whose next value comes from the previous page is inherently sequential.
6. Committed HEAD is the compatibility baseline. Dirty-tree code is evidence and candidate implementation material only.
7. The dirty tree belongs to the user. Implementation occurs in isolation without overwriting, stashing, committing, or normalizing it.
8. Generated live datasets are required for performance admission at the approved scales. Deterministic correctness does not depend on them, but a speed claim cannot ship without the user-approved live seed or an explicit user waiver naming the missing cell.
9. If live facts remain unknown, the affected profile or optimization remains unadmitted.
10. Compatibility wrappers represent explicit legacy traversal requests. Their reports use caller-asserted assurance unless an applicable reviewed profile is supplied internally by an explicit wrapper contract.

## 4. Release definition

### 4.1 Required 2.0 core

The release requires:

- immutable request, plan, policy, report, and evidence values;
- a coherent redacted error hierarchy;
- separated protocol, transport, retry/rate, and traversal responsibilities;
- explicit replay safety and transport failure phase;
- universal operation budgets;
- one pagination driver;
- `SingleResponsePlan`;
- `OffsetSequentialPlan`;
- `CountedOffsetPlan`;
- `KeysetPlan`;
- `ItemCursorPlan`;
- committed fail-fast batch behavior;
- typed tolerant batch outcomes;
- bounded direct and batch reference execution;
- stream lifecycle and terminal reports;
- committed wrapper compatibility tests;
- deterministic model, fixture, socket, static, and secret-leak gates;
- the endpoint-profile schema, chooser, and explainer.

The core may ship with zero default optimized profiles if no profile obtains conclusive evidence. Explicit plans and compatibility wrappers remain usable with clearly reported assurance.

### 4.2 Conditional optimizations

Evaluate during the 2.0 effort:

- `PartitionedKeysetPlan`;
- automatic direct-versus-batch dispatch;
- proactive operating-time governance;
- one-round reference or partition prefetch.

A candidate that fails correctness or lacks conclusive required performance evidence is absent from public, opt-in, and default release paths. It may remain only as non-distributed research code until a later reviewed specification admits it.

### 4.3 Performance compatibility

Committed parallel behavior is preserved where an oracle-correct equivalent exists and a demonstrated corpus use case benefits from it.

No generic parallel replacement is required when the endpoint exposes no independent work through a trusted count/offset contract, partitionable key domain, independent references, or independent cursors. In such cases:

- correctness takes precedence;
- the wrapper uses the fastest proven exact plan;
- the regression is measured on relevant callsites;
- release notes identify the change;
- a default regression requires explicit release-candidate approval.

## 5. Correctness and assurance model

### 5.1 Terminal state

```python
class TerminalState(Enum):
    NOT_STARTED = "not_started"
    COMPLETED = "completed"
    INCOMPLETE = "incomplete"
    FAILED = "failed"
    CANCELLED = "cancelled"
```

`report.completed` is derived as `state is COMPLETED`.

`COMPLETED` means:

- the plan reached a declared terminal condition;
- every locally checkable invariant held;
- no budget was exhausted;
- no unresolved command, page, or reference remained.

It does not by itself mean an untrusted remote server told the truth.

### 5.2 Assurance

```python
class CompletionAssurance(Enum):
    ORACLE_VERIFIED = "oracle_verified"
    PROFILE_VERIFIED = "profile_verified"
    CALLER_ASSERTED = "caller_asserted"
```

- `ORACLE_VERIFIED`: an independent qualified oracle established the expected result for this run.
- `PROFILE_VERIFIED`: a reviewed applicable profile authorized the plan and runtime checks found no contradiction.
- `CALLER_ASSERTED`: the caller or compatibility wrapper asserted the endpoint contract; runtime checks found no contradiction.

There is no `BEST_EFFORT` completed grade. A mode that cannot establish its declared terminal condition ends `INCOMPLETE`.

A runtime probe may change `PROFILE_VERIFIED` execution into refusal or `INCOMPLETE`; it cannot elevate `CALLER_ASSERTED` to `PROFILE_VERIFIED`.

### 5.3 Snapshot consistency

```python
class SnapshotState(Enum):
    NOT_REQUESTED = "not_requested"
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    CHANGED = "changed"
```

Traversal completion and snapshot consistency are independent.

A stream may be operationally complete with `snapshot=UNVERIFIED` only when the requested policy is traversal-only. If the policy requires a stable snapshot, an unverified or changed snapshot produces `INCOMPLETE`.

### 5.4 Non-negotiable invariants

1. Warnings never convert incompleteness into completion.
2. Runtime probes falsify or downgrade only.
3. Budget exhaustion is typed and produces `completed=False`.
4. Caller-owned requests are never mutated.
5. Identity extraction, filter key, and order key are independent roles.
6. Retry uses replay safety and failure phase.
7. Default evidence is bounded and redacted.
8. Compatibility wrappers contain no second traversal engine.
9. Correctness gates run before performance gates.
10. Cancellation or deliberate early close is never completion.
11. Late validation may fail after earlier rows were consumed; consumed rows cannot be retracted.
12. Plans with the same result, assurance, snapshot, and failure contracts compete on end-to-end performance.
13. Profile applicability requires the complete predicate to match.

## 6. Architecture

```text
Bitrix24 facade and compatibility wrappers
    |
    v
Request + ExecutionPolicy + explicit Plan
    |
    v
ProtocolCodec and Redactor
    |
    v
ExecutionEngine
    +-- Transport
    +-- RetryRateCoordinator
    +-- BatchExecutor
    +-- DirectExecutor
    |
    v
PaginationDriver or ReferenceScheduler
    |
    v
ItemStream / ReferenceStream / OutcomeStream
    |
    v
Terminal report
```

Initial boundaries:

- `api.py`: facade and compatibility wrappers.
- `models.py`: immutable values, plans, policies, outcomes, reports, and evidence.
- `protocol.py`: query encoding, envelope parsing, batch parsing, and redaction.
- `execution.py`: transport, retry/rate coordination, batch execution, and direct execution.
- `pagination.py`: list-plan state machines and shared driver.
- `references.py`: reference scheduler and per-reference state.
- `profiles.py`: profile loading, validation, selection, and explanation.
- `error.py`: exception hierarchy and compatibility aliases.
- `settings.py`: environment-backed defaults and validation.
- `evidence/`: deterministic portal, fixtures, live harness, manifests, and artifact schemas.

A file is split only when its responsibility or size warrants it.

Exclusive ownership:

- transport owns HTTP lifecycle and failure phase;
- the codec owns wire encoding and envelope interpretation;
- redaction owns safe rendering and serialization;
- retry/rate coordination owns permits, cooldown, and retry budgets;
- plans are immutable descriptions and perform no I/O;
- drivers determine terminal state;
- schedulers order independent work without changing plan semantics;
- profiles authorize plans;
- observations only invalidate authorization.

The package remains a low-level API client. It adds no repositories, domain entities, Unit of Work, or skill-specific application services.

## 7. Public interfaces and schemas

The following signatures and semantics are normative.

### 7.1 JSON and request inputs

```python
JsonScalar = None | bool | int | float | str
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]

RequestInput = (
    Request
    | Mapping[str, JsonValue]
    | tuple[Request | Mapping[str, JsonValue], object]
)
```

A mapping request must contain `method` and may contain `parameters` and `replay_safety`. Unknown top-level fields are rejected.

### 7.2 Facade

```python
class Bitrix24:
    async def call(
        self,
        request: Request | Mapping[str, JsonValue],
        *,
        raw: bool = False,
        policy: ExecutionPolicy | None = None,
        retry: bool | None = None,
    ) -> JsonValue | Response: ...

    def batch(
        self,
        requests: Iterable[RequestInput] | AsyncIterable[RequestInput],
        *,
        batch_size: int | None = None,
        list_method: bool = False,
        with_payload: bool = False,
        policy: ExecutionPolicy | None = None,
    ) -> AsyncIterator[JsonValue | tuple[JsonValue, object]]: ...

    def batch_outcomes(
        self,
        requests: Iterable[RequestInput] | AsyncIterable[RequestInput],
        *,
        batch_size: int | None = None,
        policy: ExecutionPolicy | None = None,
        fallback_failed: Literal["none", "direct"] = "none",
    ) -> BatchOutcomeStream: ...

    def iter_list(
        self,
        request: Request | Mapping[str, JsonValue],
        *,
        plan: ListPlan,
        selector: ResultSelector = ResultSelector.root(),
        identity: IdentitySpec | None = None,
        policy: ExecutionPolicy | None = None,
    ) -> ItemStream: ...

    def fan_out(
        self,
        requests: Iterable[ReferenceRequest] | AsyncIterable[ReferenceRequest],
        *,
        dispatch: DispatchPlan,
        output_order: ReferenceOutputOrder = ReferenceOutputOrder.READY,
        tolerant: bool = False,
        policy: ExecutionPolicy | None = None,
    ) -> ReferenceStream: ...

    def iter_reference(
        self,
        request: Request | Mapping[str, JsonValue],
        bindings: Iterable[ReferenceBinding] | AsyncIterable[ReferenceBinding],
        *,
        plan: ListPlan,
        dispatch: DispatchPlan,
        selector: ResultSelector = ResultSelector.root(),
        identity: IdentitySpec | None = None,
        output_order: ReferenceOutputOrder = ReferenceOutputOrder.READY,
        tolerant: bool = False,
        policy: ExecutionPolicy | None = None,
    ) -> ReferenceStream: ...
```

`raw=True` returns the parsed `Response`; it does not expose raw HTTP bytes or bypass redaction.

New multi-page methods require an explicit plan. Compatibility wrappers supply their documented legacy plan.

### 7.3 Request and response

```python
class ReplaySafety(Enum):
    SAFE = "safe"
    UNSAFE = "unsafe"
    UNKNOWN = "unknown"


class Request:
    method: str
    replay_safety: ReplaySafety = ReplaySafety.UNKNOWN

    @property
    def parameters(self) -> Mapping[str, JsonValue]: ...

    def copy_parameters(self) -> dict[str, JsonValue]: ...

    def to_wire_parameters(self) -> dict[str, JsonValue]: ...


class Response:
    result: JsonValue
    time: ResponseTime
    total: int | None
    next: int | str | None
    evidence: ResponseEvidence

    @property
    def list_result(self) -> list[JsonValue]: ...

    def list_items(self, selector: ResultSelector) -> list[JsonValue]: ...
```

Construction validates JSON compatibility and makes a canonical deep copy. `parameters`, `copy_parameters`, and wire conversion never expose the stored mutable tree.

`repr(Request)` and normal serialization use `RequestSummary`. Exact parameters remain available through a copied programmatic accessor.

`Response.list_result` preserves committed behavior. New code uses `list_items`. No public `.items()` method is added.

### 7.4 Paths, controls, selectors, and identity

```python
PathPart = str | int


class ParameterPath:
    path: tuple[PathPart, ...]


class ResultSelector:
    path: tuple[PathPart, ...]

    @classmethod
    def root(cls) -> "ResultSelector": ...


class IdentitySpec:
    item_path: tuple[PathPart, ...]
    filter_key: str
    order_key: str
    coercion: IdentityCoercion
```

Paths are evaluated without case folding.

Control injection rejects before I/O when:

- an injected path already contains a conflicting caller value;
- two case-insensitive sibling keys could represent the same control;
- traversal through the path encounters a non-mapping value;
- a required filter/order container is missing and the plan does not allow creating it.

Matching an existing parameter name’s casing is permitted only when exactly one case-insensitive match exists.

`IdentityCoercion` supports exact strings, exact integers, and named built-in normalization modes. Arbitrary callable coercions are internal only because they are not serializable or reproducible in reports.

### 7.5 Policies and budget scopes

```python
class ExecutionPolicy:
    max_requests: int = 10_000
    max_pages: int = 10_000
    max_pages_per_reference: int = 10_000
    max_elapsed: float = 900.0
    max_attempts_per_request: int = 5
    max_retry_elapsed_per_request: float = 120.0
    max_buffered_rows: int = 1_000
    max_direct_concurrency: int = 10
    max_active_references: int = 100
    retry: RetryPolicy = RetryPolicy()
    consistency: ConsistencyPolicy = ConsistencyPolicy.traversal()
    debug_evidence: bool = False
```

Counter semantics:

- `max_requests`: physical HTTP attempts, including retries, across the stream;
- `max_pages`: logical page responses across the stream;
- `max_pages_per_reference`: logical pages for one reference;
- `max_elapsed`: monotonic wall time from first I/O until terminal cleanup;
- `max_attempts_per_request`: total attempts, including the initial attempt;
- `max_retry_elapsed_per_request`: monotonic time for one logical request;
- `max_buffered_rows`: decoded rows retained but not yet delivered;
- `max_direct_concurrency`: in-flight direct HTTP requests;
- `max_active_references`: reference states admitted simultaneously, not total input length.

Budget checks occur before scheduling work that would exceed the budget. A physical attempt already dispatched is counted even if cancellation follows.

Defaults are conservative operational ceilings, not promises of endpoint capacity. Settings and per-call policies may lower or raise them explicitly. The portal batch command cap remains a separate hard maximum.

### 7.6 Consistency policy

```python
class DuplicatePolicy(Enum):
    ERROR = "error"
    ALLOW_DECLARED_MULTISET = "allow_declared_multiset"
    REPORT = "report"


class TotalSemantics(Enum):
    IGNORE = "ignore"
    ADVISORY = "advisory"
    FILTERED_EXACT = "filtered_exact"
    GLOBAL = "global"


class SnapshotRequirement(Enum):
    TRAVERSAL_ONLY = "traversal_only"
    FROZEN_MANIFEST = "frozen_manifest"
    INDEPENDENT_PRE_POST_ORACLE = "independent_pre_post_oracle"


class ConsistencyPolicy:
    duplicate_policy: DuplicatePolicy
    total_semantics: TotalSemantics
    identity_requirement: IdentityRequirement
    order_semantics: OrderSemantics
    snapshot_requirement: SnapshotRequirement
    confirmation_policy: ConfirmationPolicy
```

`strict: bool` is not canonical.

### 7.7 Streams

`ItemStream`, `ReferenceStream`, and `BatchOutcomeStream` are async iterators and async context managers.

```python
stream.report
await stream.aclose()
```

Lifecycle:

- construction performs no I/O;
- first `__anext__` or `__aenter__` starts execution;
- natural exhaustion performs cleanup and freezes the terminal report;
- an iteration exception performs cleanup before propagating;
- `aclose()` cancels pending work, awaits cleanup, and is idempotent;
- abandoning a started stream without `aclose()` is caller misuse; a finalizer may warn but must not be relied upon for correctness.

Using `async for` without `async with` is supported when iteration naturally exhausts or the caller explicitly calls `aclose()` after early exit.

Report snapshots are immutable. The final report object is frozen.

### 7.8 Reports

Reports contain:

- terminal state and derived `completed`;
- completion assurance;
- snapshot state;
- plan and dispatch identifiers;
- profile ID, version, applicability, and evidence provenance;
- emitted and unique row counts;
- request, page, batch, command, attempt, retry, and cooldown counts;
- buffered-row high-water mark;
- typed violations;
- cancellation or terminal reason;
- bounded safe evidence.

Exceptions caused by iteration carry the same final report available from the stream.

### 7.9 Batch and reference outcomes

```python
BatchOutcome = BatchSuccess | BatchFailure
ReferenceOutcome = ReferenceItem | ReferenceFailure
```

`BatchFailure` contains:

- command index and stable key;
- payload;
- typed error;
- replay disposition;
- `BatchCommandEvidence`;
- exact immutable original request.

`ReferenceFailure` contains:

- reference and payload;
- original request;
- cursor/page state;
- partial row count;
- typed error;
- replay disposition.

Their `repr`, text, logs, and default serialization contain only redacted summaries.

For a terminal chunk-level transport or protocol failure, tolerant batch execution synthesizes one `BatchFailure` for every unresolved command in the chunk. It never drops correlation because a shared HTTP response was unavailable.

### 7.10 Reference ordering

```python
class ReferenceOutputOrder(Enum):
    READY = "ready"
    INPUT = "input"
```

- `READY`: preserves order within each reference but permits cross-reference interleaving according to readiness.
- `INPUT`: preserves input-reference order and per-reference order; later-reference output may be held behind earlier references, subject to the global row buffer.

If `INPUT` ordering would exceed the buffer budget, execution backpressures scheduling. It does not spill silently or switch order.

Compatibility wrappers use the ordering observed in committed behavior, verified by characterization tests.

## 8. Plan schemas

### 8.1 Common controls

Every plan declares:

- result selector or its required external selector;
- parameter paths for offset, limit, filter, order, select, and cursor controls;
- whether missing containers may be created;
- continuation type;
- terminal confirmation;
- identity and order requirements;
- duplicate and total semantics;
- locally checked invariants.

Plans are frozen values and contain no clients, loggers, callbacks, or mutable state.

### 8.2 Authorization

A plan is authorized by:

1. an applicable reviewed profile, producing `PROFILE_VERIFIED`; or
2. explicit caller or compatibility-wrapper selection, producing `CALLER_ASSERTED`.

An independent harness oracle may produce `ORACLE_VERIFIED` for that run.

Automatic unprofiled multi-page selection is forbidden.

### 8.3 `SingleResponsePlan`

```python
class SingleResponsePlan:
    reject_continuation: bool = True
    reject_positive_total_over_result: bool = True
```

Makes one request. A contradictory continuation, qualified total, or result shape produces `CapabilityError`.

### 8.4 `OffsetSequentialPlan`

```python
class OffsetSequentialPlan:
    offset_path: ParameterPath = ParameterPath(("start",))
    limit_path: ParameterPath | None = None
    requested_page_size: int | None = None
    continuation: OffsetContinuation
    terminal: OffsetTerminalRule
    allow_create_controls: bool = True
```

Continuation modes:

- `SERVER_NEXT`: every non-terminal page must provide a valid advancing `next`;
- `SERVER_NEXT_OR_OBSERVED_COUNT`: follow `next`; otherwise advance by returned count;
- `OBSERVED_COUNT`: ignore `next` and advance by returned count.

Terminal rules are explicit combinations of:

- empty page;
- qualified exact total reached;
- absent continuation under a profile-declared contract;
- short page under a profile-declared cap contract.

A short page is never implicitly terminal.

The driver detects repeated fingerprints, duplicate identities, non-advancing offsets, cycles, and budget exhaustion.

### 8.5 `CountedOffsetPlan`

```python
class CountedOffsetPlan:
    mode: CountedOffsetMode
    offset_path: ParameterPath
    limit_path: ParameterPath | None
    requested_page_size: int | None
    fixed_stride: int | None
```

Modes:

- `SEQUENTIAL_NEXT`;
- `PARALLEL_FIXED_STRIDE`.

Parallel mode requires an applicable profile proving filtered exact total, offset honoring, stable deterministic order, and fixed stride for the query shape.

A head response cannot independently authorize parallel mode. Requested-size mismatch causes capability failure unless the profile explicitly supplies the returned stride.

Parallel output is merged by logical offset. Every planned offset must resolve to a page or typed failure.

### 8.6 `KeysetPlan`

```python
class KeysetPlan:
    direction: Literal["asc", "desc"]
    filter_path: ParameterPath
    order_path: ParameterPath
    limit_path: ParameterPath | None
    requested_page_size: int | None
    start_suppression_path: ParameterPath | None
    terminal: KeysetTerminalRule
```

Every request reapplies the caller filter plus the exact lower or upper identity bound and order.

The driver validates:

- identity presence and declared coercion;
- strict monotonicity;
- bound honoring;
- requested or profiled page cap;
- advancing cursor;
- terminal confirmation.

Missing, composite, or non-orderable identities produce a typed remedy-bearing error.

### 8.7 `ItemCursorPlan`

```python
class ItemCursorPlan:
    cursor_request_path: ParameterPath
    cursor_item_path: tuple[PathPart, ...]
    direction: Literal["asc", "desc"]
    cursor_take: Literal["first", "last", "min", "max"]
    limit_path: ParameterPath | None
    requested_page_size: int | None
    terminal: CursorTerminalRule
```

Repeated, cyclic, wrong-direction, or non-advancing cursors produce an incomplete typed failure.

Within one reference, pages remain sequential. Independent references may advance concurrently.

### 8.8 `PartitionedKeysetPlan`

Internal until admitted.

It uses fixed disjoint intervals `(lo, hi]`. Each lane:

- reapplies both bounds;
- traverses sequentially;
- validates bound and order behavior before releasing its first page;
- maintains lane-level evidence and budgets.

Admission requires:

- applicable reviewed profile;
- model proof over dense, uniform sparse, clustered, skewed, deleted, and boundary distributions;
- independent live oracle equality where feasible;
- bounded request and memory behavior;
- benefit over sequential keyset in at least two representative cells.

The merge order is explicit. Every lane must finish or the overall stream is incomplete.

### 8.9 Rejected adaptive window scan

The reviewed adaptive `IdWindowScan` does not ship. It could omit its tail, emit before reconciliation, mis-detect ignored descending order, and lacked an unconditional sparse/clustered cost bound.

Future adaptive partitioning is a new design requiring its own proof and gates.

## 9. Compatibility contract

### 9.1 Preserved committed surface

Preserve:

- `Bitrix24.call`;
- `Bitrix24.batch`;
- `Bitrix24.list_sequential`;
- `Bitrix24.list_batched`;
- `Bitrix24.list_batched_no_count`;
- `Bitrix24.reference_batched_no_count`;
- `Bitrix24.reference_cursor_no_count`;
- `Bitrix24.aclose`;
- async context management;
- `Bitrix24.host`.

Preserve committed module paths pending the final census:

- `Request`;
- `ListRequest`;
- `ListRequestParameters`;
- `Response`;
- `ResponseTime`;
- `ApiResponseError`;
- `RetryApiResponseError`;
- `RetryHTTPStatusError`;
- `ApiSettings`;
- `Settings`.

Committed root exports remain valid.

### 9.2 Exact wrapper mapping

| Wrapper | Preserved committed arguments | Engine mapping | Default assurance |
|---|---|---|---|
| `call(request, *, raw=False)` | Existing calls remain valid; new `policy` and temporary `retry` are keyword-only. | Single execution through `ExecutionEngine`. | Caller asserted unless request/profile supplies stronger evidence. |
| `batch(requests, *, batch_size=None, list_method=False, with_payload=False)` | Exact committed arguments preserved; `policy` is keyword-only. | Fail-fast `BatchExecutor`, `halt=true`. | Caller asserted. |
| `list_sequential(request, *, list_size=None)` | Exact signature remains callable. | `OffsetSequentialPlan`, server `next` then observed returned count; terminal on qualified total or empty page. | Caller asserted unless explicitly profiled. |
| `list_batched(request, *, list_size=None, batch_size=None)` | Exact signature remains callable. | `CountedOffsetPlan(PARALLEL_FIXED_STRIDE)` only when profile-authorized; otherwise sequential counted traversal. | Profile verified or caller asserted sequential fallback. |
| `list_batched_no_count(request, *, id_key="ID", list_size=None, batch_size=None)` | Exact signature remains callable. | Profile-authorized partitioned plan when admitted; otherwise exact `KeysetPlan`. | Profile verified or caller asserted. |
| `reference_batched_no_count(request, updates, *, id_key="ID", list_size=None, batch_size=None, with_payload=False)` | Exact signature remains callable. | Per-reference exact keyset traversal with `BatchDispatch` or profiled direct dispatch. | Caller asserted or profile verified. |
| `reference_cursor_no_count(request, updates, *, cursor_param="LAST_ID", cursor_field="id", cursor_take="max", list_size=None, list_size_param="LIMIT", batch_size=None, result_key=None, with_payload=False)` | Exact signature remains callable. | `ItemCursorPlan` plus explicit `BatchDispatch`. | Caller asserted or profile verified. |

Compatibility wrappers may internally expose reports only through diagnostics or exceptions; their yielded legacy value shape remains unchanged.

### 9.3 Retry bridge

For `call`:

- `retry=False`: one attempt;
- `retry=True`: enable eligible retries subject to replay safety;
- `retry=None`: use policy defaults.

A request with `UNKNOWN` replay safety may retry only failures proven to occur before possible server acceptance. It does not retry ambiguous post-dispatch failures.

List and reference wrappers declare their operations as read traversal and therefore use `SAFE`, unless an explicitly supplied request states otherwise. A caller cannot use a wrapper keyword to override an explicitly `UNSAFE` request.

### 9.4 Authorized deviations

Only these deviations are pre-authorized:

- identity and casing correction;
- counted-offset stride correction;
- non-advancing continuation detection;
- contract-qualified short-page handling;
- duplicate detection and classification;
- ignored-offset detection;
- structured error normalization and redaction;
- replay-safe retry behavior;
- correction of incomplete range traversal;
- typed refusal where committed behavior could silently omit data.

Each receives a regression snapshot and release-note entry.

### 9.5 Working-tree disposition

The dirty tree is inventoried and reviewed hunk by hunk. It is not adopted wholesale.

Likely accepted concepts:

- structured error code, original code, and description;
- `call(raw=...)` overloads;
- tolerant batch outcomes;
- result-path extraction;
- custom and nested control paths;
- distinct identity roles;
- keyset traversal;
- ambiguity-aware casing reuse;
- duplicate and total semantics;
- bounded direct fan-out;
- request, response, and response-time values.

Redesign:

- raw request fields become redacted summaries in normal presentation;
- canonical retry becomes policy plus replay safety;
- `batch(errors="yield")` becomes `batch_outcomes`;
- nullable outcomes become tagged unions;
- per-command evidence is not an HTTP response;
- `strict` becomes `ConsistencyPolicy`;
- identity keywords become `IdentitySpec`;
- tolerant reference output becomes a typed union.

Reject from the canonical API:

- `BatchErrorMode`;
- a range/sequential strategy string;
- unconditional `start=-1`;
- automatic OR-filter rewriting;
- `strict=False` as silent loss;
- public promotion of every helper.

## 10. Endpoint profiles and selection

Profiles are packaged versioned JSON resources validated into immutable models. They contain no credentials, row bodies, or portal-specific secrets.

Each profile contains:

- schema version and profile ID;
- endpoint and method;
- verification date;
- portal/build applicability;
- required scopes;
- exact query-shape predicate;
- accepted control, filter, and order forms;
- control paths and casing;
- result selector;
- identity roles;
- page cap;
- total and continuation semantics;
- offset, order, filter, cursor, and batch capabilities;
- replay safety;
- allowed plans;
- evidence anchors and expiry/revalidation policy.

Selection is explicit:

```python
def choose_plan(
    profile: EndpointProfile,
    query: QueryShape,
    policy: ExecutionPolicy,
) -> PlanDecision: ...


def explain_plan(decision: PlanDecision) -> PlanExplanation: ...
```

The executor does not search profiles by method name.

Unknown build, expired evidence, missing scope, or unmatched query shape rejects the profile. The caller may then select an explicit plan with caller-asserted assurance.

Optimized plans retain local bound, order, stride, and continuation validation.

Capability probes are bounded, read-only, minimal-select, and within the caller filter by default. They downgrade or refuse only.

## 11. Batch execution

### 11.1 Fail-fast compatibility

`batch` preserves:

- arbitrarily long synchronous and asynchronous input streams;
- bounded chunking;
- input order;
- payload correlation;
- committed value shape;
- `halt=true`.

A chunk failure terminates the legacy stream. Previously yielded results remain consumed.

### 11.2 Tolerant outcomes

`batch_outcomes`:

- sends `halt=false`;
- produces exactly one outcome per submitted command;
- preserves command order and correlation;
- represents missing or malformed result-map entries as failures;
- produces failures for every unresolved command after a chunk transport failure.

`fallback_failed="direct"` reruns only replay-safe reads. Unsafe and unknown commands remain failures.

### 11.3 Batch size and retries

Batch size is validated in `1..portal_command_cap`.

On a qualified operating/time-limit response, safe-read execution may reduce later chunks and retry only commands not proven successful.

It never blindly replays:

- a mixed read/write chunk;
- successful commands;
- unsafe commands after dispatch;
- unresolved commands whose safety is unknown.

### 11.4 Evidence

`BatchCommandEvidence` records command correlation and Bitrix result/error-map evidence. It does not invent per-command HTTP status or timing.

## 12. Reference scheduling and dispatch

Dispatch plans:

```python
class DirectDispatch:
    max_concurrency: int


class BatchDispatch:
    batch_size: int
```

Scheduling is:

- round-robin fair;
- work-conserving;
- bounded by active-reference, request, command, buffer, and time budgets;
- cancellation-aware;
- per-reference ordered;
- tolerant only through typed reference outcomes.

Async reference input may be arbitrarily long. The scheduler admits at most `max_active_references` and pulls more input as states complete.

Batch-hostile scalar or positional methods use direct dispatch for correctness.

Automatic dispatch remains internal until correctness-equivalent live cells show:

- at least 15% median normalized wall-time improvement;
- paired interval excluding zero;
- no material small-case loss;
- acceptable throttling and failure isolation.

## 13. Retry and rate coordination

### 13.1 Failure phases

Transport records:

- not dispatched;
- connection established but request not sent;
- dispatch started;
- headers received;
- body partially received;
- response complete.

### 13.2 Replay rules

- `SAFE`: eligible transient transport, HTTP, rate, and API failures may retry within budgets.
- `UNSAFE`: retry only before possible acceptance or under explicit idempotency.
- `UNKNOWN`: ambiguous post-dispatch failures do not retry.
- unsafe or unknown ambiguity raises `AmbiguousExecutionError`.

### 13.3 Shared rate coordinator

Direct and batch execution share a coordinator.

Reactive cooldown honors explicit throttling and reset hints, applies jitter, and consumes budgets.

`time.operating` is recorded but does not by itself prove a limit.

A proactive governor requires a measured, versioned, expiring profile. Otherwise it remains disabled.

Fairness uses bounded round-robin admission. Reserved interactive capacity is enabled only when the portal contract permits it.

## 14. Protocol, errors, and redaction

### 14.1 Parse precedence

A readable structured Bitrix error is parsed before generic HTTP status handling, including on 4xx and 5xx.

The Bitrix description remains primary; HTTP status and gateway evidence remain attached.

### 14.2 Origins

Errors distinguish:

- `REST_MODULE`;
- `BATCH_COMMAND`;
- `HTTP_GATEWAY`;
- `TRANSPORT`;
- `PROTOCOL`;
- `CAPABILITY`;
- `PAGINATION`;
- `BUDGET`;
- `AMBIGUOUS_EXECUTION`.

They preserve exact original code, normalized code, description, HTTP status, safe request IDs and headers, bounded redacted body preview, and cause chaining.

### 14.3 Hierarchy

```text
B24ApiError
+-- TransportError
+-- HTTPGatewayError
+-- ProtocolError
+-- ApiResponseError
|   +-- RetryApiResponseError
+-- BatchCommandError
+-- CapabilityError
+-- PaginationError
+-- BudgetExceededError
+-- AmbiguousExecutionError
```

`RetryHTTPStatusError` remains import-compatible.

### 14.4 Redaction

Default redaction removes:

- webhook URLs and tokens;
- authorization headers;
- cookies;
- configured secret paths;
- configured PII fields;
- nested sensitive-key values.

The same scrubber serves logs, exception text, reports, pytest output, and JSON/CSV artifacts.

Raw HTTP bodies require `debug_evidence=True`, remain bounded and redacted, and are excluded from default serialization.

## 15. Logging

Use standard Python `logging` and install no handler.

Child loggers:

- `.transport`;
- `.retry`;
- `.batch`;
- `.pagination`;
- `.references`.

Levels:

- `DEBUG`: redacted request summaries, response metadata, cursor transitions;
- `INFO`: periodic bounded progress;
- `WARNING`: retry sleeps, throttling, isolated failures, terminal incompleteness.

Progress is interval/page based, never per row.

`DEBUG` alone never enables raw-body logging.

## 16. Evidence harness

### 16.1 Layers

1. deterministic model portal;
2. sanitized fixtures and local socket integration;
3. opt-in live contract and benchmark execution.

Live tests require an explicit marker and command-line flag and cannot run under ordinary `pytest`.

### 16.2 Harness CLI

The normative entry point is:

```text
uv run python tools/b24api_evidence.py plan
uv run python tools/b24api_evidence.py seed
uv run python tools/b24api_evidence.py verify
uv run python tools/b24api_evidence.py benchmark
uv run python tools/b24api_evidence.py resume
uv run python tools/b24api_evidence.py cleanup
```

Common options include:

```text
--manifest PATH
--artifact-dir PATH
--run-id ID
--live
--allow-writes
```

`seed` and `cleanup` require both `--live` and `--allow-writes`. `benchmark` is read-only.

Exit codes:

- `0`: requested operation completed;
- `2`: invalid configuration or manifest;
- `3`: environment unavailable or scope-gated;
- `4`: correctness failure;
- `5`: incomplete or inconclusive;
- `6`: cleanup left verified orphans;
- `7`: secret-leak scan failure.

### 16.3 Artifact schema

Every artifact contains:

- `schema_version`;
- `run_id`;
- non-reversible portal fingerprint;
- host without path or token;
- command and phase;
- candidate SHA;
- baseline SHA where applicable;
- profile and plan versions;
- start and finish timestamps;
- outcome classification;
- redacted metrics and evidence references.

Artifacts are written atomically. Partial progress uses append-only JSON Lines with sequence numbers. Resume rejects incompatible schema, run ID, portal fingerprint, or manifest lineage.

### 16.4 Secret contract

`BITRIX24_API_WEBHOOK_URL` is read only by live transport/session setup from the environment or OS keyring.

The harness validates without printing, stores only host plus non-reversible run fingerprint, scrubs all outputs, and fails if artifact/repository scanning finds a webhook or token pattern.

### 16.5 Live preflight

Preflight calls `scope` and `app.info`.

Missing scopes, tariff restrictions, endpoint absence, or version variance produce named `SKIP` or `INCONCLUSIVE`, not strategy failure.

Reads are default. Writes require:

- `--live --allow-writes`;
- a reviewed plan artifact;
- a unique namespace;
- supported disposable entities;
- read-back verification;
- resumable cleanup.

Irreversible and cascading methods are excluded.

### 16.6 Dataset lifecycle

`plan` is read-only and reports counts, estimated commands, batches, duration, scope, quota impact, and cleanup feasibility.

`seed`:

- creates only namespace-marked entities;
- records every identity and relationship in an append-only manifest;
- is resumable and idempotent;
- reconciles ambiguous writes by marker/read-back before replacement.

`verify` independently validates manifest identities and freezes expected sets, multisets, and order relations.

`benchmark` does not mutate the selection.

`cleanup` deletes exact manifest-owned identities in dependency order, verifies absence, and emits an orphan report. Prefix-only deletion is forbidden.

### 16.7 Dataset scale

The deterministic model implements exact:

- empty and 1–19 rows;
- 500 rows;
- 10,000 dense matches;
- 10,000 sparse matches below 10% selectivity;
- uniform sparse, clustered, skewed, deleted-ID, and mutation variants.

Live scale is conditional:

- small and boundary fixtures are expected;
- 500 rows are expected where safe;
- 10,000 dense or more than 100,000 base records are optional stress evidence;
- unavailable large live data does not block the core;
- claims requiring it remain unadmitted.

## 17. Oracle and benchmark contracts

### 17.1 Oracle qualification

Priority:

1. immutable project-owned manifest;
2. bounded point-read enumeration with local predicate;
3. independent cross-method normalized identities;
4. serial per-reference loop only when it does not share the candidate’s failure assumption.

A walk sharing candidate filter, order, or cursor assumptions is diagnostic only.

Every oracle records defects, visibility differences, missing-row behavior, normalization, and its qualification cross-check.

Outcomes:

- `PASS`;
- `FAIL`;
- `ORACLE_SUSPECT`;
- `INCONCLUSIVE`;
- `SKIP`.

### 17.2 Mutation

When snapshot verification is required, compute a full independent oracle multiset hash before and after the candidate.

A mismatch quarantines and retries at most three times. Persistent churn is `INCONCLUSIVE`.

Sampled totals, extrema, or timestamps may quarantine but cannot verify.

### 17.3 Metrics

Every case records:

- HTTP attempts;
- logical pages;
- batch requests and commands;
- time to first row;
- wall time;
- summed server operating time;
- retries and cooldown;
- buffered-row high-water mark and RSS delta;
- raw and unique identity counts;
- overlap, duplicates, shortfall, and overfetch;
- reference failures;
- terminal, assurance, and snapshot states;
- plan/profile provenance.

Default summaries store identities or hashes, not row bodies.

### 17.4 Performance gates

Correctness precedes timing.

Advisory timing uses a warmup and at least five runs. Blocking speed claims use nine interleaved A/B pairs unless a preregistered deterministic latency model supplies the gate.

Each live block includes before/after RTT and representative operating controls. Report raw values and normalized ratios.

Track 0 preregisters drift thresholds. Excessive control drift quarantines the block.

Admission requires:

- oracle-equivalent results;
- identical requested consistency contract;
- exact request/command accounting;
- bounded memory;
- no failure-semantics regression;
- candidate-specific median improvement;
- paired statistical support;
- no material small-case loss.

Wall clock alone never authorizes an optimization under unstable load.

## 18. Required evidence matrix

Every cell records:

- method and stable selection;
- expected identity, multiset, and order;
- cap and continuation behavior;
- applicable and deliberately inapplicable plans;
- oracle and its qualification;
- committed baseline;
- correctness and performance thresholds.

Cross-cuts:

- fewer than 20 rows;
- approximately 500 rows;
- 10,000 dense model rows and optional live rows;
- 10,000 sparse model rows below 10% selectivity and optional live rows.

### Tasks

- approximately 5 and 500 rows;
- sparse numeric span;
- exact 50/100 boundaries;
- mixed item/filter/order casing;
- wrong-casing refusal;
- control collisions;
- ignored or rewritten array/OR filters;
- sequential, counted, no-count, keyset, and admitted partition plans.

### Task history, elapsed rows, and comments

- `task.elapseditem.getlist` ignored offset;
- orphan elapsed rows;
- `tasks.task.history.list` at zero, one, exact-cap, and multi-page sizes;
- serial and bounded parallel direct execution;
- batch-hostile behavior;
- legacy comment shapes and positional parameters;
- empty/error version variance.

### CRM

- `crm.item.list` lowercase identity;
- `crm.activity.list` duplicate, ignored-offset, and drifting-total cases;
- `crm.timeline.comment.list` per-binding traversal;
- `crm.timeline.historyitem.list` global/filter-ignoring total;
- `crm.address.list` composite or absent identity behavior.

### IM

- `im.dialog.messages.search` with accessible and hostile references;
- `im.dialog.messages.get` deep descending history and wrong-direction refusal;
- `im.recent.list` with `total=-1`, overlap, pseudo-dialogs, and heterogeneous IDs;
- advisory dialog-user counters;
- non-advancing notification cursor.

### Other families

- `voximplant.statistic.get` requested-size mismatch;
- `socialnetwork.api.usertogroup.list` spurious continuation and advisory totals;
- string and camel-case IDs;
- nested controls in `landing.site.getlist`;
- true single-response methods;
- userfield fan-out with `result.field`.

Every reference family includes zero, one, exact-cap, and multi-page cardinalities.

## 19. Deterministic, integration, and static gates

Model state machines cover:

- ignored controls;
- lying, missing, negative, global, and drifting totals;
- arbitrary short pages;
- requested-size mismatch;
- overlap and duplicates;
- casing and nested controls;
- numeric, string, composite, and absent identity;
- dense, sparse, clustered, skewed, and deleted IDs;
- mutation;
- per-command failure;
- non-advancing cursors;
- cancellation;
- every budget.

Fixtures cover structured Bitrix errors on 4xx/5xx, nginx HTML/plain/JSON, empty bodies, malformed JSON, batch maps, legacy positional parameters, profile provenance, and redaction.

Socket integration covers failure phases, ambiguous writes, cooldown, timeout, cancellation, elapsed budgets, and mixed-workload fairness.

Static gates:

- Python 3.12;
- pytest;
- Ruff;
- strict mypy for production with narrow documented test overrides;
- signature and root-export snapshots;
- import-cycle checks;
- secret scans;
- wrapper wire/yield equivalence;
- authorized-deviation snapshots.

Ruff policy:

- remove inert broad `FA`;
- enforce `D100`–`D107` in production;
- permit narrow test `D100`/`D103` ignores;
- require meaningful reviewed docstrings;
- prohibit indiscriminate documentation autofix.

## 20. Implementation work packages

Each package has explicit dependencies and acceptance outputs.

### W0 — Inventory and isolation

Dependencies: none.

Outputs:

- committed/dirty/corpus symbol ledger;
- callsite frequency ledger;
- isolated implementation worktree and baseline SHA;
- existing-test baseline;
- preregistered evidence schemas.

Acceptance:

- user dirty tree unchanged;
- every committed public symbol classified;
- every dirty-tree public addition classified;
- baseline test results recorded.

### W1 — Redaction and errors

Dependencies: W0.

Outputs:

- redactor;
- safe evidence models;
- error hierarchy and aliases;
- body-before-status parser.

Acceptance:

- fixture matrix passes;
- no raw webhook in representations or serialization;
- committed error aliases import successfully.

### W2 — Immutable values and policies

Dependencies: W0.

Outputs:

- request, response, selector, identity, plan, policy, and report models;
- validation for paths, collisions, and budgets.

Acceptance:

- nested request mutation tests pass;
- invalid plan combinations fail before I/O;
- policy counter semantics are unit tested.

### W3 — Transport, retry, and rate foundation

Dependencies: W1, W2.

Outputs:

- transport failure phases;
- budget accounting;
- retry/rate coordinator.

Acceptance:

- socket failure-phase tests pass;
- unsafe ambiguous dispatch never retries;
- all attempt/time budgets terminate deterministically.

### W4 — Batch executor

Dependencies: W3.

Outputs:

- fail-fast batch;
- tolerant outcome stream;
- total correlation;
- replay-safe failed-command fallback.

Acceptance:

- unlimited sync/async streams remain bounded;
- every tolerant command receives one outcome;
- chunk-level failure produces outcomes for all unresolved commands.

### W5 — Sequential traversal engine

Dependencies: W2, W3.

Outputs:

- stream lifecycle;
- shared pagination driver;
- single, sequential offset, counted-next, keyset, and cursor plans.

Acceptance:

- model state machines pass;
- terminal/assurance/snapshot states are correct;
- cancellation performs cleanup;
- no plan mutates caller parameters.

### W6 — Reference scheduler

Dependencies: W4, W5.

Outputs:

- bounded reference admission;
- direct and batch dispatch;
- ready/input ordering;
- tolerant reference outcomes.

Acceptance:

- fairness and backpressure tests pass;
- per-reference order is preserved;
- asynchronous input remains bounded;
- cancellation closes pending work.

### W7 — Compatibility wrappers

Dependencies: W4, W5, W6.

Outputs:

- every committed wrapper mapped to the engine;
- signature snapshots;
- wire/yield compatibility tests;
- authorized-deviation snapshots.

Acceptance:

- no wrapper contains a pagination loop;
- committed valid cases are equivalent;
- every deviation maps to the authorized list.

### W8 — Profiles and explanation

Dependencies: W5, W6.

Outputs:

- packaged profile schema;
- validator;
- pure chooser;
- explainer;
- contradiction probes.

Acceptance:

- expired and inapplicable profiles refuse;
- probes never promote assurance;
- explanations contain no secrets or row bodies.

### W9 — Evidence harness

Dependencies: W1–W8.

Outputs:

- deterministic portal;
- socket fixtures;
- CLI;
- manifest and artifact schemas;
- oracle qualification;
- normalized benchmark runner.

Acceptance:

- deterministic scale matrix passes;
- resume rejects incompatible lineage;
- normal pytest cannot invoke live writes;
- secret scan passes.

### W10 — Optional optimizations

Dependencies: W8, W9.

Outputs: independently reviewed candidate implementations and evidence packets.

Acceptance: candidate-specific correctness and benefit gates. Failed candidates are removed from public/default paths.

### W11 — Release candidate

Dependencies: W0–W9 and accepted parts of W10.

Outputs:

- frozen exports and signatures;
- final compatibility ledger;
- documentation and release notes;
- same-SHA review packet.

Acceptance: all definition-of-done gates pass.

## 21. Review checkpoints

Evidence cannot authorize decisions until the evidence-producing code is reviewed.

Required checkpoints:

1. W0 inventory plus dataset manifest/oracle before large seeding;
2. W3/W4 instrumentation before retry, rate, or batch measurements are trusted;
3. each W5/W6/W10 strategy before its evidence authorizes a default;
4. W11 same-SHA release candidate.

Each packet contains:

- immutable SHA;
- diff and symbol inventory;
- assumptions;
- tests and counterexamples;
- sanitized artifacts;
- known gaps;
- exact dependent decisions.

Material changes to traversal, oracle, dataset, retry, scheduling, or measurement semantics invalidate dependent evidence. Documentation-only and unrelated changes do not; the packet records the non-impact determination.

## 22. Traceability

| Requirements | Primary work packages | Primary gates |
|---|---|---|
| R1, R8 | W0, W7, W11 | signature, export, wire/yield, deviation snapshots |
| R2, R5, R11 | W2, W3, W5, W6 | model lifecycle, cancellation, budget tests |
| R3, R12 | W8 | profile applicability and no-promotion tests |
| R4 | W2, W5 | deep immutability and collision tests |
| R6 | W3, W4 | failure-phase and ambiguous-write socket tests |
| R7 | W1, W9 | fixture redaction and repository/artifact scans |
| R9 | W4, W6 | total outcome and reference-correlation tests |
| R10, R13 | W9, W10 | oracle-first admission and normalized benchmarks |
| R14 | W9 | deterministic state-machine matrix |
| R15 | W9 | CLI gating, manifest lineage, cleanup verification |
| R16 | W0 | dirty-tree fingerprint unchanged |

## 23. Definition of done

The correctness core is done when:

- W0–W9 and W11 acceptance criteria pass;
- committed wrappers delegate to the shared engine;
- every new stream exposes terminal, assurance, and snapshot state;
- incomplete execution cannot appear as completed;
- unsafe ambiguous execution cannot retry;
- reports, errors, logs, and artifacts pass redaction tests;
- deterministic, fixture, socket, type, lint, import, and compatibility gates pass;
- every default profile has conclusive applicable evidence;
- every default optimization passes correctness and benefit gates;
- unproven profiles and optimizations are absent from defaults;
- accepted performance regressions are quantified and approved;
- the final same-SHA review packet is approved.

Large live stress data is not required for the core. Claims depending on unavailable live evidence remain internal, opt-in, or unclaimed.

## 24. Rejected and deferred alternatives

### Rejected

- reviewed adaptive `IdWindowScan`;
- runtime observation as capability proof;
- automatic OR-filter rewriting;
- method-suffix replay classification;
- global retry boolean as canonical policy;
- silent duplicate suppression;
- total equality as universal proof;
- unconditional total-mismatch failure;
- a second wrapper traversal engine;
- application/domain repository layers;
- `strict=False` silent-loss behavior;
- generic parallel guarantees for dependent cursors;
- mandatory 100,000-record live seeding;
- automatic unprofiled multi-page selection;
- arbitrary callable identity coercions in public plans;
- treating operational completion as equivalent to profile or oracle verification.

### Deferred

- migration of `skills/bitrix24`;
- adaptive density partitioning;
- default automatic dispatch until admitted;
- proactive rate governance without an expiring profile;
- public prefetch without measured benefit;
- profiles that cannot be qualified on the supplied portal.

## 25. Open validation items

These determine profile and optimization admission, not public interface shape:

- portal build, scope, tariff, and endpoint availability;
- stable selections around 5 and 500 rows;
- feasibility and quota cost of larger fixtures;
- actual caps, total/next semantics, and casing;
- which task history/comment methods exceed one page;
- direct-versus-batch timing and throttling;
- independent point-read or cross-method oracles;
- corpus callsites for which committed parallel performance is release-critical.

If unresolved, the affected profile or optimization remains unadmitted.

## 26. Decision Ledger

| ID | Decision | Status | Rationale | Source |
|---|---|---|---|---|
| D001 | Use one plan-driven execution engine for new APIs and compatibility wrappers. | adopted | Prevents behavioral drift and duplicated pagination defects. | current specification; council synthesis |
| D002 | Preserve committed public methods and positional arguments. | adopted | Committed behavior has the strongest compatibility presumption. | current specification |
| D003 | Treat uncommitted working-tree APIs as evidence, not automatic contract. | adopted | Avoids freezing experimental surface accidentally. | current specification |
| D004 | Preserve completeness-or-incomplete semantics and typed terminal reports. | adopted | This is the central correctness invariant. | current specification; reviewer strength |
| D005 | Let probes falsify but never prove endpoint capability. | adopted | Finite observations cannot establish traversal completeness. | current specification; reviewer strength |
| D006 | Require explicit plans for new multi-page traversal. | adopted | Removes unsafe automatic unprofiled pagination. | round-1 refinement |
| D007 | Permit caller-asserted plans while recording their weaker assurance. | adopted | Keeps the low-level client usable without presenting assertions as profile proof. | round-1 refinement; round-2 clarification |
| D008 | Separate item, filter, and order identity roles. | adopted | Repository evidence shows casing and role divergence. | current specification |
| D009 | Replace canonical `strict: bool` with typed consistency policy. | adopted | A boolean cannot express duplicate, total, order, identity, and snapshot semantics. | current specification |
| D010 | Make requests immutable by canonical copying and copied accessors. | adopted | Prevents caller mutation and nested mutable aliasing. | current specification; round-1 refinement |
| D011 | Base retry on replay safety and failure phase. | adopted | Prevents duplicate unsafe writes after ambiguous dispatch. | current specification |
| D012 | Preserve committed retry attempt counts only for replay-safe compatibility operations. | adopted | Balances compatibility with safe-by-default 2.0 behavior. | round-1 refinement |
| D013 | Parse structured Bitrix bodies before generic HTTP status errors. | adopted | Preserves the more informative API error and matches committed behavior. | current specification; reviewer-verified repository fact |
| D014 | Use one redaction path for logs, exceptions, reports, tests, and artifacts. | adopted | Prevents inconsistent secret handling. | current specification |
| D015 | Expose fail-fast `batch` and separate typed `batch_outcomes`. | adopted | Maintains compatibility without heterogeneous legacy return modes. | current specification |
| D016 | Use disjoint success/failure outcome types. | adopted | Eliminates invalid nullable result-plus-error states. | current specification |
| D017 | Retry only unexecuted or individually replay-safe batch commands. | adopted | Avoids replaying successful or unsafe commands. | current specification |
| D018 | Schedule independent references fairly while keeping each dependent cursor sequential. | adopted | Preserves concurrency without violating cursor dependencies. | current specification |
| D019 | Require streams to support bounded buffering, cancellation, and `aclose()`. | adopted | Enables prompt resource release and predictable memory. | current specification |
| D020 | Distinguish traversal completion from stable-snapshot certification. | adopted | Offset traversal can finish without proving an immutable snapshot. | round-1 refinement |
| D021 | Preserve committed parallel performance only where an exact parallel plan is possible. | adopted | Avoids an impossible generic batching mandate while retaining real optimizations. | round-1 refinement |
| D022 | Make optional optimizations independently admissible rather than blocking the correctness core. | adopted | Reduces release risk and makes failure outcomes implementable. | round-1 refinement |
| D023 | Keep fixed partitioning internal until proof and benefit gates pass. | adopted | Correctness and bounded cost must precede exposure. | current specification |
| D024 | Reject the reviewed adaptive `IdWindowScan`. | rejected | It lacks a valid completeness and sparse-cost proof. | current specification |
| D025 | Reject automatic OR-filter rewriting. | rejected | It can change query semantics and multiset behavior. | current specification |
| D026 | Reject runtime observations as authorization. | rejected | Observations may miss adversarial portal behavior. | current specification |
| D027 | Reject silent duplicate suppression and silent-loss modes. | rejected | They can disguise incomplete results. | current specification |
| D028 | Reject a second engine for legacy wrappers. | rejected | It would duplicate correctness logic. | current specification |
| D029 | Reject a generic mandatory parallel replacement for every no-count traversal. | rejected | Some cursor contracts have an unavoidable sequential dependency. | round-1 refinement |
| D030 | Make large live dataset generation optional and explicitly authorized. | adopted | Large datasets may be unsafe, costly, or infeasible. | round-1 refinement |
| D031 | Require exact manifest-owned cleanup rather than prefix deletion. | adopted | Prevents destructive cleanup outside benchmark ownership. | current specification |
| D032 | Normalize live performance against contemporaneous controls. | adopted | Portal and network load make cross-time comparisons unreliable. | current specification; reviewer strength |
| D033 | Retain deterministic model coverage at full dense and sparse scales. | adopted | Supplies exhaustive correctness coverage without portal risk. | round-1 refinement |
| D034 | Remove inert Ruff `FA` and narrow documentation ignores. | adopted | Repository verification confirms the current broad ignores are unnecessary or excessive. | current specification; reviewer-verified repository fact |
| D035 | Keep the package a low-level client without domain services. | adopted | Application abstractions belong outside this scope. | current specification |
| D036 | Defer skill migration. | deferred | It is a separate compatibility consumer requiring its own plan. | current specification |
| D037 | Defer adaptive density partitioning pending a new proof. | deferred | Heuristic tuning cannot repair the rejected proof. | current specification |
| D038 | Keep automatic dispatch internal until it clears correctness-equal gates. | deferred | Speed cannot compensate for different contracts. | current specification |
| D039 | Keep proactive rate governance profile-gated. | deferred | Operating time alone does not establish a limit. | current specification |
| D040 | Consolidate components into a small initial module set. | adopted | Keeps the refactor proportional while preserving exclusive responsibilities. | round-1 refinement |
| D041 | Pause for user cross-review at every evidence-bearing implementation SHA. | adopted | Prevents one implementing model's omissions from controlling later decisions. | user correction; section 21 and 27.10 |
| D042 | Preserve both committed reference wrappers. | adopted | Both exist in committed HEAD. | repository inspection; round-1 refinement |
| D043 | Separate terminal completion, assurance, and snapshot state. | adopted | Eliminates the ambiguity between a finished state machine and independently verified completeness. | round-2 refinement |
| D044 | Define exact budget counter scopes and pre-scheduling enforcement. | adopted | Makes exhaustion behavior testable and consistent across executors. | round-2 refinement |
| D045 | Bound active reference states rather than total reference input. | adopted | Supports unlimited async input with controlled memory. | round-2 refinement |
| D046 | Define ready and input-ordered reference modes explicitly. | adopted | Makes concurrency, determinism, and buffering trade-offs observable. | round-2 refinement |
| D047 | Synthesize per-command failures after chunk-level tolerant-batch failure. | adopted | Preserves the one-outcome-per-command contract. | round-2 refinement |
| D048 | Package profiles as validated versioned data and allow a zero-profile core release. | adopted | Prevents unverified defaults from blocking the architecture or being silently trusted. | round-2 refinement |
| D049 | Add a normative evidence CLI, artifact schema, resume lineage, and exit codes. | adopted | Makes the harness executable without further interface design. | round-2 refinement |
| D050 | Decompose implementation into dependency-ordered work packages with acceptance outputs. | adopted | Improves weak-model executability and review isolation. | round-2 refinement |
| D051 | Reject arbitrary public callable identity coercions. | rejected | They are not reproducible, serializable, or safely explainable. | round-2 refinement |
| D052 | Preserve legacy wrapper signatures through an explicit wrapper-to-plan table. | adopted | Removes ambiguity about how committed methods survive the new architecture. | round-2 refinement |

## 27. Normative resolution after round 3

This section is authoritative where earlier wording is incomplete or conflicts with it. It resolves the final review findings without a fourth review round.

### 27.1 Compatibility wrappers cannot finish silently incomplete

Every committed list/reference wrapper MUST raise `IncompleteTraversalError(report=final_report)` if its terminal state is not `COMPLETED`. `FAILED`, `INCOMPLETE`, `CANCELLED`, and budget exhaustion never become normal generator exhaustion. Rows already yielded remain consumed and cannot be retracted; the exception is raised at the first detected violation or terminal validation point.

This is an authorized 2.0 correction to committed behavior and receives regression snapshots for C1b, ignored offsets, repeated cursors, duplicate pages, short-page contradictions, and budget exhaustion.

Existing positional parameters remain valid. The list/reference wrappers add only these keyword-only bridges:

```python
plan: ListPlan | None = None
profile: EndpointProfile | None = None
identity: IdentitySpec | None = None
policy: ExecutionPolicy | None = None
```

Resolution order is `plan` > explicit `profile` > deterministic wrapper default. Supplying mutually inconsistent values raises before I/O. There is no hidden global profile lookup.

`list_batched_no_count(id_key=...)` maps the committed key to all three identity roles only when `identity` is absent. Every planned range/keyset response validates the actual item, filter, and order bounds. Thus the known `tasks.task.list` `id`/`ID` mismatch raises instead of under-fetching. Callers needing split roles pass `IdentitySpec(item_path=("id",), filter_key="ID", order_key="id", ...)` or a reviewed profile.

`list_batched` preserves parallel counted execution when an exact in-band verification contract is available: every planned offset resolves, returned offsets/continuations are consistent, the exact total is reached, and an explicit/profile/inferred unambiguous identity proves no duplicates. Without identity it preserves the committed parallel attempt only at `CALLER_ASSERTED` assurance and MUST raise on every detectable contradiction; callers requiring stronger assurance supply identity/profile or select sequential counted traversal. Profile authorization is required for `PROFILE_VERIFIED`, not for a caller-requested attempt-then-verify run.

### 27.2 Definitive public disposition and exports

Root `b24api.__all__` for 2.0 is:

```python
[
    "ApiResponseError",
    "BatchFailure",
    "BatchSuccess",
    "Bitrix24",
    "ExecutionPolicy",
    "IdentitySpec",
    "ReferenceFailure",
    "ReferenceItem",
    "Request",
    "Response",
    "ResultSelector",
]
```

Plans are public from `b24api.plans`; policies/reports from `b24api.models`; the complete error hierarchy from `b24api.error`. Existing committed imports remain aliases for one major release. `b24api.type.ApiTypes` and `b24api.query.build_query` remain importable and are included in signature/import snapshots. `Response.next` remains `int | None` until protocol evidence and an authorized deviation justify widening.

`ApiResponseError.code` preserves committed semantics exactly: it is the lowercase string form used by existing consumers. `original_code: str | int` retains the unmodified server value, and `normalized_code: str` provides the documented comparison form without overwriting the original. Numeric and mixed-case fixtures snapshot all three attributes; changing `.code` itself is not authorized.

| Dirty-tree symbol/concept | Final disposition |
|---|---|
| `list_keyset` | Accept as a thin public wrapper over `KeysetPlan`; keep out of root `__all__`. |
| `reference_batch` | Redesign as `fan_out`; no compatibility promise solely from the dirty tree. |
| `CursorStrategy` | Reject; callers choose distinct plans. |
| `BatchErrorMode` and `errors=` polymorphism | Reject canonically; use separate `batch_outcomes`. |
| nullable `BatchOutcome` dataclass | Replace with `BatchSuccess | BatchFailure`; keep the name only as a type alias if useful. |
| `Response.items` | Replace with unambiguous `list_items`; no dict-like `.items()` public method. |
| `Request.dump_parameters` | Internal wire conversion; no public contract. |
| `ErrorResponse.to_error` | Internal codec operation. |
| `RequestLike` / `entity_types.py` | Internal protocol used to break cycles. |
| `result_key` | Accept as `ResultSelector`. |
| `offset_param`, `limit_param`, `params_path`, `select_param=None` | Accept through plan paths. |
| split item/filter/order keys | Accept as `IdentitySpec`. |
| `strict` | Reject as canonical; map a temporary compatibility shim into `ConsistencyPolicy` only if the final census proves a consumer. |
| automatic OR splitting | Reject. |
| `retry: bool` dirty-tree keyword | Do not freeze; use `ExecutionPolicy`. |
| structured `.code`, `.description`, request context | Accept with the exact error contract below. |

The exhaustive generated W0 ledger remains a validation artifact, but it may not reverse these normative dispositions without a user-approved specification change.

### 27.3 Complete fixed partitioned keyset algorithm

`PartitionedKeysetPlan` is an evidence candidate with explicit `lane_count` (`2..min(batch_size, portal_cap)`) and numeric identity. It is not exposed unless admitted, but its algorithm is fixed before implementation:

1. In one boundary batch request the lowest and highest two matching identities using original filter, exact ASC/DESC order, count suppression, and `limit=2`. Validate strict direction, identity roles, filter preservation, and reversed extrema when more than one result exists. A reviewed applicable profile is required because finite boundary probes cannot prove order support.
2. Let `min_id` and `max_id` be verified occupied extrema. For `L-1` evenly spaced numeric guesses `g_i` in `[min_id,max_id)`, batch independent anchor probes with original filter plus `>g_i` and `<=max_id`, ASC, count suppression, `limit=1`.
3. Every non-empty anchor must be an occupied matching identity and satisfy its bounds. Sort and deduplicate anchors with `min_id` and `max_id`.
4. Build adjacent exhaustive lanes `(min_id-1, a_1]`, `(a_1,a_2]`, ..., `(a_k,max_id]`. Every upper fence is an observed matching identity. The union covers the whole numeric domain and lanes do not overlap.
5. Traverse each lane by sequential keyset. Reapply both lane bounds and original filter on every page; validate strict monotonic order and identities before emission. A lane completes only when its observed upper-fence identity is reached. Empty/non-advancing response before that fence is `INCOMPLETE`; no short-page assumption is needed.
6. Schedule one ready page per lane per batch, work-conserving within command and buffer limits. `READY` order may emit lanes as ready. Global ASC order buffers later lanes and always reserves scheduling/buffer capacity for the lowest unfinished lane.
7. Command bound is `2 + (L-1) + sum(ceil(n_i / effective_page_cap))`; no term depends on numeric ID span. With `L <= batch_size`, boundary and anchors each need at most one batch round trip and lane rounds are bounded by the largest lane page count. Skew may reduce speed but cannot affect coverage or make cost span-proportional.
8. Re-probe the highest occupied identity after traversal. A changed fence marks `SnapshotState.MUTATED` and applies the requested snapshot policy; it cannot silently strengthen assurance.

Admission requires exact model/oracle equality, no budget breach, and normalized median wall-time improvement of at least 15% over sequential keyset on both a dense-large and sparse/clustered-large cell, with a paired 95% interval excluding parity. Small `<20` p95 wall time may regress by at most 5%, server operating time by at most 10%, and request/command bounds must match the proof. Failure removes the plan from public and opt-in release paths.

W0 also performs one read-only Bitrix batch `$result[...]` command-chaining probe. If the portal can feed one command's cursor into the next, a separate fixed-depth chained-keyset candidate is specified and reviewed before use. Until that evidence exists, dependent pages remain sequential across batch round trips.

### 27.4 Normative benchmark cells and baselines

Every artifact records three immutable SHAs: `original_head_sha`, `fixed_1x_sha`, and `candidate_sha`, plus the pinned `skills` corpus SHA. `TBD-LIVE` means Track A0 must populate the field before the cell can decide admission; it is not permission to choose a threshold after seeing a candidate.

| ID | Method / generated fixture | Known baseline or generated cardinality | Compared plans | Oracle | Blocking gate |
|---|---|---:|---|---|---|
| T-S | `tasks.task.list`, owned marker, exact 1..19 | exact manifest | sequential, counted, no-count, keyset, partition candidate | manifest point-read qualification | exact identities; first-row latency and probes reported; candidate p95 <= 1.05x fixed baseline |
| T-M | same, exact 500 | exact manifest | all applicable plans and original/fixed baselines | manifest | exact identities; no unexplained duplicates; normalized requests/round trips/operating no worse than fixed correct baseline |
| T-D | generated 10,000 dense numeric IDs | 10,000 | keyset, fixed partition, corrected range, counted where valid | manifest | exact; partition >=15% normalized median win with paired 95% interval |
| T-SP | 10,000 matches interleaved in >100,000 owned base, <10% selectivity; uniform and clustered | 10,000 | same plus chooser | manifest | exact; warning/choice recorded; no span-proportional command growth; same 15% gate |
| T-B | exact 49/50/51/99/100/101 | exact manifest | sequential/count/no-count | manifest | exact boundaries, no extra overlap, request counts equal preregistered formulas |
| T-ID | `tasks.task.list` split roles | ISSUES: wrong `>id` ignored; `>ID` works; observed 162 vs 821/902 | legacy mapping, explicit identity, counted | qualified manual/manifest set | wrong mapping raises within first contradictory page; explicit identity exact |
| T-OR | known array/OR task filters | ISSUES: server may ignore/rewrite | no automatic rewrite | manifest/local predicate | return server contract plus typed `FILTER_MAY_BE_IGNORED`; never synthesize a different query |
| E-OFF | `task.elapseditem.getlist` | ISSUES: 86 rows, offset ignored | keyset vs counted/offset | manifest or qualified one-row walk | keyset exact; offset plan raises within 3 repeated/contradictory pages |
| H-REF | generated task histories: 0/1/cap/cap+1/multipage | exact graph | serial direct, parallel direct, batch probe | manifest per reference | exact per reference; batch-hostile failures correlated; direct performance normalized |
| C-REF | generated legacy task comments: 0/1/cap/cap+1 | exact graph | tolerant reference plans | manifest | empty/version error classified; no other reference aborts |
| CRM-I | generated `crm.item.list` dense/sparse | exact fixture | counted/keyset/partition | manifest and cross-method where valid | exact lowercase identity and filter/order casing |
| CRM-A | `crm.activity.list` duplicate/drifting-total fixture | ISSUES: observed 126 raw/92 unique and 278 vs 266 drift, recharacterize before gating | counted/keyset | manifest | duplicate and total semantics typed; no silent dedup |
| CRM-T | timeline comments over parents with 0/1/cap/multipage | exact graph | reference offset/keyset | manifest per binding | exact per reference; repeated IDs across bindings preserved by composite identity |
| CRM-NO-ID | generated address/composite rows | exact multiset | counted fallback; keyset refusal | manifest multiset | identity-required plans refuse before I/O; fallback exact multiset |
| IM-X | generated cross-chat messages plus designated failing reference | reproduce ISSUES shape 189 chats/773 messages/1 batch-only failure where feasible | cursor batch, serial/direct fallback | manifest/serial loop | exact successes + exact failures; rerun only failed requests |
| IM-DEEP | one chat with >3,000 messages | exact manifest | cursor directions and page sizes | manifest | correct direction exact; wrong direction raises by page 2; bounded memory |
| IM-RECENT | generated/qualified recent list | ISSUES: `total=-1`, seam overlap, pseudo IDs | sequential | qualified manifest/cross-read | declared seam only; negative total never used arithmetically |
| V-SIZE | `voximplant.statistic.get` controlled window | ISSUES: 18 rows became 90 with requested 2 | original/fixed counted | independent exact set | fixed exact 18; original failure retained as regression evidence |
| S-SHORT | `socialnetwork.api.usertogroup.list` controlled collab | ISSUES: 1 row, `next=1`, large total | single/sequential | direct call plus owned membership | cannot report complete without declared advisory contract; no short-page ValueError |
| L-NEST | nested legacy controls (`landing.site.getlist`) | generated exact scopes | sequential nested paths | manifest | exact and wire-path snapshot |
| SR | true single-response methods | generated/qualified exact result | single response | direct response | exactly one physical request; contradictory continuation raises |

Large fixture creation is itself gated by the reviewed generator SHA, dry-run estimate, explicit write authorization, manifest verification, and resumable cleanup. Algorithmic 100,000-base stress may use the cheapest safe representative entity; endpoint-specific cells remain mandatory at smaller exact scales.

### 27.5 Retry and rate executable rules

`Request.replay_safety` is `ReplaySafety | None`; `None` means unset. A wrapper may set `SAFE` only when it is `None`; explicit `UNKNOWN` is preserved.

| Failure phase | SAFE | UNKNOWN / UNSAFE |
|---|---|---|
| proven before any bytes could be sent (DNS/connect/TLS/permit acquisition) | retry within budgets | retry within budgets |
| sending or after dispatch before response | retry transient failure | raise `AmbiguousOutcomeError`; retry only with independently verified idempotency contract |
| HTTP response with retryable status | retry only configured status | retry only when the status/profile proves non-acceptance; otherwise terminal/ambiguous |
| structured API throttle/operating error | retry configured code | retry only with profile proof of non-execution |
| batch command error | retry failed command only if error is fallback-eligible | never automatic |
| malformed/uncorrelated batch envelope | retry all only if every unresolved command is SAFE | synthesize ambiguous failures for unresolved commands |

Default transient HTTP statuses preserve configured 423, 425, 429, 500, 502, 503, and 507; default structured retry codes preserve `query_limit_exceeded` and `operation_time_limit`. Existing `Settings` values for retry statuses/errors/attempts/delay/backoff and list/batch size map explicitly into default `ExecutionPolicy`; per-call policy wins.

Coordinator states are `OPEN`, `COOLDOWN(until, reason)`, and `CLOSED`. A throttle moves to `COOLDOWN`; concurrent hints merge by the latest safe reset. Permit queues use weighted round-robin between interactive direct, traversal direct, batch, and retry classes. Every non-empty class receives service within one complete scheduling cycle; retries cannot consume all permits. Proactive rate limiting is disabled unless a reviewed profile defines threshold and expiry. Unknown limits are never inferred from `time.operating` alone.

`fallback_failed="direct"` requires both `SAFE` replay classification and a retry/fallback-eligible error (including a profile-marked batch-hostile error). It never retries arbitrary authorization/not-found/validation failures merely because they are reads.

### 27.6 Prefetch and automatic dispatch candidates

One-round prefetch may schedule only a continuation whose cursor was already validated from the preceding page. At most one page per active reference/lane may be ready but undelivered. All prefetched rows count toward `max_buffered_rows`; cancellation cancels queued work and awaits active work according to transport semantics. No page is predicted from an unknown cursor.

Automatic dispatch compares `T_direct(C)` and `T_batch(B)` from measured HTTP RTT, batch RTT, server operating time, serialization, command count, throttle/cooldown, and failure-isolation cost. It is admitted only after exact result equality in at least two small and two large cells, normalized median wall-time improvement >=15% with paired 95% interval excluding parity, small-case p95 loss <=5%, server operating increase <=10%, and zero increase in unresolved failures. Prefetch uses the same gates except the primary metric may be total wall time; time-to-first-row must not regress more than 5%.

Any candidate failing these gates is absent from public, opt-in, and default release paths. Evaluation is mandatory during 2.0; exposure is conditional.

### 27.7 Missing schema definitions

The following definitions are normative:

- `ListPlan = SingleResponsePlan | OffsetSequentialPlan | CountedOffsetPlan | KeysetPlan | ItemCursorPlan | PartitionedKeysetPlan`.
- `DispatchPlan = BatchDispatch | DirectDispatch`; each is frozen and contains only limits/order/fallback policy.
- `QueryShape` is a frozen value of method, parameter paths, filter operator/key structure, order, selector, and scope/build applicability; secrets and literal PII values are hashed/omitted.
- `PlanDecision` contains selected plan, assurance, profile, rejected alternatives, and typed reasons; `PlanExplanation` is its redacted serializable form.
- `ReferenceRequest` contains immutable request plus reference key; `ReferenceBinding` contains redacted reference summary and payload correlation key.
- `ResponseEvidence`, `BatchCommandEvidence`, and `RequestSummary` are bounded redacted values and exclude payload/reference values by default.
- `IdentityCoercion = EXACT_STRING | EXACT_INTEGER | DECIMAL_STRING_INTEGER`.
- `IdentityRequirement = REQUIRED | OPTIONAL | COMPOSITE`.
- `OrderSemantics = UNORDERED | ASCENDING | DESCENDING | INPUT`.
- `ConfirmationPolicy = NONE | EMPTY_AFTER_BOUNDARY | BOUNDARY_ID_SEEN | QUALIFIED_TOTAL | INDEPENDENT_ORACLE`.
- `OffsetContinuation = SERVER_NEXT | SERVER_NEXT_OR_OBSERVED_COUNT | OBSERVED_COUNT`.
- `OffsetTerminalRule = EMPTY_PAGE | QUALIFIED_TOTAL | PROFILE_ABSENT_NEXT | PROFILE_SHORT_PAGE` and explicit combinations thereof.
- `KeysetTerminalRule = BOUNDARY_ID_SEEN | EMPTY_CONFIRMATION | PROFILE_SHORT_PAGE`; default without a cap/boundary is `EMPTY_CONFIRMATION`.
- `CursorTerminalRule = EMPTY_CONFIRMATION | PROFILE_SHORT_PAGE | PROFILE_CURSOR_EXHAUSTED`; default is `EMPTY_CONFIRMATION`.
- `CountedOffsetMode = SEQUENTIAL_NEXT | PARALLEL_FIXED_STRIDE`.
- `RetryPolicy` defines transient sets, attempt/time budgets, jitter, and idempotency evidence.

`Response.time` is optional. Exact payload/reference values are accessible programmatically only from caller-owned inputs/outcomes and are excluded from repr, default serialization, logs, and artifacts.

### 27.8 Bounded identity memory and reference liveness

`ExecutionPolicy` adds:

```python
max_tracked_identities: int = 100_000
identity_tracker: Literal["monotonic", "memory", "sqlite"] = "memory"
```

Strict keyset/partition lanes use `monotonic` and retain only last/boundary identities. Offset/count plans requiring exact duplicate detection use an exact memory set up to the limit or an explicitly selected temporary SQLite tracker. Crossing the configured limit raises `BudgetError` before accepting another identity; Bloom filters never certify correctness. Harness cells declare their override and record tracker high-water/storage metrics.

For `ReferenceOutputOrder.INPUT`, scheduling always permits the earliest unfinished reference to progress. Effective active references are capped by `max(1, floor(max_buffered_rows / expected_page_cap) + 1)` unless a larger safe buffer is supplied. Later references backpressure first; the head-of-line reference never waits for buffer held exclusively by later references. W6 includes deterministic deadlock-freedom tests with slow/failed first references and full later buffers.

### 27.9 Evidence tooling packaging and recovery

Destructive/live evidence tooling lives under repository-top-level `tools/b24api_evidence/`, not under `b24api`. `pyproject.toml` explicitly includes only `b24api*` packages and excludes `tools*`/evidence code. A wheel-content test proves no `seed`, `cleanup`, credential loader, or live harness module ships in the library distribution.

The normative development command is `uv run python tools/b24api_evidence.py ...`. It requires `--live`; writes additionally require `--allow-writes` and the reviewed manifest plan. Ordinary pytest uses only the committed dummy webhook literal, which is explicitly allowlisted by the leak scanner; live credentials are never sourced from pytest-env.

If a local manifest is lost, `recover-manifest --run-id <uuid>` performs a read-only exact-marker scan, emits a preview and candidate manifest, and requires a second explicit confirmation before cleanup. It never deletes from a prefix-only search.

### 27.10 Review, baseline, and lint closure

The human cross-review checkpoints in section 21 are blocking for every evidence-bearing SHA, not merely four broad phases. A code or measurement change invalidates evidence derived from the prior SHA.

Performance blocks use contemporaneous RTT and representative operating controls before and after, interleaved original/fixed/candidate executions, raw values and normalized ratios, and Track-A0 preregistered drift quarantine thresholds.

Ruff removes broad `FA`; production enables `D100`-`D107`; tests narrowly ignore `D100`/`D103`. Python 3.12 remains the package minimum and Python 3.14 is included in the CI matrix because the consumer corpus runs it. `fast-depends` DI behavior remains covered by explicit strict-mypy compatibility tests rather than broadening production ignores.

### 27.11 Final dissent ledger

| Disputed position | Resolution |
|---|---|
| Ship the reviewed adaptive `IdWindowScan` | Rejected: completeness and sparse-cost proof failed. |
| Runtime probes can authorize optimized profiles | Rejected: probes only falsify. |
| Profile is always required for parallel counted offset | Modified: caller-requested complete in-band attempt-then-verify is allowed; profile is required for profile-verified assurance/default auto-choice. |
| All dependent cursors are necessarily separate HTTP round trips | Unresolved fact; W0 must test Bitrix `$result[...]` chaining before relying on the assumption. |
| Keep automatic OR splitting | Rejected: changes query semantics. |
| Infer replay safety from method suffix | Rejected. |
| Total equality universally proves completeness | Rejected. |
| Any total mismatch must raise | Rejected; semantics are typed. |
| Operational completion equals verified completeness | Rejected; terminal, assurance, and snapshot axes remain separate. |
| Keep failed-but-correct optimizations as public opt-in | Rejected for 2.0: failed admission removes exposure. |
| Ship live seed/cleanup tooling in the wheel | Rejected. |
