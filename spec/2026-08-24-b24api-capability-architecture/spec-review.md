# b24api 2.x capability and architecture refactor

**Status:** implementation-ready council specification  
**Date:** 2026-08-24  
**Repository:** `/Users/alex/Develop/b24api/client`  
**Target runtime:** Python 3.12+  
**Frozen predecessor:** b24api 1.0.1 at `08277c4d921b83b9252177b3e72a21a4c0c86109`  
**Prior correctness specification:** recoverable as the parent of deletion commit `6542800570d5f252f83f1b73ee7526757897c174`  
**Historical issue source:** read-only `/Users/alex/Develop/b24api/skills/ISSUES.md`  
**Design council:** GPT-5.6-sol high and Claude Opus 5 high

## 1. Executive decision

Refactor the correctness-first client into one thin, method-agnostic, Python-idiomatic v2 API. Preserve the proven execution, protocol, pagination, batch, cancellation, reporting, budgeting, immutability and redaction kernel. Remove the 1.x compatibility surface rather than maintaining parallel APIs.

The result must answer five business questions:

1. Does the client prevent the mechanical failure classes observed with 1.0.1?
2. Does the final API retain every useful capability even when names and signatures change?
3. Can arbitrary caller correlation travel beside requests, remain off the wire and return with every outcome?
4. Can compatibility-only, dead, duplicate and placeholder code be removed?
5. Can the architecture become thinner and clearer without losing correctness, boundedness or established request efficiency?

The answer must be demonstrated by:

- a five-way business capability matrix;
- a detailed capability-preservation ledger with an explicit `Missing/regressed?` column;
- issue-ID traceability to a bounded sample from `ISSUES.md`;
- executable characterization and regression tests;
- focused reuse of the existing evidence and profiling harness;
- an explicit public export manifest;
- an installed CLI;
- a README mapping v1 capabilities to v2 operations.

No final capability may be reported as preserved solely because a replacement method exists. It requires executable evidence.

## 2. Normative language and conservative assumptions

“Must” and “must not” are acceptance requirements. “Should” is a strong default that may be changed only with a documented cohesion or correctness reason.

The following assumptions resolve missing design inputs:

- This is a breaking major-version refactor. No v1 import path, signature or runtime adapter is retained.
- The existing correctness kernel is the behavioral baseline unless this specification explicitly strengthens it.
- `Settings` remains the environment-backed credential and transport configuration type.
- `Request`, not a method registry, carries replay-safety truth.
- Generic Bitrix wire conventions may be represented as defaults, but all control paths are overridable through typed mechanics contracts.
- A response-size ceiling is added because row-count ceilings alone cannot bound an oversized encoded response.
- Reference bindings use explicit path updates; ambiguous recursive merge semantics are not introduced.
- Batch operations preserve input order. Fan-out and reference operations expose explicit delivery order.
- No operation infers endpoint applicability, business-filter semantics, write reconciliation, entity identity or storage identity.
- A sampled historical issue may contain multiple failure mechanisms. Each mechanism, rather than the issue as a whole, is classified into one failure-matrix row.
- The bounded issue sample for this refactor is exactly `B1`, `B2`, `B3`, `B4`, `B5`, `C1b`, `C5`, `C34` and `C35`, including documented subcases such as `B1a` and `B1b`. Additional issues require an explicit scope amendment.
- Issue identifiers are traceability anchors, not permission to add endpoint-specific production behavior.
- There are no unresolved design questions. Discoveries that would alter a public contract require a specification amendment.

## 3. Scope

### 3.1 In scope

- Refactor the Python package in this repository.
- Define a closed, documented public export surface.
- Replace booleans that alter Python return or yield types with separate operations.
- Make the basic list operation an obvious sequential method.
- Give counted, keyset and cursor traversals explicit names.
- Make logical, finite-or-unbounded batch the canonical meaning of batch.
- Preserve arbitrary typed off-wire correlation for batch, fan-out and references.
- Define exact stream ownership, report, early-close, cancellation and partial-result behavior.
- Define exact tolerant outcome unions, ambiguous states and not-executed states.
- Remove compatibility wrappers, aliases, profiles, dead variants and dependencies proven unused.
- Split oversized modules after behavior is characterized.
- Add an installed `b24api` CLI with direct call/response and explicitly selected list strategies.
- Add an encoded-response byte ceiling.
- Re-run focused old/new request, wall/CPU and memory checks.
- Rewrite README for the final API, lifecycle, limitations and capability mapping.
- Produce a final capability report containing the matrices required by Section 5.

### 3.2 Explicitly out of scope

- Backward-compatible names, signatures, models or import paths.
- Editing or migrating `/Users/alex/Develop/b24api/skills/bitrix24`.
- Editing `ISSUES.md`.
- Editing the historical `/Users/alex/Develop/rag24/rag-bitrix24` corpus.
- Shipping a concrete registry of Bitrix methods, entities, identities or storage keys.
- Repeating the complete W0-W12 research program.
- Reopening generic partitioned-keyset optimization.
- Claiming that the generic client can prove business-filter semantics, verify writes or select application storage keys.
- Batch or fan-out CLI protocols.
- A synchronous facade.
- Release, PyPI publication or live portal mutation.
- Preserving runtime endpoint-profile selection or automatic traversal guessing.

## 4. Established baseline

The following current-kernel properties are assets, not redesign targets:

- one replay-aware executor across direct, batch and traversal calls;
- structured Bitrix error and protocol validation;
- fail-closed incomplete traversal;
- exact identity, duplicate, range, total and non-progress checks where required evidence exists;
- bounded request, row, page, reference and identity budgets;
- logical batch support over sync and async sources;
- cancellation-resistant owned cleanup and terminal reports;
- detached or immutable public request and response data;
- HTTP/2 with HTTP/1 fallback;
- structural request parity between current counted batching and frozen 1.0.1: `1/2/5` physical requests at `19/500/10,000` dense rows;
- safe generic no-count as exact sequential keyset;
- `PartitionedKeysetPlan` refusing before I/O because it was never admitted;
- the profiling runner at `tools/b24api_evidence/profile_runtime.py`.

The refactor may simplify exposure but must not weaken these properties.

## 5. Capability matrices

### 5.1 Five-way business capability matrix

This matrix is mandatory in the implementation’s final capability report. It has exactly the five primary business dimensions and an explicit missing/regressed disposition.

| Business gate | Baseline capability or risk | Required v2 outcome | Required evidence | Missing/regressed disposition |
|---|---|---|---|---|
| Old failure prevention | Historical mechanical failures include partial batch, unsafe replay, control desynchronization, counted overlap and identity-path mismatch | Every sampled client-side mechanism is prevented, typed or failed closed; application-only failures are not overclaimed | Issue traceability matrix plus named regression tests | Any sampled mechanism without a tested disposition is `missing`; any return to silent success is `regressed` and blocks acceptance |
| Capability preservation | v1 exposes direct/response, batch, generators, offset/count/keyset/cursor and reference workflows through mixed canonical and compatibility APIs | Every useful capability has one typed v2 operation or an explicit safe replacement | Detailed preservation ledger, README mapping and executable examples | An unmapped useful capability is `missing`; a replacement that loses correctness, boundedness or correlation is `regressed` |
| Correlation | Payload support exists but is shape-dependent and can be lost in failure paths | Generic `Command[C]` and `Binding[C]` correlation survives every correlated terminal variant and remains off-wire | Success/failure/not-executed/unknown tests across physical chunks and references | Any variant lacking correlation is `missing`; serialization, hashing or logging of correlation is `regressed` |
| Legacy removal | Compatibility wrappers, profile selection, duplicate models and aliases remain | One documented public surface; compatibility and automatic-profile scaffolding absent | Root-export snapshot, package-content audit, dependency reachability and symbol search | Any required adapter or duplicate public route is `missing`; any retained compatibility-only surface is `regressed` |
| Architecture and operations | Correctness state machines are concentrated in oversized modules and exposed through low-level plans | Thin facade, typed public mechanics, bounded transport and streams, explicit ownership, installed CLI | Import-boundary tests, memory/profile gates, wheel test and lifecycle tests | An unbounded path or cross-layer dependency is `missing`; moving algorithms into facade/CLI or weakening lifecycle is `regressed` |

A row may be marked complete only when its required evidence exists. Narrative confidence is not evidence.

### 5.2 Detailed capability-preservation ledger

The final implementation report must reproduce this ledger and replace every planned evidence entry with the concrete test, example or profile artifact. The `Missing/regressed?` column is blocking: any final `Yes` prevents acceptance.

| Capability | v1/current form | Final v2 form | Required evidence | Missing/regressed? |
|---|---|---|---|---|
| Decoded direct call | `call()` | `call()` | Detached JSON test and README example | No, if evidence passes |
| Immutable response envelope | `call(raw=True)` | `call_response()` | Metadata, immutability and structured-error tests | No; renamed with stable type |
| Replay control | `retry` boolean plus historical unsafe behavior | `Request.replay_safety` and typed policy | `B2` regression tests for safe, unsafe and unknown dispatch | No; boolean intentionally replaced |
| Finite physical batch | `batch()` | logical `batch()` over `Command[C]` | Multi-command ordering and request-shape tests | No |
| Unbounded logical batch | existing kernel capability | canonical `batch()` | 100,000-command plateau and source-closure tests | No |
| Tolerant batch | `errors="yield"`/current outcome path | `batch_outcomes()` four-state union | `B1`, `B1a`, `B1b` synthetic regressions | No |
| Sync generator input | tuple/request conventions | `Iterable[Command[C]]` or `Iterable[Binding[C]]` | Exact iterator close on success/failure/cancel | No |
| Async generator input | documented unevenly | `AsyncIterable[Command[C]]` or `AsyncIterable[Binding[C]]` | `B4` characterization and bounded admission | No |
| Off-wire payload | `(request, payload)` and `with_payload` | generic `correlation` | Wire snapshots for every outcome variant | No; representation replaced |
| Sequential offset list | `list_sequential` | `iter_list` | Continuation, contradiction and early-close tests | No |
| Counted list | `list_batched` | `iter_list_counted` | `B5`, exact identity and `1/2/5` tests | No; identity becomes mandatory |
| No-count keyset | `list_batched_no_count`/`list_keyset` | `iter_list_keyset` | `C1b` separate-path and exactness tests | No |
| Dependent cursor | cursor wrappers | `iter_list_cursor` | monotonic, repeated-boundary and empty-confirmation tests | No for admitted unique cursors |
| Permissive repeated cursor boundary | endpoint-specific dedup conventions such as `C5` | no generic permissive traversal | fail-closed `C5` test; application fallback documented | Intentional non-preservation, not a regression |
| Independent fan-out | reference/batch wrappers | `fan_out` and `fan_out_outcomes` | input/ready order and bounded concurrency tests | No |
| Bound reference traversal | `reference_*` wrappers | `Binding[C]` plus `iter_references` | parent correlation, partial rows and exact completion | No |
| Empty reference success | previously conflated with no rows emitted | `ReferenceComplete(row_count=0)` | zero-row binding test | No; strengthened |
| Response selector | `result_key` and plan selectors | `ResultSelector` relative to `Response.result` | `B4` nested-result test | No |
| Page-size control | wrapper-specific `list_size`/`LIMIT` behavior | explicit `limit_path` plus conflict validation | `B3` no-silent-override test | No |
| Early close and cancellation | report/cancellation kernel | `OperationStream` terminal contract | context break, helper, client-close and cancellation tests | No |
| Partial bounded consumption | manual break | `first()` and `collect(limit=...)` | conservative `EARLY_CLOSED` tests | No; strengthened |
| Final operation report | mutable intermediate report field | one immutable post-cleanup report | exactly-once identity and cleanup-failure tests | No |
| Endpoint profiles | runtime profile selection | explicit public mechanics | root-export and package-content audit | Intentional removal, not a regression |
| Shell direct/list use | no installed final CLI | `b24api call` and `b24api list` | built-wheel entry-point tests | No; new capability |
| Business-filter proof | caller assumption | remains application-owned | `C34`/`C35` non-overclaim tests and limitations documentation | Not a generic-client capability |

“Intentional non-preservation” is permitted only for unsafe or endpoint-specific behavior that remains achievable through direct calls and application logic. It must be documented in README limitations and cannot be counted as a missing useful generic capability.

## 6. Sampled issue traceability

The bounded issue sample is taken from read-only `skills/ISSUES.md`. Production runtime code must not embed these endpoint names or issue rules. Tests may name issue IDs and use synthetic method strings or deterministic fixtures.

| Issue ID | Observed failure or capability | Generic v2 disposition | Required regression or characterization | Application remainder | Fixed-by-v2 claim |
|---|---|---|---|---|---|
| `B1`, including `B1a`/`B1b` | One failed subcommand halts unrelated batch/reference work and loses exact tolerant handling | Separate fail-fast and tolerant operations; four command states; five reference variants; bounded failure windows | `test_issue_b1_tolerant_batch_preserves_all_correlated_states`; equivalent reference test | Consumer must handle every tolerant variant | Yes, for generic partial-batch mechanics |
| `B2` | Automatic retry policy historically allowed ambiguous write replay and documentation did not match execution | `ReplaySafety.UNKNOWN` default; unsafe/unknown possible-dispatch failures never auto-replay; physical batch replay requires all-safe commands | `test_issue_b2_unknown_write_is_not_replayed_after_dispatch`; safe and pre-dispatch companions | Caller classifies safety and reconciles ambiguous writes | Yes, for retry mechanics; business reconciliation remains external |
| `B3` | Cursor wrapper silently replaces/ignores caller `LIMIT` | Explicit `limit_path`; traversal owns controls; conflicting request values reject before I/O | `test_issue_b3_limit_conflict_rejects_before_network`; equal-value acceptance test | Caller supplies the endpoint’s actual limit path | Yes, for silent control desynchronization |
| `B4` | Async reference sources, payload correlation and result selection are useful but inconsistently exposed | `AsyncIterable[Binding[C]]`, opaque correlation and `ResultSelector` | `test_issue_b4_async_binding_correlation_and_selector` | Per-reference business-date stopping remains application logic | Yes, for the generic capabilities |
| `B5` | Counted tail stepped by caller `list_size` rather than observed head width, producing overlap and duplicates | Derive counted tail stride from the first returned page; require identity; validate total, duplicates and range | `test_issue_b5_counted_stride_uses_observed_head_width`; overlap contradiction test | Caller asserts stable offset mechanics and filtered exact total | Yes |
| `C1b` | One `id_key` could not represent different item, filter and order casing, causing silent under-fetch | `IdentitySpec.item_path`, `filter_key` and `order_key` remain separate; `KeysetSpec` owns container paths | `test_issue_c1b_distinct_item_filter_order_paths_are_exact` | Caller supplies the concrete paths and casing | Yes |
| `C5` | A date-cursor endpoint repeats a boundary item and needs endpoint-specific deduplication | Generic cursor requires strict unique monotonic progress and fails closed on repeated boundary | `test_issue_c5_repeated_cursor_boundary_is_not_reported_complete` | Application chooses a tie-breaker, direct-call workflow or explicit dedup/reconciliation | No generic fix claimed; safe refusal is intended |
| `C34` (`employees-011`) | A syntactically accepted but wrong scalar filter is silently ignored and returns match-all | Runtime exposes returned rows and mechanical evidence but does not claim filter semantics | `test_issue_c34_ignored_filter_requires_application_reconciliation` using a synthetic ignored-filter portal | Application validates expected owners/business set | No; explicitly application-owned |
| `C35` (`marketing-003`) | Multifield presence filters silently overmatch, creating correctness and PII risk | No method-specific rule is embedded; README warns that successful traversal is not semantic-filter proof | `test_issue_c35_overmatched_multifield_is_not_claimed_verified` using synthetic data | Application sweeps/classifies or independently reconciles | No; explicitly application-owned |

The final capability report must list every sampled ID, its test result and one of:

- `fixed mechanically`;
- `preserved capability`;
- `fail-closed, application contract required`;
- `application-only, no client fix claimed`;
- `missing`;
- `regressed`.

`missing` or `regressed` blocks acceptance.

## 7. Business failure matrix

Each distinct sampled failure mechanism is assigned to one row.

| Failure class | Generic client responsibility | Application responsibility |
|---|---|---|
| Pagination non-progress or contradictory continuation | Detect and fail closed | Select an applicable traversal |
| Counted tail overlap, missing page, duplicate identity or total mismatch | Derive observed stride, require identity and validate exact-total evidence | Assert stable offset mechanics and filtered exact totals |
| Ambiguous write after dispatch | Preserve an unknown outcome and never replay unless explicitly safe | Reconcile or verify business state |
| Partial batch | Return exact typed per-command states | Choose fail-fast or handle every tolerant state |
| Lost caller payload | Preserve typed correlation off-wire | Choose correlation contents and storage meaning |
| Item/filter/order casing mismatch | Support independent paths and keys | Supply concrete paths |
| Child IDs repeated across parents | Scope identity tracking per binding and preserve parent correlation | Choose composite storage identity |
| Portal ignores or overmatches a business filter | Detect only visible mechanical contradictions; never claim semantic proof | Reconcile the expected business set independently |
| Equal or non-unique cursor values | Reject when strict progress cannot be proved | Supply a unique tie-breaker or use another workflow |
| Early close or cancellation | Stop admission, close owned resources and report non-completion | Do not present partial consumption as complete |
| Oversized encoded response | Abort at the byte ceiling and fail closed | Raise the ceiling explicitly only when justified |

“Fixed by v2” is allowed only when the issue matrix names executable evidence.

## 8. Architectural principles

1. **Thin runtime.** Production code knows Bitrix REST mechanics, not concrete entities or application rules.
2. **One public surface.** There is no canonical-plus-compatibility split.
3. **Stable return types.** Separate Python methods replace return-type-changing booleans.
4. **Explicit optimization.** The unsuffixed list operation is sequential; specialized strategies are named.
5. **Opaque correlation.** Correlation is never interpreted, serialized, hashed or used as an internal key.
6. **Fail closed.** Missing proof produces refusal, an explicit negative outcome or an incomplete report.
7. **Bounded by construction.** Input admission, decoded rows, active references and encoded response bytes have explicit ceilings.
8. **Owned lifecycle.** Context managers and `aclose()` are canonical for clients and streams.
9. **Immutable terminal evidence.** Each stream publishes exactly one immutable final report after termination.
10. **Adapters remain thin.** CLI code contains no retry, pagination or endpoint algorithm.
11. **Mechanical restructuring.** File movement follows characterization and is separated from semantic changes.
12. **No hidden inference.** Method spelling never determines replay safety, traversal strategy or entity behavior.
13. **Traceable delivery.** Every business gate and sampled issue maps to a named test or profile artifact.

## 9. Public data and transport contracts

### 9.1 JSON and paths

```python
type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type PathPart = str | int
```

JSON object keys are strings. Floats must be finite. Cyclic and non-JSON values reject locally.

```python
@dataclass(frozen=True, slots=True)
class ParameterPath:
    path: tuple[PathPart, ...]

@dataclass(frozen=True, slots=True)
class ResultSelector:
    path: tuple[PathPart, ...] = ()

    @classmethod
    def root(cls) -> Self: ...
```

A string path component must be non-empty. An integer component must be non-negative and must not be a boolean. `ParameterPath` is non-empty; root `ResultSelector` is `()`.

Selectors apply to `Response.result`, not the outer HTTP envelope. If Bitrix returns `{"result": {"items": [...]}}`, the selector is `("items",)`.

### 9.2 Requests and replay safety

```python
class ReplaySafety(StrEnum):
    SAFE = "safe"
    UNSAFE = "unsafe"
    UNKNOWN = "unknown"

class RequestSpec(TypedDict):
    method: Required[str]
    parameters: NotRequired[Mapping[str, object]]
    replay_safety: NotRequired[ReplaySafety]

type RequestLike = Request | RequestSpec
```

```python
@dataclass(frozen=True, slots=True, init=False)
class Request:
    method: str
    replay_safety: ReplaySafety

    def __init__(
        self,
        method: str,
        parameters: Mapping[str, object] | None = None,
        replay_safety: ReplaySafety = ReplaySafety.UNKNOWN,
    ) -> None: ...

    @property
    def parameters(self) -> Mapping[str, JsonValue]: ...
    def copy_parameters(self) -> dict[str, JsonValue]: ...
```

`RequestSpec` is closed. Unknown keys reject locally. Public operations canonicalize it immediately.

Retry rules:

- `SAFE` authorizes automatic replay after possible dispatch.
- `UNSAFE` forbids it.
- `UNKNOWN` is the default and has the same conservative consequence as `UNSAFE`.
- A conclusively pre-dispatch failure may retry regardless of safety.
- Exhausted retries do not prove non-execution.
- Method spelling never changes safety.

### 9.3 Responses

```python
@dataclass(frozen=True, slots=True, init=False)
class Response:
    total: int | None
    next: int | None
    time: ResponseTime | None
    evidence: ResponseEvidence

    @property
    def result(self) -> JsonValue: ...
    def list_items(self, selector: ResultSelector = ResultSelector.root()) -> list[JsonValue]: ...
```

`Response` stores deeply frozen data. Accessors return detached JSON copies. Structured Bitrix errors never appear as successful responses.

### 9.4 Transport

```python
@dataclass(frozen=True, slots=True)
class WireResponse:
    status_code: int
    headers: tuple[tuple[str, str], ...]
    body: bytes

class Transport(Protocol):
    async def send(
        self,
        request: Request,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse: ...
```

A transport must:

- honor cancellation and `attempt_timeout`;
- classify failures by the last conclusive dispatch phase;
- return at most `max_response_bytes`;
- abort a streamed body when the ceiling would be exceeded;
- exclude credentials and full bodies from exception text.

The built-in HTTPX transport performs bounded streaming reads.

## 10. Policy and lifecycle

### 10.1 Execution policy

```python
@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    max_requests: int = 10_000
    max_pages: int = 10_000
    max_pages_per_reference: int = 10_000
    max_elapsed: float = 900.0
    max_attempts_per_request: int = 5
    max_retry_elapsed_per_request: float = 120.0

    max_response_bytes: int = 16 * 1024 * 1024
    max_buffered_commands: int = 50
    max_buffered_rows: int = 2_500
    max_direct_concurrency: int = 10
    max_active_references: int = 100
    max_tracked_identities: int = 100_000

    identity_tracker: IdentityTracker = IdentityTracker.MEMORY
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    consistency: ConsistencyPolicy = field(default_factory=ConsistencyPolicy.traversal)
    debug_evidence: bool = False
```

Ceilings are checked before scheduling new work when possible. Remote data exceeding a ceiling fails when detected.

A per-operation policy replaces the client default as a whole; policies are not merged.

### 10.2 Client lifecycle and transport ownership

```python
class Bitrix24:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        policy: ExecutionPolicy | None = None,
        transport: Transport | None = None,
    ) -> None: ...

    async def aclose(self) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *_exc: object) -> None: ...

    @property
    def host(self) -> str: ...
```

Rules:

- `settings=None` loads environment-backed `Settings`.
- `host` derives from settings, not an injected transport.
- `policy` becomes the client default.
- A constructed transport is client-owned.
- An injected transport is caller-owned and is never closed by the client.
- `aclose()` is idempotent.
- Closing the client closes active streams before its owned transport.
- Cleanup is cancellation-resistant and preserves the primary exception.
- Starting work after client closure raises `RuntimeError` before I/O.
- A stream cannot continue through a closed client transport.

## 11. Direct calls

```python
async def call(
    self,
    request: RequestLike,
    *,
    policy: ExecutionPolicy | None = None,
) -> JsonValue: ...

async def call_response(
    self,
    request: RequestLike,
    *,
    policy: ExecutionPolicy | None = None,
) -> Response: ...
```

`call()` always returns detached JSON. `call_response()` always returns the immutable envelope. `call(raw=True)` is removed.

Both operations:

- use the same executor and retry rules;
- raise typed structured, transport, protocol, budget and ambiguous-execution errors;
- apply the response-byte ceiling;
- redact credentials and parameter values.

## 12. Batch and command outcomes

### 12.1 Input and output types

```python
@dataclass(frozen=True, slots=True)
class Command(Generic[C]):
    request: Request
    correlation: C
```

Correlation:

- may be any Python object, including unhashable or mutable values;
- is retained by reference;
- is excluded from `repr`, safe serialization and reports;
- never enters parameters or batch command text;
- is not used as a key;
- is not copied, frozen or interpreted.

Each consumed command receives a zero-based global index.

```python
@dataclass(frozen=True, slots=True)
class CommandSuccess(Generic[C]):
    index: int
    correlation: C
    request_summary: RequestSummary
    response: Response

    @property
    def result(self) -> JsonValue: ...

@dataclass(frozen=True, slots=True)
class CommandFailure(Generic[C]):
    index: int
    correlation: C
    request_summary: RequestSummary
    error: B24ApiError

@dataclass(frozen=True, slots=True)
class CommandNotExecuted(Generic[C]):
    index: int
    correlation: C
    request_summary: RequestSummary
    reason: NotExecutedReason

@dataclass(frozen=True, slots=True)
class CommandOutcomeUnknown(Generic[C]):
    index: int
    correlation: C
    request_summary: RequestSummary
    error: B24ApiError

type CommandOutcome[C] = (
    CommandSuccess[C]
    | CommandFailure[C]
    | CommandNotExecuted[C]
    | CommandOutcomeUnknown[C]
)
```

Definitions:

- `CommandSuccess`: a successful command response was decoded.
- `CommandFailure`: conclusive command failure evidence exists.
- `CommandNotExecuted`: non-execution is proved.
- `CommandOutcomeUnknown`: server acceptance cannot be excluded and no conclusive response exists.

`NotExecutedReason` is a closed enum containing at least `HALTED`, `SOURCE_FAILED`, `LOCAL_VALIDATION_FAILED` and `SCHEDULER_STOPPED`.

### 12.2 Operations

```python
def batch(
    self,
    commands: Iterable[Command[C]] | AsyncIterable[Command[C]],
    *,
    batch_size: int | None = None,
    policy: ExecutionPolicy | None = None,
) -> OperationStream[CommandSuccess[C]]: ...

def batch_outcomes(
    self,
    commands: Iterable[Command[C]] | AsyncIterable[Command[C]],
    *,
    batch_size: int | None = None,
    policy: ExecutionPolicy | None = None,
) -> OperationStream[CommandOutcome[C]]: ...
```

Rules:

- Both emit in input order.
- `batch_size` is a physical chunk size, not a logical limit.
- Its default is `min(50, policy.max_buffered_commands)`.
- Explicit values must be between 1 and both ceilings.
- Input is incrementally admitted and never materialized.
- At most one bounded physical result window is retained unless a pipeline proves identical ceilings.
- A pulled item is consumed and admitted.
- No outcome exists for an item never pulled.
- The exact owned iterator is closed on every terminal path.
- An active sync pull is awaited before iterator closure.

`batch()` uses fail-fast physical semantics. It yields preceding successes, then raises `BatchFailed[C]`.

`batch_outcomes()` uses non-halting physical batches and emits one typed outcome for each admitted command whenever correlation remains possible.

A source iterator failure:

- stops consumption;
- does not dispatch the partially filled chunk;
- classifies already admitted partial-chunk commands as `SOURCE_FAILED`;
- emits those states in tolerant mode;
- closes the source;
- terminates with `InputSourceError` or bounded `BatchFailed`.

A malformed or ambiguous physical response that remains correlated to a submitted chunk produces one negative outcome per command. Loss of chunk correlation terminates the operation.

### 12.3 Physical retry safety

A physical batch is replay-safe only when every command in the chunk is `SAFE`.

If acceptance cannot be excluded:

- unsafe or unknown chunks are not replayed;
- unresolved commands become unknown;
- fail-fast mode raises;
- tolerant mode may continue only while shared state and budgets remain usable.

### 12.4 Bounded fail-fast exception

```python
class BatchFailed(B24ApiError, Generic[C]):
    outcomes: tuple[CommandOutcome[C], ...]
    report: OperationReport
```

The exception contains only the bounded current physical or admission window. Historical successes remain represented by counters and already yielded values.

## 13. Traversal mechanics

### 13.1 Generic mechanics

```python
@dataclass(frozen=True, slots=True)
class OffsetSpec:
    parameter_path: ParameterPath = ParameterPath(("start",))
    limit_path: ParameterPath | None = None
    allow_create_controls: bool = True

@dataclass(frozen=True, slots=True)
class IdentitySpec:
    item_path: tuple[PathPart, ...]
    filter_key: str
    order_key: str
    coercion: IdentityCoercion = IdentityCoercion.EXACT_STRING

@dataclass(frozen=True, slots=True)
class KeysetSpec:
    filter_path: ParameterPath = ParameterPath(("filter",))
    order_path: ParameterPath = ParameterPath(("order",))
    start_suppression_path: ParameterPath | None = ParameterPath(("start",))
    limit_path: ParameterPath | None = None
    direction: Literal["ascending", "descending"] = "ascending"
    allow_create_controls: bool = True

@dataclass(frozen=True, slots=True)
class CursorSpec:
    parameter_path: ParameterPath
    item_path: tuple[PathPart, ...]
    coercion: IdentityCoercion
    direction: Literal["ascending", "descending"]
    take: Literal["first", "last"]
    limit_path: ParameterPath | None = None
    allow_create_controls: bool = True
```

Keyset continuation adds `>` or `<` to the plain `IdentitySpec.filter_key`.

The admitted public keyset and cursor terminal rule is empty confirmation. Permissive short-page or repeated-boundary completion is not exposed.

Traversal controls are validated before the first request for the affected base request or binding. Ambiguous, overlapping or conflicting paths reject rather than being overwritten.

### 13.2 List methods

```python
def iter_list(
    self,
    request: RequestLike,
    *,
    selector: ResultSelector = ResultSelector.root(),
    identity: IdentitySpec | None = None,
    page_size: int = 50,
    offset: OffsetSpec = OffsetSpec(),
    policy: ExecutionPolicy | None = None,
) -> OperationStream[JsonValue]: ...

def iter_list_counted(
    self,
    request: RequestLike,
    *,
    identity: IdentitySpec,
    selector: ResultSelector = ResultSelector.root(),
    page_size: int = 50,
    batch_size: int | None = None,
    offset: OffsetSpec = OffsetSpec(),
    policy: ExecutionPolicy | None = None,
) -> OperationStream[JsonValue]: ...

def iter_list_keyset(
    self,
    request: RequestLike,
    *,
    selector: ResultSelector,
    identity: IdentitySpec,
    page_size: int = 50,
    keyset: KeysetSpec = KeysetSpec(),
    policy: ExecutionPolicy | None = None,
) -> OperationStream[JsonValue]: ...

def iter_list_cursor(
    self,
    request: RequestLike,
    *,
    selector: ResultSelector,
    cursor: CursorSpec,
    identity: IdentitySpec | None = None,
    page_size: int = 50,
    policy: ExecutionPolicy | None = None,
) -> OperationStream[JsonValue]: ...
```

Semantics:

- `iter_list` is sequential offset/server-next traversal.
- `iter_list_counted` performs one direct head request and physically batched tail requests.
- `iter_list_keyset` is exact sequential no-count traversal.
- `iter_list_cursor` is dependent sequential cursor traversal.

For sequential, keyset and cursor strategies, `page_size` is the requested size and buffer contract. It is injected only when the corresponding `limit_path` exists.

For counted traversal:

1. The optional limit is injected into the head request when `offset.limit_path` exists.
2. If the exact total is no greater than the head row count, traversal completes after the head.
3. If more rows remain, the tail stride is the observed head row count, not the caller’s nominal `page_size`.
4. A zero-row head with positive total is contradictory and fails.
5. Tail offsets use that observed stride.
6. Identity uniqueness, offset ranges, emitted count and exact total are validated.
7. Later inconsistent page widths may be tolerated only where the final identity/range/total proof remains exact; otherwise traversal fails closed.

This preserves the established `B5` fix: a caller hint smaller than the server’s actual head width cannot silently create overlapping tail windows.

`iter_list_counted` requires identity. Without it, duplicate replacement cannot be distinguished from an exact result.

Counted caller assertions are limited to:

- filtered exact total semantics;
- stable offset addressing;
- the selected head is representative of the server’s offset stride;
- the identity path is present and unique.

With defaults, dense request parity remains `1/2/5` at `19/500/10,000`.

Keyset requires strict monotonic identity, separate item/filter/order roles and an empty confirmation page.

Cursor requires unique monotonic cursor values and empty confirmation. An optional separate identity checks item duplication. A repeated boundary such as `C5` fails closed; the generic client does not silently deduplicate an unprovable cursor contract.

### 13.3 High-level traversal contracts for references

```python
@dataclass(frozen=True, slots=True)
class SequentialTraversal:
    selector: ResultSelector = ResultSelector.root()
    identity: IdentitySpec | None = None
    page_size: int = 50
    offset: OffsetSpec = OffsetSpec()

@dataclass(frozen=True, slots=True)
class CountedTraversal:
    identity: IdentitySpec
    selector: ResultSelector = ResultSelector.root()
    page_size: int = 50
    batch_size: int | None = None
    offset: OffsetSpec = OffsetSpec()

@dataclass(frozen=True, slots=True)
class KeysetTraversal:
    selector: ResultSelector
    identity: IdentitySpec
    page_size: int = 50
    keyset: KeysetSpec = KeysetSpec()

@dataclass(frozen=True, slots=True)
class CursorTraversal:
    selector: ResultSelector
    cursor: CursorSpec
    identity: IdentitySpec | None = None
    page_size: int = 50

type TraversalSpec = (
    SequentialTraversal
    | CountedTraversal
    | KeysetTraversal
    | CursorTraversal
)
```

These are public mechanics values, not internal state-machine plans.

## 14. Fan-out and bound references

### 14.1 Dispatch

```python
class DeliveryOrder(StrEnum):
    READY = "ready"
    INPUT = "input"

class DispatchMode(StrEnum):
    DIRECT = "direct"
    BATCH = "batch"

@dataclass(frozen=True, slots=True)
class DispatchSpec:
    mode: DispatchMode = DispatchMode.DIRECT
    concurrency: int = 10
    batch_size: int = 50
    output_order: DeliveryOrder = DeliveryOrder.READY
```

Only mode-relevant controls are used. Contradictory values reject locally. Effective limits are narrowed by policy.

`READY` may interleave results. `INPUT` applies bounded buffering and backpressure.

### 14.2 Independent fan-out

```python
def fan_out(
    self,
    commands: Iterable[Command[C]] | AsyncIterable[Command[C]],
    *,
    dispatch: DispatchSpec = DispatchSpec(),
    policy: ExecutionPolicy | None = None,
) -> OperationStream[CommandSuccess[C]]: ...

def fan_out_outcomes(
    self,
    commands: Iterable[Command[C]] | AsyncIterable[Command[C]],
    *,
    dispatch: DispatchSpec = DispatchSpec(),
    policy: ExecutionPolicy | None = None,
) -> OperationStream[CommandOutcome[C]]: ...
```

Fan-out allows bounded independent dispatch and explicit delivery order while reusing command outcomes.

### 14.3 Binding

```python
@dataclass(frozen=True, slots=True, init=False)
class ParameterUpdate:
    path: ParameterPath
    value: JsonValue

@dataclass(frozen=True, slots=True)
class Binding(Generic[C]):
    summary: str
    updates: tuple[ParameterUpdate, ...]
    correlation: C
```

Binding updates apply before traversal controls:

- exact leaves are replaced;
- missing mapping containers may be created;
- missing list indices reject;
- case-insensitive ambiguity rejects;
- overlapping update paths reject;
- traversal-control collisions reject before binding I/O.

`summary` is bounded redacted diagnostic text. `binding_index`, not summary or correlation, is scheduler identity.

### 14.4 Reference events

```python
@dataclass(frozen=True, slots=True)
class ReferenceItem(Generic[C]):
    binding_index: int
    correlation: C
    item_index: int
    item: JsonValue

@dataclass(frozen=True, slots=True)
class ReferenceComplete(Generic[C]):
    binding_index: int
    correlation: C
    row_count: int

@dataclass(frozen=True, slots=True)
class ReferenceFailure(Generic[C]):
    binding_index: int
    correlation: C
    error: B24ApiError
    partial_rows: int

@dataclass(frozen=True, slots=True)
class ReferenceNotExecuted(Generic[C]):
    binding_index: int
    correlation: C
    reason: NotExecutedReason

@dataclass(frozen=True, slots=True)
class ReferenceOutcomeUnknown(Generic[C]):
    binding_index: int
    correlation: C
    error: B24ApiError
    partial_rows: int

type ReferenceEvent[C] = ReferenceItem[C] | ReferenceComplete[C]
type ReferenceOutcome[C] = (
    ReferenceItem[C]
    | ReferenceComplete[C]
    | ReferenceFailure[C]
    | ReferenceNotExecuted[C]
    | ReferenceOutcomeUnknown[C]
)
```

```python
def iter_references(
    self,
    request: RequestLike,
    bindings: Iterable[Binding[C]] | AsyncIterable[Binding[C]],
    *,
    traversal: TraversalSpec,
    dispatch: DispatchSpec = DispatchSpec(mode=DispatchMode.BATCH),
    policy: ExecutionPolicy | None = None,
) -> OperationStream[ReferenceEvent[C]]: ...

def iter_reference_outcomes(
    self,
    request: RequestLike,
    bindings: Iterable[Binding[C]] | AsyncIterable[Binding[C]],
    *,
    traversal: TraversalSpec,
    dispatch: DispatchSpec = DispatchSpec(mode=DispatchMode.BATCH),
    policy: ExecutionPolicy | None = None,
) -> OperationStream[ReferenceOutcome[C]]: ...
```

Rules:

- `ReferenceComplete` follows every successfully completed binding, including zero rows.
- No complete event follows a negative terminal state.
- Identity tracking is scoped per binding.
- Fail-fast reference traversal raises `ReferenceFailed[C]`.
- Tolerant traversal emits exact negative states and continues where safe.
- `partial_rows` counts delivered rows for that binding.
- Exceptions retain only bounded active-window states.

## 15. Streams, helpers and final reports

### 15.1 Operation stream

```python
class OperationStream(Generic[T], AsyncIterator[T], AsyncContextManager["OperationStream[T]"]):
    @property
    def report(self) -> OperationReport | None: ...

    async def aclose(self) -> None: ...
    async def first(self) -> PartialResult[tuple[T, ...]]: ...
    async def collect(self, *, limit: int) -> PartialResult[list[T]]: ...
```

Rules:

- streams are single-use;
- construction and context entry do not prefetch;
- first iteration starts work;
- natural exhaustion finalizes cleanup before `StopAsyncIteration`;
- close is idempotent;
- context exit closes;
- concurrent next/helper/close calls reject;
- `report` is `None` before termination;
- after termination, it returns the same immutable object permanently;
- stream exceptions carry that report where applicable.

Canonical use remains context-managed because `break` does not automatically close an arbitrary async iterator.

### 15.2 Terminal states

```python
class TerminalState(StrEnum):
    COMPLETED = "completed"
    COMPLETED_WITH_FAILURES = "completed_with_failures"
    EARLY_CLOSED = "early_closed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    INCOMPLETE = "incomplete"
```

- `COMPLETED`: natural exhaustion and success proof.
- `COMPLETED_WITH_FAILURES`: tolerant enumeration exhausted with negative outcomes.
- `EARLY_CLOSED`: explicit bounded stop before observed exhaustion.
- `CANCELLED`: external cancellation or interrupt.
- `FAILED`: execution, source, protocol or cleanup failure.
- `INCOMPLETE`: traversal lacks required terminal proof.

Cleanup failure overrides apparent completion. Finalization occurs after cleanup.

### 15.3 Report

```python
@dataclass(frozen=True, slots=True)
class OperationReport:
    state: TerminalState
    operation: str
    terminal_reason: str

    admitted: int = 0
    emitted: int = 0
    successes: int = 0
    failures: int = 0
    not_executed: int = 0
    unknown: int = 0
    unique_rows: int = 0

    physical_requests: int = 0
    logical_pages: int = 0
    batch_requests: int = 0
    batch_commands: int = 0
    retries: int = 0
    cooldown_seconds: float = 0.0

    buffered_commands_high_water: int = 0
    buffered_rows_high_water: int = 0
    active_references_high_water: int = 0

    violations: tuple[Violation, ...] = ()
```

Reports contain bounded redacted counters and evidence only. They exclude request values, results and correlation.

Properties:

- `successful` only for `COMPLETED`;
- `exhausted` for both completed states;
- `partial` for all other states.

### 15.4 Partial helpers

```python
@dataclass(frozen=True, slots=True)
class PartialResult(Generic[T]):
    value: T
    report: OperationReport
```

`first()` returns a zero-or-one tuple. `collect(limit=N)` requires a positive integer.

Both helpers own and close the stream, preserve exceptions and report completion only when exhaustion was observed. They do not pull an extra item solely to prove exhaustion.

### 15.5 Partition helper

Provide a pure finite-iterable helper that partitions command or terminal reference outcomes into explicit variant buckets. It must not consume async streams or discard unknown/not-executed states.

## 16. Error contract

Public errors include:

- `B24ApiError`;
- `TransportError`;
- `HTTPGatewayError`;
- `ProtocolError`;
- `ApiResponseError`;
- `BatchCommandError`;
- `CapabilityError`;
- `PaginationError`;
- `BudgetExceededError`;
- `ResponseTooLargeError`;
- `AmbiguousExecutionError`;
- `IncompleteTraversalError`;
- `InputSourceError`;
- `BatchFailed[C]`;
- `ReferenceFailed[C]`.

Every public error:

- has bounded redacted text and `to_safe_dict()`;
- excludes correlation and request values from safe serialization;
- carries `RequestSummary` where applicable;
- carries the final report for terminated streams where applicable;
- preserves the primary exception and chains cleanup failures.

Direct ambiguous calls raise `AmbiguousExecutionError`. Batch/reference operations use correlated unknown variants when possible.

## 17. Console command

### 17.1 Packaging and boundary

```toml
[project.scripts]
b24api = "b24api.cli:main"
```

Use `argparse`. CLI responsibilities are argument parsing, settings composition, public contract construction, public API invocation, JSONL serialization and exit mapping.

CLI must not import private execution, pagination, batch, reference or transport-driver modules.

Credentials come only from `Settings`; no webhook argument exists.

### 17.2 Commands

```text
b24api call METHOD [--params JSON|@FILE|-] [--raw]
b24api list METHOD [--params JSON|@FILE|-]
b24api list METHOD --strategy sequential [--contract @FILE] [--params ...]
b24api list METHOD --strategy counted --contract @FILE [--params ...]
b24api list METHOD --strategy keyset --contract @FILE [--params ...]
b24api list METHOD --strategy cursor --contract @FILE [--params ...]
```

`--raw` invokes `call_response()` and does not restore the Python raw boolean.

Parameter JSON must be exactly one object. Duplicate keys, arrays, scalars and trailing data reject locally.

### 17.3 Contract JSON v1

Contracts are closed and require `version: 1`.

Common fields:

```json
{
  "version": 1,
  "selector": ["items"],
  "page_size": 50,
  "limit_path": ["LIMIT"],
  "allow_create_controls": true
}
```

Offset:

```json
{
  "offset": {
    "parameter_path": ["start"]
  }
}
```

Identity:

```json
{
  "identity": {
    "item_path": ["id"],
    "filter_key": "ID",
    "order_key": "id",
    "coercion": "decimal_string_integer"
  }
}
```

Keyset:

```json
{
  "keyset": {
    "filter_path": ["filter"],
    "order_path": ["order"],
    "start_suppression_path": ["start"],
    "direction": "ascending",
    "terminal": "empty_confirmation"
  }
}
```

Cursor:

```json
{
  "cursor": {
    "parameter_path": ["LAST_ID"],
    "item_path": ["id"],
    "coercion": "decimal_string_integer",
    "direction": "ascending",
    "take": "last",
    "terminal": "empty_confirmation"
  }
}
```

Applicability:

- sequential: common, offset and identity optional;
- counted: identity required, offset optional;
- keyset: selector, identity and keyset required;
- cursor: selector and cursor required, identity optional.

Forbidden strategy fields reject. Paths contain non-empty strings or non-negative integers. Contracts contain no method, host, credentials, entity names or business IDs.

### 17.4 Output and exits

Stdout contains data only:

- call: one compact JSON value;
- raw call: one compact response object;
- list: one compact JSON value per row.

Stderr uses compact safe report/error objects. A final list report is emitted once.

A broken pipe stops work, closes the stream and exits 4.

| Code | Meaning |
|---:|---|
| 0 | Successful declared terminal state |
| 2 | Usage or local JSON/contract error |
| 3 | Configuration unavailable |
| 4 | Remote, protocol, correctness, incomplete or output failure |
| 130 | Cancellation or interrupt |

## 18. Legacy removal and exports

Candidate removals include:

- all `list_*` and `reference_*` compatibility wrappers;
- `_legacy_*` and `_ImplicitCompatibilityString`;
- shape-changing `raw`, `retry`, `with_payload`, `list_method`, `fallback_failed` and `tolerant` bridges;
- tuple payload conventions;
- compatibility Pydantic entities;
- obsolete `helper.py`, `api.py`, `type.py`;
- endpoint profile authoring, probing, selection and packaged registry;
- dead plan/enum variants;
- partitioned production scaffolding;
- proven compatibility-only dependencies.

For each symbol:

1. characterize the capability;
2. enter it in the preservation ledger;
3. add the README mapping;
4. migrate internal use;
5. remove it;
6. verify exports, contents and reachability.

Root exports contain only the client/settings, public contracts, public errors and pure helpers. Internal engines, plans, schedulers, codecs and evidence types are not root exports.

## 19. Module boundaries

```text
b24api/
  client.py
  cli.py
  settings.py
  errors.py
  redaction.py
  contracts/
  execution/
  transport/
  batch/
  traversal/
  references/
```

Constraints:

- client is composition/delegation only;
- CLI imports public surfaces only;
- contracts perform no I/O;
- transport does not import batch/traversal/references;
- runtime never imports evidence tools;
- no mutable global registry exists;
- no concrete Tasks/CRM/IM catalog exists in production;
- the universal batch endpoint is permitted;
- movement is separate from semantic change;
- ordinary modules should remain below 400 lines and state machines below 700 unless justified.

## 20. Capability characterization

Required archetypes:

1. Direct decoded and response calls.
2. Safe, unsafe and unknown replay, traced to `B2`.
3. Tolerant batch/reference states, traced to `B1`.
4. Counted overlap and observed stride, traced to `B5`.
5. Page-control conflict, traced to `B3`.
6. Async reference source, correlation and selector, traced to `B4`.
7. Separate keyset identity roles, traced to `C1b`.
8. Repeated cursor boundary refusal, traced to `C5`.
9. Ignored scalar business filter non-overclaim, traced to `C34`.
10. Overmatched multifield non-overclaim, traced to `C35`.
11. Sync and async generator closure.
12. 100,000-command logical batch.
13. Composite parent correlation.
14. Empty reference completion.
15. Ready/input delivery.
16. Early close, client close, cancellation and cleanup failure.
17. Response-byte ceiling.
18. Final report immutability.

No production endpoint registry may be encoded by these tests.

## 21. Performance and memory regression

Reuse `tools/b24api_evidence/profile_runtime.py`.

Required cases include direct calls, 100,000 commands with buffer 7, counted `19/500/10,000`, sparse cases, representative references, input-order backpressure, lifecycle leaks, oversized response, Memray and repeated traversal.

Record identity equality, request counts, wall/CPU, first-row time, high-water counters, response bytes, allocation sites and retained resources.

Blocking invariants:

- counted request shape is `1/2/5`;
- exact identities match;
- policy high-water ceilings hold;
- memory does not scale with total logical input;
- 100,000-command peak is no more than 10,000-command peak plus the greater of 25% or 32 MiB;
- no orphan work remains;
- repeated traversal grows no more than 1 MiB per 100 iterations after warm-up;
- transport stores no more than the response ceiling plus one chunk;
- correlation never enters wire data.

Stable performance regression requires two discarded warm-ups, seven measurements, two independent reproductions and unchanged request counts. Stable regressions over 10% block completion.

## 22. Optional live portal

Read-only use through `BITRIX24_API_WEBHOOK_URL` is permitted. The literal credential must never enter artifacts or history.

Allowed use includes read-only calls, traversals, comparisons and CLI smoke tests. Writes require a separate plan and explicit authorization. Live evidence supplements deterministic evidence only.

## 23. Existing no-count decision

Generic exact no-count remains sequential keyset with separate identity roles, strict progress and empty confirmation.

Unsafe historical no-count behavior is not preserved. Adaptive and partitioned variants remain rejected or deferred and refuse before I/O.

## 24. README deliverable

README must cover:

- fixes versus application responsibility;
- settings, lifecycle and replay safety;
- direct calls;
- logical/tolerant batch;
- off-wire correlation;
- all four list strategies;
- cursor uniqueness limitation, including the `C5` class;
- references and empty completion;
- partial helpers and terminal reports;
- ambiguous writes;
- response/memory ceilings;
- CLI;
- limitations for ignored filters, including `C34`/`C35`;
- v1-to-v2 map;
- evidence reproduction.

Every example executes in tests.

## 25. CLI tests

Cover wheel entry point, parameter sources, duplicate/trailing JSON, raw output, all list strategies, selector semantics, invalid contracts, control collisions, configuration failure, incomplete traversal, Ctrl-C, broken pipe, stdout/stderr separation, exactly-once reports and secret/correlation redaction.

No batch/reference CLI is implemented.

## 26. Implementation decomposition

1. Freeze exports, request shapes, both capability matrices, issue traceability and baselines.
2. Add transport response-byte budgeting.
3. Add `call_response`; remove raw/retry booleans.
4. Introduce command correlation and four-state outcomes.
5. Make logical batch canonical and characterize source ownership.
6. Introduce final `OperationStream`, reports and helpers.
7. Add sequential `iter_list`.
8. Add counted traversal with observed-head stride and required identity.
9. Add keyset/cursor traversal.
10. Normalize fan-out.
11. Add path bindings and reference outcomes.
12. Remove profiles, compatibility and dead dependencies.
13. Split modules mechanically.
14. Add CLI and wheel tests.
15. Run regression and memory evidence.
16. Rewrite README.
17. Run full gates.
18. Produce the final capability report with:
    - the five-way matrix;
    - the detailed preservation ledger;
    - issue-ID results;
    - explicit `missing` and `regressed` entries, even when both are empty.

No task mixes a public semantic change, state-machine rewrite and broad movement.

## 27. Acceptance criteria

### 27.1 Business gates

All five rows of the five-way matrix must be complete.

### 27.2 Traceability gates

- All nine sampled issue IDs appear in the issue matrix and final report.
- Every ID has a named executable test.
- `C5`, `C34` and `C35` are not falsely claimed fixed.
- Missing and regressed columns are explicit, not inferred from prose.
- Any final `missing` or `regressed` capability blocks acceptance.

### 27.3 Contract gates

- Stable Python return types.
- Required counted identity.
- Closed tolerant unions.
- Bounded fail-fast exceptions.
- Immutable post-cleanup report.
- Exact ownership behavior.
- Bounded response reading.
- Exact root exports.

### 27.4 Regression gates

- Static and full tests pass.
- Existing correctness tests migrate rather than disappear.
- Counted parity and identity equality pass.
- Keyset remains exact.
- Logical-batch memory plateaus.
- No orphan or accumulating work remains.
- Stable performance regressions are reviewed.
- Wheel contents are correct.
- README examples execute.
- Production runtime contains no method/entity catalog.

## 28. Operational and rollback decisions

This iteration does not publish.

A later release uses a new major version. Migration is documentation-led. Rollback installs the frozen prior version. No compatibility feature flag or dual-surface package is produced.

## 29. Rejected and deferred alternatives

Rejected:

- compatibility surfaces;
- counted as default;
- optional counted identity;
- shape-changing Python booleans;
- tuple payloads;
- endpoint registries;
- automatic profiles;
- method-name safety inference;
- early close as complete;
- unbounded failure history;
- post-buffer response limits;
- recursive binding merge;
- endpoint-specific CLI flags;
- CLI framework dependency;
- batch/reference CLI;
- corpus migration;
- reopening partition research;
- permissive generic handling of repeated cursor boundaries;
- weakening correctness for speed.

Deferred:

- partitioned/adaptive no-count;
- application method-contract package;
- recipe migration;
- streaming batch CLI;
- sync facade;
- release/publication.

## 30. Risks and controls

| Risk | Control |
|---|---|
| Capability silently disappears | Detailed ledger with `Missing/regressed?` |
| Historical failure lacks evidence | Issue-ID matrix and named tests |
| Endpoint issue leaks into runtime registry | Synthetic fixtures; IDs only in tests/docs |
| False claim on ignored filters | `C34`/`C35` explicitly application-owned |
| Repeated cursor is silently deduplicated | `C5` fail-closed regression |
| Counted overlap returns | Observed head stride plus identity/total validation |
| Infinite input grows memory | Bounded admission and exceptions |
| Response bypasses row budget | Streaming byte ceiling |
| Correlation leaks | Wire and safe-serialization snapshots |
| `break` leaks work | Managed streams and helpers |
| Client closes before streams | Client closes streams first |
| Input order buffers indefinitely | Backpressure ceilings |
| Refactor changes state machines | Characterize first; move separately |
| Benchmark noise blocks work | Numeric stability protocol |

## 31. Decision ledger

| ID | Decision | Status | Rationale |
|---|---|---|---|
| D001 | Preserve the correctness kernel and refactor its public boundary | adopted | Existing reliability is the main asset |
| D002 | Backward compatibility is not required | adopted | One clear major-version API is preferred |
| D003 | Keep `Bitrix24` as the client class name | adopted | Clear name; renaming adds no value |
| D004 | Separate `call` and `call_response` | adopted | Stable Python return types |
| D005 | Define batch as a logical finite-or-unbounded source over physical chunks | adopted | Matches workloads while retaining bounded admission |
| D006 | Use typed opaque `Command[C]` and `Binding[C]` correlation | adopted | Preserves caller context without endpoint knowledge |
| D007 | Make `iter_list` the basic sequential operation | adopted | Basic name denotes least specialized strategy |
| D008 | Name optimized variants explicitly | adopted | Preconditions remain visible |
| D009 | Keep the client method/entity agnostic | adopted | Maintains a reusable boundary |
| D010 | Do not ship a concrete method registry | adopted | Concrete truth belongs to applications |
| D011 | Do not edit or migrate skills and recipes | adopted | Scope is the client and documentation |
| D012 | Use recipes/issues only as selectively sampled requirements | adopted | Broad insight without infeasible review |
| D013 | Remove legacy after characterization, not external migration | adopted | Compatibility is out of scope |
| D014 | Reuse the established no-count decision | adopted | Partition research is already rejected |
| D015 | Repeat profiling proportionally | adopted | Detect regression without repeating research |
| D016 | Install a stdlib-based CLI | adopted | Useful shell access with minimal dependency cost |
| D017 | CLI minimum is call/response and sequential list | adopted | Covers primary shell use |
| D018 | Advanced CLI uses a closed contract JSON | adopted | Avoids flag explosion |
| D019 | Exclude batch/fan-out CLI now | adopted | Requires a separate streaming protocol |
| D020 | Keep Python raw/tolerant/payload shape booleans | rejected | They destabilize return types |
| D021 | Make counted traversal the default | rejected | Exact totals are not universal |
| D022 | Infer endpoint behavior from method spelling | rejected | Hidden inference is unsafe |
| D023 | Preserve unsafe generic no-count | rejected | It silently omitted data |
| D024 | Add concrete CRM/Tasks/IM rules to runtime | rejected | Business knowledge belongs outside |
| D025 | Review or migrate every recipe | rejected | Not required or feasible |
| D026 | Add sync facade | deferred | Separate ergonomic decision |
| D027 | Add partitioned keyset | deferred | Needs separate admission |
| D028 | Add batch/fan-out CLI | deferred | Separate protocol work |
| D029 | Use the webhook only for read-only regression | adopted | Realistic smoke without stored credentials |
| D030 | Use public mechanics types instead of internal plans | adopted | Avoids plan archaeology |
| D031 | Require identity for counted traversal | adopted | Exact overlap detection needs identity |
| D032 | Input-order batch and explicit fan-out/reference order | adopted | Deterministic batch with concurrent flexibility |
| D033 | Publish one immutable report after cleanup | adopted | Avoids contradictory terminal evidence |
| D034 | Represent empty reference success explicitly | adopted | Distinguishes empty from non-executed |
| D035 | Complete every successful binding explicitly | adopted | Unambiguous per-binding terminal state |
| D036 | Bound failure exceptions to active windows | adopted | Preserves unbounded-source memory safety |
| D037 | Suppress partial-chunk dispatch after source failure | adopted | Prevents unintended work |
| D038 | Add transport-enforced response ceiling | adopted | Row budgets do not bound encoded memory |
| D039 | Use exact path updates for bindings | adopted | Avoids merge ambiguity |
| D040 | Return zero-or-one tuple from `first()` | adopted | `None` is valid JSON |
| D041 | Select relative to `Response.result` | adopted | Matches response semantics |
| D042 | Client closes streams before owned transport | adopted | Prevents resource inversion |
| D043 | Keep filter reconciliation application-owned | adopted | Generic mechanics cannot prove business sets |
| D044 | Retain endpoint profiles | rejected | Conflicts with explicit thin boundary |
| D045 | Retain historical outcomes in failure exceptions | rejected | Violates boundedness |
| D046 | Use recursive binding merge | rejected | Ambiguous lists/casing/controls |
| D047 | Publish a dual-surface transition | rejected | Major rollback is simpler |
| D048 | Derive counted tail stride from observed head width | adopted | Directly prevents the `B5` overlap mechanism |
| D049 | Maintain an explicit five-way and detailed capability matrix | adopted | Makes missing or regressed capabilities reviewable |
| D050 | Trace the bounded issue sample by identifier and named test | adopted | Closes evidence traceability without adding endpoint runtime knowledge |
| D051 | Treat `C5` repeated-boundary cursor behavior as fail-closed | adopted | Generic dedup cannot prove completeness |
| D052 | Treat `C34` and `C35` as application-semantic failures | adopted | The thin client must not overclaim filter correctness |
| D053 | Block acceptance on any final missing or regressed matrix entry | adopted | Prevents narrative sign-off with capability gaps |

## 32. Final council amendments after round 3

This section is normative and overrides any conflicting earlier shorthand. It incorporates the bounded residual findings from the third and final review round; it does not start a fourth review iteration.

### 32.1 Exact sequential state machine and assurance

`iter_list` uses this exact continuation algorithm:

1. The first request uses the request's existing offset when present, otherwise zero.
2. A present `Response.next` is authoritative only after it is validated as an integer that strictly advances beyond the current offset and has not appeared before.
3. When `next` is present, the observed `current_offset + selected_row_count` is corroborating evidence, not a universal equality rule: Bitrix may filter rows before returning them. A concrete caller contract may strengthen this to equality, but the basic sequential operation does not invent that endpoint claim.
4. When `next` is absent and the page is non-empty, advance by the observed selected-row count.
5. When `next` is absent and the page is empty, empty confirmation is the successful terminal evidence.
6. Zero advancement, repeated offsets, malformed `next`, a selected-row count larger than the declared local page cap, or a strengthened-contract contradiction raises `PaginationError` and finalizes as `INCOMPLETE`.

Identity is optional only for the conservative default's mechanics-only assurance. Add:

```python
class TraversalAssurance(StrEnum):
    MECHANICS_ONLY = "mechanics_only"
    IDENTITY_EXACT = "identity_exact"
```

`OperationReport.assurance` records it. Without `IdentitySpec`, successful empty-confirmed exhaustion is `COMPLETED` with `MECHANICS_ONLY`; it is not described as an exact identity proof, and the CLI emits one stderr warning. With identity, duplicate/substitution checks apply and assurance is `IDENTITY_EXACT`. The final capability report and README must preserve this distinction.

### 32.2 Terminal state to exception matrix

| Final state | Python iteration/helper behavior |
|---|---|
| `COMPLETED` | ordinary `StopAsyncIteration` after cleanup; helpers return normally |
| `COMPLETED_WITH_FAILURES` | tolerant stream exhausts normally with every negative state already emitted; report remains non-successful |
| `EARLY_CLOSED` | explicit close/helper limit returns typed `PartialResult`; a later pull raises `RuntimeError` |
| `CANCELLED` | raises the original `asyncio.CancelledError` with cancellation count/payload preserved where the runtime permits attachment |
| `FAILED` | raises the typed primary `B24ApiError` or source error |
| `INCOMPLETE` | raises `IncompleteTraversalError`; it never becomes ordinary exhaustion |

Every raised terminal error exposes the same frozen final `OperationReport` object. Cleanup failure is recorded as a blocking violation; it replaces apparent success but does not replace an already-failing primary exception.

`aclose()` is exempt from the concurrent-pull rejection rule. It is always permitted, cancels or awaits an in-flight pull, performs owned cleanup and preserves the primary cancellation/error. Concurrent `__anext__`/helper calls still reject. Add the in-flight `__anext__` plus cancellation plus `aclose()` regression.

The client stream registry uses weak references and deregisters on termination. A long-lived-client create/consume/close loop must show no retained-registry growth.

### 32.3 Exact counted traversal

For `iter_list_counted`:

1. The head response must provide an exact non-negative integer total.
2. `total < selected_head_count` is contradictory and fails closed.
3. If `total == selected_head_count`, the head is complete.
4. If `total > selected_head_count`, a valid positive `Response.next` is mandatory and is the in-band stride.
5. `Response.next` must equal the selected head count for the fixed-offset public strategy; absence or disagreement fails before tail scheduling.
6. Tail offsets are precomputed from that validated stride. Every non-final page must contain exactly the stride count and the final page exactly the remaining count. Any width disagreement cancels/awaits the remaining tail, emits no rows from the contradictory page and raises `IncompleteTraversalError`.
7. Exact identity uniqueness, range ownership and final emitted total are mandatory.
8. `batch_size=None` resolves to `min(50, policy.max_buffered_commands)`; explicit values are `1..50` and narrowed by policy.

`page_size` is a local expected/buffer cap unless `offset.limit_path` exists. With a limit path it is also injected as a wire control. Without a limit path the client must not imply that it controls the portal: if the observed head exceeds the cap, fail locally as soon as observed. This closes the silent-ignore direction of `B3`; the README and CLI use the term `page-cap` for this argument.

The B5 fixture must cover `page_cap=2` against a server head/next width of 50 and prove fail-closed behavior rather than overlap or 90-row inflation. Another fixture covers `total > head_count` with missing `next`.

### 32.4 Identity tracking and large counted selections

Tracker rules are explicit:

- strict keyset/cursor uses `MONOTONIC` O(1) boundary tracking;
- sequential/counted identity-exact traversal uses `MEMORY` up to `max_tracked_identities`;
- `SQLITE` is retained as the bounded exact spill strategy for offset/count traversal above the memory identity ceiling and is deterministically closed/removed;
- `MONOTONIC` is not used for offset/count because it cannot detect arbitrary replacement/overlap.

When a known counted total exceeds the selected tracker's capacity, fail before tail scheduling with `BudgetExceededError` naming the remedies: increase the explicit ceiling or select `IdentityTracker.SQLITE`. Add an exact >100,000-row counted characterization with SQLite and a default-MEMORY refusal companion. The capability ledger records the default ceiling as a documented operating boundary, not as silently unlimited behavior.

### 32.5 Cursor capability boundary

The accepted generic cursor remains strict and fail-closed. A repeated non-unique date boundary cannot generically prove that all tied rows were returned, even with identity deduplication; therefore the council does not adopt a speculative tie-group rule in this refactor.

The detailed matrix must mark the old permissive repeated-boundary chat behavior as **intentionally removed as unsafe / available only through explicit application direct-call reconciliation**, not as preserved and not as a generic fix. Strict unique cursor traversal remains preserved. This is a known capability boundary disclosed in README, not hidden by a green test.

### 32.6 Transport and response ceiling

The injected transport contract exposes a redacted portal identity:

```python
class Transport(Protocol):
    @property
    def host(self) -> str: ...

    async def send(
        self,
        request: Request,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse: ...
```

Client construction rejects an injected transport whose normalized `host` differs from `Settings` before I/O. Credentials remain transport-owned and are never exposed by the protocol.

The byte ceiling counts decompressed response-body bytes presented to the JSON decoder. Exceeding it aborts the stream and raises `ResponseTooLargeError`; truncated bytes are never returned as `WireResponse`. The implementation may retain at most one transport chunk beyond the ceiling while detecting overflow. For an unsafe/unknown request after possible dispatch, the public outcome is ambiguous (`AmbiguousExecutionError` / `CommandOutcomeUnknown`), not a conclusive response-size failure.

The 16 MiB default is retained as a configurable initial ceiling because it is more than twice the largest currently recorded full live allocation (7.98 MiB). A focused characterization must validate it against existing fixtures; changing the default requires an amendment rather than silent tuning.

### 32.7 Dispatch ownership

Replace invalid-combination `DispatchSpec` with a discriminated union:

```python
@dataclass(frozen=True, slots=True)
class DirectDispatch:
    concurrency: int = 10
    output_order: DeliveryOrder = DeliveryOrder.READY

@dataclass(frozen=True, slots=True)
class BatchDispatch:
    batch_size: int = 50
    concurrency: int = 1
    output_order: DeliveryOrder = DeliveryOrder.READY

type DispatchSpec = DirectDispatch | BatchDispatch
```

`BatchDispatch.concurrency` is the maximum number of independent physical batch requests in flight, so fan-out is not an alias of ordered `batch()`. Policy narrowing is `min(dispatch value, corresponding policy ceiling)`. For reference counted traversal, `BatchDispatch` is the single owner of physical batch size; `CountedTraversal` carries no competing batch-size field. A direct dispatch with counted tail batching is invalid before binding I/O.

`NotExecutedReason` is exactly `HALTED`, `SOURCE_FAILED`, `LOCAL_VALIDATION_FAILED`, `SCHEDULER_STOPPED`; extensions require a spec amendment. Per-binding `INCOMPLETE` maps to `ReferenceFailure` carrying `IncompleteTraversalError`, not `ReferenceOutcomeUnknown`. `Binding.summary` is redacted UTF-8 text capped at 256 code points.

### 32.8 CLI contract routing and safety

CLI JSON uses exact `StrEnum.value` strings:

- identity coercion: `exact_string`, `exact_integer`, `decimal_string_integer`;
- direction: `ascending`, `descending`;
- cursor take: `first`, `last`.

Remove the orphan `terminal` key: empty confirmation is fixed by the public keyset/cursor methods and is not CLI-configurable. `limit_path` and `allow_create_controls` live only inside the selected `offset`, `keyset` or `cursor` object; duplicate top-level copies reject. A routing table in code/tests maps every JSON field one-to-one to the same public Python mechanics dataclass, and a round-trip equality test is mandatory.

Add `--replay-safety safe|unsafe|unknown` to `call`, default `unknown`. Safe is an explicit operator assertion; unsafe/unknown are never automatically replayed after possible dispatch. Diagnostic redaction applies to stderr only; successful stdout payload is intentionally unredacted application data and may contain sensitive values.

Broken output pipe uses exit 5 (`output consumer closed before complete delivery`) after deterministic closure, not remote-failure exit 4 and not success.

### 32.9 Capability dispositions and bounded migration examples

The five required disposition values are:

1. `preserved directly`;
2. `preserved under a different primitive`;
3. `intentionally removed as unsafe`;
4. `missing/regressed`;
5. `available but too difficult to discover/use safely`.

The final capability report starts the disposition/evidence columns unresolved and fills them from tests; this design document's entries are targets, not pre-approved results. Any final `missing/regressed` blocks acceptance. Low-level current plan/profile composition is classified as “available but too difficult”; its replacement is the typed list/reference surface.

Bounded before/after archetypes (analysis only; no recipe edits):

```python
# direct response
await client.call(request, raw=True)
await client.call_response(request)

# off-wire batch payload
client.batch((request, payload) for request, payload in source, with_payload=True)
client.batch(Command(request, correlation=payload) for request, payload in source)

# counted list
client.list_batched(request, identity=identity)
client.iter_list_counted(request, identity=identity)

# no-count identity casing
client.list_batched_no_count(request, id_key="ID")
client.iter_list_keyset(request, identity=IdentitySpec(item_path=("id",), filter_key="ID", order_key="id"))

# bounded partial consumption
break  # implicit partial result and uncertain cleanup
await client.iter_list(request).collect(limit=100)
```

These snippets demonstrate migration shape only; the skill/recipe corpus remains untouched.

Additional capability dispositions:

- mapping-shaped result collections are preserved by an explicit `ResultCollectionShape.SEQUENCE | MAPPING_VALUES` supplied beside `ResultSelector`; mapping values retain source order and keys are not emitted unless the caller selects them;
- `retry=False` maps to an operation policy with `max_attempts_per_request=1`;
- automatic safe-direct fallback for selected failed batch commands is intentionally removed from the canonical tolerant operation; callers inspect outcomes and explicitly issue a new direct command only when replay safety and business intent permit;
- arbitrary low-level plan composition is intentionally removed from root API as too difficult/unsafe; concrete irregular direct-call workflows remain possible through calls, while adding a new generic traversal requires an amendment.

### 32.10 Approaches, current-state answers and data flow

Approaches considered:

1. Keep compatibility facade and update docs — rejected because it preserves two surfaces and hidden endpoint assumptions.
2. Expose only low-level plans — rejected because ordinary users must reconstruct unsafe mechanics.
3. Adopt one thin typed public surface over the proven kernel — adopted.

Current-state factual answers:

- The kernel already closes the sampled mechanical silent-failure/replay/cancellation classes; the public boundary and discoverability remain the refactor target.
- Useful direct/raw/batch/generator/count/keyset/cursor/reference primitives exist, but several are hidden behind compatibility names or low-level plans.
- Correlation exists on selected paths; the new closed unions make it structural across every correlated terminal state.
- Compatibility wrappers, profiles, duplicate entities and oversized mixed-responsibility modules are real legacy candidates.
- Counted request parity and prior memory evidence already exist; this iteration only checks that refactoring preserves them.

Data flow:

```text
Python caller or CLI
  -> immutable Request / Command / Binding / mechanics contract
  -> Bitrix24 composition facade
  -> shared Executor (policy, retry, budgets, reports)
  -> batch/traversal/reference scheduler
  -> Transport
  -> validated immutable Response / typed outcome
  -> OperationStream + one final OperationReport
```

### 32.11 Exact module/export decisions

`OperationStream` is a public `Protocol` in `contracts/stream.py`; subsystem streams implement it without inheritance-heavy shared state. Shared immutable lifecycle/report coordination may be extracted internally only after characterization proves identical behavior.

```text
b24api/
  client.py
  cli.py
  settings.py
  errors.py
  redaction.py
  contracts/
    json.py request.py response.py policy.py report.py stream.py
    command.py traversal.py reference.py dispatch.py
  execution/
    context.py executor.py retry.py rate.py
  transport/
    base.py httpx.py protocol.py
  batch/
    engine.py stream.py
  traversal/
    sequential.py counted.py keyset.py cursor.py identity.py stream.py
  references/
    scheduler.py stream.py
```

`TerminalState`, `TraversalAssurance` and report counters live in `contracts/report.py`; `NotExecutedReason` in `contracts/command.py`; `DeliveryOrder` and dispatch types in `contracts/dispatch.py`; `IdentityTracker` and mechanics values in `contracts/traversal.py`.

The exact root export manifest is frozen by a snapshot test before deletion and contains only: `Bitrix24`, `Settings`, `Request`, `Response`, `Command`, `Binding`, `ResultSelector`, `ResultCollectionShape`, `ParameterPath`, `ParameterUpdate`, `OffsetSpec`, `IdentitySpec`, `KeysetSpec`, `CursorSpec`, `SequentialTraversal`, `CountedTraversal`, `KeysetTraversal`, `CursorTraversal`, `DirectDispatch`, `BatchDispatch`, `DeliveryOrder`, `ReplaySafety`, `ExecutionPolicy`, `IdentityTracker`, `OperationStream`, `OperationReport`, `PartialResult`, `TerminalState`, `TraversalAssurance`, command/reference outcome classes, pure outcome partition helpers, and the public error hierarchy named in Section 16. No engine, scheduler, internal plan, profile or evidence type is a root export.

### 32.12 Performance comparator and final cases

Refactor overhead is compared against the exact pre-refactor HEAD frozen at implementation start. Frozen 1.0.1 is used only for structural request/identity parity where already applicable. The runner uses seven measured repetitions after two warm-ups; a >10% median regression is blocking only when an independent repeat reproduces it. Memory gates remain the numeric plateau/retention gates already specified.

Add these mandatory cases to the closed characterization list:

- counted total >100,000: default MEMORY refusal and SQLITE exact success;
- counted total > head rows with missing `next`;
- `page_cap` with no wire `limit_path` and oversized observed head;
- sequential `next` advancement/repetition and identity-less mechanics-only warning;
- `aclose()` during in-flight `__anext__` under cancellation;
- injected transport host mismatch;
- per-binding `INCOMPLETE` mapping;
- CLI JSON-to-Python-contract round trip;
- safe stdout payload containing token-like data remains data while stderr stays redacted.

### 32.13 Decision ledger additions

| ID | Decision | Status | Rationale |
|---|---|---|---|
| D054 | Separate mechanics-only from identity-exact traversal assurance | adopted | Basic sequential remains usable without overclaiming exact identity proof |
| D055 | Bind `INCOMPLETE` to `IncompleteTraversalError` | adopted | Incomplete work cannot look like normal exhaustion |
| D056 | Retain SQLite as bounded exact identity spill | adopted | Preserves large counted capability without unbounded memory |
| D057 | Validate counted stride from in-band `next` and observed head | adopted | Preserves established B5 guard |
| D058 | Keep repeated-boundary cursor outside generic admission | adopted | Identity dedup alone cannot prove all tied rows were returned |
| D059 | Require injected transport host identity to match settings | adopted | Prevents diagnostic/portal divergence |
| D060 | Use discriminated direct/batch dispatch contracts | adopted | Removes invalid field combinations and one-owner ambiguity |
| D061 | Permit cancellation-safe concurrent `aclose()` | adopted | Cleanup must not mask cancellation |
| D062 | Use exit 5 for incomplete output delivery | adopted | Broken pipe is neither portal failure nor complete success |
| D063 | Compare refactor performance to frozen pre-refactor HEAD | adopted | Isolates refactor regression from 1.0.1 architectural cost |

## 33. Open questions and amendment triggers

No implementer may silently choose a different public behavior. The following are deliberate future amendment triggers rather than implementation TODOs:

- a complete generic repeated-boundary/date-cursor proof;
- a partitioned/adaptive no-count candidate;
- a JSONL batch/fan-out CLI protocol;
- a concrete application-owned method-contract package;
- migration of the external recipe corpus.

## 34. Final business outcome

The accepted result is:

- stricter than 1.0.1 about incomplete and ambiguous work;
- traced to concrete historical issue identifiers without embedding endpoint rules;
- complete against an explicit five-way and detailed capability matrix;
- free of missing or regressed accepted capabilities;
- capable across direct, response, batch, generator, list, cursor, fan-out, reference and correlation workflows;
- conservative for repeated cursors and ignored business filters;
- bounded across input, active work, decoded rows, exceptions and response bytes;
- deterministic in ownership and reporting;
- free of compatibility and endpoint-profile scaffolding;
- installed with a compact JSONL CLI;
- supported by focused request, performance and memory evidence;
- documented so users can choose an operation without reading internal state machines.
