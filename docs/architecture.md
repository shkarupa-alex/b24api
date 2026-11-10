# Architecture and correctness guarantees

`b24api` is a method-agnostic asynchronous transport client. Its runtime is split into contracts,
transport, execution, batching, traversal, reference scheduling and a thin public facade. The
client does not contain endpoint catalogs, persistence, business reconciliation or recipe logic.

## Public capability surface

| Capability | Public operation | Guarantee |
|---|---|---|
| Decoded direct request | `call()` | Detached decoded JSON with explicit replay policy. |
| Response envelope | `call_response()` | Immutable result, pagination, timing and bounded evidence. |
| Binary response | `call_bytes()` | Every bounded successful body byte returned without JSON sniffing. |
| Arbitrary logical batch | `batch()` / `batch_outcomes()` | Lazy input, bounded physical batches, off-wire correlation and typed outcomes. |
| Independent command fan-out | `fan_out()` / `fan_out_outcomes()` | Explicit dispatch mode, concurrency and delivery order. |
| Sequential offset list | `iter_list()` | Server-continuation validation and terminal empty-page confirmation. |
| Counted list | `iter_list_counted()` | Direct head, batched tail and exact total/range/identity validation. |
| No-count keyset | `iter_list_keyset()` | Deterministic auto default plus explicit sequential, range, or partitioned execution for caller-asserted stable integer keysets. |
| Dependent cursor | `iter_list_cursor()` | Strict unique monotonic cursor progress. |
| Response-driven continuation | `iter_list_continuation()` | Pure caller rule over declared controls, cycle detection and a rule-declared end. |
| Inclusive timestamp window | `iter_list_window()` | Classified paging rows, strict boundary progress and overlap delivered once. |
| Correlated cursor set | `iter_cursors()` | Lazy one/many-parent cursor traversal with isolated seeds and shared batching. |
| Keyset qualification | `verify_keyset_capability()` | Explicit fail-closed strict-bound report for one portal and request shape. |
| Per-parent traversal | `iter_references()` / `iter_reference_outcomes()` | Isolated traversal state and correlation for every accepted binding. |
| Heterogeneous references | `iter_reference_sources()` / `iter_reference_source_outcomes()` | Per-source request, traversal and state over one shared dispatcher, row pool and report. |
| Shell access | `b24api call` / `b24api list` | JSON/JSONL stdout and diagnostics on stderr. |

## Failure ownership

The client fails closed when transport or pagination evidence cannot prove the requested mechanical
result. Contradictory continuation, missing counted ranges, duplicates, total mismatch, response
size overflow and unsafe ambiguous replay never become ordinary success.

The application still owns facts that a generic REST client cannot infer:

- whether Bitrix honored the business meaning of a filter;
- the correct composite storage identity for application data;
- endpoint-specific applicability of total, offset, keyset and cursor mechanics;
- reconciliation of a write whose result is ambiguous after possible dispatch.

## Correlation and source failures

`Command.correlation` and `Binding.correlation` are caller-owned objects retained by reference and
excluded from wire data and safe diagnostics. Every accepted command or binding keeps that
correlation through its terminal outcome.

A malformed source object that is not a `Command` or `Binding` has no valid correlation. The source
therefore terminates with `InputSourceError`; the runtime does not fabricate an outcome identity.
Items accepted before that source error retain their actual success, failure or non-execution
outcome.

## Resource and lifecycle model

- Logical sources are consumed incrementally; physical Bitrix batches contain at most 50 commands.
- Response bytes, buffered rows and commands, retained unordered identity keys, request/page
  budgets, concurrency and elapsed time are bounded by `ExecutionPolicy`.
- The default response ceiling is enforced while streaming, before JSON decoding.
- Exact sequential, counted, and multi-reference traversal shares one operation-wide
  `max_identity_keys` ceiling and rejects an overflowing page atomically. Sequential and counted
  offset traversal may instead record identities in a caller-owned `IdentityStore`, keeping
  in-process identity memory bounded by one page (per-page fingerprints remain bounded by
  `max_pages`); a reported duplicate withdraws identity assurance.
- Streams publish one immutable terminal report after cleanup. Early close and cancellation never
  claim completion.
- The client owns its default transport and active streams; injected transports remain caller-owned.

## Reference scheduling

Every reference source, homogeneous or not, is lowered once into a per-source program before its
binding is admitted; a single pure compatibility check refuses what the operation cannot run as a
correlated local non-execution before any request. One scheduler holds at most one pending pull from
the source, one dispatcher coalesces ready pages of every binding into physical batches whose
commands share decoding options, and one completion gate builds the terminal report after cleanup.

Ordinary and counted bindings reserve rows per page from one shared row pool and release them when
the consumer acknowledges the rows. Under `INPUT` delivery, rows behind the frontier stay held until
it advances, so the pool keeps the frontier's next page free: rows the frontier already holds count
toward that reserve, and the reserve covers the largest page of any admitted binding; a counted
binding reads its head and its known tail offsets as ordinary pages of that pool and commits them in
offset order. A fast or automatic keyset binding instead holds one row lease from activation to its
terminal event. Its runtime plans and fits every wave against a view of the operation policy that
differs only in `max_buffered_rows`, equal to the lease; requests, pages, attempts, elapsed time and
response bytes are charged to the single operation context, and each buffered-row change is applied
once to the shared counter. Its waves and its sequential finish pages are commands of the shared
dispatcher, and under `READY` delivery a binding awaiting the consumer's acknowledgement that has
not reached its terminal, and so will hand over another row group or send its next command, is a
pending continuation the dispatcher waits for, as an ordinary binding's page is. A command whose
batch was lost in transit leaves its binding unknown, as a direct page does. The lease never grows:
a binding is granted at least its planning floor and at most the larger of that floor and an equal
share of the pool among active fast bindings, rows kept for ordinary pages excepted. Once the plan
is chosen at the granted lease, a sequential or boundary-only plan, which only drains the rows it
holds and then holds one finish page at a time, trims the lease to that bound and gives the rest
back to the pool; a lane plan keeps it whole. Under `INPUT` delivery the pool hands rows out in
binding order: while a binding waits for its lease, no later binding is granted a lease or reserves
an ordinary page, and the frontier waits for a lease that has not been settled by its binding's plan
yet, since that lease may still shrink. An automatic binding whose floor exceeds the whole pool
waits to run alone with it (under `INPUT` delivery only as the frontier, since rows behind the
frontier cannot be released), a binding that cannot obtain its floor while no other binding can
release rows fails finitely, and every lease wait is bounded by the operation's elapsed budget.

## Package boundaries

Architecture tests enforce one-way imports, absence of removed compatibility modules and module-size
ceilings. The wheel contains only the runtime library and its console entry point; evidence tooling,
tests and credentials are excluded.

The client deliberately has no automatic endpoint profiles, shape-changing Python flags, tuple
payload conventions, public low-level execution plans or mutable global registries.

## Wire, traversal, and evidence boundaries

`Request` owns an explicit route, immutable body encoding, scoped headers, replay safety, and an optional declarative
embedded-result error contract. Legacy transports continue to support ordinary JSON requests;
advanced request representations require an advertised `WireTransport` capability and fail before
I/O otherwise. No ordinary wire value can name an absolute destination. Native REST 3.0 batch commands travel as
an internal JSON-array envelope that the transport must advertise; one physical-attempt classifier
decides, before any retry, whether an envelope failure proves non-execution, attributes per-command
results, or leaves the envelope unresolved for bounded `SAFE` isolation.

Traversal contracts separately declare progression (`server next`, observed width, or fixed step),
completion (empty confirmation or caller-qualified exact total), collection shape, and identity.
A fixed step without a qualified total fails closed after a short page; a sparse raw bound reads its
qualified extent from a result path or the envelope `total`.
Page evidence contains only bounded counters and enum classifications—never rows, identities,
parameters, headers, URLs, or body fragments.

Fast keyset execution has a planning barrier before emission. Boundary-only and automatic
sequential continuation rely on the same ordered-prefix assurance as sequential traversal; numeric
range and partition plans are caller-asserted and retain positive bound validation on every page.
The separate verifier owns diagnostic canaries and never changes runtime mode. Advisory totals are isolated from
completion and admission. A planner (`keyset_plan.py`) owns boundary analysis, automatic selection
and anchor probing, and freezes a plan of kind, reason and bounds; the pure integer geometry behind
it lives in `keyset_geometry.py`. One runtime (`keyset_runtime.py`) owns wave correlation, ordered
admission, row-buffer deltas, finishing continuation, cleanup, and the immutable selection report.
The planner and the stateless transaction functions reach the runtime only through the
`KeysetTransactionHost` protocol and define no persistent state of their own.

Selection and validation operate on the response's existing frozen tree. A synchronous `PageAdapter`
is invoked once at the two canonical row funnels (driver and fast-keyset receipt) before commit;
published rows alone are thawed. Reference cursor drivers share a capacity-aware dispatcher whose
absolute per-wave coalescing deadline defaults to 20 ms and is skipped when no eligible producer can
add a command. A command whose task has started but has not reached the queue yet, such as a counted
tail page or a fast keyset wave command, counts as one about to arrive, under either delivery order,
until it is queued or its task ends: its producer has already left the runnable set.
