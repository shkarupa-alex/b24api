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

Whether a command may have run is one execution fact, recorded on its failure by the code that sent or
withheld it: the executor for its own sends, the classic and REST v3 batch dialects for the commands of
an envelope they decode, and the batch page dispatcher of references and list traversal for a page
cancelled after its wave was sent. A page whose fetch ended before its wave left, cancelled or past its
admission deadline, is never sent, so it stays proven not run. `b24api.execution.facts` holds the one
mapping from that fact to the public outcome variant (`ambiguity_of`) and to the completion settlement
(`Execution.settlement`), and `b24api.traversal.page_failure` maps it to a failed page's trace: unknown
only when it may have run, otherwise rejected. Classic and REST v3 batch, `call()`, direct and batch
fan-out, references and list traversal all use them, and none of them chooses from the exception type,
which names the cause of a failure and never proves that a command did not run. A sent command is
unknown unless a listed refusal proves it did not run or a conclusive answer settles it. A conclusive
answer is a structured Bitrix error, top-level or found in the result by the command's own contract, or
any status outside 2xx that the ambiguity policy does not hold ambiguous, whatever its body, so a
malformed error body on such a status is conclusive too. A REST v3 batch envelope is the one exception:
a top-level structured error on it is an abort, which may come after some of its commands ran, so it
settles none of them; a command is answered only by its own element or its own isolated send, or proven
not run by a transport failure that proves the envelope was not accepted or a listed refusal status of
the envelope. A 2xx answer the client cannot decode settles nothing, whichever part is malformed: the
envelope, a command's paging metadata or its embedded error.

A command keeps one send lineage across executor retries, classic batch replay rounds and REST v3
isolation. The commands still pending in a round are always a subset of those of the round before, so
the lineage of an envelope group is the lineage of each of its pending commands, and an isolated send
continues it on a per-command copy. The lineage is an explicit state: nothing sent yet, or the latest
send answered or possibly run (with the failure that left it so), and whether a send before it may have
run. When a later conclusive answer ends a command after an earlier send may have run (the retried
envelope, the command's own entry in it, a REST v3 element or an isolated send), the outcome is unknown
and its reason and cause name that earlier send; the conclusive answer that ended it does not appear in
the cause chain. `HALTED` means the command was proven not sent and no earlier send of it may have run:
the server's own halt in a classic fail-fast batch, a later envelope that fail-fast did not send, or a
window refused before dispatch. A success that arrives after the time budget is returned, not discarded.
The operation deadline of direct and batch fan-out and references bounds admission only: once a send
left, the executor's own budgets end it, so a deadline that passes mid-send gives the same evidence on
every path. The executor ends a send only by cancelling it, so a transport that resists cancellation
holds the operation for as long as it resists, on every path. A list traversal that fetches a page
directly raises the page's own failure, as `call()` does, rather than an ambiguity; the page's
settlement and trace still come from the fact. One settlement does not come from the fact: when
`iter_list_counted()` stops at a failed page, a page of its batch tail whose outcome it never read
settles as unknown.

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
- Budgets are charged to one operation-local ledger of plain counters updated in place, so a
  reservation, its settlement and a row-buffer change cost the same however many bindings the
  operation has read. Per-binding page counts are kept only while a binding is open; the validated,
  immutable counters are built only for a snapshot or the terminal report.
- The default response ceiling is enforced while streaming, before JSON decoding.
- Exact sequential, counted, and multi-reference traversal shares one operation-wide
  `max_identity_keys` ceiling and rejects an overflowing page atomically. Sequential and counted
  offset traversal may instead record identities in a caller-owned `IdentityStore`, keeping
  in-process identity memory bounded by one page (per-page fingerprints remain bounded by
  `max_pages`); a reported duplicate withdraws identity assurance.
- Streams publish one immutable terminal report after cleanup. Early close and cancellation never
  claim completion.
- A cancellation is delivered once. Owned cleanup finishes under cancellation protection and
  replays a cancellation it absorbed only when another failure leaves in its place. A request treats
  as pending only a cancellation requested after it began, so a call made in the cleanup of a
  cancelled task is sent, while a later cancellation still ends it, even when a foreign transport
  suppressed it into a response or a failure. An isolated pull that absorbs its reader's
  cancellation by failing in cleanup hands the cancellation back to the reader's next await. A
  stream's close and cleanup defer a cancellation only while the operation's `max_elapsed` budget
  lasts; cleanup still running then finishes in the background and the report records it as failed.
- Caller code a stream's read or cleanup runs (a page-stop callback, an async command, binding or
  reference source, an injected transport, or a task one of them starts) cannot close that stream
  or its client while the read or cleanup is in progress, nor at all on a reference-scheduler
  stream, which runs such code between reads too: the close could wait for the code that requested
  it, so it raises `RuntimeError` and changes nothing. Stop a binding with `CallerStop`, end the
  source, or close from the code that iterates.
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
results, or leaves the envelope unresolved for bounded `SAFE` isolation. Isolation sends each `SAFE`
command alone and returns its success; a failure stays unknown, because the envelope may have run the
command, and is explained by that envelope like any other earlier send that may have run, also when
the budget leaves no isolated send.

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
add a command. A pulled binding is a producer from the moment it leaves the source, before its task
first runs, until it reaches its terminal: it stops counting before its cleanup and its terminal
event. A local non-execution never is one. A command whose task has started but has not reached the queue yet, such as a counted
tail page or a fast keyset wave command, counts as one about to arrive, under either delivery order,
until it is queued or its task ends, since its producer may already have left the runnable set; one
that must still reserve its logical page counts only while the page budget admits it. A counted tail
whose next page waits only for its consumer to move past a receipt it holds is a pending refill: its
consumer frees window slots one by one, and those pages coalesce. Unlike a pending continuation, its
own pages already in the batch do not exclude it, the rows the held receipt returns count toward the
next page's reservation (a window may hold the whole row pool), and under `INPUT` delivery only the
frontier's refill is waited for, as only the frontier's consumer progresses on its own.
