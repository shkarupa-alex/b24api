# Architecture and correctness guarantees

`b24api` is a method-agnostic asynchronous transport client. Its runtime is split into contracts,
transport, execution, batching, traversal, reference scheduling and a thin public facade. The
client does not contain endpoint catalogs, persistence, business reconciliation or recipe logic.

## Public capability surface

| Capability | Public operation | Guarantee |
|---|---|---|
| Decoded direct request | `call()` | Detached decoded JSON with explicit replay policy. |
| Response envelope | `call_response()` | Immutable result, pagination, timing and bounded evidence. |
| Arbitrary logical batch | `batch()` / `batch_outcomes()` | Lazy input, bounded physical batches, off-wire correlation and typed outcomes. |
| Independent command fan-out | `fan_out()` / `fan_out_outcomes()` | Explicit dispatch mode, concurrency and delivery order. |
| Sequential offset list | `iter_list()` | Server-continuation validation and terminal empty-page confirmation. |
| Counted list | `iter_list_counted()` | Direct head, batched tail and exact total/range/identity validation. |
| No-count keyset | `iter_list_keyset()` | Strict monotonic identity progress without a count request. |
| Dependent cursor | `iter_list_cursor()` | Strict unique monotonic cursor progress. |
| Per-parent traversal | `iter_references()` / `iter_reference_outcomes()` | Isolated traversal state and correlation for every accepted binding. |
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
- Response bytes, buffered rows and commands, request/page budgets, concurrency and elapsed time are
  bounded by `ExecutionPolicy`.
- The default response ceiling is enforced while streaming, before JSON decoding.
- Exact sequential/counted traversal retains observed identities in memory and warns once above
  100,000 identities; it has no database, spill file or automatic cardinality refusal.
- Streams publish one immutable terminal report after cleanup. Early close and cancellation never
  claim completion.
- The client owns its default transport and active streams; injected transports remain caller-owned.

## Package boundaries

Architecture tests enforce one-way imports, absence of removed compatibility modules and module-size
ceilings. The wheel contains only the runtime library and its console entry point; evidence tooling,
tests and credentials are excluded.

The client deliberately has no automatic endpoint profiles, shape-changing Python flags, tuple
payload conventions, public low-level execution plans or mutable global registries.
