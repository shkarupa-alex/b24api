# b24api 3.x

`b24api` is a thin asynchronous Bitrix24 REST client for Python 3.12+. It knows how to send
requests, split logical batches, traverse lists, retry safely, preserve caller correlation and
close resources. It does not contain a Tasks, CRM or IM method catalog and does not impose
application storage rules.

## Install and configure

```console
uv sync --frozen
export BITRIX24_API_WEBHOOK_URL='https://portal.example/rest/.../'
```

Keep the webhook out of source, logs and command arguments. Reuse one client for a related unit of
work so its HTTP/2 connection pool and rate state are reused.

## Quickstart

<!-- tested: tests/readme_test.py::test_quickstart_runs_exactly_against_a_scripted_portal -->
```python
import os

from b24api import Bitrix24, Request

async with Bitrix24.from_webhook(os.environ["BITRIX24_API_WEBHOOK_URL"]) as client:
    deals = client.iter_list(Request.bare("crm.deal.list", {"select": ["ID", "TITLE"]}))
    async for deal in deals:
        print(deal["ID"], deal["TITLE"])
    print(deals.report.state)
```

Four things are at work:

- **The client.** `Bitrix24.from_webhook()` checks the URL and owns the connection pool it opens;
  `async with` closes it. `Bitrix24()` reads the same URL from the environment.
- **The request.** `Request.bare()` names a REST method and its parameters and sends them to the
  classic `/rest/` endpoint. The route is always explicit: `Request.v3()` targets the V3 API.
- **`iter_list()`.** It walks the list page by page and reads one more, empty, page to confirm
  the end.
- **The report.** `deals.report` says how the traversal ended. `completed` means every page was
  read; a traversal that stops early or fails records why.

A single call returns the decoded `result`:

<!-- tested: tests/readme_test.py::test_quickstart_runs_exactly_against_a_scripted_portal -->
```python
users = await client.call(Request.bare("user.get", {"ID": 1}))
```

The client owns the transport it creates. An injected transport remains caller-owned. `aclose()` is
idempotent and closes active streams before the owned transport. To prove that no row is missing or
repeated, give the traversal an identity (see [Choosing a list operation](#choosing-a-list-operation)).

## Direct calls

Use `call()` for detached decoded JSON and `call_response()` when you also need the immutable
response envelope: `result`, `total`, `next`, timing and bounded diagnostic evidence.

Use `call_bytes()` when a successful method response is a file rather than a Bitrix JSON envelope.
The operation is explicit and never hides malformed JSON by falling back to bytes.

<!-- tested: tests/client_findings_3_test.py::test_binary_call_returns_every_success_byte_without_json_sniffing -->
```python
from b24api import RouteKind
archive = await client.call_bytes(Request("example.export.download", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE))
payload = archive.body
```

<!-- tested: tests/client_v2_test.py::test_call_and_call_response_have_stable_detached_types -->
```python
from b24api import RouteKind
from b24api import ReplaySafety

request = Request("example.item.get", {"id": 7}, ReplaySafety.SAFE, route=RouteKind.BARE)
decoded = await client.call(request)
response = await client.call_response(request)
```

### Replay safety

`Request.replay_safety` describes what the client may do when a connection fails after the request
may already have reached Bitrix:

| Value | Meaning | After possible dispatch |
|---|---|---|
| `SAFE` | Repeating the request cannot create a second business effect. Typical reads and explicitly idempotent operations belong here. | Automatic retry is allowed within policy budgets. |
| `UNSAFE` | Repeating the request is known to risk a duplicate effect, for example creating an entity without an idempotency key. | No automatic replay; the caller receives an ambiguous-execution error and reconciles state. It is retried only when the failure proves it did not run. |
| `UNKNOWN` | The caller has not established whether replay is safe. This is the default. | Same conservative behavior as `UNSAFE`, while diagnostics preserve that safety was unknown rather than known unsafe. |

A failure that proves the request did not run is retried whatever its safety: a transport failure
before dispatch, or a refusal listed in `AmbiguityPolicy`, which by default is an unstructured 423, 425
or 429 (`refusal_http_statuses`) or a `QUERY_LIMIT_EXCEEDED` / `OPERATION_TIME_LIMIT` refusal
(`refusal_api_codes`). A code or status added only to `RetryPolicy` is retried for `SAFE` work alone.
A physical batch applies these rules to each command: after a failure, only the commands that
may run again are sent again, in a smaller batch, and an `UNSAFE` command of a batch that may have run
arrives as unknown. Replay rounds spend the same attempt and retry-time budget as the sends before
them. Method names never imply safety;
mark a request `SAFE` only when the operation's semantics justify it.

Use `ExecutionPolicy` to narrow attempts or resource budgets for one operation:

<!-- tested: tests/execution_test.py::test_ambiguous_dispatch_never_retries_unproven_request -->
```python
from b24api import ExecutionPolicy

one_attempt = ExecutionPolicy(max_attempts_per_request=1)
result = await client.call(request, policy=one_attempt)
```

A `policy=` argument replaces the client's default policy wholesale; fields are never merged. The
client default is `ExecutionPolicy.from_settings(settings)`: the library defaults with
`max_retry_elapsed_per_request` taken from `Settings.http_timeout` (30 s by default, while a bare
`ExecutionPolicy()` allows 120 s). To change one field and keep the configured timeout, derive the
per-call policy from that default:

<!-- tested: tests/settings_test.py::test_a_per_call_policy_replaces_the_client_default_without_merging -->
```python
import dataclasses

from b24api import ExecutionPolicy

one_attempt = dataclasses.replace(ExecutionPolicy.from_settings(settings), max_attempts_per_request=1)
result = await client.call(request, policy=one_attempt)
```

## Logical batch and correlation

`batch()` accepts an arbitrary-length synchronous or asynchronous command source. It consumes the
source incrementally and splits it into physical Bitrix batches of at most 50 commands; the full
input is never materialized.

`Command.correlation` is arbitrary caller-owned state. It is retained by reference, returned with
the outcome, never serialized to Bitrix and never included in safe diagnostics. This is useful for
matching a result to the object, file, chat or database row that produced its request.

<!-- tested: tests/client_v2_test.py::test_logical_batch_is_unbounded_ordered_and_correlation_is_strictly_off_wire -->
```python
from b24api import RouteKind
from b24api.contracts import Command, CommandSuccess

commands = (
    Command(
        Request("example.item.get", {"id": item_id}, ReplaySafety.SAFE, route=RouteKind.BARE),
        correlation=item_id,
    )
    for item_id in source_ids
)

async with client.batch(commands, batch_size=25) as stream:
    async for outcome in stream:
        assert isinstance(outcome, CommandSuccess)
        consume(outcome.correlation, outcome.result)
```

`batch()` is fail-fast. `batch_outcomes()` continues where safe and yields one of
`CommandSuccess`, `CommandFailure`, `CommandNotExecuted` or `CommandOutcomeUnknown` in input order.

<!-- tested: tests/client_v2_test.py::test_batch_outcomes_retains_typed_failure_without_halting_later_commands -->
```python
from b24api.contracts import CommandFailure, CommandNotExecuted, CommandOutcomeUnknown

async with client.batch_outcomes(commands) as stream:
    async for outcome in stream:
        match outcome:
            case CommandSuccess() as success:
                consume(success.correlation, success.result)
            case CommandFailure() | CommandNotExecuted() | CommandOutcomeUnknown():
                handle(outcome)
```

For independently dispatchable commands, use `fan_out()` or `fan_out_outcomes()` with
`DirectDispatch` or `BatchDispatch`. Delivery order is explicitly `READY` or `INPUT`.

## Choosing a list operation

The unsuffixed operation is the basic strategy with the fewest endpoint assumptions. Faster or
more specialized mechanics have explicit names and explicit preconditions.

| Operation | Use it when | Network mechanics | Completion proof |
|---|---|---|---|
| `iter_list` | The method supports ordinary offset pagination. | Sequential requests follow server `next` (the next offset). Ordinary counted Bitrix list endpoints do server-side COUNT for `total` plus LIMIT/OFFSET page retrieval. | Continuation and empty terminal page; add identity for duplicate detection. |
| `iter_list_counted` | The first response provides an exact filtered `total` and stable offset pages. | Head page is direct; all known tail offsets are grouped into physical Bitrix batches. | Exact total, ranges and identities. |
| `iter_list_keyset` | The method may omit `total`, but reliably supports ordering and filtering by a unique integer identity. | Auto by default: it plans first, then selects boundary-only, sequential, range, or partitioned execution; runtime sends no diagnostic canaries. | Caller-asserted keyset contract, strict monotonic identity, active bound validation, and terminal empty confirmation. |
| `iter_list_cursor` | Each next request depends on a cursor from the previous response. | Sequential dependent cursor requests. | Strict unique monotonic cursor and empty terminal page. |
| `iter_cursors` | One or many parent-bound cursor traversals need correlation and shared batching. | Lazy per-parent drivers share the reference batch queue; each binding may have `start_cursor`. | Isolated strict cursor progress and terminal event per binding. |
| `iter_references` | The same list method must run for many parent parameter sets, such as comments per owner or messages per chat. | Bindings are scheduled with direct or physical-batch dispatch; each binding has its own traversal state. | Per-binding rows, completion/failure and caller correlation. |

`page_size` is a local decoded-page cap. It is sent to Bitrix only when you provide the endpoint's
exact `limit_path`; the client never guesses method-specific parameter names.

### List traversal comparison

![List traversal comparison](https://raw.githubusercontent.com/shkarupa-alex/b24api/master/list-traversal-comparison.svg)

The animation replays traces executed against a scripted portal with 1,000 dense IDs at 50 rows
per page. `iter_list` sends 20 pages and one empty confirmation (21 HTTP). `iter_list_counted` sends
the head and one batch of 19 pages (2 HTTP). `iter_list_keyset` in auto mode selects range
execution: it reads both ends of the range in one batch, the 19 ranges between them in a second, and
confirms the end with one call for `ID > 1000` (3 HTTP). A `BoundedIdentityRange` with a qualified
upper ID needs no confirmation: sequential execution stops when it receives that ID. On a real portal
auto chooses from the observed geometry, so the plan and its request count can differ; the report's
`keyset_selection` says which plan ran.

### Sequential offset

This is the canonical default. It follows the `next` returned by the server and confirms the end
with an empty page. For ordinary counted Bitrix list endpoints, `next` is an offset for the next
LIMIT/OFFSET page, not a keyset cursor. Producing `total` involves a separate server-side count
query in addition to retrieving the page. These database operations are performed inside the same
REST request: the client does not issue an additional HTTP call just for the count. This distinction
matters for performance: not making a separate HTTP count call does **not** mean avoiding server-side
COUNT work. `iter_list` does not suppress that work; a returned `total` is observational and does not
control this strategy's completion. Exact database implementation is endpoint-specific.

<!-- tested: tests/client_v2_test.py::test_iter_list_is_sequential_mechanics_only_and_report_is_post_cleanup -->
```python
from b24api import RouteKind
from b24api import IdentityCoercion, IdentitySpec, ResultSelector

identity = IdentitySpec(
    item_path=("ID",),
    filter_key="ID",
    order_key="ID",
    coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
)

stream = client.iter_list(
    Request("example.item.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    selector=ResultSelector(("items",)),
    identity=identity,
)
async with stream:
    async for item in stream:
        consume(item)
```

Without `identity`, successful exhaustion is reported as `MECHANICS_ONLY`: pagination completed,
but the client cannot prove that the portal did not duplicate or substitute rows.

Mapping-backed collections are explicit as well. `MAPPING_VALUES` yields values from a selected
mapping in insertion order; `MAPPING_VALUES_OR_EMPTY` additionally accepts only an empty terminal
sequence and records that degradation in the operation report.

<!-- tested: tests/client_findings_3_test.py::test_shape_rejection_is_retained_as_zero_admission_page_evidence -->
```python
from b24api import RouteKind
from b24api.contracts import ResultCollectionShape

stream = client.iter_list(
    Request("example.dictionary.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    selector=ResultSelector(("items",)),
    collection_shape=ResultCollectionShape.MAPPING_VALUES,
)
async with stream:
    async for value in stream:
        consume(value)
```

### Counted, physically batched tail

The first direct page must contain an exact filtered `total` and, when more rows exist, `next`.
The client derives all remaining offsets from the observed head width and sends tail pages through
bounded physical batches.

<!-- tested: tests/client_v2_test.py::test_counted_traversal_preserves_frozen_request_shape_and_exact_identity -->
```python
from b24api import RouteKind
stream = client.iter_list_counted(
    Request("example.item.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    selector=ResultSelector(("items",)),
    identity=identity,
    page_size=50,
    batch_size=50,
)
```

Use it only when `total` is exact for the supplied filter and offset pages are stable. Any missing
range, overlap, duplicate identity or total contradiction raises `IncompleteTraversalError`.

A filtered call that matches nothing may omit `total` entirely, as `user.get` does with
`result: []`. A first page with no rows, no `next` and no usable `total` (missing, `null`, or the
`-1` unknown sentinel) therefore completes as an observed empty source after that one request: the
report is `completed` and `exhausted`, but its assurance is `mechanics_only` (`identity_exact` with an
identity), never a count-matched claim, and no total is invented. A first page that reports
`total: 0` keeps the count-matched result. Rows without a usable total, a remaining `next`, a
positive total with no rows, a fixed step, or a `ConsistencyPolicy` whose `confirmation_policy` is
`QUALIFIED_TOTAL` (from `b24api.contracts.policy.ConfirmationPolicy`) stay strict and raise
`IncompleteTraversalError`.

Physical batching reduces HTTP exchanges, but does not suppress server-side COUNT in ordinary
counted list subrequests. Each command still performs its own offset page retrieval and associated
total calculation on the server. Do not confuse batching these commands with a no-count traversal.

### No-count keyset

Keyset traversal uses automatic execution by default. Omitting `execution` is equivalent to
`AutoKeysetExecution(StableIntegerKeysetContract())`: the client captures both ordered boundaries,
then selects boundary-only, sequential, range, or partitioned execution from the observed geometry
and available policy capacity. Planning completes before any row is emitted, so partial consumption
still pays that barrier cost. A sequential selection made by auto is a cost decision; a failed or
contradictory fast plan is never silently restarted as sequential.

By using the default, the caller asserts that the endpoint has a stable, unique integer key, honors
strict numeric bounds and ordering, and satisfies empty-confirmation completion. Concurrent mutation
outside the captured middle is handled by the finishing sweep; mutation inside it is outside this
assertion. Pass `SequentialKeysetExecution()` explicitly when an endpoint cannot satisfy the fast
contract or when the previous request-by-request behavior is required.

Static incompatibility with the auto contract raises `CapabilityError` from the
`iter_list_keyset(...)` call before iteration begins. A portal that accepts but contradicts the
declared controls fails before emission with `IncompleteTraversalError`; auto never restarts that
operation silently. `KeysetTraversal` inside reference traversal remains sequential-only. The
terminal report carries a compact `keyset_selection` (`requested_kind`, `selected_kind`, `reason`)
for every keyset traversal. A `page_stop` callback needs the ordered page stream, so auto reports
`AUTO`, `SEQUENTIAL`, `PAGE_STOP`; an explicit `SequentialKeysetExecution()` reports
`EXPLICIT_SEQUENTIAL`. The detailed `keyset_execution` report is present only when the fast path ran.

Use `await client.verify_keyset_capability(...)` as a development/CI/staging guard on a stable
representative fixture. It performs five strict-bound checks and returns only a `VERIFIED` report;
unsupported and inconclusive verdicts raise `KeysetCapabilityError`. The ordinary
`iter_list_keyset()` remains a separate caller-asserted operation with zero verifier canaries and
may emit a partial prefix before a late endpoint contradiction is detected.
Keep the guard beside the traversal, for example under
`if os.environ.get("ENV") != "PROD":`; set `ENV=PROD` only after qualifying the exact portal,
credentials, method, request/filter, identity, ordering representation, and page cap.

Every list operation also accepts an immutable `PageAdapter` strategy. The adapter synchronously
maps selected frozen items using sibling result metadata while preserving cardinality, order and
configured identities. The identity adapter is the default and preserves existing JSON output.

An advisory `total` may only raise an automatic cost estimate and never proves completion. Reports
record the selected strategy and reason: unbounded auto continuation has the same
`ordered_prefix_only` assurance as sequential traversal, while bounded plans additionally report
`caller_asserted_bounds`. Runtime traversal does not perform verifier canaries.

<!-- tested: tests/keyset_fast_test.py::test_omitted_execution_defaults_to_auto -->
```python
import os

from b24api import KeysetSpec, ParameterPath, ReplaySafety, Request, ResultSelector, RouteKind

request = Request("example.item.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)
selector = ResultSelector(("items",))
keyset = KeysetSpec(
    filter_path=ParameterPath(("filter",)),
    order_path=ParameterPath(("order",)),
)

if os.environ.get("ENV") != "PROD":
    # Accepting an ID filter does not prove strict bounds or ordering.
    await client.verify_keyset_capability(
        request,
        selector=selector,
        identity=identity,
        page_size=50,
        keyset=keyset,
    )

stream = client.iter_list_keyset(
    request,
    selector=selector,
    identity=identity,
    page_size=50,
    keyset=keyset,
)
```

### Dependent cursor

Use a cursor when the next boundary is returned or derived from the previous page, as with many
message-list methods.

<!-- tested: tests/client_v2_test.py::test_keyset_and_cursor_are_explicit_strict_alternatives -->
```python
from b24api import RouteKind
from b24api import CursorSpec, ParameterPath

stream = client.iter_list_cursor(
    Request("example.message.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    selector=ResultSelector(("items",)),
    cursor=CursorSpec(
        parameter_path=ParameterPath(("LAST_ID",)),
        item_path=("ID",),
        coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
        direction="ascending",
        take="last",
    ),
)
```

Cursor values must be unique and strictly monotonic. If an endpoint exposes only a non-unique
boundary, use an application-owned direct-call workflow or supply a unique tie-breaker.

For multiple parent-bound cursor chains, `iter_cursors()` keeps cursor progress and correlation
isolated per binding while ready pages share the physical batch queue.

![Cursor batching across independent chats](https://raw.githubusercontent.com/shkarupa-alex/b24api/master/cursor-batching.svg)

See [architecture](https://github.com/shkarupa-alex/b24api/blob/master/docs/architecture.md), [migration](https://github.com/shkarupa-alex/b24api/blob/master/docs/migration.md),
[performance](https://github.com/shkarupa-alex/b24api/blob/master/docs/performance.md), and [endpoint recipes](https://github.com/shkarupa-alex/b24api/blob/master/docs/recipes.md) for the complete
contracts and selection guidance.

### One list method across many parent entities

`Binding` applies exact parameter updates to a base request and carries parent correlation. The
client remains unaware of entity types: a binding can represent a deal, lead, chat or any other
caller-defined parent.

![Reference batching across leads and deals](https://raw.githubusercontent.com/shkarupa-alex/b24api/master/references-batching.svg)

<!-- tested: tests/client_v2_test.py::test_bound_references_apply_nested_updates_off_wire_and_emit_exact_completion -->
```python
from b24api import RouteKind
from b24api import BatchDispatch, Binding, ParameterPath, ParameterUpdate, SequentialTraversal
from b24api.contracts import ReferenceComplete, ReferenceItem

bindings = (
    Binding(
        summary=f"owner {parent_id}",
        updates=(ParameterUpdate(ParameterPath(("filter", "OWNER_ID")), parent_id),),
        correlation=parent_id,
    )
    for parent_id in parent_ids
)

stream = client.iter_references(
    Request("example.comment.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    bindings,
    traversal=SequentialTraversal(selector=ResultSelector(("items",)), identity=identity),
    dispatch=BatchDispatch(batch_size=25, concurrency=2),
)
async with stream:
    async for event in stream:
        if isinstance(event, ReferenceItem):
            consume(event.correlation, event.item)
        elif isinstance(event, ReferenceComplete):
            record_completion(event.correlation, event.row_count)
```

For messages across chats, use the same `iter_references()` shape: each binding updates the chat
parameter and carries the chat correlation; choose `CursorTraversal` when the message endpoint is
cursor-based. Identity tracking and completion remain scoped to each binding, so equal child IDs
under different parents are not conflated.

<!-- tested: tests/client_v2_test.py::test_bound_references_apply_nested_updates_off_wire_and_emit_exact_completion -->
```python
from b24api import RouteKind
from b24api import (
    Binding,
    CursorSpec,
    CursorTraversal,
    DirectDispatch,
    IdentityCoercion,
    ParameterPath,
    ParameterUpdate,
    ResultSelector,
)

chat_bindings = (
    Binding(
        summary=f"chat {chat_id}",
        updates=(ParameterUpdate(ParameterPath(("DIALOG_ID",)), chat_id),),
        correlation={"chat_id": chat_id},
    )
    for chat_id in chat_ids
)

messages = client.iter_references(
    Request("example.message.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    chat_bindings,
    traversal=CursorTraversal(
        selector=ResultSelector(("items",)),
        cursor=CursorSpec(
            parameter_path=ParameterPath(("LAST_ID",)),
            item_path=("ID",),
            coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
            direction="ascending",
            take="last",
        ),
    ),
    dispatch=DirectDispatch(concurrency=4),
)
```

`iter_reference_outcomes()` additionally yields correlated `ReferenceFailure`,
`ReferenceNotExecuted` and `ReferenceOutcomeUnknown`. A malformed source object that is not a
`Binding` has no valid caller correlation, so it terminates the source with `InputSourceError`
rather than fabricating a reference outcome. Already accepted bindings retain their real outcomes.

## Streams, partial results and reports

Every multi-item operation returns an `OperationStream`. Prefer `async with`: a plain `break` does
not close an arbitrary async iterator. After cleanup, `stream.report` permanently exposes one
immutable `OperationReport`; before termination it is `None`.

<!-- tested: tests/client_v2_test.py::test_partial_helper_closes_without_claiming_completion -->
```python
first = await client.iter_list(request).first()
page = await client.iter_list(request).collect(limit=100)

assert first.report.partial
assert page.report.partial
```

Helpers do not pull an extra row just to prove exhaustion. Reaching a requested limit is therefore
`EARLY_CLOSED`, never a false `COMPLETED`. Cancellation and cleanup preserve the primary exception
and publish the same final report where the Python exception type permits it.

## Resource boundaries

`ExecutionPolicy` bounds requests, pages, elapsed time, attempts, decompressed response bytes,
buffered commands and rows, retained unordered identity keys, direct concurrency and active
references. The default response ceiling is 16 MiB and is enforced while streaming, before JSON
decoding.

Sequential, counted, and multi-reference exact traversal retain at most `max_identity_keys`
observed identities per operation in memory (100,000 by default). All active reference bindings
share that ceiling. A page that would exceed it is rejected atomically with typed budget evidence.
Set a larger finite ceiling when the expected aggregate cardinality is known, or pass
`identity_store=` to `iter_list`/`iter_list_counted` so a caller-owned `IdentityStore` (for example a
SQLite table keyed by `identity_store_key(...)`) proves uniqueness while in-process identity memory
stays bounded by one page; the client never closes that store. Repeated-page detection still keeps
one short fingerprint per page, so raise `max_pages` deliberately for very long traversals.
Strict keyset and cursor traversal retain only monotonic progression state when sufficient.

## CLI

The wheel installs `b24api`. Stdout contains only result data; list rows are JSONL. Reports and safe
errors go to stderr. Credentials come only from `Settings` and cannot be passed as CLI arguments.

<!-- tested-console: tests/cli_test.py::test_call_routes_replay_safety_and_keeps_success_data_on_stdout -->
```console
b24api call profile --route bare
b24api call example.item.get --route bare --params '{"id":7}' --raw --replay-safety safe
b24api list example.item.list --route bare --params @params.json
b24api list example.item.list --route bare --strategy counted --contract @counted-contract.json
```

The `--raw` CLI option selects the response envelope; it does not alter the Python API. Advanced
list strategies use closed JSON `version: 1` contracts. The entire contract is validated before
client construction. Run `b24api --help` and `b24api list --help` for the compact option surface.

Exit codes are `0` success, `2` usage/contract error, `3` unavailable configuration, `4`
remote/protocol/correctness/incomplete failure, `5` broken output consumer and `130` cancellation.

## Correctness boundaries

The client fails closed on contradictory pagination, missing counted ranges, duplicate identities,
unsafe ambiguous replay, oversized responses and incomplete cleanup. It can prove only facts visible
through the transport contract: continuation, totals, identity, order, budgets and lifecycle.

It cannot generically prove that Bitrix honored the business meaning of a filter, choose an
application's composite storage key or reconcile an ambiguous write. Applications must validate
expected business sets and verify writes where needed.

## Performance and profiling

The current deterministic profile covers request counts, wall/CPU time, time to first row,
high-water counters, retained resources and optional Memray allocations:

```console
uv run python tools/b24api_evidence/profile_runtime.py --capability-suite
uv run python tools/b24api_evidence/profile_runtime.py --samples 7 --warmups 2
uv run --with memray python tools/b24api_evidence/profile_runtime.py \
  --case dense-10k --plan counted_batch --samples 7 --warmups 2 \
  --memray-output /tmp/b24api.bin
uv run --with memray memray stats /tmp/b24api.bin
```

These deterministic fixtures characterize local resources and network shape; they are not live
portal latency admission. See [docs/performance.md](https://github.com/shkarupa-alex/b24api/blob/master/docs/performance.md) for current measurements
and [docs/architecture.md](https://github.com/shkarupa-alex/b24api/blob/master/docs/architecture.md) for guarantees and ownership boundaries.

Projects moving from an earlier API surface can use [docs/migration.md](https://github.com/shkarupa-alex/b24api/blob/master/docs/migration.md).

## Verification

```console
uv sync --frozen
make qc
git diff --check
```

`make qc` runs the lint, type and default test checks that CI blocks on. It leaves out the internal
benches (the pytest marker `slow`: the evidence harness contracts and the 50k/100k-scale runs, which
take several minutes). `make bench` runs them, and so does the blocking CI job `slow`.

The wheel regression installs into an isolated environment, executes the `b24api` entry point and
checks that tests, live/evidence tooling and credentials are excluded.
