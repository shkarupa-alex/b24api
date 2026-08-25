# b24api 2.x

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

<!-- tested: tests/client_v2_test.py::test_call_and_call_response_have_stable_detached_types -->
```python
from b24api import Bitrix24, Request

async with Bitrix24() as client:
    profile = await client.call(Request("profile"))
```

The client owns its default transport. An injected transport remains caller-owned. `aclose()` is
idempotent and closes active streams before the owned transport.

## Direct calls

Use `call()` for detached decoded JSON and `call_response()` when you also need the immutable
response envelope: `result`, `total`, `next`, timing and bounded diagnostic evidence.

<!-- tested: tests/client_v2_test.py::test_call_and_call_response_have_stable_detached_types -->
```python
from b24api import ReplaySafety

request = Request("example.item.get", {"id": 7}, ReplaySafety.SAFE)
decoded = await client.call(request)
response = await client.call_response(request)
```

### Replay safety

`Request.replay_safety` describes what the client may do when a connection fails after the request
may already have reached Bitrix:

| Value | Meaning | After possible dispatch |
|---|---|---|
| `SAFE` | Repeating the request cannot create a second business effect. Typical reads and explicitly idempotent operations belong here. | Automatic retry is allowed within policy budgets. |
| `UNSAFE` | Repeating the request is known to risk a duplicate effect, for example creating an entity without an idempotency key. | No automatic replay; the caller receives an ambiguous-execution error and reconciles state. |
| `UNKNOWN` | The caller has not established whether replay is safe. This is the default. | Same conservative behavior as `UNSAFE`, while diagnostics preserve that safety was unknown rather than known unsafe. |

A failure proved to occur before dispatch may still be retried. Method names never imply safety;
mark a request `SAFE` only when the operation's semantics justify it.

Use `ExecutionPolicy` to narrow attempts or resource budgets for one operation:

<!-- tested: tests/execution_test.py::test_ambiguous_dispatch_never_retries_unproven_request -->
```python
from b24api import ExecutionPolicy

one_attempt = ExecutionPolicy(max_attempts_per_request=1)
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
from b24api import Command, CommandSuccess

commands = (
    Command(
        Request("example.item.get", {"id": item_id}, ReplaySafety.SAFE),
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
from b24api import CommandFailure, CommandNotExecuted, CommandOutcomeUnknown

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
| `iter_list` | The method supports ordinary offset pagination. | Pages are requested sequentially using server `next`; no separate count request is made. | Continuation and empty terminal page; add identity for duplicate detection. |
| `iter_list_counted` | The first response provides an exact filtered `total` and stable offset pages. | Head page is direct; all known tail offsets are grouped into physical Bitrix batches. | Exact total, ranges and identities. |
| `iter_list_keyset` | The method may omit `total`, but reliably supports ordering and filtering by a unique identity. | Sequential pages advance an identity boundary; no count request. | Strict monotonic identity and empty terminal page. |
| `iter_list_cursor` | Each next request depends on a cursor from the previous response. | Sequential dependent cursor requests. | Strict unique monotonic cursor and empty terminal page. |
| `iter_references` | The same list method must run for many parent parameter sets, such as comments per owner or messages per chat. | Bindings are scheduled with direct or physical-batch dispatch; each binding has its own traversal state. | Per-binding rows, completion/failure and caller correlation. |

`page_size` is a local decoded-page cap. It is sent to Bitrix only when you provide the endpoint's
exact `limit_path`; the client never guesses method-specific parameter names.

### Sequential offset

This is the canonical default. It follows the `next` returned by the server and confirms the end
with an empty page. A `total` present in the response is observational; this strategy does not add
a separate count request.

<!-- tested: tests/client_v2_test.py::test_iter_list_is_sequential_mechanics_only_and_report_is_post_cleanup -->
```python
from b24api import IdentityCoercion, IdentitySpec, ResultSelector

identity = IdentitySpec(
    item_path=("ID",),
    filter_key="ID",
    order_key="ID",
    coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
)

stream = client.iter_list(
    Request("example.item.list", replay_safety=ReplaySafety.SAFE),
    selector=ResultSelector(("items",)),
    identity=identity,
)
async with stream:
    async for item in stream:
        consume(item)
```

Without `identity`, successful exhaustion is reported as `MECHANICS_ONLY`: pagination completed,
but the client cannot prove that the portal did not duplicate or substitute rows.

### Counted, physically batched tail

The first direct page must contain an exact filtered `total` and, when more rows exist, `next`.
The client derives all remaining offsets from the observed head width and sends tail pages through
bounded physical batches.

<!-- tested: tests/client_v2_test.py::test_counted_traversal_preserves_frozen_request_shape_and_exact_identity -->
```python
stream = client.iter_list_counted(
    Request("example.item.list", replay_safety=ReplaySafety.SAFE),
    selector=ResultSelector(("items",)),
    identity=identity,
    page_size=50,
    batch_size=50,
)
```

Use it only when `total` is exact for the supplied filter and offset pages are stable. Any missing
range, overlap, duplicate identity or total contradiction raises `IncompleteTraversalError`.

### No-count keyset

Keyset traversal does not ask the server for a count. The method must honor ordering and a strict
identity boundary such as `filter[>ID]`. It is intentionally sequential because a future boundary
cannot be known safely before the preceding page arrives.

<!-- tested: tests/client_v2_test.py::test_keyset_and_cursor_are_explicit_strict_alternatives -->
```python
from b24api import KeysetSpec, ParameterPath

stream = client.iter_list_keyset(
    Request("example.item.list", replay_safety=ReplaySafety.SAFE),
    selector=ResultSelector(("items",)),
    identity=identity,
    keyset=KeysetSpec(
        filter_path=ParameterPath(("filter",)),
        order_path=ParameterPath(("order",)),
    ),
)
```

### Dependent cursor

Use a cursor when the next boundary is returned or derived from the previous page, as with many
message-list methods.

<!-- tested: tests/client_v2_test.py::test_keyset_and_cursor_are_explicit_strict_alternatives -->
```python
from b24api import CursorSpec, ParameterPath

stream = client.iter_list_cursor(
    Request("example.message.list", replay_safety=ReplaySafety.SAFE),
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

### One list method across many parent entities

`Binding` applies exact parameter updates to a base request and carries parent correlation. The
client remains unaware of entity types: a binding can represent a deal, lead, chat or any other
caller-defined parent.

<!-- tested: tests/client_v2_test.py::test_bound_references_apply_nested_updates_off_wire_and_emit_exact_completion -->
```python
from b24api import (
    BatchDispatch,
    Binding,
    ParameterPath,
    ParameterUpdate,
    ReferenceComplete,
    ReferenceItem,
    SequentialTraversal,
)

bindings = (
    Binding(
        summary=f"owner {parent_id}",
        updates=(ParameterUpdate(ParameterPath(("filter", "OWNER_ID")), parent_id),),
        correlation=parent_id,
    )
    for parent_id in parent_ids
)

stream = client.iter_references(
    Request("example.comment.list", replay_safety=ReplaySafety.SAFE),
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
    Request("example.message.list", replay_safety=ReplaySafety.SAFE),
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
buffered commands and rows, direct concurrency and active references. The default response ceiling
is 16 MiB and is enforced while streaming, before JSON decoding.

Sequential and counted exact traversal retain observed identities in memory. There is no database,
spill file or identity-count refusal. Crossing 100,000 distinct identities emits one
`RuntimeWarning`; exact tracking continues. Strict keyset and cursor traversal retain only
monotonic progression state when sufficient.

## CLI

The wheel installs `b24api`. Stdout contains only result data; list rows are JSONL. Reports and safe
errors go to stderr. Credentials come only from `Settings` and cannot be passed as CLI arguments.

<!-- tested-console: tests/cli_test.py::test_call_routes_replay_safety_and_keeps_success_data_on_stdout -->
```console
b24api call profile
b24api call example.item.get --params '{"id":7}' --raw --replay-safety safe
b24api list example.item.list --params @params.json
b24api list example.item.list --strategy counted --contract @counted-contract.json
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
portal latency admission. See [docs/performance.md](docs/performance.md) for current measurements
and [docs/architecture.md](docs/architecture.md) for guarantees and ownership boundaries.

Projects moving from an earlier API surface can use [docs/migration.md](docs/migration.md).

## Verification

```console
uv sync --frozen
.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/ruff check . --no-fix --no-cache
.venv/bin/ruff format --check . --no-cache
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check
```

The wheel regression installs into an isolated environment, executes the `b24api` entry point and
checks that tests, live/evidence tooling and credentials are excluded.
