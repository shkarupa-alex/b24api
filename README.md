# b24api 2.x

`b24api` is a thin asynchronous Bitrix24 REST client for Python 3.12+. It is method-agnostic: the
library knows how Bitrix requests, batches, pagination, retries, cancellation and reports work, but
does not contain a Tasks/CRM/IM method catalog or application storage rules.

Version 2 is intentionally a breaking release. It keeps the useful capabilities of 1.0.1 while
removing compatibility wrappers, automatic endpoint profiles and return-type-changing flags. The
result has one typed API and fails closed when it cannot prove completion.

## Status

The correctness-first v2 surface is implemented for:

- decoded and immutable-envelope direct calls;
- arbitrary-length logical batch over bounded physical Bitrix batches;
- tolerant per-command outcomes and off-wire correlation;
- sequential, counted, keyset and strict cursor list traversal;
- independent fan-out and bound reference traversal;
- explicit stream ownership, cancellation, partial consumption and immutable terminal reports;
- a compact installed `b24api` JSON/JSONL command.

For the counted list path, v2 preserves the frozen 1.0.1 physical request structure and exact
identities: `1 / 2 / 5` requests for `19 / 500 / 10,000` rows. This is structural request parity,
not a claim that local Python CPU time or every live portal response is faster. The safe generic
no-count path is sequential keyset traversal and can make more requests than unsafe historical
shortcuts.

## Install and configure

Until a 2.x wheel is published, install the reviewed checkout:

```console
uv sync --frozen
```

Provide the webhook only through the environment:

```console
export BITRIX24_API_WEBHOOK_URL='https://portal.example/rest/.../'
```

The credential must not be embedded in source, logs or command arguments. Reuse one client for a
related unit of work so the HTTP/2 connection pool and rate state are reused:

<!-- tested: tests/client_v2_test.py::test_call_and_call_response_have_stable_detached_types -->
```python
from b24api import Bitrix24, ReplaySafety, Request

async with Bitrix24() as client:
    result = await client.call(Request("profile", replay_safety=ReplaySafety.SAFE))
```

`aclose()` is idempotent. A client closes its active streams before its owned transport; an injected
transport remains caller-owned.

## Direct calls

`call()` always returns detached decoded JSON. `call_response()` always returns an immutable
`Response` carrying `result`, `total`, `next`, timing and bounded evidence.

<!-- tested: tests/client_v2_test.py::test_call_and_call_response_have_stable_detached_types -->
```python
request = Request("example.item.get", {"id": 7}, ReplaySafety.SAFE)
decoded = await client.call(request)
response = await client.call_response(request)
```

There is no `raw=True` flag because one method must not change its Python return type.

### Replay safety

`Request.replay_safety` is `SAFE`, `UNSAFE` or `UNKNOWN` (the default). A failure conclusively before
dispatch may be retried. After possible dispatch, only `SAFE` may be replayed automatically.
`UNSAFE` and `UNKNOWN` surface an ambiguous result instead of risking a duplicate write. Method
spelling never changes this rule.

Use an `ExecutionPolicy` to narrow attempts or budgets for one operation:

<!-- tested: tests/execution_test.py::test_ambiguous_dispatch_never_retries_unproven_request -->
```python
from b24api import ExecutionPolicy

one_attempt = ExecutionPolicy(max_attempts_per_request=1)
result = await client.call(request, policy=one_attempt)
```

## Logical batch and correlation

`batch()` means an arbitrary-length logical command source. It incrementally consumes a synchronous
or asynchronous iterable and splits it into bounded physical Bitrix batches (at most 50 commands).
It does not materialize the input. The exact source iterator is closed on success, failure,
cancellation or early close.

Each `Command` carries arbitrary caller-owned correlation. Correlation is retained by reference,
never serialized to Bitrix, never used as a key and never included in safe diagnostics.

<!-- tested: tests/client_v2_test.py::test_logical_batch_is_unbounded_ordered_and_correlation_is_strictly_off_wire -->
```python
from b24api import Command, CommandSuccess, Request

commands = (
    Command(Request("example.item.get", {"id": item_id}, ReplaySafety.SAFE), correlation=item_id)
    for item_id in source_ids
)

async with client.batch(commands, batch_size=25) as stream:
    async for outcome in stream:
        assert isinstance(outcome, CommandSuccess)
        consume(outcome.correlation, outcome.result)
```

`batch()` is fail-fast. `batch_outcomes()` exhausts where safe and yields the closed union
`CommandSuccess | CommandFailure | CommandNotExecuted | CommandOutcomeUnknown` in input order.

<!-- tested: tests/client_v2_test.py::test_batch_outcomes_retains_typed_failure_without_halting_later_commands -->
```python
from b24api import CommandFailure, CommandNotExecuted, CommandOutcomeUnknown

async with client.batch_outcomes(commands) as stream:
    async for outcome in stream:
        match outcome:
            case CommandSuccess(correlation=correlation, result=result):
                consume(correlation, result)
            case CommandFailure() | CommandNotExecuted() | CommandOutcomeUnknown():
                handle(outcome)
```

For independently dispatchable commands use `fan_out()` or `fan_out_outcomes()` with
`DirectDispatch` or `BatchDispatch`. Delivery order is explicitly `READY` or `INPUT`.

## List traversal

The unsuffixed operation is the conservative strategy. Faster or more specialized mechanics have
explicit names and preconditions.

| Operation | Mechanics | Completion claim |
|---|---|---|
| `iter_list` | sequential offset/server `next`, empty confirmation | mechanics-only without identity; exact duplicate detection with identity |
| `iter_list_counted` | direct head plus physically batched fixed-offset tail | exact total, range and identity proof |
| `iter_list_keyset` | sequential no-count keyset | strict monotonic identity plus empty confirmation |
| `iter_list_cursor` | dependent cursor | strict unique monotonic cursor plus empty confirmation |

`page_size` is a local decoded page cap. It is sent to Bitrix only when the caller supplies the
endpoint's exact `limit_path`; the client never guesses a method-specific parameter.

### Sequential — canonical safe default

<!-- tested: tests/client_v2_test.py::test_iter_list_is_sequential_mechanics_only_and_report_is_post_cleanup -->
```python
from b24api import IdentitySpec, ResultSelector
from b24api.contracts import IdentityCoercion

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
assert stream.report is not None
```

Without `identity`, successful sequential exhaustion is deliberately reported as
`MECHANICS_ONLY`: pagination controls completed, but duplicate/substitution identity was not proven.

### Counted — old `list_batched` capability

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

Counted traversal requires identity. It validates the exact total, the observed head stride,
ownership of every tail range, duplicate identities and final emitted count. Contradiction produces
`IncompleteTraversalError`, never ordinary exhaustion.

### Keyset and cursor

<!-- tested: tests/client_v2_test.py::test_keyset_and_cursor_are_explicit_strict_alternatives -->
```python
from b24api import CursorSpec, KeysetSpec, ParameterPath

keyset_stream = client.iter_list_keyset(
    Request("example.item.list", replay_safety=ReplaySafety.SAFE),
    selector=ResultSelector(("items",)),
    identity=identity,
    keyset=KeysetSpec(
        filter_path=ParameterPath(("filter",)),
        order_path=ParameterPath(("order",)),
    ),
)

cursor_stream = client.iter_list_cursor(
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

The generic cursor requires unique strictly monotonic values. Repeated date boundaries cannot prove
that every tied row was returned; they fail closed and require an application-owned direct-call
workflow or a unique tie-breaker.

## Bound references

`Binding` applies exact parameter-path updates to one base request while preserving parent
correlation. It supports finite/unbounded sync and async sources, explicit direct/batch dispatch,
all four traversal contracts and zero-row completion.

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
        summary=f"parent {parent_id}",
        updates=(ParameterUpdate(ParameterPath(("filter", "PARENT_ID")), parent_id),),
        correlation=parent_id,
    )
    for parent_id in parent_ids
)

stream = client.iter_references(
    Request("example.child.list", replay_safety=ReplaySafety.SAFE),
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

`iter_reference_outcomes()` adds correlated `ReferenceFailure`, `ReferenceNotExecuted` and
`ReferenceOutcomeUnknown`. Identity tracking is scoped per binding, so equal child IDs under
different parents are not conflated.

## Streams, partial results and reports

Every multi-item operation returns an `OperationStream`. Canonical use is `async with`, because a
plain `break` does not close an arbitrary async iterator. After cleanup the stream permanently
exposes one immutable `OperationReport`; before termination `report` is `None`.

<!-- tested: tests/client_v2_test.py::test_partial_helper_closes_without_claiming_completion -->
```python
first = await client.iter_list(request).first()
page = await client.iter_list(request).collect(limit=100)

assert first.report.partial
assert page.report.partial
```

Helpers do not pull an extra row merely to prove exhaustion, so reaching a requested limit is
`EARLY_CLOSED`, not a false `COMPLETED`. Cancellation and cleanup preserve the primary exception and
publish the same final report object where the Python exception type permits it.

## Resource boundaries

`ExecutionPolicy` bounds physical requests, pages, elapsed time, attempts, decompressed response
bytes, buffered commands/rows, direct concurrency and active references. The default response
ceiling is 16 MiB and is enforced while streaming, before JSON decoding.

Sequential and counted exact traversal retain identities in memory for the operation lifetime.
There is no database, spill file or arbitrary identity-count refusal. Crossing 100,000 distinct
identities emits one `RuntimeWarning`; exact tracking continues. Strict keyset/cursor traversal keeps
only monotonic progression state when that is sufficient.

## CLI

The wheel installs `b24api`. Stdout is data only; list rows are JSONL. Reports and safe errors go to
stderr. Credentials are read only from `Settings`; there is no webhook command-line option.

<!-- tested-console: tests/cli_test.py::test_call_routes_replay_safety_and_keeps_success_data_on_stdout -->
```console
b24api call profile --replay-safety safe
b24api call example.item.get --params '{"id":7}' --raw
b24api list example.item.list --params @params.json
b24api list example.item.list --strategy counted --contract @counted-contract.json
```

Advanced list contracts are closed JSON `version: 1` documents. Counted requires identity; keyset
requires selector, identity and keyset; cursor requires selector and cursor. Unknown/duplicate keys,
trailing JSON and control collisions reject locally. `b24api --help` documents the compact surface.

Exit codes are `0` success, `2` usage/contract error, `3` unavailable configuration, `4`
remote/protocol/correctness/incomplete failure, `5` broken output consumer and `130` cancellation.

## v1.0.1 capability map

Old names are not kept as wrappers. Migrate by capability:

| v1 capability | v2 operation |
|---|---|
| `call()` | `call()` |
| `call(raw=True)` | `call_response()` |
| `retry=False` | `ExecutionPolicy(max_attempts_per_request=1)` |
| finite `batch()` | logical `batch(Command(...))` |
| payload tuples / `with_payload` | `Command.correlation` |
| tolerant/errors-yield batch | `batch_outcomes()` |
| `list_sequential` | `iter_list()` |
| `list_batched` | `iter_list_counted()` |
| `list_batched_no_count` / `list_keyset` | `iter_list_keyset()` |
| cursor list wrappers | `iter_list_cursor()` |
| independent request wrappers | `fan_out()` / `fan_out_outcomes()` |
| `reference_*` wrappers | `Binding` + `iter_references()` / `iter_reference_outcomes()` |
| low-level public plans/profiles | typed mechanics contracts beside the operation |

Automatic endpoint profiles, compatibility Pydantic entities, tuple payload conventions,
return-shaping booleans, permissive cursor deduplication and unsafe automatic direct fallback are
removed rather than deprecated.

## What v2 fixes—and what remains application-owned

The regression suite traces the bounded historical sample:

- `B1`: partial batch/reference work has closed correlated terminal variants;
- `B2`: unknown or unsafe possible-dispatch work is not replayed;
- `B3`: conflicting page controls reject instead of being silently replaced;
- `B4`: async binding sources, nested selectors and correlation are structural;
- `B5`: counted tail stride comes from validated in-band head evidence;
- `C1b`: item path, filter key and order key are independent;
- `C5`: repeated non-unique cursor boundaries fail closed;
- `C34` and `C35`: ignored/overmatching business filters are explicitly not claimed fixed.

The client can prove protocol, continuation, identity, duplicate, total, order, budget and lifecycle
facts that are visible on the wire. It cannot prove that a portal honored the business meaning of a
filter, select an application's composite storage key, or reconcile an ambiguous write. Applications
must validate expected business sets and verify writes where needed.

## Performance and profiling

The deterministic harness compares v2 counted traversal with an independent frozen 1.0.1
head-plus-batched-tail model on identical row identities and request observations. The profiling
runner records request counts, wall/CPU, first-row time, high-water counters and retained resources:

```console
uv run python tools/b24api_evidence/profile_runtime.py --samples 7 --warmups 2
```

Optional Memray captures measure allocations without becoming a runtime dependency:

```console
uv run --with memray python -m memray run -o /tmp/b24api.bin \
  tools/b24api_evidence/profile_runtime.py --case dense-10k --plan counted_batch --samples 1 --warmups 2
uv run --with memray memray stats /tmp/b24api.bin
```

Local deterministic timing is not live portal latency admission. A stable regression requires two
discarded warm-ups, seven measurements, unchanged request counts and an independent reproduction.

## Verification

Repository gates:

```console
uv sync --frozen
.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/ruff check . --no-fix --no-cache
.venv/bin/ruff format --check . --no-cache
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check
```

The built-wheel regression checks the `b24api = b24api.cli:main` entry point, includes only the
runtime library and excludes tests, live/evidence tooling and credentials.
