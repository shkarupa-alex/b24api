# API client for Bitrix24

A correctness-first asynchronous Bitrix24 REST client. Version 2.0 uses one
replay-aware executor for direct calls, batches, pagination, and reference
fan-out. Traversal strategies are immutable plans; incomplete traversal never
looks like normal generator exhaustion.

## Current status

The 2.0 correctness release is ready for the reviewed call, batch, counted-list,
keyset-list, and reference traversal contracts. It restores HTTP/2 with HTTP/1
fallback, keeps retries conditional on replay safety, bounds decoded rows and
identity tracking, and raises a typed incomplete result instead of silently
returning a partial list.

The default `list_batched` has structural performance parity with the frozen
1.0.1 head-plus-batched-tail implementation: 19, 500, and 10,000-row model
selections take 1, 2, and 5 physical requests in both clients and return the
same identities. A read-only portal replay on 24 August 2026 also found no
statistically stable wall-time difference: the median paired new/old ratios
were 1.010 for 19 Tasks, 0.993 for 19 CRM deals, 1.013 for 932 Tasks, and 0.999
for 1,060 CRM deals; all bootstrap intervals included parity.
The complete methodology and measurements are in
[`runtime-profile-2026-08-24.md`](docs/bitrix24-client-2.0/w12/runtime-profile-2026-08-24.md).

This is not a claim that the new validation engine is free. On a zero-latency
local model it uses roughly 4-6x the CPU of the minimal 1.0.1 traversal while
checking totals, ranges, identities, budgets, correlation, and immutable
evidence. Memray 1.20.0 measured about 4.52 MiB peak allocation for a 10,000-row
ID-only traversal (2.52 MiB for the frozen baseline), 7.98 MiB for the live
932-row full Tasks response, and 5.93 MiB for the live 1,060-row full CRM
response. Twenty consecutive synthetic 10,000-row traversals peaked at 4.90
MiB versus 4.52 MiB for one traversal, with no accumulating-memory signature.

The release does not claim normative performance admission, a fast generic
no-count traversal, or an admitted `PartitionedKeysetPlan`. The safe generic
`list_batched_no_count` fallback can make more requests than 1.0.1 because the
old strategy was observed returning an incomplete Tasks selection as if it
were complete.

## Installation

The package requires Python 3.12 or newer. Until the reviewed 2.0 candidate is
published, install this checkout rather than `pip install b24api` (PyPI still
serves the 1.0.1 predecessor):

~~~text
uv sync --frozen
~~~

## Configuration and lifecycle

Set BITRIX24_API_WEBHOOK_URL and keep one client open for related work:

~~~python
from b24api import Bitrix24

async with Bitrix24() as b24:
    profile = await b24.call({"method": "profile"})
~~~

The webhook is never included in exception text or reports. The host property
returns only the configured host. Calling aclose explicitly is also supported.

## Requests and retries

Mapping requests contain method and optional parameters and replay_safety.
Unknown top-level keys are rejected. Canonical Request values make replay
intent explicit:

~~~python
from b24api import Request
from b24api.models import ReplaySafety

read = Request("crm.item.get", {"id": 7}, ReplaySafety.SAFE)
write = Request("crm.item.add", {"fields": {"TITLE": "Example"}}, ReplaySafety.UNSAFE)
~~~

call returns the decoded result. With raw=True it returns an immutable Response
including total, next, time, and redacted HTTP evidence.

~~~python
result = await b24.call(read)
response = await b24.call(read, raw=True)
~~~

retry=False limits the call to one attempt. retry=True enables only retries that
are permitted by the request replay safety and the ExecutionPolicy. UNKNOWN and
UNSAFE writes are not repeated after a failure that may have reached the server.

## Batch execution

batch is fail-fast, preserves input order, and accepts bounded or unlimited
synchronous and asynchronous inputs:

~~~python
async for result in b24.batch(requests, batch_size=25):
    consume(result)
~~~

Use with_payload=True for (request, payload) inputs. Use list_method=True only
for the committed compatibility flattening of a list or one-key list envelope.
The returned stream owns the supplied iterator: closing the stream early also
closes that exact sync or async iterator and runs its cleanup. It cannot be
resumed afterwards.

For independent commands that must each receive a correlated result, use the
separate tolerant API:

~~~python
from b24api import BatchFailure, BatchSuccess

async for outcome in b24.batch_outcomes(requests):
    if isinstance(outcome, BatchSuccess):
        consume(outcome.result)
    else:
        handle(outcome.error)
~~~

There is no errors= mode on batch. The explicit split keeps fail-fast and
tolerant return types unambiguous. The codec accepts the observed Bitrix/PHP
batch polymorphism: an empty associative map may arrive as JSON [] while a
non-empty map arrives as a JSON object.
Decoded array results are charged by their top-level length against the row
buffer. An empty array has row weight zero, while command and outcome ceilings
still apply and the portal batch limit remains 50 commands.

## Explicit traversal

New multi-page code supplies a canonical plan:

~~~python
from b24api import IdentitySpec, Request, ResultSelector
from b24api.models import IdentityCoercion
from b24api.plans import KeysetPlan

identity = IdentitySpec(
    item_path=("id",),
    filter_key="ID",
    order_key="id",
    coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
)

stream = b24.iter_list(
    Request("tasks.task.list"),
    plan=KeysetPlan(),
    selector=ResultSelector(("tasks",)),
    identity=identity,
)

async with stream:
    async for item in stream:
        consume(item)

print(stream.report)
~~~

Plans are exported from b24api.plans. Policies and reports are exported from
b24api.models. Every stream owns a deterministic final OperationReport. If a
legacy list/reference wrapper reaches FAILED, INCOMPLETE, CANCELLED, or a budget
ceiling, it raises IncompleteTraversalError with that same final report.

fan_out handles already independent ReferenceRequest values. iter_reference
binds ReferenceBinding updates to a base request and applies one explicit list
plan with DirectDispatch or BatchDispatch. Tolerant reference mode yields
ReferenceItem or ReferenceFailure instead of losing correlation.

## Compatibility wrappers

The committed wrappers remain callable and delegate to the same engine:

- list_sequential uses OffsetSequentialPlan.
- list_batched preserves the committed direct-head plus batched-tail counted
  traversal and verifies every in-band range, continuation, total, and supplied
  identity before normal completion.
- list_batched_no_count uses exact sequential KeysetPlan.
- reference_batched_no_count uses per-reference keyset traversal with
  BatchDispatch.
- reference_cursor_no_count uses ItemCursorPlan with BatchDispatch.
- list_keyset is a thin public KeysetPlan wrapper and is intentionally not a
  root export.

Each wrapper accepts only the committed arguments plus the keyword-only bridges
plan, profile, identity, and policy. Resolution is explicit plan, then explicit
profile, then the deterministic wrapper default. plan and profile together
refuse before I/O. A raw profile also refuses when the facade has no exact
portal-build/scope applicability context; it never guesses those facts.

The compatibility `list_size` value is enforced as a caller-declared decoded
page cap. The cursor wrapper also sends it through its committed `LIMIT`
control. Other wrappers do not invent an endpoint-specific limit parameter.
The default decoded-row buffer is bounded at 2,500 rows: one complete Bitrix
batch of 50 commands at the committed 50-row page cap. Smaller caller policy
limits continue to narrow batching and fail closed before an oversized page is
accepted.

The deterministic model compares the counted wrapper with an independent
frozen 1.0.1 head-plus-batched-tail implementation and reports request parity.
Its offset comparison is diagnostic efficiency context, not a latency win over
the predecessor and not performance admission. A fast generic no-count
partition remains unadmitted; `list_batched_no_count` deliberately keeps its
correct sequential keyset fallback.

For a counted endpoint, pass an identity when completeness and duplicate
checks must be explicit:

~~~python
from b24api import Bitrix24, IdentitySpec, Request
from b24api.models import IdentityCoercion

deal_identity = IdentitySpec(
    item_path=("ID",),
    filter_key="ID",
    order_key="ID",
    coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
)

async with Bitrix24() as b24:
    async for deal in b24.list_batched(
        Request("crm.deal.list", {"select": ["ID", "TITLE"], "order": {"ID": "ASC"}}),
        identity=deal_identity,
    ):
        consume(deal)
~~~

Compatibility wrappers fail closed. Code that wants to retain the terminal
report can catch the typed error explicitly:

~~~python
from b24api.errors import IncompleteTraversalError

try:
    async for deal in b24.list_batched(request, identity=deal_identity):
        consume(deal)
except IncompleteTraversalError as error:
    record_terminal_report(error.report)
    raise
~~~

## Reproducing performance and memory profiles

The repository-only runner compares production counted traversal with the
independent frozen 1.0.1 algorithm on the same deterministic portal. It never
uses a webhook:

~~~text
uv run python tools/b24api_evidence/profile_runtime.py --samples 25 --warmups 5
~~~

To capture only measured samples, excluding imports and warmups, use the
official [Bloomberg Memray](https://github.com/bloomberg/memray) profiler. The
version used for the figures above was 1.20.0:

~~~text
uv run --with memray==1.20.0 python tools/b24api_evidence/profile_runtime.py \
  --case dense-10k --plan counted_batch --samples 1 --warmups 3 \
  --memray-output /tmp/b24api-dense-10k.bin
uv run --with memray==1.20.0 memray stats /tmp/b24api-dense-10k.bin
uv run --with memray==1.20.0 memray flamegraph /tmp/b24api-dense-10k.bin
~~~

These local timings measure Python overhead with a zero-latency portal. They
must not be presented as network-latency admission or as proof of a faster
production portal.

ItemCursorPlan requires cursor values on continued pages to be unique and
strictly monotonic in the declared direction. Under that contract, min/max are
compatibility aliases of first/last, not additional traversal semantics. The
profile-authorized cursor-exhausted terminal delivers a final page only when
all cursor values are missing or null; mixed present and missing cursor values
fail before any row from that page is emitted.

Read wrappers set SAFE only when replay_safety was not supplied. An explicitly
UNSAFE request is never upgraded. When a reviewed profile is selected, its
replay-safety value is applied exactly and any explicit conflict refuses before
I/O.

Legacy wrappers retain the historical one-key result fallback. Canonical
iter_list remains exact: ResultSelector.root() means the root itself must be a
list. Use an explicit selector for nested or multi-key envelopes.

## Endpoint profiles

`b24api.profiles` loads immutable, versioned profile JSON, derives value-free
`QueryShape` values, and exposes the pure `choose_plan` and `explain_plan`
functions. Applicability requires an exact query shape, known portal build,
required scopes, unexpired reviewed evidence, and a compatible execution
policy. Unknown build, expired evidence, or a mismatched shape refuses the
profile; callers may still choose an explicit plan at caller-asserted assurance.

Runtime probes are contradiction checks only. Missing, inconclusive, oversized,
or contradictory observations can downgrade a decision but can never promote
one to profile-verified assurance. The core package intentionally ships with an
empty default profile set until a live evidence package passes review.

## Authorized 2.0 corrections

The compatibility suite snapshots these deliberate corrections:

- identity and parameter-casing validation;
- counted-offset stride correction;
- non-advancing continuation and ignored-offset detection;
- contract-qualified short-page handling;
- duplicate detection and classification;
- structured normalized errors with redacted request summaries;
- replay-safe retry behavior;
- incomplete range detection and typed refusal instead of silent omission.

The reviewed batch wire uses integer halt values and stable correlation keys.
These are internal wire-shape changes inherited from the accepted execution
foundation; yielded values and command order remain compatible.

Dirty prototype features such as errors=, reference_batch, automatic OR
splitting, hidden strategy selection, and Response.items() are not public 2.0
contracts.

## Public imports

The root package exports exactly:

~~~python
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
~~~

For one major compatibility release, b24api.entity keeps ListRequest,
ListRequestParameters, ErrorResponse, and BatchResult importable and aliases
Request, Response, and ResponseTime to the canonical immutable models.
b24api.type.ApiTypes and b24api.query.build_query also remain importable.
