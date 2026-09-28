# Endpoint recipes for b24api 3.x

The complete executable scenario index is in [examples/README.md](../examples/README.md). It links
the 19 frozen public-API recipes to their independent offline oracles and records the evidence
boundary for the separate opt-in live fixtures.

The runtime remains method-agnostic. These recipes are caller-owned configurations for endpoint
contracts that have been verified separately; revalidate them against the portal and filter used by
your application.

## Fixed server stride

Use a fixed step only when the endpoint accepts absolute offsets but advances them by a server page
width instead of the number of decoded rows. If `total` is stable and exact for the same filter and
snapshot, qualify it explicitly:

```python
from b24api import RouteKind
from b24api import OffsetContinuation, OffsetSpec, ReplaySafety, Request, TotalTermination

stream = client.iter_list(
    Request("log.blogpost.get", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    page_size=50,
    offset=OffsetSpec(
        continuation=OffsetContinuation.FIXED_STEP,
        step=50,
        total_termination=TotalTermination.EXACT_QUALIFIED,
    ),
)
```

Without `EXACT_QUALIFIED` a fixed step has no closure witness after a short page. The client
treats `next` as non-canonical and completes, with `mechanics_only` assurance (`identity_exact` when
an `IdentitySpec` is declared), only when every page is a full step and a confirming empty page follows. After a short page it fails closed with
`IncompleteTraversalError` whose cause is `PaginationError("fixed-step traversal cannot prove closure
after a short page")` right after yielding that page, without requesting the next window: under a
rounded server stride a short page does not distinguish the end of the source from a skipped or
repeated window, so no answer could close it. Rows already yielded stay yielded; the report is not
exhausted.

For endpoints with drifting or known-inexact totals, choose a contract that can prove closure:

- qualify an exact total for a stable filter and snapshot (`TotalTermination.EXACT_QUALIFIED`);
- if the endpoint advances by the rows it returned, use `OffsetContinuation.OBSERVED_COUNT`, where an
  empty page after a short page is an ordinary terminal witness;
- for a page-number control, use `OffsetSpec(page_index=PageIndex(...))`, which accepts a short page
  followed by an empty page as closure;
- for a sparse selected result with a qualified raw extent, use `SparseRawBound` with `iter_list`
  from offset zero; reference traversal refuses it.

Scenario 17 in [examples](../examples/README.md) shows the fail-closed outcome.

For an endpoint whose declared contract is exactly "step `start` by a fixed window until a page is
shorter than that window" (no usable `total`, no `next`), declare that stop rule explicitly with
`short_page_termination` below.

## Declared short-page closure

Some endpoints honor `start` only at multiples of a fixed server window, report no usable total
(`booking.v1.booking.list` always answers `totalCount: 0`), send no `next`, and end when a page is
shorter than the window. `ShortPageTermination.DECLARED_TERMINAL` makes that stop rule a supported
opt-in for `FIXED_STEP`; without it a short page still fails closed as described above.

```python
from b24api import OffsetContinuation, OffsetSpec, ReplaySafety, Request, ResultSelector, RouteKind
from b24api.contracts import PageStride, ShortPageTermination, TraversalAssurance

BOOKING_PAGE = 50  # server-fixed: `start` is honored only at multiples of 50

BOOKING_OFFSET = OffsetSpec(
    # parameter_path stays the default ("start",); the endpoint has no limit parameter.
    continuation=OffsetContinuation.FIXED_STEP,
    step=BOOKING_PAGE,
    page_stride=PageStride(
        server_granularity=BOOKING_PAGE,
        wire_increment=BOOKING_PAGE,
        max_decoded_rows=BOOKING_PAGE,
    ),
    short_page_termination=ShortPageTermination.DECLARED_TERMINAL,
)

stream = api.iter_list(
    Request(
        "booking.v1.booking.list",
        {
            "filter": {"within": {"dateFrom": date_from, "dateTo": date_to}},
            "order": {"id": "ASC"},  # required: without order the row sequence is undefined
        },
        replay_safety=ReplaySafety.UNKNOWN,
        route=RouteKind.BARE,
    ),
    selector=ResultSelector(("booking",)),
    page_size=BOOKING_PAGE,
    offset=BOOKING_OFFSET,
    # policy=None -> default ConsistencyPolicy (IGNORE / NONE / TRAVERSAL_ONLY) is compatible.
)
async with stream:
    async for booking in stream:
        ...
report = stream.report
assert report is not None and report.successful and report.exhausted
assert report.assurance is TraversalAssurance.MECHANICS_ONLY
```

The declaration is strict, and everything outside it is refused before the first request:

- `page_stride` is required, and `step`, `wire_increment`, `max_decoded_rows` and `page_size` are one
  window of at least 2 rows; `total_termination`, `page_index` and `SparseRawBound` do not combine
  with it, and `CountedTraversal` cannot use it.
- The traversal starts at offset 0. Leave `start` out of the request (the client creates it as 0);
  any other value raises `CapabilityError`.
- The effective `ConsistencyPolicy` must keep `total_semantics=IGNORE`, `confirmation_policy=NONE` and
  `snapshot_requirement=TRAVERSAL_ONLY`; a total such as `totalCount: 0` is never interpreted.
- The client does not inspect `order`. For `booking.v1.booking.list` an explicit lowercase
  `{"id": "ASC"}` is part of the method profile: without it the row sequence is undefined, and an
  uppercase `ID` key is silently discarded.

Each page is judged by its row count after page adaptation, with `W` the window:

| Page | Outcome |
|---|---|
| `W` rows, `next` absent or equal to `start + W` | rows delivered, next window requested |
| `1..W-1` rows, no `next` | rows delivered, traversal closes without another request (`declared short page reached`) |
| 0 rows, no `next` | closes as an ordinary empty page; a source of exactly `k * W` rows still needs this page |
| more than `W` rows, `next` on a short or empty page, or any other `next` | page rejected with none of its rows; `IncompleteTraversalError`, not exhausted |

`exhausted=True` here means the declared stop rule was met, not that the snapshot is complete: the
report stays `mechanics_only` even with an `IdentitySpec`, because a short page does not prove that
later windows are empty. Do not use `exhausted` alone as the basis for deleting local records that
were absent from the export. A `PageStopPolicy` stop on a continuing full page is reported as a
bounded prefix; on the closing short page the natural closure stands.

The same `BOOKING_OFFSET` works in references:
`SequentialTraversal(selector=ResultSelector(("booking",)), page_size=BOOKING_PAGE, offset=BOOKING_OFFSET)`.
A binding may set parameters outside `start` (for example its own `filter.within` window); every
`ReferenceComplete` names its own `closure`, `DECLARED_SHORT_PAGE` or `SOURCE_EMPTY`, while the
aggregate report stays `mechanics_only`.

Qualify an endpoint before relying on this profile: on a portal with more than one window of data,
confirm a full page followed by a short one, an exact multiple of the window followed by an empty
page, a stable explicit order, and no pagination gaps against an independent listing. This is a
one-off qualification of the method, not an extra request of every traversal; an observation of a
single short page does not prove full-window behavior. Sibling endpoints such as
`booking.v1.resource.list`, `booking.v1.resourcetype.list` and `booking.v1.waitlist.list` need their
own selector, ordering and qualification; do not apply this recipe to them by analogy.

## Split keyset ordering

Some endpoints use separate flat sort-field and direction controls:

```python
from b24api import KeysetSpec, ParameterPath
from b24api.contracts import SplitOrderSpec

keyset = KeysetSpec(
    order_path=None,
    split_order=SplitOrderSpec(
        field_path=ParameterPath(("SORT",)),
        direction_path=ParameterPath(("ORDER",)),
        field_value="CONFIG_ID",
    ),
)
```

## Opt-in batched keyset execution

Use bounded execution only after verifying stable integer identities, both order directions,
strict numeric bounds, and the endpoint's page-completion behavior. Construction is I/O-free, but
the first pull completes the planning barrier before yielding rows.

<!-- tested: tests/keyset_fast_test.py::test_explicit_modes_match_sparse_ordered_oracle -->
```python
import os

from b24api import (
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    ParameterPath,
    RangeKeysetExecution,
    ResultSelector,
    StableIntegerKeysetContract,
)

identity = IdentitySpec(("id",), "ID", "id", IdentityCoercion.DECIMAL_STRING_INTEGER)
keyset = KeysetSpec(
    filter_path=ParameterPath(("filter",)),
    order_path=ParameterPath(("order",)),
)

if os.environ.get("ENV") != "PROD":
    # Accepting an ID filter does not prove strict bounds or ordering.
    await client.verify_keyset_capability(
        request,
        selector=ResultSelector.root(),
        identity=identity,
        page_size=50,
        keyset=keyset,
    )

stream = client.iter_list_keyset(
    request,
    selector=ResultSelector.root(),
    identity=identity,
    page_size=50,
    keyset=keyset,
    execution=RangeKeysetExecution(contract=StableIntegerKeysetContract()),
)
```

The guard returns only `VERIFIED`; unsupported and inconclusive observations raise
`KeysetCapabilityError` with the complete immutable report. Normal traversal sends no canaries and
does not remember that the guard ran.

## Correlated cursor traversal

Use one lazy binding source for one or many parents. Each binding may resume from an independent
strict cursor and keeps its caller-owned correlation off wire:

```python
from b24api import BatchDispatch, Binding, ParameterPath, ParameterUpdate, ResultSelector

bindings = (
    Binding(
        summary=f"parent {parent_id}",
        updates=(ParameterUpdate(ParameterPath(("PARENT_ID",)), parent_id),),
        correlation=parent_id,
        start_cursor=checkpoints.get(parent_id),
    )
    for parent_id in parent_ids
)

stream = client.iter_cursors(
    request,
    bindings,
    selector=ResultSelector(("items",)),
    cursor=cursor,
    dispatch=BatchDispatch(coalesce_wait=0.020),
)
```

Use `coalesce_wait=0` when per-wave latency matters more than physical batch density.

## Keyset references with per-owner filters

A keyset binding may set constant fields directly inside `KeysetSpec.filter_path`, beside the cursor
key the traversal writes there. `crm.item.productrow.list` has no global listing and needs both owner
fields on every page, while each owner advances its own `>id` cursor:

```python
from b24api import (
    BatchDispatch,
    Binding,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    ParameterPath,
    ParameterUpdate,
    Request,
    ResultSelector,
    RouteKind,
    SequentialKeysetExecution,
)

request = Request("crm.item.productrow.list", route=RouteKind.BARE)
traversal = KeysetTraversal(
    selector=ResultSelector(("productRows",)),
    identity=IdentitySpec(
        item_path=("id",), filter_key="id", order_key="id",
        coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
    ),
    page_size=50,
    keyset=KeysetSpec(
        filter_path=ParameterPath(("filter",)),
        order_path=ParameterPath(("order",)),
    ),
    execution=SequentialKeysetExecution(),
)
binding = Binding(
    "deal 7573",
    updates=(
        ParameterUpdate(ParameterPath(("filter", "=ownerType")), "D"),
        ParameterUpdate(ParameterPath(("filter", "=ownerId")), 7573),
    ),
    correlation=7573,
)

stream = client.iter_references(request, [binding], traversal=traversal, dispatch=BatchDispatch())
async with stream:
    async for event in stream:
        ...
```

Every owner gets its own `filter[>id]` and `order[id]=ASC`; one owner's cursor never appears in
another owner's requests. An update inside the keyset filter is admitted only when it is:

- exactly one level below the filter (not the filter itself, and no nested structure);
- a simple field name after an optional operator prefix such as `=`, `>=` or `!%` (not a number, and
  not `LOGIC`, `AND` or `OR`);
- a different field from the identity `filter_key`, compared case-insensitively whatever its operator
  (`id`, `>id`, `<=ID` and `=id` are all refused), and only while `filter_key` is itself a simple name;
- a JSON scalar or a flat list of scalars.

Any other update is refused locally before that binding sends anything: `iter_reference_outcomes()`
reports it as `ReferenceNotExecuted(LOCAL_VALIDATION_FAILED)` with its correlation while the other
bindings continue, and fail-fast `iter_references()` raises `ReferenceFailed`. Order, `start`, limit
and split-order controls stay exclusive as before. The base request keeps its own rules: a
non-mapping `filter`, or a base key equal to the managed cursor key (such as `>ID`), is refused with
`CapabilityError` before any binding is read, while other identity constraints such as `<=id` remain
allowed. `traversal_control_paths()` still lists the containers a traversal writes, including the
whole filter; it names those containers and does not decide which binding updates compose.
References still accept only `SequentialKeysetExecution` without a `KeysetSpec.boundary`, and the
recipe does not promise that every REST method treats arbitrary constant filters the same way.

## Mapping-backed collections

Use strict mapping values when the selected collection is always an ID-keyed object:

```python
from b24api import ResultSelector
from b24api.contracts import ResultCollectionShape

stream = client.iter_list(
    request,
    selector=ResultSelector(("items",)),
    collection_shape=ResultCollectionShape.MAPPING_VALUES,
)
```

Use `MAPPING_VALUES_OR_EMPTY` only for an endpoint proven to return a mapping on data pages and an
empty list at termination. A non-empty sequence remains a contract error, and the accepted terminal
degradation is recorded as a warning violation.

## Composite identity

Sequential and counted traversal can prove uniqueness using a tuple without inventing tuple order:

```python
from b24api import IdentityCoercion
from b24api.contracts import CompositeIdentitySpec, IdentityComponent

identity = CompositeIdentitySpec(
    (
        IdentityComponent(("entityTypeId",), IdentityCoercion.EXACT_INTEGER),
        IdentityComponent(("entityId",), IdentityCoercion.EXACT_INTEGER),
    )
)
```

Composite identities are intentionally unavailable for keyset and cursor traversal.

## Legacy positional list methods

Some legacy PHP methods bind arguments by position, not by name. `task.elapseditem.getlist` takes
`taskId, order, filter, select, params`; send all five slots with `PositionalArguments` and an
explicit `PositionalLayout`. A shorter request without the task slot can still return HTTP 200
while selecting a different scope, so a successful response does not prove the arguments were bound
as intended. The client has no endpoint catalog: it never reorders a named mapping by method name and
does not turn a server error into a method-specific hint.

The supported route is ordinary `iter_list()` paging through `NAV_PARAMS.iNumPage`. Check each row's
task scope in the application; scenario 13 in [examples](../examples/README.md) does this offline.

<!-- tested: tests/examples/elapsed_task_items_test.py::test_elapsed_task_items_recipe_uses_five_json_slots -->
```python
from b24api import OffsetSpec, ParameterPath, ReplaySafety, Request, RouteKind
from b24api.contracts import (
    EmptyArray,
    EmptyObject,
    PageIndex,
    PositionalArguments,
    PositionalLayout,
    Present,
    SlotContract,
    SlotShape,
)

page = ParameterPath((4, "NAV_PARAMS", "iNumPage"))
layout = PositionalLayout(
    "task.elapseditem.getlist.five.v1",
    (
        SlotContract("taskId", SlotShape.SCALAR, fixed=True),
        SlotContract("order", SlotShape.OBJECT),
        SlotContract("filter", SlotShape.OBJECT),
        SlotContract("select", SlotShape.ARRAY),
        SlotContract("params", SlotShape.OBJECT),
    ),
    control_paths=frozenset({page.path}),
)
arguments = PositionalArguments(
    (Present(42), EmptyObject(), EmptyObject(), EmptyArray(), Present({"NAV_PARAMS": {"iNumPage": 1}})),
    layout.layout_id,
    layout=layout,
)
stream = client.iter_list(
    Request("task.elapseditem.getlist", arguments, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    identity=identity,
    page_size=50,
    offset=OffsetSpec(parameter_path=page, page_index=PageIndex(page, max_rows=50)),
)
```

Bitrix physical batch cannot encode positional slots, so `iter_list_counted()` and the range,
partitioned, and default auto keyset executions reject positional requests before I/O.

An explicit sequential keyset profile is mechanically available when both object slots are present
and the layout declares the order leaf `(1, "ID")` and the filter leaf `(2, ">ID")` as writable
controls; those are the only leaves the traversal writes. The capability guard also writes `<ID` and
the exact `ID` filter leaves for its bound checks, so this layout declares them as well. The profile
is covered only by an offline regression of exact slot order and strict ID progression; it is not
qualified against a portal. Before relying on it, verify the task scope and the ID filter against an
independent oracle for your portal and keep `iter_list()` as the default.

<!-- tested: tests/positional_keyset_test.py::test_documented_positional_keyset_guard_verifies_offline -->
```python
import os

from b24api import (
    KeysetSpec,
    ParameterPath,
    ReplaySafety,
    Request,
    ResultSelector,
    RouteKind,
    SequentialKeysetExecution,
)
from b24api.contracts import (
    EmptyArray,
    PositionalArguments,
    PositionalLayout,
    Present,
    SlotContract,
    SlotShape,
)

layout = PositionalLayout(
    "task.elapseditem.getlist.five.keyset.v1",
    (
        SlotContract("taskId", SlotShape.SCALAR, fixed=True),
        SlotContract("order", SlotShape.OBJECT),
        SlotContract("filter", SlotShape.OBJECT),
        SlotContract("select", SlotShape.ARRAY),
        SlotContract("params", SlotShape.OBJECT),
    ),
    control_paths=frozenset({(1, "ID"), (2, ">ID"), (2, "<ID"), (2, "ID")}),
)
arguments = PositionalArguments(
    (Present(42), Present({}), Present({}), EmptyArray(), Present({"NAV_PARAMS": {"nPageSize": 50}})),
    layout.layout_id,
    layout=layout,
)
elapsed = Request("task.elapseditem.getlist", arguments, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)
keyset = KeysetSpec(
    order_path=ParameterPath((1,)),
    filter_path=ParameterPath((2,)),
    start_suppression_path=None,
)

if os.environ.get("ENV") != "PROD":
    # Accepting an ID filter does not prove strict bounds or ordering.
    await client.verify_keyset_capability(
        elapsed,
        selector=ResultSelector.root(),
        identity=identity,
        page_size=50,
        keyset=keyset,
    )

stream = client.iter_list_keyset(
    elapsed,
    selector=ResultSelector.root(),
    identity=identity,
    page_size=50,
    keyset=keyset,
    execution=SequentialKeysetExecution(),
)
```

A missing or empty-placeholder object slot, an undeclared control path, or near-match key casing fails
before I/O with `CapabilityError`; its message names the value-free reason and its cause keeps the
original positional control error.

## Form bodies, scoped headers, and binary responses

```python
from b24api import RouteKind
from b24api import Request
from b24api.contracts import BodyEncoding, RequestHeaders

form_request = Request(
    "socialnetwork.workgroup.creategroup",
    {"groupName": "Example", "viewMode": "closed", "avatarColor": "29AD49"},
    encoding=BodyEncoding.FORM_URLENCODED,
    route=RouteKind.BARE,
)

header_request = Request(
    "baas.serverport.lead.verificationack",
    headers=RequestHeaders({"X-Domain-Ack": "caller-owned-value"}),
    route=RouteKind.BARE,
)

download = await client.call_bytes(Request("crm.item.import.downloadexample", route=RouteKind.BARE))
write_file(download.body, media_type=download.content_type)
```

Form and scoped-header requests require an advertising wire transport and are rejected from physical
Bitrix batches before dispatch. `call_bytes()` is selected before dispatch and returns every
successful body byte-for-byte; it never acts as a JSON-decoding fallback.

## Development qualification for counted traversal

Before using optimized counted traversal for a method, run it and an independent sequential traversal
or frozen oracle against the same stable fixture. Compare row counts and a multiset digest of
canonical frozen-JSON row digests. When an identity is supplied, also compare the identity set and
uniqueness count. Persist configuration, aggregate counts, digests, and pass/fail only—not rows,
credentials, URLs, or parameters.
