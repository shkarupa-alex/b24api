# Endpoint recipes for b24api 2.x

The runtime remains method-agnostic. These recipes are caller-owned configurations for endpoint
contracts that have been verified separately; revalidate them against the portal and filter used by
your application.

## Fixed server stride

Use a fixed step only when the endpoint accepts absolute offsets but advances them by a server page
width instead of the number of decoded rows. If `total` is stable and exact for the same filter and
snapshot, qualify it explicitly:

```python
from b24api import OffsetContinuation, OffsetSpec, ReplaySafety, Request, TotalTermination

stream = client.iter_list(
    Request("log.blogpost.get", replay_safety=ReplaySafety.SAFE),
    page_size=50,
    offset=OffsetSpec(
        continuation=OffsetContinuation.FIXED_STEP,
        step=50,
        total_termination=TotalTermination.EXACT_QUALIFIED,
    ),
)
```

For endpoints with drifting or known-inexact totals, omit `total_termination`. The client then
requires a confirming empty page and treats `next` as non-canonical.

## Split keyset ordering

Some endpoints use separate flat sort-field and direction controls:

```python
from b24api import KeysetSpec, ParameterPath, SplitOrderSpec

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

stream = client.iter_list_keyset(
    request,
    selector=ResultSelector.root(),
    identity=identity,
    keyset=keyset,
    execution=RangeKeysetExecution(contract=StableIntegerKeysetContract()),
)
```

## Mapping-backed collections

Use strict mapping values when the selected collection is always an ID-keyed object:

```python
from b24api import ResultCollectionShape, ResultSelector

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
from b24api import CompositeIdentitySpec, IdentityCoercion, IdentityComponent

identity = CompositeIdentitySpec(
    (
        IdentityComponent(("entityTypeId",), IdentityCoercion.EXACT_INTEGER),
        IdentityComponent(("entityId",), IdentityCoercion.EXACT_INTEGER),
    )
)
```

Composite identities are intentionally unavailable for keyset and cursor traversal.

## Form bodies, scoped headers, and binary responses

```python
from b24api import BodyEncoding, Request, RequestHeaders

form_request = Request(
    "socialnetwork.workgroup.creategroup",
    {"groupName": "Example", "viewMode": "closed", "avatarColor": "29AD49"},
    encoding=BodyEncoding.FORM_URLENCODED,
)

header_request = Request(
    "baas.serverport.lead.verificationack",
    headers=RequestHeaders({"X-Domain-Ack": "caller-owned-value"}),
)

download = await client.call_bytes(Request("crm.item.import.downloadexample"))
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
