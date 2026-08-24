# Bitrix24 client 2.0 final compatibility ledger

This W11 ledger freezes the release-candidate public surface against the
original committed client at `08277c4d921b83b9252177b3e72a21a4c0c86109`
and the preregistered W0 inventory. Executable snapshots in
`tests/facade_test.py` are authoritative when prose and runtime disagree.

## Root exports

`b24api.__all__` is frozen to exactly these eleven names, in this order:

```python
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
```

Plans remain in `b24api.plans`; reports, policies, and supporting enums remain
in `b24api.models`. `list_keyset` is a facade method but not a root export.

## Frozen facade signatures

The release candidate preserves the committed positional inputs and adds only
the reviewed keyword-only bridges shown below:

```text
Bitrix24(settings=None)
call(request, *, raw=False, policy=None, retry=None)
batch(requests, *, batch_size=None, list_method=False, with_payload=False, policy=None)
batch_outcomes(requests, *, batch_size=None, policy=None, fallback_failed="none")
iter_list(request, *, plan, selector=ResultSelector.root(), identity=None, policy=None)
iter_reference(request, bindings, *, plan, dispatch,
               selector=ResultSelector.root(), identity=None,
               output_order=READY, tolerant=False, policy=None)
fan_out(requests, *, dispatch, output_order=READY, tolerant=False, policy=None)
list_sequential(request, *, list_size=None, plan=None, profile=None,
                identity=None, policy=None)
list_batched(request, *, list_size=None, batch_size=None, plan=None,
             profile=None, identity=None, policy=None)
list_batched_no_count(request, *, id_key="ID", list_size=None,
                      batch_size=None, plan=None, profile=None,
                      identity=None, policy=None)
reference_batched_no_count(request, updates, *, id_key="ID", list_size=None,
                           batch_size=None, with_payload=False, plan=None,
                           profile=None, identity=None, policy=None)
reference_cursor_no_count(request, updates, *, cursor_param="LAST_ID",
                          cursor_field="id", cursor_take="max",
                          list_size=None, list_size_param="LIMIT",
                          batch_size=None, result_key=None,
                          with_payload=False, plan=None, profile=None,
                          identity=None, policy=None)
list_keyset(request, *, identity, plan=None,
            selector=ResultSelector.root(), policy=None)
```

The signature regression additionally freezes parameter kinds, annotations,
return annotations, compatibility defaults, and constructor settings injection.

## Preserved compatibility surface

| Surface | Final disposition |
|---|---|
| `Bitrix24` construction, `aclose`, async context management, and `host` | Preserved. The host is credential-free. |
| `call(..., raw=False)` | Preserved. `policy` and the temporary replay-safe `retry` bridge are keyword-only. |
| `batch` | Preserved fail-fast order, list-method flattening, payload correlation, and sync/async sources. |
| Five committed list/reference wrappers | Preserved signatures and yield shapes; all delegate to the shared engines. |
| `Request.query`, `ListRequest`, `ListRequestParameters` | Preserved through canonical immutable request translation. |
| `Response`, `Response.list_result`, `ResponseTime` | Preserved import and valid-envelope behavior. |
| `ErrorResponse`, `BatchResult` | Preserved module-level compatibility values. |
| `ApiResponseError`, `RetryApiResponseError`, `RetryHTTPStatusError` | Preserved import paths and aliases. |
| `Settings`, `ApiSettings`, `api_settings` | Preserved environment-backed configuration surface. |
| `build_query`, `ApiTypes` | Preserved import paths and behavior. |
| Legacy helper classes | Remain importable but are not root exports or a second traversal engine. |

## Deliberate 2.0 corrections

The following are reviewed corrections, not compatibility promises to preserve
the former unsafe result:

- incomplete traversal raises a typed error carrying the final report instead
  of ending as successful generator exhaustion;
- request replay depends on explicit replay safety and failure phase;
- counted pagination validates stride, total, ranges, continuation, and every
  returned page before completion;
- keyset and cursor traversal validate identity, order, progress, duplicates,
  and bounded budgets;
- batch and reference streams own their sources and finish deterministic
  cleanup on close, failure, or cancellation;
- structured errors, settings, transport failures, and evidence diagnostics use
  the shared redaction boundary;
- `batch_outcomes` and tolerant reference output use disjoint correlated
  success/failure values rather than polymorphic legacy modes.

Rejected prototype APIs (`errors=`, `reference_batch`, automatic OR rewriting,
hidden strategy selection, and `Response.items()`) are not release contracts.

## Defaults, profiles, and evidence

The packaged endpoint profile set is the empty JSON array. No profile-derived
plan, optional optimization, or live performance claim is a default. Explicit
plans remain available at caller-asserted assurance.

The live evidence harness is repository-only and excluded from wheels. Portal
identity uses the normalized host plus the HMAC role/principal fingerprint.
The current preflight has no documented exact portal build, so plans preserve
`portal.build: null`; this does not block correctness evidence, but it keeps all
build-scoped profiles inapplicable.

The first two small live plans passed their separate human approval checkpoint.
Tasks stopped `INCONCLUSIVE` on one ambiguous create and its harness cleanup
proved absence with zero orphans. CRM seeded and independently verified five
exact-marker deals; cleanup exposed the portal's actual post-delete absence
tuple. An exact-manifest operator recovery removed the remaining four deals
with zero marker mismatches, and the fixed adapter subsequently classified all
five IDs absent in read-only live replay. The original CRM run is deliberately
not represented as a harness cleanup PASS. Full sanitized accounting is in
`../w9/live-dataset-execution-8d76a07.md`.

No performance regression is accepted by this ledger. Deterministic model
coverage is a correctness gate, not a live latency claim. W10 supplied no
accepted optional candidate.

## Executable freeze gates

- `test_facade_signature_snapshot_preserves_committed_and_keyword_bridges`;
- `test_root_export_snapshot_and_legacy_import_paths`;
- `test_compatibility_wrappers_contain_no_pagination_engine`;
- complete facade wire/yield/failure characterization;
- strict mypy over `b24api` and the evidence tool;
- wheel-content regression excluding `tools/b24api_evidence` and live tooling;
- full deterministic, fixture, cancellation, socket, profile, artifact, and
  compatibility suite.
