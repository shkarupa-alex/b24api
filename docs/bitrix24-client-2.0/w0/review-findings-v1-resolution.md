# Resolution of W0 review findings

This document resolves the two independent reviews of evidence SHA
`d760c438826271b941abee291696ed7eea5c352d`. That SHA and its packet remain
historical; the corrected evidence receives a new SHA.

## Blocking findings

| Finding | Resolution | Executable regression |
|---|---|---|
| Oracle allowed `PASS` with a changed or unverifiable snapshot. | Schema v1.1 requires qualified non-diagnostic evidence, non-null hashes for `PASS`, a verified state for requested snapshot guarantees, and `INCONCLUSIVE` after three persistent-churn attempts. The semantic validator additionally requires equal non-null independent pre/post hashes and `unique_count <= raw_count`. | `PASS changed snapshot`, `PASS with unequal pre/post hashes`, `PASS without hashes`, and `unique exceeds raw` are rejected. A persistent-churn `INCONCLUSIVE` example is accepted. |
| Blocking benchmark gates could be weaker than 15%, 1.05, and 1.10; `TBD-LIVE` could reach admission. | A blocking gate now requires improvement `>= 0.15`, paired interval exclusion, small p95 ratio `<= 1.05`, and operating ratio `<= 1.10`. `preregistered` requires numeric drift thresholds; `tbd_live` is valid only for `draft`, never `admission_ready`. | `weak blocking gates`, `preregistered TBD-LIVE`, and `admission with TBD-LIVE` are rejected; a draft TBD-LIVE plan is accepted. |
| Evidence artifact allowed contradictory success and incomplete benchmark metrics. | `PASS` requires `terminal_state=completed`; requested snapshot guarantees require `snapshot_state=verified`; successful benchmark artifacts require `case_id`, `oracle_verified`, a frozen manifest hash, controls, and the complete benchmark metric set including `overlap`. Final artifacts cannot use `not_started`. Non-benchmark commands use a separate minimal operation-metric shape rather than fabricated zeros. | `PASS incomplete`, `PASS changed snapshot`, `benchmark caller asserted`, `benchmark missing case`, and `benchmark missing overlap` are rejected; a cleanup operation artifact is accepted. |
| Lineage could not bind oracle/artifact to dataset, manifest, or dirty corpus. | Oracle, benchmark, manifest, and evidence schemas now carry `lineage_id`, dataset/manifest content hashes as applicable, committed corpus SHA, and dirty recipe-tree SHA-256. Benchmark case references are content-addressed and must match the root dataset lineage. Manifest records add a canonical record hash and previous-record hash. | `cross-lineage dataset ref`, `missing lineage id`, `missing dirty corpus hash`, and an incorrect manifest record hash are rejected. |
| `safe_*` accepted arbitrary nested secret-bearing values and the packet overclaimed schema guarantees. | `safe_error` and `safe_violations` are closed typed structures; evidence refs are content hashes or relative paths; all free text is bounded and rejects known credential forms. `validate_contracts.py` recursively scans every string. The inventory now explicitly states that schemas alone do not prove redaction; W1 redaction and W9 leak scanning remain mandatory. | Secret-bearing error, note, evidence ref, and violation cases are rejected. Repository webhook-pattern scan remains separate. |
| Corpus size was internally inconsistent. | Scope is now explicit: 1,255 recipe files plus 2 top-level Python files, 1,257 total. | Counts were reproduced from the current read-only corpus. |

## Additional review findings

| Finding | Resolution |
|---|---|
| `format: uuid` was annotation-only. | All UUIDs use an exact anchored pattern; degenerate UUID and namespace cases are rejected. |
| Namespace could be 36 dashes and was unrelated to `run_id`. | Schema requires the full UUID shape; semantic validation requires exact `b24api-evidence-{run_id}` equality. |
| Duplicate cell/case IDs remained possible. | Semantic validation rejects duplicates by `id`; JSON `uniqueItems` is no longer presented as sufficient. |
| Manifest `created` could omit identity; ambiguous dispatch could omit request fingerprint. | Event-conditional schema rules require identity/marker for owned states and request fingerprints for dispatch/ambiguity states. |
| `known_irreversible_effects` was an unusable zero-length field. | It was removed. `cleanup.feasible` remains `true`; an irreversible entity family cannot produce a valid dataset plan. |
| Preview allowed live access without explaining intent. | This is now explicit: preview may inspect a live portal read-only, while `allow_writes=false`; only reviewed `approved_for_seed` permits writes. |
| Baseline and candidate used different Python versions. | Original HEAD was remeasured on the project pin CPython 3.12.10 with the same pytest/Ruff/mypy/httpx versions as the candidate; results remain 54 pass, 3 Ruff findings, and 24 mypy errors. |
| Probe had no schema, ambiguous runner SHA, mismatched command names, and undocumented fingerprint. | A committed environment-only runner and sixth schema were added. The probe was repeated from runner commit `4d9cdf83b4b55571cbc8375cb712198856502b8f`; source hash, Python/httpx versions, exact request keys, bounded response, and HMAC algorithm are recorded. |
| Public host retention was unclear. | Host-only metadata is intentionally retained per specification section 16.3; webhook path/token never are. |
| Dirty diff hash lacked a reproducible command. | Inventory records Git 2.55.0 and the exact locale/config/path command that reproduces the hash. |
| Corpus census omitted imports. | Added 1,246 root `Bitrix24` import files, 647 `b24api.error` import files, and zero helper-class mentions. |

## Validator command

```text
uv run --with jsonschema python docs/bitrix24-client-2.0/w0/validate_contracts.py --self-test
```

The current suite accepts 9 positive contracts and rejects 29 negative
contracts. Cross-field validation is normative; passing JSON Schema alone is
not sufficient for evidence admission.
