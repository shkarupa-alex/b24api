# W0 v2 review resolution

This ledger resolves the second pair of independent reviews against corrected
W0 v2. Contract versions are now 1.2. The prior v1 resolution remains historical
evidence rather than being rewritten.

| Finding | Resolution | Executable regression |
|---|---|---|
| Benchmark PASS admitted correctness shortfall, overfetch, unresolved reference failures, and blocking violations. | PASS requires all three counters to be zero. Violations are typed with `warning` or `blocking`; a PASS cannot contain a blocking violation. | Four independent invalid PASS cases are rejected; a warning-only PASS is accepted. |
| Approved live-write methods were arbitrary strings. | Dataset cells name a profile from the content-addressed reviewed disposable allowlist and must exactly match its family, create/read/delete methods, marker field, and scopes. | Reversed `crm.deal.delete/get/add` is rejected. |
| Ambiguous or dispatched creates could omit reconciliation data. | Both events require a namespace-derived marker value, its hash, and the request fingerprint. Marker values are retained because a hash alone cannot drive exact-marker read-back. | `ambiguous` and `create_dispatched` without markers are rejected. |
| Benchmark PASS could omit every drift observation or retain quarantined drift. | All four PASS drift observations are non-null numbers and `drift_quarantined=false`; recursive validation additionally requires every float to be finite. | Missing controls and quarantined PASS are rejected. |
| The CLI accepted Python's non-JSON `NaN` and `Infinity`. | All JSON input uses `parse_constant` rejection, and a recursive `math.isfinite` guard protects programmatically constructed documents. | Parser `NaN`, metric `NaN`, and metric `Infinity` are rejected. |
| Probe PASS did not constrain dependent row count. | PASS requires exactly one dependent row and exact equality with the profile identity. | Row counts 0 and 7 are rejected. |
| Content-address refs were not tied to adjacent hashes. | Dataset and oracle refs must equal `sha256:` plus their respective content hashes. | Both mismatch forms are rejected. |
| Dataset estimates could understate requested writes. | `estimated.entities` covers the sum of `max(target_count, base_count)` and each cell is bounded by the explicitly reviewed per-cell ceiling. | Understated totals and ceiling violations are rejected. |
| Manifest chains lacked a genesis anchor. | `sequence == 0` iff `previous_record_hash` is null. | Both directions are rejected independently after recomputing the record hash. |
| Admission needed only one blocking benchmark case. | Every `admission_ready` case must be blocking. | Mixed/advisory admission is rejected. |
| A blocking comparison could contain one plan. | Blocking cases require at least two distinct compared plans. | A one-plan blocking case is rejected. |
| Portal HMAC fingerprint duplicated public host metadata. | The HMAC input is the canonical tuple `(host, role, principal_id)`; the ID and key are discarded and no token-derived value is used. | Schema and probe artifact require algorithm `hmac-sha256-portal-role-principal-v1`. |
| Runner discarded unexpected command-error keys. | Expected keys are recorded and unexpected keys are counted. Because Bitrix serializes its PHP array as `[]` when empty and as an object when it has associative command keys, exactly empty `[]` or an object is accepted; a nonempty JSON array is malformed. Either malformed shape or any error blocks PASS. | Unexpected-count and malformed-envelope PASS cases are rejected. |
| `identity_mode=none` could retain an identity hash; empty exact results were ambiguous. | `none` requires a null identity hash. Exact empty boundary results remain intentionally valid when the qualified hashes and snapshot invariants hold. | Non-null `none` identity is rejected; explicit empty oracle PASS is accepted. |
| Secret residuals were not concrete W1 inputs. | W1 receives bare token, JSON `auth`, and `AUTH_ID`/`APPLICATION_TOKEN` dump forms. W0 already detects the latter two; W1 must implement context-aware bare-token redaction and W9 performs final leak scanning. | JSON auth and application-token strings are rejected in W0; bare-token coverage remains an explicit W1 gate. |
| Exact Python patch wording relied on a minor-line selector. | The ignored local `.python-version` is not treated as evidence. Every W0 command and artifact names exact CPython 3.12.10. | Validation, probe, pytest, Ruff, and mypy evidence records the exact version or invocation. |
| Unexpected runner failures could render sensitive exception data. | A top-level safe entrypoint catches unexpected exceptions and writes only a fixed message. | Source review and strict static checks cover the fixed-message path. |

The live probe was rerun from committed runner SHA
`cf153551640934a215a465959831a401a83358ff`. It observed one matched dependent
row, a recognized PHP-array envelope, no known or unexpected command errors,
and retained no ID, response body, webhook path, token, or HMAC key.
