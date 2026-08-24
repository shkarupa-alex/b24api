# W9 evidence harness implementation

The evidence harness lives only under `tools/b24api_evidence/`; setuptools
includes only `b24api*`. The normative entry point remains
`uv run python tools/b24api_evidence.py`.

## Mandatory obligation closure

| W9 obligation | Executable closure |
|---|---|
| Pin disposable entity allowlist by ID and reviewed content hash. | Runtime and dataset schema require `w0-disposable-entities-v1` plus `425cdca3...b754`; edited allowlists fail before plan/live setup. |
| Prevent plan-self-raised scale. | Reviewed constants cap the whole plan at 500 entities, each cell at 500, and the plan at two unique disposable profiles/cells; schema and semantic validation enforce every ceiling plus any lower plan ceiling. |
| Conservative non-empty estimates. | Generated direct plans report the complete nominal seed/verify/cleanup lifecycle as `7 * entities + 6` requests: two preflight calls per command, create/read-back, two independent verification reads, and ownership-read/delete/absence-read. Retry and ambiguity paths can only increase recorded actuals. Validation retains `5 * entities + 4` as the minimum acceptable legacy-plan floor. |
| Bind marker hash/value/correlation. | Marker is exactly `<namespace>:<sha256(run, cell, index)>`; `marker_hash` is SHA-256 over UTF-8 marker bytes. Both are verified on every manifest read/append. |
| Frozen and independent PASS snapshot hashes. | Oracle schema/validator require verified state and equal non-null pre/post SHA-256 values. Persistent changed state after three tries is only `INCONCLUSIVE`. |
| Every PASS rejects blocking violations; cleanup proves absence. | Artifact schema and semantic validator apply the blocking rule to every command. Cleanup metrics are closed and require `orphan_count == 0` plus `absence_verified == true`. |
| Benchmark lineage and drift. | Every benchmark artifact requires `benchmark_plan_content_hash`, four positive finite controls, preregistered maximum ratios, derived observed ratios, and derived quarantine. |
| Correctness metric algebra. | `duplicates == raw_rows - unique_rows`; PASS also requires zero shortfall, overfetch, and reference failures plus oracle assurance. |
| Immutable oracle/result binding. | Oracle records carry separate expected and actual result hashes; PASS requires equality. Parent evidence references stable oracle contents by SHA-256 rather than mutable paths. |
| Draft thresholds are non-normative. | Draft plans require `thresholds_normative: false`; admission-ready plans require preregistered drift and the 15% / 1.05 / 1.10 blocking gates with at least two distinct plans. |
| PHP-aware probe shape. | `result_error` must be present and is recorded as `empty_array` or `associative_object`; missing/non-empty arrays/other shapes refuse. Exact success is `dependent_ids == [who_id]`. |
| Strong fingerprint key. | The only accepted format is 43-character unpadded base64url decoding to exactly 32 bytes, with minimum byte diversity and Shannon entropy. It is validated before the webhook environment variable is read. |

## Artifact and recovery guarantees

Plans, manifests, oracles, benchmark plans, and evidence artifacts have strict
Draft 2020-12 schemas and an additional semantic validator. RFC JSON parsing
rejects `NaN`, `Infinity`, and numeric overflow such as `1e400`. Safe nested
structures have no arbitrary extension point.

Normal artifacts use file and directory `fsync` plus same-directory atomic
replace. Manifest JSONL appends one canonical record at a time with contiguous
sequence, genesis iff sequence zero, immutable run/lineage/plan/portal/candidate
fields, previous-record SHA-256, and current-record SHA-256.
An adjacent exclusive lock covers chain reload, validation, append, and fsync,
so a stale concurrent writer cannot append a second genesis or sequence.

Multi-file evidence publication holds one non-blocking OS lock for the whole
artifact directory. Its fixed pending marker is a durable ownership journal:
before each canonical write it records the relative path, predecessor JSON, and
the exact SHA-256 of the value being written. A concurrent publisher therefore
refuses before mutation. The reserved marker is recognized by its lexical
directory entry before file-type filtering and must be a private, single-link
regular file; dangling links and directory links remain fail-closed. After a
crash, the next bundle-bound command acquires the released OS lock and rolls
back only paths whose current hash is either the journaled write or the
already-restored predecessor. Any foreign content or failed restore retains the
marker and keeps bundle scanning fail-closed. A successful terminal scan
removes the marker as the commit point; an exception
delivered after that unlink reports an operational error without rolling back
the already committed bundle. Journal paths retain their lexical atomic-replace
identity; symlinks in a canonical path are rejected before mutation and during
recovery, including symlinked artifact-directory components. The persistent lock
is opened without following symlinks and accepted only as a single-link regular
inode with mode `0600`. It is included in bundle secret and size accounting and
is the sole permitted residue when retrying `plan` after a complete rollback or
pre-publication refusal.

Resume validates the complete chain, its per-correlation lifecycle state machine,
planned cell membership/cardinality, and exact run, lineage, plan content,
portal fingerprint, candidate SHA, and namespace. An ambiguous create never
causes a blind retry: resume first performs paginated exact-marker read-back. Cleanup
also reconciles ambiguous/dispatched creates by exact marker before it can claim absence. It
first point-reads the exact manifest-owned ID and verifies its marker, then
deletes only that ID and verifies absence. A reused/mismatched ID is recorded as
an orphan and is never deleted.

Lost-manifest recovery derives the finite expected marker set from the reviewed
plan. The first invocation writes only a preview. A second explicit
`--confirm-recovery` must bind the exact preview file SHA-256 and writes a
candidate manifest only when a fresh exact-marker scan has the same candidate
set; recovery itself never deletes.

Human plan review hashes the complete proposed authorization while projecting
both post-decision fields, `approved_at` and the circular `plan_review_sha`, to
null. The review commit therefore binds run, lineage, portal identity, cells,
the lower operational entity ceiling, and every other executable field without
claiming that approval has already occurred. After the explicit human response,
the harness operator records that response time and review commit in the final
schema-valid plan. Its full-content SHA-256 is then supplied separately to the
write command as an execution confirmation; it is not represented as a value
the reviewer saw before deciding.

## Offline evidence and live boundary

Live dataset identity is bound to the normalized portal host plus the existing
HMAC fingerprint over `(host, credential_role, principal_id)`. The host is the
stable portal name; the fingerprint distinguishes the reviewed role/principal
without persisting either the principal ID or a credential.

The documented `app.info.VERSION` value is the installed application version,
not the portal build. No `app.info` field is admitted as portal-build evidence
until Bitrix24 documents that meaning. In particular, `VERSION`, `version`,
`BUILD`, and `build` are ignored for this purpose. Dataset plans preserve the
unknown state as `portal.build: null` instead of substituting `LICENSE`, a
response header, an inferred release, or a synthetic value. A null build does
not block correctness plan/seed/verify work, but it keeps every build-scoped
endpoint profile inapplicable and cannot authorize `PROFILE_VERIFIED` claims.

This nullable field matches the preregistered W0 dataset-plan contract. Portal
identity and cleanup ownership do not depend on build; they remain bound to the
host, HMAC fingerprint, scope hash, candidate, run, lineage, and exact manifest
identities.

The first approved CRM lifecycle exposed the portal's actual post-delete
absence envelope: HTTP 400 with an empty `error` value and exact
`error_description: "Not found"`, rather than `ERROR_NOT_FOUND`. The live
boundary now normalizes only the complete observed tuple — method
`crm.deal.get`, HTTP 400, empty code, and exact description — to its internal
typed `error_not_found`. Other methods, statuses, empty codes, or descriptions
remain correctness failures. Positive and neighboring negative wire
regressions are committed.

The deterministic portal drives the production executor and traversal engine
over empty, 1, 19, 500, dense/sparse/clustered/skewed/deleted 10,000-row, and
persistent-mutation states with offset and keyset plans. The sparse case has
10,000 matches over more than 100,000 base identities. Mutation produces
distinct independent pre/post hashes through actual model mutation and three retries, ending `INCONCLUSIVE`;
every stable model oracle has equal hashes and PASS.

The offline matrix preregisters a deterministic latency model of 1 ms per
request. Its `wall_seconds` and first-row fields are normalized model values,
not sampled host-clock performance: every observation is recomputed exactly
during bundle validation, so timings cannot be redistributed while preserving
only aggregate sums. Measured wall-clock evidence remains a live-benchmark
obligation and this draft model matrix cannot authorize a live speed claim.

Ordinary pytest has no path that supplies both live/write flags. `seed` and
`cleanup` require both flags, reject ordinary pytest, and require a plan with explicit human approval, exact
candidate SHA, exact HMAC portal identity, reviewed profile tuple, scope and
`app.info` preflight. Live verify performs two independent full point-read snapshots and
quarantines persistent change. Live benchmark currently refuses as unavailable until a reviewed
live benchmark cell exists; it never falls back to model evidence. No live command has been run in
the pre-live candidate. The subsequently approved small lifecycle and its
cleanup recovery are recorded without overclaim in
`live-dataset-execution-8d76a07.md`.

The leak scanner covers every tracked repository file and every artifact while
reporting paths only. It narrowly recognizes committed dummy redaction fixtures.
The wheel-content gate rejects any `tools/`, credential loader, seed, cleanup,
or live harness module in the distributable library.
