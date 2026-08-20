# Bitrix24 evidence harness

This directory is repository-only W9 tooling. It is excluded from the `b24api`
wheel and must be invoked from a reviewed checkout with:

```text
uv run python tools/b24api_evidence.py plan
uv run python tools/b24api_evidence.py seed
uv run python tools/b24api_evidence.py verify
uv run python tools/b24api_evidence.py benchmark
uv run python tools/b24api_evidence.py resume
uv run python tools/b24api_evidence.py cleanup
uv run python tools/b24api_evidence.py recover-manifest
```

`plan`, offline `verify`, and the deterministic model benchmark do not need a
webhook. `seed` and `cleanup` refuse unless both `--live` and `--allow-writes`
are present and `--plan` names a human-approved `approved_for_seed` plan.
`benchmark` is always read-only. Recovery is a two-step read-only exact-marker
scan: the first run writes a preview; only a second run with
`--confirm-recovery --recovery-preview-sha256 <exact-file-hash>` writes a
candidate manifest, and only if a fresh exact-marker scan matches that reviewed
preview byte-for-byte. Recovery never deletes.

## Credential boundary

The webhook is read only by `harness/live.py`, from
`BITRIX24_API_WEBHOOK_URL`, after the fingerprint key has passed validation.
`BITRIX24_EVIDENCE_FINGERPRINT_KEY` must be an unpadded 43-character base64url
encoding of exactly 32 random bytes. Evidence stores only the host, role,
portal build, scope hash, key-format assertion, and HMAC-SHA256 over the
canonical `(host, role, principal_id)` tuple. The principal ID and key are not
persisted. Low-diversity or low-entropy 32-byte values are rejected rather than
passing on length alone.

The reviewed disposable entity set is pinned by both
`w0-disposable-entities-v1` and SHA-256
`425cdca3d9f0682974c50afc9af4d4d3fa90dc6233ee785290ba7632bd30b754`.
The hard write ceiling is 500 entities per cell; a plan may lower it but cannot
raise it. The current lifecycle strategy is direct, and its request/quota
estimate includes preflight, create, create read-back, pre-delete ownership
read, exact-ID delete, and point-read absence verification.

## Recovery and artifacts

Dataset plans and evidence documents are strict Draft 2020-12 JSON Schema
objects plus semantic validation. Non-finite numbers, including `1e400`, are
rejected. Manifest records are append-only JSON Lines with contiguous sequence,
genesis iff `sequence == 0`, immutable lineage, previous-record links, and a
canonical SHA-256 record hash. An adjacent exclusive lock serializes chain
reload, validation, append, and `fsync`, so stale writers cannot add a second
genesis or sequence. Each marker is exactly
`<namespace>:<correlation-key>` and its hash is SHA-256 of UTF-8 marker bytes.

Non-manifest artifacts use same-directory temporary files, file `fsync`, atomic
replace, and directory `fsync`. Every persisted artifact and every tracked
repository file passes the leak scanner. Scanner diagnostics report a path,
never the matched value. The committed redaction fixtures are narrowly
allowlisted by exact fixture identity.

The deterministic model matrix exercises the production executor and traversal
stack over empty, 1, 19, 500, dense 10,000, sparse 10,000 below 10% selectivity,
clustered, skewed, deleted-ID, and persistent-mutation cases with both offset
and keyset plans. Persistent mutation is expected to be `INCONCLUSIVE` after
three attempts and records distinct independent pre/post hashes. Draft timing
thresholds are explicitly non-normative; live
admission must use preregistered interleaved controls and derived drift ratios.

No live command is part of ordinary pytest, and the wheel-content regression
proves that this directory, its credential loader, `seed`, and `cleanup` do not
ship in the library distribution.
