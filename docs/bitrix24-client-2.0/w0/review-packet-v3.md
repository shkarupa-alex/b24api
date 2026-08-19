# W0 cross-review packet v3

## Immutable subject

- Corrected evidence SHA: `8df8b98bbe93c82f7ab24d414ecd5996e1754606`
- Reviewed probe-runner SHA: `cf153551640934a215a465959831a401a83358ff`
- Contract hardening SHA: `2bc6c6702ef08a9049f6ae9d49b7dcccd9da8468`
- Prior v2 evidence SHA: `d162970da03efdb319010ce9875e42f634acd27e`
- Original baseline SHA: `08277c4d921b83b9252177b3e72a21a4c0c86109`
- Branch: `codex/bitrix24-client-benchmarks`
- This packet commit is documentation-only and does not alter the evidence SHA.

Review only the second-review corrections:

```text
git diff d162970da03efdb319010ce9875e42f634acd27e..8df8b98bbe93c82f7ab24d414ecd5996e1754606
```

Review the complete corrected W0:

```text
git diff 08277c4d921b83b9252177b3e72a21a4c0c86109..8df8b98bbe93c82f7ab24d414ecd5996e1754606
```

## Resolution map

`review-findings-v2-resolution.md` maps every explicit P1/P2 and every
reproducible additional finding to a contract change and regression. In
particular:

- benchmark PASS is impossible with correctness shortfall, blocking violations,
  missing drift controls, quarantined drift, `NaN`, or `Infinity`;
- live-write plans bind exact methods/scopes/markers to a content-addressed
  disposable-entity allowlist and cannot understate or exceed reviewed scale;
- create dispatch and ambiguity retain a usable exact marker plus hash;
- manifest genesis, content refs, admission gates, and comparison cardinality
  are machine-enforced;
- the probe requires exactly one matched row, recognizes PHP empty-array versus
  associative-object serialization, and blocks malformed or unexpected errors;
- the portal fingerprint distinguishes host/role/principal without retaining an
  ID or deriving anything from a token;
- the ignored minor-line Python selector is no longer described as an exact pin;
  evidence commands explicitly select CPython 3.12.10.

## Checks run

| Check | Result |
|---|---|
| Draft 2020-12 schema check + semantic regression suite | 7 schemas valid; 12 positive accepted; 56 negative rejected. |
| Disposable allowlist instance validation | Passed; content SHA-256 `425cdca3d9f0682974c50afc9af4d4d3fa90dc6233ee785290ba7632bd30b754`. |
| Corrected live probe artifact validation | Passed. |
| Probe source hash against runner commit | Exact `8bda6eae912a956871c9106e1ed209751823a1db4e09b4a6e3cdb92c9f8bab75`. |
| W0 runner/validator Ruff 0.15.12 | Passed. |
| W0 runner/validator mypy 1.20.2 `--strict` | Passed. |
| Current candidate pytest on CPython 3.12.10 | 73/73 passed in 2.00s. |
| Full current-tree Ruff 0.15.12 | Passed. |
| Production mypy 1.20.2 | Passed for 11 source files. |
| W0 webhook/token URL pattern scan | Passed. |
| Pre-existing dirty-file SHA-256 verification | All 8 hashes unchanged. |

The original baseline remains the previously reproduced 54/54 pytest pass, 3
Ruff findings, and 24 mypy errors on CPython 3.12.10.

## Corrected live observation

At `2026-08-19T20:06:59Z`, the committed runner made one read-only request with
the `admin_full` role. The response had HTTP 200, the expected Bitrix/PHP
`result_error=[]` empty representation, no structured or command errors, and
exactly one dependent `user.get` row matching the `profile` identity.

The artifact contains no ID, response body, webhook path, token, or HMAC key.
Its fingerprint is keyed over canonical `(host, role, principal_id)` and has
`authorization_effect: none`. The observation proves only the recorded query
shape and authorizes no traversal plan or live write.

## Assumptions to challenge

1. JSON Schema plus the committed semantic validator is the admission contract;
   JSON Schema alone remains explicitly insufficient.
2. The two current disposable profiles are safe only when their exact tuple,
   content hash, reviewed plan SHA, scale ceiling, and cleanup contract all match.
3. The keyed host/role/principal fingerprint provides local linkage without
   retaining an ID or token-derived fingerprint.
4. For Bitrix/PHP batch errors, exactly empty JSON `[]` and an associative JSON
   object are the only accepted wire forms; nonempty arrays are malformed.
5. Preview is read-only. Nothing in W0 authorizes seeding; writes still require a
   concrete separately reviewed plan and explicit user approval.
6. CPython 3.12.10 is controlled by exact command arguments and artifact fields,
   not by the ignored local `.python-version` minor selector.

## Known gaps

- W1 must add canonical, context-aware redaction tests for bare tokens, JSON
  `auth`, and `AUTH_ID`/`APPLICATION_TOKEN` dumps; W9 owns the final bundle and
  repository leak scan.
- W7 still needs executable characterization against corpus consumers.
- No concrete dataset plan, manifest, oracle, endpoint performance profile,
  optimization, or performance threshold instance is approved.
- Original static failures remain to be repaired without weaker configuration.
- The specification and corpus remain dirty user-owned inputs locked by hashes.

## Decisions requested

Approve or reject corrected W0 v3:

1. schema 1.2 PASS/correctness/drift invariants and strict finite JSON handling;
2. content-addressed disposable-entity allowlist and scale authorization model;
3. manifest reconciliation/genesis and benchmark lineage/admission invariants;
4. corrected PHP-aware, principal-distinguishing read-only probe;
5. proceeding to W1 and W2.

Approval does not authorize live writes, retry/batch measurements, traversal
defaults, optimizations, regressions, or release. Each remains behind its later
blocking checkpoint.
