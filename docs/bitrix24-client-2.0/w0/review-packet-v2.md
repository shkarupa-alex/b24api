# W0 cross-review packet v2

## Immutable subject

- Corrected evidence SHA: `d162970da03efdb319010ce9875e42f634acd27e`
- Reviewed probe-runner SHA: `4d9cdf83b4b55571cbc8375cb712198856502b8f`
- Rejected evidence SHA: `d760c438826271b941abee291696ed7eea5c352d`
- Original baseline SHA: `08277c4d921b83b9252177b3e72a21a4c0c86109`
- Branch: `codex/bitrix24-client-benchmarks`
- This packet commit is documentation-only and does not alter the evidence SHA.

Review corrections only:

```text
git diff d760c438826271b941abee291696ed7eea5c352d..d162970da03efdb319010ce9875e42f634acd27e
```

Review the complete corrected W0:

```text
git diff 08277c4d921b83b9252177b3e72a21a4c0c86109..d162970da03efdb319010ce9875e42f634acd27e
```

## Resolution map

`review-findings-v1-resolution.md` maps every blocking and additional finding
from both reviews to its schema/code change and executable regression. The
important corrections are:

- oracle PASS/snapshot/hash invariants;
- immutable 15%, 1.05, and 1.10 blocking gates and admission-blocking TBD-LIVE;
- coherent successful evidence reports and complete benchmark metrics;
- lineage, manifest, dataset, committed-corpus, and dirty-corpus hashes;
- typed bounded error/violation structures plus recursive secret scanning;
- exact 1,257-file corpus and import census;
- pinned CPython 3.12.10 baseline with tool versions;
- a committed, separately schematized, HMAC-fingerprinted probe runner.

## Checks run

| Check | Result |
|---|---|
| Original HEAD pytest on pinned CPython 3.12.10 | 54/54 passed in 2.53s. |
| Original HEAD Ruff 0.15.12 | Same 3 pre-existing findings. |
| Original HEAD mypy 1.20.2 | Same 24 pre-existing errors. |
| Current dirty candidate pytest on CPython 3.12.10 | 73/73 passed in 2.03s. |
| Full current-tree Ruff | Passed. |
| Production strict mypy | Passed for 11 source files. |
| W0 runner/validator strict mypy | Passed for 2 files. |
| Draft 2020-12 schema check + semantic regression suite | 6 schemas valid; 9 positive accepted; 29 negative rejected. |
| Corrected live probe artifact validation | Passed. |
| Probe source hash against runner commit | Exact match. |
| W0 webhook/token pattern scan | Passed. |
| Pre-existing dirty-file SHA-256 verification | All 8 hashes unchanged. |

## Corrected live observation

The `admin_full` read-only probe again observed working `$result[...]` batch
substitution. The artifact records no IDs, response bodies, path, token, or HMAC
key. It records public host metadata intentionally, as required by specification
section 16.3. The observation only falsifies the universal separate-round-trip
assumption and has `authorization_effect: none`.

## Assumptions to challenge

1. JSON Schema plus the committed semantic validator is the preregistered
   admission contract; JSON Schema alone is explicitly insufficient.
2. Artifact-level lineage fields are sufficient inputs for W9 to enforce
   cross-file lineage and content-hash equality before resume or admission.
3. HMAC-SHA-256 keyed portal fingerprints provide stable local linkage without
   exposing a reversible token-derived fingerprint; the host itself is intended
   public metadata.
4. Preview may access the live portal read-only, but no state except a reviewed
   `approved_for_seed` plan may set `allow_writes=true`.
5. Original committed HEAD remains the behavior baseline; dirty code remains
   candidate material until adopted by a later reviewed work package.

## Known gaps

- W7 still needs executable characterization against corpus consumers.
- W9 must implement bundle-level lineage checking, canonical W1 redaction, and
  the final repository/artifact leak scanner; W0 only preregisters them.
- No concrete dataset plan, manifest, oracle, endpoint profile, optimization, or
  performance threshold instance is approved.
- Original static failures remain to be repaired without weaker configuration.
- The specification and corpus are dirty user-owned inputs locked by hashes.

## Decisions requested

Approve or reject this corrected W0 packet:

1. corrected symbol/import/callsite census and dirty-tree dispositions;
2. six structural schemas plus the normative semantic validator;
3. lineage and redaction responsibility split between W0, W1, and W9;
4. corrected batch-chaining observation with no authorization effect;
5. proceeding to W1 (redaction/errors) and W2 (immutable models/policies).

Approval does not authorize live writes, retry/batch measurements, traversal
defaults, optimizations, regressions, or release. Each remains behind its later
blocking checkpoint.
