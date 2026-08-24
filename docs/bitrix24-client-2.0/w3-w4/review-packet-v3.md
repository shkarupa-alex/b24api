# W3/W4 review packet v3 — typed success-contract failures

## Decision requested

Review replacement implementation SHA
`521f0eb7cb107ec948c693496154f94e57dbf7c9` and either accept W3/W4 as the
instrumentation foundation for W5/W6, or return findings against that exact
SHA.

The v2 subject remains accepted with respect to its previous permit, PHP-map,
transport-phase, total-sentinel, source-cleanup, and gate-lineage findings. This
packet asks for focused review of the remaining success-decoder blocker and its
effect on tolerant-batch correlation.

## Lineage and diff

- v3 implementation SHA: `521f0eb7cb107ec948c693496154f94e57dbf7c9`
- v2 implementation SHA: `5bec599ef1e47e375ada56f67c856f4c11963719`
- v2 packet commit: `9747d671023f0299c2d91ffcf3bf110049243049`
- rejected v1 implementation SHA: `8f280728fc9ff1a9287cb77109d863e8a63ccfef`

```bash
git diff 5bec599ef1e47e375ada56f67c856f4c11963719..521f0eb7cb107ec948c693496154f94e57dbf7c9 -- b24api
```

The remediation changes only:

```text
b24api/batch_test.py     | 21 insertions
b24api/execution.py      | 50 lines changed
b24api/execution_test.py | 30 lines changed
3 files changed, 82 insertions(+), 19 deletions(-)
```

## Corrected invariant

After a complete successful HTTP response is received, no JSON/model contract
violation may escape as an untyped exception:

1. JSON parsing and envelope presence are checked;
2. safe `ResponseEvidence` and `RequestSummary` are captured;
3. scalar total/next shapes are checked;
4. `ResponseTime` and `Response` are constructed inside a typed boundary;
5. their `TypeError`, `ValueError`, or `OverflowError` becomes `ProtocolError`
   with the same safe HTTP evidence.

This includes finite-number checks performed by deep immutable result storage,
so JSON `1e400` cannot bypass `parse_constant` by becoming Python infinity.

At batch chunk scope, that `ProtocolError` is a `B24ApiError`. Fail-fast
execution raises it. Tolerant execution synthesizes one `BatchFailure` for each
unresolved command and therefore preserves total correlation.

## New counterexamples

- `{"result":[],"next":-1}` → typed `ProtocolError`, status 200 evidence;
- `{"result":1e400}` → typed `ProtocolError`, status 200 evidence;
- negative `time.duration` → typed `ProtocolError`, status 200 evidence;
- batch command result `1e400` → correlated `BatchFailure`, command index zero,
  operational stream completion.

The dead `_PhaseTracker.observed` state noted by review was removed.

## Clean immutable-SHA gates

Executed in a detached, clean worktree at exactly the v3 SHA with CPython
3.12.10:

| Check | Result |
|---|---|
| Full clean pytest | 119/119 passed in 2.91s. |
| Focused W3/W4 pytest | 35/35 passed in 0.29s. |
| Scoped Ruff, six W3/W4 files | Passed. |
| Scoped strict mypy, six W3/W4 files | Passed. |
| `git diff --check` | Passed. |

Full-tree clean Ruff/mypy still reproduce only the disclosed inherited W2
baseline debt: three Ruff and 24 mypy findings in committed
`b24api/api_test.py`.

The W0-protected overlay is supplemental, not attributed to the SHA. With its
seven recorded hashes unchanged, the combined tree passes 138/138 tests,
full-tree Ruff, and strict mypy for 23 files.

## Review follow-ups and non-authorizations

- `total=-1` remains preserved data, not an endpoint capability claim. W5 must
  attach provenance before relying on it.
- Early-close ownership of caller iterators must be documented and
  characterized during W7 integration.
- These obligations are recorded in
  `docs/bitrix24-client-2.0/w5-w7/review-obligations.md`.
- No live webhook or portal data was used.
- No strategy default, prefetch, pagination, proactive rate governance, or
  automatic dispatch is authorized by this packet.
- W9 obligations remain open.
