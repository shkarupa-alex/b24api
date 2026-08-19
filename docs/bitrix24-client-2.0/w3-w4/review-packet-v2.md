# W3/W4 review packet v2 — corrected execution instrumentation

## Decision requested

Review immutable implementation SHA
`5bec599ef1e47e375ada56f67c856f4c11963719` and either accept it as the
instrumentation foundation for W5/W6, or return findings against that exact
SHA.

The previous subject `8f280728...` is rejected and superseded. No retry, rate,
or batch measurement may authorize a strategy or default until this v2
checkpoint is accepted.

## Immutable subject and lineage

- Replacement implementation SHA: `5bec599ef1e47e375ada56f67c856f4c11963719`
- Rejected implementation SHA: `8f280728fc9ff1a9287cb77109d863e8a63ccfef`
- Original packet commit: `2f98cb47fc15845fcab86ee78b0147da1d851175`
- W2 acceptance SHA: `82f2d9ddafa50e5229c4191b66b0ad6db4ad4600`
- W2 implementation SHA: `abe42e097b81052fdca5cd6873ef065a1e422d2f`
- Branch: `codex/bitrix24-client-benchmarks`

Review only the remediation:

```bash
git diff 8f280728fc9ff1a9287cb77109d863e8a63ccfef..5bec599ef1e47e375ada56f67c856f4c11963719 -- b24api
```

Review the complete W3/W4 implementation:

```bash
git diff 82f2d9ddafa50e5229c4191b66b0ad6db4ad4600..5bec599ef1e47e375ada56f67c856f4c11963719 -- b24api
```

The resolution mapping is in `review-findings-v1-resolution.md`.

## Corrected behavior

### Permit cancellation

A future granted by `_grant_locked()` owns one active slot even if its task has
not resumed. Cancellation now distinguishes that state from a still-queued
future and returns the granted slot under the same condition lock before
admitting more work. Capacity therefore cannot decay through grant/cancel
races.

### Conservative transport phases

Trace evidence may strengthen a phase to connection, headers, or partial body.
The absence of trace evidence may not weaken an exception whose class implies
possible dispatch:

- connect/connect-timeout/pool-timeout may remain pre-dispatch;
- write/write-timeout, read/read-timeout, remote-protocol, and unclassified
  transport errors are at least `DISPATCH_STARTED`;
- `UNKNOWN` and `UNSAFE` requests therefore never replay those failures.

### PHP batch maps

Bitrix/PHP map fields use one symmetric rule:

- `{...}` is an associative command map;
- `[]` is an empty command map;
- a non-empty JSON list is malformed.

Consequently an all-failed response with `result: []` and populated
`result_error: {...}` retains every original command error and its fallback
eligibility. Batch `halt` is encoded as integer `1`/`0`.

### Totals, sources, and accounting

- `total=-1` is preserved as the known Bitrix sentinel so W5 can apply
  `IGNORE` or `ADVISORY` semantics. Values below `-1` become typed errors with
  HTTP evidence.
- Early stream close closes the actual sync or async source iterator and runs
  its cleanup.
- Permit-wait expiry counts no physical attempt. After the final elapsed check,
  a reserved attempt flows directly into bounded transport invocation;
  dispatched cancellation remains counted.

## Immutable clean-checkout gates

The following results were produced from a detached worktree containing only
`5bec599ef1e47e375ada56f67c856f4c11963719`, with no protected overlay:

```bash
git worktree add --detach /tmp/b24api-w3w4-v2 5bec599ef1e47e375ada56f67c856f4c11963719
cd /tmp/b24api-w3w4-v2
uv run --python 3.12.10 pytest -q
uv run --python 3.12.10 pytest -q b24api/execution_test.py b24api/batch_test.py
uv run --python 3.12.10 ruff check --no-fix \
  b24api/error.py b24api/models.py b24api/execution.py \
  b24api/execution_test.py b24api/batch.py b24api/batch_test.py
uv run --python 3.12.10 mypy \
  b24api/error.py b24api/models.py b24api/execution.py \
  b24api/execution_test.py b24api/batch.py b24api/batch_test.py
git diff --check
```

| Clean-SHA check | Result |
|---|---|
| Full pytest | 115/115 passed in 2.97s. |
| Focused W3/W4 pytest | 31/31 passed in 0.28s. |
| Scoped Ruff, six W3/W4 files | Passed. |
| Scoped strict mypy, six W3/W4 files | Passed. |
| `git diff --check` | Passed. |

The clean full-tree static commands are intentionally not labelled passing.
They reproduce the same inherited baseline debt already recorded in W0
`inventory.md`: three Ruff findings in committed `b24api/api_test.py` and 24
mypy `union-attr` findings on its raw-response assertions. The W2 parent
`82f2d9d...` produces those same 3/24 findings; W3/W4 add none. Adopting the
protected facade/test overlay or resolving the committed compatibility tests is
W7 work.

## Supplemental protected-overlay observation

The main working tree is not presented as an immutable SHA. It is a separately
identified W0-protected overlay. With the seven files at these exact SHA-256
values, the current combined tree produced 134/134 pytest, full-tree Ruff
success, and strict mypy success for 23 source files:

| Protected file | SHA-256 |
|---|---|
| `README.md` | `e0978c6ca3b0f91ef698d8b09cb0a5312663808045d4fc2f8f9811a337dd4e8f` |
| `b24api/__init__.py` | `cf888ecafe019990ede729d8efc107541be95de5c9715f95bd4846dd3990f88f` |
| `b24api/api.py` | `721779832ed359b79db5375e9a050807bb8c84998dae36a23ce3bcac2034aa0f` |
| `b24api/api_test.py` | `b001009a4895db56a5d27197ee4fb5d52de252cc0d7df6e214126e86bb14c01e` |
| `b24api/entity.py` | `f9b2086eb731a38cda7d867035921dbe77e18e8d6eab3c34ad8fd90c38aa479b` |
| `b24api/helper.py` | `87076e6f54d0027597096d0ea7bad190c4c32a6aa44bfdd8eb41c71a9f24759b` |
| `b24api/entity_types.py` | `d0d7a052c78d0a4a365f21da278e0ecb5c2549220954f3e451a765a16268a757` |

These overlay numbers are compatibility information only. They are not
attributed to the clean SHA and are not needed to reproduce its W3/W4 gates.

## New regression matrix

The nine additional focused cases cover:

- deterministic cancellation after permit grant and subsequent capacity reuse;
- no-trace unsafe read failure with exactly one attempt;
- no-trace read, write, remote-protocol, and generic transport classification;
- `total=-1` preservation and typed rejection below the sentinel;
- all-failed PHP `result: []` in tolerant and fail-fast modes, including direct
  fallback through that shape;
- cleanup of original infinite sync and async sources after early close.

The original socket, cooldown, fairness, budget, correlation, malformed-map,
and bounded-input cases remain present.

## Sanitization, assumptions, and remaining gaps

- Tests use synthetic bodies, a custom in-memory HTTPX transport, and loopback
  ephemeral sockets only.
- No supplied webhook, portal host, token, principal, or live row was used or
  committed.
- Exception class semantics are trusted for connect failures: HTTPX
  `ConnectError`, `ConnectTimeout`, and `PoolTimeout` prove that application
  request dispatch did not begin. All less-specific failures fall closed.
- HTTPX body-start tracing remains deliberately conservative: replay safety is
  already post-dispatch regardless of the exact number of response bytes.
- Proactive governance, chunk shrinking, automatic dispatch, pagination, and
  reference scheduling remain unauthorized.
- W7 still owns facade integration and the inherited committed static-test
  debt. W9 obligations remain open.

## Dependent decisions

Acceptance authorizes W5/W6 to depend on this corrected instrumentation. It
does not authorize a traversal, reference, batch, retry, rate, prefetch, or
automatic-dispatch default; those retain their own evidence and review gates.
