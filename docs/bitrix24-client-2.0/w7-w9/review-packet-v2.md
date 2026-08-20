# W7-W9 adversarial remediation review packet

## Blocking decision

Review exact immutable candidate
`31c60563e70e90f392f4e817c56c4b6f68b8ae58` and either accept it as the
corrected pre-live W7-W9 foundation or return prioritized findings against that
SHA.

Acceptance does not authorize a webhook, live access, writes, cleanup, W10,
profiles/defaults, or a performance claim. The packet commit is documentation
only and is not the review subject.

## Inputs

- branch: `codex/bitrix24-client-benchmarks`;
- rejected predecessor: `e5ef566bcc1e3d3c3b8669170ee185a2c85e97b1`;
- review subject: `31c60563e70e90f392f4e817c56c4b6f68b8ae58`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

Review in a detached clean worktree. Do not use live webhooks or inspect
credential-bearing Git remote URLs.

```bash
git diff e5ef566bcc1e3d3c3b8669170ee185a2c85e97b1..31c60563e70e90f392f4e817c56c4b6f68b8ae58 -- \
  b24api tests tools/b24api_evidence
```

## Findings addressed

### P1

1. A live-write plan can no longer authorize itself. `seed` and `cleanup`
   require external CLI confirmation of both the exact review SHA and canonical
   plan SHA-256. The review SHA must resolve to a Git commit containing an exact
   `Dataset-Plan-SHA256: <hash>` trailer.
2. Live scope lists require non-empty strings. Scope maps require non-empty
   string keys and literal boolean values. `app.info` build values reject
   booleans and other hostile semantic types; live mutation admission also
   refuses a missing build.
3. Tracked-tree cleanliness is rechecked immediately before every exact
   `adapter.create` and `adapter.delete`. Deterministic race regressions mutate
   the cleanliness state after plan loading/preflight and prove zero mutation
   calls.

### P2

1. The counted head uses actual remaining physical/logical budget after
   retries, before first-row emission.
2. Partial batch page reservations are appended transactionally and all
   acquired reservations roll back if later admission is cancelled.
3. Legacy string defaults use private `str` sentinels. Public signatures and
   values stay unchanged while explicit default-valued arguments are
   distinguishable from omission. Explicit identity/cursor controls must match
   either an explicit plan or selected profile before I/O.
4. Profile query-shape derivation uses the selected keyset/partition plan's
   declared nested filter and order paths.
5. All evidence anchors in one profile must share one candidate SHA. That SHA
   is retained in every profile-backed terminal report in addition to artifact
   hashes.
6. Direct-head/batched-tail traversal moved into
   `PaginationDriver.counted_batch_pages`. The facade retains only compatibility
   value emission and report translation; it performs no requests or offset
   scheduling.

### P3

The head-retry regression now uses an actually retryable HTTP 503 `text/plain`
gateway response and asserts two physical attempts, one retry, and zero emitted
rows before budget refusal.

## Exact gates

Executed twice: once in the protected main tree with a clean tracked overlay,
then in detached clean worktree `/private/tmp/b24api-31c6056.a0iHMu` at the
exact candidate.

| Gate | Exact candidate result |
|---|---:|
| Full pytest | 373 passed |
| Ruff `check . --no-fix` | Passed |
| Ruff `format --check .` | 41 files already formatted |
| strict mypy | 25 source files, passed |
| `git diff --check` | Passed |

The main worktree's pre-existing untracked `b24api/entity_types.py` and
`spec/...` paths were not staged, modified, deleted, or used as candidate
evidence.

## Reviewer attack list

Verify the implementation rather than this summary, especially:

- whether a forged plan can bypass the external confirmation or Git trailer;
- whether a review commit can bind a different plan hash;
- hostile `scope` and `app.info` values including bool/int aliasing;
- the exact interval between cleanliness guard and live mutation;
- cleanup/create ambiguity when the pre-mutation guard refuses after a
  dispatched manifest record was persisted;
- retry accounting and refusal before head emission;
- partial reservation cancellation and shared-context liveness;
- explicit default controls versus omitted defaults for plan and profile paths;
- nested profile shape derivation and full applicability;
- mixed evidence candidate provenance in loaders and reports;
- cancellation/cleanup failure precedence in the moved counted batch driver;
- unique-row, buffered-row, command, page, and physical-request accounting.

## Required response

Return findings first as P1/P2/P3 with exact file/line and minimal reproducer.
If there are none, state `P1/P2/P3 = 0/0/0` and give one verdict:

`ACCEPT` or `REJECT 31c60563e70e90f392f4e817c56c4b6f68b8ae58`.

Stop after the verdict. Do not perform live access, writes, cleanup, W10, or
release actions.
