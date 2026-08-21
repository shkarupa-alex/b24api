# W9 portal-build source correction closure packet v14

## Decision and frozen subject

The functional portal-build correction is closed on exact SHA
`c72c905da12bb2236355fd9f4148348a906cc292`.

- last accepted functional base:
  `8e03127621fa1892e629721d4b166b30846e755b`;
- exact subject: `c72c905da12bb2236355fd9f4148348a906cc292`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- unchanged specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

The user accepted the two independent functional reviews after the final P3
documentation and regression corrections. No additional review cycle is
requested by this closure packet.

## Cumulative functional change

Review and verification use the complete range from the accepted base:

```bash
git diff 8e03127621fa1892e629721d4b166b30846e755b..c72c905da12bb2236355fd9f4148348a906cc292 -- \
  docs/bitrix24-client-2.0/w9/implementation.md \
  tools/b24api_evidence/harness/cli.py \
  tools/b24api_evidence/harness/contracts.py \
  tools/b24api_evidence/harness/contracts_test.py \
  tools/b24api_evidence/harness/live.py \
  tools/b24api_evidence/harness/live_test.py \
  tools/b24api_evidence/schemas/dataset-plan.schema.json
```

The false source predates this range: accepted SHA `8e03127` interpreted
`app.info.VERSION` as portal build even though the official contract defines
it as the installed application version. Superseded subject `18bc9b2` added an
exact-build gate over that source; it did not introduce the parser defect.

No documented `app.info` field is now admitted as exact portal-build evidence.
`VERSION`, `version`, `BUILD`, and `build` are ignored for this purpose, and
`LivePortal.preflight()` returns `build=None`. Live plan generation therefore
refuses before artifact publication rather than substituting `LICENSE`, a
header, an inferred release, or a synthetic value.

Plan creation and later matching use the same explicit-build helper. Persisted
plans require a non-empty normalized string through schema and semantic
validation. The positive string-build test is synthetic, bypasses the
independently tested candidate/clean-tree gate, and is not reachable from the
current production wire path.

The prior app-info-specific hostile build classification and redaction test
were removed as dead behavior: the application-version/build-shaped fields are
no longer read or retained.

## Exact committed coverage

The tests use an observed-field reconstruction rather than a committed wire
capture. Its `SCOPE` value type is fixture data and is not claimed as an exact
recording of the portal response.

Both the preflight boundary and public `main()` cover these six shapes:

1. `SCOPE + LICENSE`, with no build field;
2. `VERSION`;
3. lowercase `version`;
4. `VERSION + BUILD`;
5. `BUILD`;
6. lowercase `build`.

Each public case proves exit `3`, the exact-build diagnostic, no artifact
directory, and exactly two preflight calls (`scope` plus `app.info`). The
preflight unit boundary independently asserts the same two-call contract. A
mutation that removes `app.info` therefore fails committed tests, preserving
the requirement in specification section 16.5 and the two-attempt accounting.

The required non-empty `portal.build` is explicitly documented in
`docs/bitrix24-client-2.0/w9/implementation.md` as a harness-level provenance
strengthening. It is not attributed to the artifact field list in section
16.3.

## Exact reproducible commands

```bash
git worktree add --detach /tmp/b24api-human-review-c72c905 \
  c72c905da12bb2236355fd9f4148348a906cc292
cd /tmp/b24api-human-review-c72c905
uv sync --frozen --group dev

.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/pytest -q -p no:cacheprovider \
  tools/b24api_evidence/harness/live_test.py \
  -k 'portal_build or wire_app_info'
.venv/bin/ruff check . --no-fix
.venv/bin/ruff format --check .
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check 8e03127621fa1892e629721d4b166b30846e755b..c72c905da12bb2236355fd9f4148348a906cc292
git status --short --branch
```

Reproduced results:

```text
full pytest: 485 passed in 171.54s
targeted public/preflight command: 12 passed
ruff check: PASS
ruff format --check: 41 files
strict mypy: PASS, 25 source files
cumulative diff-check: PASS
tracked status: clean detached HEAD
specification SHA-256: exact match
```

Read-only live replay on the exact SHA:

```text
scope/app.info preflight: completed
plan: exit 3
diagnostic: live portal did not provide an exact build identifier
artifact directory: absent
```

The credential and fingerprint key were environment-only. No seed, cleanup,
entity mutation, W10, release action, or other portal write ran.

## Review history and closure

The two preceding independent reports agreed that the functional correction
works. The last report's residual P3 items are closed as follows:

- mandatory `app.info` execution is pinned by exact two-call assertions at
  both preflight and public-main boundaries;
- the unreproducible `35 passed` statement is removed and replaced by the exact
  12-test command above;
- `real response shape` is replaced by `observed-field reconstruction`;
- the section 16.3 harness-strengthening distinction is recorded explicitly.

With those corrections, the user directed that both preceding functional
reviews be treated as ACCEPT. The accepted closure state is:

```text
P1/P2/P3: 0/0/0
Verdict: ACCEPT c72c905da12bb2236355fd9f4148348a906cc292
```
