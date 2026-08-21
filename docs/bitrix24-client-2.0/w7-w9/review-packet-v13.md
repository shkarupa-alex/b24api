# W9 portal-build source correction review packet v13

## Decision requested

Review exact candidate
`c4aa1a5be086995b0c0f8fb6a46c922305eba131` and decide whether the live
evidence harness correctly refuses plan publication when no documented exact
portal-build source is available.

This checkpoint requests neither dataset-plan approval nor authorization for
portal writes. The current portal's `app.info` response contains `SCOPE` and
`LICENSE`, but no documented exact portal build. Consequently this candidate
intentionally cannot produce a live dataset plan. It fixes false admission; it
does not invent or supply the missing portal-build source.

## Frozen inputs and complete review range

- last accepted functional SHA:
  `8e03127621fa1892e629721d4b166b30846e755b`;
- superseded exact-build-gate review subject:
  `18bc9b2e344d34d47883bec94d2c1efb9bfe9a3f`;
- superseded portal-build-source correction subject:
  `ff8f5fdd204cf95b8e46f4e1c3d2b364d3c3fc43`;
- review subject:
  `c4aa1a5be086995b0c0f8fb6a46c922305eba131`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- unchanged specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

Review the cumulative functional change from the accepted base, not only the
last remediation commit:

```bash
git diff 8e03127621fa1892e629721d4b166b30846e755b..c4aa1a5be086995b0c0f8fb6a46c922305eba131 -- \
  docs/bitrix24-client-2.0/w9/implementation.md \
  tools/b24api_evidence/harness/cli.py \
  tools/b24api_evidence/harness/contracts.py \
  tools/b24api_evidence/harness/contracts_test.py \
  tools/b24api_evidence/harness/live.py \
  tools/b24api_evidence/harness/live_test.py \
  tools/b24api_evidence/schemas/dataset-plan.schema.json
```

The range also contains documentation-only review-packet commits. `18bc9b2`
added an exact-build refusal gate but did not introduce the false source and
did not correct it. The false `VERSION` interpretation already existed in the
accepted `8e03127`; later subjects supersede the incomplete gate while
retaining its valid refusal-before-publication coverage.

## Functional defect and correction

The accepted base interpreted `app.info.VERSION` as `LivePreflight.build`, even
though the official `app.info` contract defines `VERSION` as the installed
application version rather than the portal build. A wire response such as
`{"VERSION": 4}` therefore satisfied the later exact-build gate, while
`{"VERSION": 4, "BUILD": "26.500.0"}` selected `"4"`.

No documented `app.info` field has been established as the exact portal-build
source required by the evidence contract. `LivePortal.preflight()` still makes
the required read-only `scope` and `app.info` calls, but admits none of
`VERSION`, `version`, `BUILD`, or `build` as portal-build evidence. It returns
`build=None`, causing live plan generation to refuse before artifact
publication. `LICENSE`, response headers, inferred releases, and synthetic
values are not substitutes.

Plan creation and later plan matching share one explicit-build helper. It
accepts only a non-empty string and returns its whitespace-normalized value.
The dataset-plan schema requires `portal.build` to be a non-empty string;
semantic validation also rejects surrounding whitespace.

This implementation decision is recorded in
`docs/bitrix24-client-2.0/w9/implementation.md`. The frozen normative
specification is not amended: it already requires exact portal-build provenance
and permits conservative refusal when that evidence is unavailable. Changing
the approved normative file would change its pinned hash and invalidate the
existing review chain rather than clarify this implementation choice.

## Explicitly removed behavior

The harness no longer semantically validates application-version/build-shaped
fields inside `app.info`, because it no longer reads them. Hostile `VERSION`
values such as booleans, whitespace, or oversized integers therefore no longer
produce the former app-info-specific correctness exit `4`. The corresponding
app-info build-specific hostile-response/redaction regression was removed as
dead behavior. Generic live response handling and redaction remain covered by
their existing tests.

## Exact committed coverage

Both the preflight boundary and public `main()` are parametrized with all six
wire shapes below:

1. the real response shape `SCOPE + LICENSE`, with no build field;
2. `VERSION`;
3. lowercase `version`;
4. `VERSION + BUILD`;
5. `BUILD`;
6. lowercase `build`.

For every public case the committed test proves exit `3`, the exact-build
diagnostic, and absence of the artifact directory.

Additional committed tests prove:

- explicit `None`, integer, boolean, empty, and whitespace-only preflight
  builds refuse;
- a future explicit string build is normalized and yields a valid five-file
  plan bundle;
- schema and semantic validation reject null, empty, whitespace-only, and
  non-normalized persisted build values.

The positive build test is deliberately synthetic. It constructs
`LivePreflight(build=" 26.500.0 ")` and monkeypatches the independently tested
candidate/clean-tree gate so it can isolate normalization and publication.
`LivePortal.preflight()` currently always returns `build=None`; therefore no
production wire path reaches the positive helper branch on this SHA. The test
does not independently prove candidate binding, and the packet makes no such
claim.

## Reproduced exact-SHA gates

```bash
git worktree add --detach /tmp/b24api-human-review-c4aa1a5 \
  c4aa1a5be086995b0c0f8fb6a46c922305eba131
cd /tmp/b24api-human-review-c4aa1a5
uv sync --frozen --group dev
.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/ruff check . --no-fix
.venv/bin/ruff format --check .
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check 8e03127621fa1892e629721d4b166b30846e755b..c4aa1a5be086995b0c0f8fb6a46c922305eba131
git status --short --branch
```

```text
pytest: 485 passed in 169.68s
ruff check: PASS
ruff format --check: 41 files
mypy --strict b24api tools/b24api_evidence: PASS, 25 source files
cumulative candidate diff-check: PASS
tracked status: clean detached HEAD
specification SHA-256: exact match
```

Read-only live replay on the exact SHA with the full-rights administrator
credential:

```text
scope/app.info preflight: completed
plan: exit 3
diagnostic: live portal did not provide an exact build identifier
artifact directory: absent
```

The credential and fingerprint key were environment-only. No seed, cleanup,
entity mutation, W10, release action, or other portal write ran. This is an
operator observation of the refusal boundary, not an admission artifact.

## Independent clean-room result

An independent read-only/offline clean-room review of the cumulative exact
candidate returned:

```text
Verdict: ACCEPT
Exact SHA: c4aa1a5be086995b0c0f8fb6a46c922305eba131
P1/P2/P3: 0/0/0
pytest: 485 passed in 170.39s
targeted portal-build regressions: 35 passed
Ruff check/format: PASS, 41 files
strict mypy: PASS, 25 source files
cumulative diff-check and detached tracked status: PASS / clean
```

The reviewer independently confirmed all six public cases, shared build gate,
schema and semantic validation, synthetic positive-path limitations, removed
dead classification, historical attribution, implementation decision, and the
unchanged normative hash. It used no network or credentials and did not edit
the candidate.

## Required functional checklist

1. Review the cumulative diff from accepted `8e03127`, including surviving
   tests from the superseded `18bc9b2` gate.
2. Confirm `app.info.VERSION` is the application version and is never treated
   as portal build.
3. Confirm all six committed wire shapes pass through public `main()`, return
   exit `3`, and create no artifact directory.
4. Confirm explicit future string build values are normalized by the shared
   helper, while bad explicit and persisted values are rejected.
5. Confirm the positive test is synthetic, bypasses only the candidate gate,
   and does not claim a currently reachable production build source.
6. Confirm removal of app-info build-content correctness classification is
   consistent with no longer interpreting those fields.
7. Confirm `LICENSE`, headers, and inferred values are not substitutes.
8. Confirm unresolved portal build leaves live plan generation and all portal
   writes blocked.
9. Keep the review functional. Do not use live credentials, perform portal
   writes, or expand into a cybersecurity review.

## Copy-paste review prompt

```text
Проведи независимое read-only функциональное ревью exact SHA
c4aa1a5be086995b0c0f8fb6a46c922305eba131 в новом clean detached worktree.

Нормативный документ:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256:
2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7

Packet находится в более позднем docs-only commit:
git show codex/bitrix24-client-benchmarks:docs/bitrix24-client-2.0/w7-w9/review-packet-v13.md

Не используй live Bitrix24, webhook/credentials, W10 или release actions;
ничего не меняй и не читай user-owned untracked spec/session files. Не
углубляйся в cybersecurity: scope ревью — функциональный источник exact portal
build, wire parsing, public exit и refusal-before-artifact-publication.

Запусти точные команды:
uv sync --frozen --group dev
.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/ruff check . --no-fix
.venv/bin/ruff format --check .
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check 8e03127621fa1892e629721d4b166b30846e755b..c4aa1a5be086995b0c0f8fb6a46c922305eba131
git status --short --branch

Проверь cumulative functional diff
8e03127621fa1892e629721d4b166b30846e755b..c4aa1a5be086995b0c0f8fb6a46c922305eba131:

1. Все шесть app.info shapes (SCOPE+LICENSE без build; VERSION; version;
   VERSION+BUILD; BUILD; build) проходят public main, дают exit 3 и zero
   artifacts.
2. Ни одно поле app.info не принимается как portal build.
3. Explicit future string build нормализуется; bad explicit/persisted build
   отвергается shared helper/schema/semantic validation.
4. Positive fixture synthetic, monkeypatches candidate gate, и сейчас
   production wire path до positive branch не существует.
5. Удаление прежней app-info build correctness/redaction классификации —
   раскрытое удаление мёртвого поведения.
6. История корректна: false VERSION source уже был в accepted 8e03127;
   18bc9b2 добавил gate и был superseded, но не являлся источником дефекта.
7. W9 implementation фиксирует консервативное решение; нормативная спека и её
   pinned hash не изменены.

Верни findings с P1/P2/P3, результаты gates и итоговый verdict. ACCEPT допустим
только при P1/P2/P3 = 0/0/0. Ничего не исправляй.
```
