# W9 portal-build source correction review packet v12

## Decision requested

Review exact candidate
`ff8f5fdd204cf95b8e46f4e1c3d2b364d3c3fc43` and decide whether the live
evidence harness now distinguishes an installed application version from an
exact portal build, refusing before artifact publication when no documented
portal-build source is available.

This checkpoint requests neither dataset-plan approval nor authorization for
portal writes. The current Bitrix24 `app.info` response supplies `SCOPE` and
`LICENSE` only. The harness therefore remains deliberately unable to produce a
live dataset plan until a documented exact portal-build source is identified.

## Frozen inputs and range

- last accepted functional SHA:
  `8e03127621fa1892e629721d4b166b30846e755b`;
- rejected predecessor:
  `18bc9b2e344d34d47883bec94d2c1efb9bfe9a3f`;
- immediate docs-only predecessor:
  `6be51033d0d95e5048193036bdfd213af749b770`;
- review subject:
  `ff8f5fdd204cf95b8e46f4e1c3d2b364d3c3fc43`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

The focused functional diff is:

```bash
git diff 6be51033d0d95e5048193036bdfd213af749b770..ff8f5fdd204cf95b8e46f4e1c3d2b364d3c3fc43 -- \
  tools/b24api_evidence/harness/cli.py \
  tools/b24api_evidence/harness/contracts.py \
  tools/b24api_evidence/harness/contracts_test.py \
  tools/b24api_evidence/harness/live.py \
  tools/b24api_evidence/harness/live_test.py \
  tools/b24api_evidence/schemas/dataset-plan.schema.json
```

## Rejected behavior and authoritative distinction

The rejected candidate parsed `app.info.VERSION` before `BUILD` and stored it
as `LivePreflight.build`. The official `app.info` contract defines `VERSION`
as the version of the installed application, not the portal build. Thus a wire
response such as `{"VERSION": 4}` could wrongly satisfy the exact-build gate,
and `{"VERSION": 4, "BUILD": "26.500.0"}` selected `"4"`.

No documented `app.info` field has been established as the exact portal-build
source required by the evidence contract. The correction therefore admits
none of `VERSION`, `version`, `BUILD`, or `build`. It also does not substitute
`LICENSE`, a response header, an inferred release, or a synthetic value.

## Functional remediation

`LivePortal.preflight()` still performs the required read-only `scope` and
`app.info` calls, but returns `build=None` until an explicitly documented
portal-build source is implemented and reviewed.

Both plan creation and later plan matching use one helper that accepts only a
non-empty string and returns its whitespace-normalized value. Integer, boolean,
empty, and whitespace-only values are rejected. The dataset-plan schema now
requires `portal.build` to be a non-empty string, while semantic validation
also rejects surrounding whitespace.

The positive build fixture is intentionally synthetic: it represents a future
documented source by constructing `LivePreflight(build=" 26.500.0 ")`. It
proves normalization to `"26.500.0"` and publication of the complete five-file
plan bundle. It does not claim that integer build values are preserved or that
the current wire response supplies a usable build.

## Committed regression coverage

Wire-shaped `httpx.MockTransport` tests exercise the public `main()` boundary.
Each of these `app.info` results produces exit `3` and no artifact directory:

- no version/build field;
- `VERSION` only;
- `version` only;
- `BUILD` only;
- `build` only;
- `VERSION` together with `BUILD`.

Additional tests prove:

- explicit `None`, integer, boolean, empty, and whitespace-only preflight
  builds refuse;
- a future explicit string build is normalized and yields a valid full bundle;
- schema and semantic validation reject null, empty, whitespace-only, and
  non-normalized persisted build values.

## Reproduced exact-SHA results

Fresh detached worktree commands:

```bash
git worktree add --detach /tmp/b24api-human-review-ff8f5fd \
  ff8f5fdd204cf95b8e46f4e1c3d2b364d3c3fc43
cd /tmp/b24api-human-review-ff8f5fd
uv sync --frozen --group dev
.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/ruff check . --no-fix
.venv/bin/ruff format --check .
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check 6be51033d0d95e5048193036bdfd213af749b770..ff8f5fdd204cf95b8e46f4e1c3d2b364d3c3fc43
git status --short --branch
```

```text
pytest: 481 passed in 175.19s
ruff check: PASS
ruff format --check: 41 files
mypy --strict b24api tools/b24api_evidence: PASS, 25 source files
candidate diff-check: PASS
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

An independent read-only/offline clean-room review of the exact candidate
returned:

```text
Verdict: ACCEPT
Exact SHA: ff8f5fdd204cf95b8e46f4e1c3d2b364d3c3fc43
P1/P2/P3: 0/0/0
pytest: 481 passed in 174.77s
Ruff check/format: PASS, 41 files
strict mypy: PASS, 25 source files
candidate diff-check and detached tracked status: PASS / clean
```

The reviewer independently reproduced all wire-shaped public-main refusals,
bad explicit build refusal, future normalized string publication, and schema
and semantic validation. It used no network or credentials and did not edit
the candidate.

## Required functional checklist

1. Confirm `app.info.VERSION` is never treated as portal build.
2. Confirm undocumented `VERSION`, `version`, `BUILD`, and `build` wire fields
   all leave portal build unavailable.
3. Confirm the public CLI returns exit `3` before artifact-directory creation
   for each wire-shaped case.
4. Confirm explicit future string build values are normalized through the
   shared helper and can produce a complete valid bundle.
5. Confirm null, non-string, empty, whitespace-only, and non-normalized
   persisted build values are rejected by the applicable boundary.
6. Confirm `LICENSE`, headers, and inferred values are not substitutes.
7. Confirm unresolved portal build means live plan generation and all later
   write work remain blocked. This correction fixes false admission; it does
   not itself supply a portal-build source.
8. Keep the review functional. Do not use live credentials, perform portal
   writes, or expand into a cybersecurity review.

## Copy-paste review prompt

```text
Проведи независимое read-only функциональное ревью exact SHA
ff8f5fdd204cf95b8e46f4e1c3d2b364d3c3fc43 в новом clean detached worktree.

Нормативный документ:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256:
2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7

Packet находится в более позднем docs-only commit:
git show codex/bitrix24-client-benchmarks:docs/bitrix24-client-2.0/w7-w9/review-packet-v12.md

Не используй live Bitrix24, webhook/credentials, W10 или release actions;
ничего не меняй и не читай user-owned untracked spec/session files. Не
углубляйся в cybersecurity: scope ревью — только функциональный источник exact
portal build, wire parsing, public exit и refusal-before-artifact-publication.

Запусти точные команды:
uv sync --frozen --group dev
.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/ruff check . --no-fix
.venv/bin/ruff format --check .
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check 6be51033d0d95e5048193036bdfd213af749b770..ff8f5fdd204cf95b8e46f4e1c3d2b364d3c3fc43
git status --short --branch

Проверь focused diff
6be51033d0d95e5048193036bdfd213af749b770..ff8f5fdd204cf95b8e46f4e1c3d2b364d3c3fc43:

1. app.info VERSION/version/BUILD/build не принимаются как portal build.
2. Wire-shaped public main для каждого варианта даёт exit 3 и zero artifacts.
3. Explicit future LivePreflight string build нормализуется и публикует полный
   валидный plan bundle.
4. Integer/bool/empty/whitespace build и invalid persisted values отвергаются.
5. LICENSE/header/inference не подменяют exact build.
6. Unresolved build по-прежнему блокирует live plan и все portal writes.

Верни findings с P1/P2/P3, результаты gates и итоговый verdict. ACCEPT допустим
только при P1/P2/P3 = 0/0/0. Ничего не исправляй.
```
