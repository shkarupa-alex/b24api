# W9 live-plan exact-build refusal review packet v11

## Decision requested

Review exact candidate
`18bc9b2e344d34d47883bec94d2c1efb9bfe9a3f` and decide whether the
evidence harness correctly refuses a live dataset plan when the portal does
not expose an exact build identifier.

This checkpoint does **not** request dataset approval, seed authorization, or
any portal write. The supplied incoming webhooks cannot currently produce a
reviewable plan because their successful `app.info` result contains `SCOPE`
and `LICENSE`, but no `VERSION` or `BUILD`. Affected live profiles therefore
remain unadmitted; the correctness core is not allowed to substitute a license,
header, inferred version, or synthetic value for the missing build.

## Frozen inputs

- accepted code predecessor:
  `8e03127621fa1892e629721d4b166b30846e755b`;
- immediate predecessor after three docs-only packet commits:
  `412a18ffb7836bf5f44765cc3ebafa540fe62b5a`;
- review subject:
  `18bc9b2e344d34d47883bec94d2c1efb9bfe9a3f`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

The range from the accepted code predecessor contains three documentation-only
commits (`332e554`, `ab8768e`, `412a18f`) followed by the single material code
commit under review. The functional diff is exactly:

```bash
git diff 412a18ffb7836bf5f44765cc3ebafa540fe62b5a..18bc9b2e344d34d47883bec94d2c1efb9bfe9a3f -- \
  tools/b24api_evidence/harness/cli.py \
  tools/b24api_evidence/harness/contracts_test.py
```

## Observed functional gap

On the accepted predecessor, both full-rights roles passed `scope` and
`app.info` with HTTP 200. `plan` then persisted a preview bundle whose
`portal.build` was `null`. Every later write command would reject that same
plan in `_require_preflight_match()` because an exact non-empty build is
mandatory. The plan was therefore reviewable in appearance but unusable by
construction.

The two preview bundles and proposed review subjects created while diagnosing
this were removed from the active evidence area and moved recoverably to:

```text
/Users/alex/.Trash/b24api-invalid-live-plans-20260821-1635
```

No seed, cleanup, entity mutation, or other portal write occurred.

The official `app.info` contract describes `VERSION` as the installed
application version. It does not authorize treating the webhook's `LICENSE`
field as a portal build. The harness also must not persist an undocumented
server header or an inferred value as normative build evidence.

## Remediation

`_plan()` now checks `preflight.build` immediately after the read-only
preflight and before constructing or persisting any plan artifact. Missing or
empty build produces `LiveUnavailableError` and public CLI exit `3`.

The regression uses a deterministic fake portal with valid identity and
required `task` scope but `build=None`. It proves:

- the error is `LiveUnavailableError` with an exact-build diagnostic;
- no dataset plan, transaction lock, or other artifact is created;
- the refusal occurs before `_persist_plan_bundle()`.

Existing valid-build fixtures remain unchanged and continue through their
normal paths.

## Reproduced exact-SHA results

Fresh detached worktree commands:

```bash
git worktree add --detach /tmp/b24api-human-review-18bc9b2 \
  18bc9b2e344d34d47883bec94d2c1efb9bfe9a3f
cd /tmp/b24api-human-review-18bc9b2
uv sync --frozen --group dev
.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/ruff check . --no-fix
.venv/bin/ruff format --check .
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check 412a18ffb7836bf5f44765cc3ebafa540fe62b5a..18bc9b2e344d34d47883bec94d2c1efb9bfe9a3f
git status --short --branch
```

```text
pytest: 470 passed in 167.60s
ruff check: PASS
ruff format --check: 41 files
mypy --strict b24api tools/b24api_evidence: PASS, 25 source files
candidate diff-check: PASS
tracked status: clean detached HEAD
```

Real read-only replay on the exact SHA with the full-rights administrator
credential:

```text
scope: HTTP 200
app.info: HTTP 200, no VERSION/BUILD
plan: exit 3, LiveUnavailableError: exact build identifier unavailable
artifact directory: absent
```

This live replay is an operator observation, not a committed admission
artifact. It only confirms the refusal boundary. The webhook and fingerprint
key were environment-only and are not present in Git or output.

Independent clean-room review of the exact candidate:

```text
Verdict: ACCEPT
Exact SHA: 18bc9b2e344d34d47883bec94d2c1efb9bfe9a3f
P1/P2/P3: 0/0/0
pytest: 470 passed
Ruff check/format: PASS, 41 files
strict mypy: PASS, 25 source files
candidate diff-check and detached tracked status: PASS / clean
```

The reviewer independently exercised `build=None`, `build=""`, string build
`"24.100.0"`, and integer build `24100`. Missing and empty values produced
exit `3` with zero files; both valid values completed and were preserved
exactly in a complete plan bundle. No network or credentials were used.

## Required functional checklist

1. Confirm missing/empty build is refused after preflight and before artifact
   persistence.
2. Confirm public classification is environment unavailable (exit `3`), not a
   correctness failure and not a successful preview plan.
3. Confirm valid exact-build fixtures still reach the existing plan and
   seed/verify/cleanup preflight paths.
4. Confirm the change does not weaken scope, portal fingerprint, candidate SHA,
   approval, manifest ownership, or cleanup gates.
5. Confirm no replacement value is derived from `LICENSE`, response headers,
   or other non-build data.
6. Confirm unresolved live build identity leaves the affected profiles and
   claims unadmitted, consistent with sections 16.5, 23, and 25.
7. Keep the review functional. Do not use live credentials, perform portal
   writes, or expand into a cybersecurity review.

## Copy-paste review prompt

```text
Проведи независимое read-only функциональное ревью exact SHA
18bc9b2e344d34d47883bec94d2c1efb9bfe9a3f в новом clean detached worktree.

Нормативный документ:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256:
2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7

Packet находится в более позднем docs-only commit:
git show codex/bitrix24-client-benchmarks:docs/bitrix24-client-2.0/w7-w9/review-packet-v11.md

Не используй live Bitrix24, webhook/credentials, W10 или release actions;
ничего не меняй и не читай user-owned untracked spec/session files. Не
углубляйся в cybersecurity: scope ревью — функциональная классификация и
refusal-before-artifact-I/O.

Запусти точные команды:
uv sync --frozen --group dev
.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/ruff check . --no-fix
.venv/bin/ruff format --check .
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check 412a18ffb7836bf5f44765cc3ebafa540fe62b5a..18bc9b2e344d34d47883bec94d2c1efb9bfe9a3f
git status --short --branch

Проверь diff
412a18ffb7836bf5f44765cc3ebafa540fe62b5a..18bc9b2e344d34d47883bec94d2c1efb9bfe9a3f:

1. Live plan с build=None даёт LiveUnavailableError / exit 3.
2. Отказ происходит до dataset-plan.json, lock и любых иных artifacts.
3. Valid-build paths не регрессировали.
4. LICENSE/header/inferred values не подменяют exact build.
5. Portal identity, scope, approval, candidate и cleanup gates не ослаблены.

Верни findings с P1/P2/P3, результаты gates и итоговый verdict. ACCEPT допустим
только при P1/P2/P3 = 0/0/0. Ничего не исправляй.
```
