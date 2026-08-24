# W9 live preflight human review packet v10

## Blocking decision

Review exact implementation candidate
`8e03127621fa1892e629721d4b166b30846e755b` and either accept it or return
prioritized functional findings against that SHA.

The previously accepted implementation SHA was
`3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5`. This packet covers the single
material code change discovered by the first read-only live preflight. The
packet itself is a later docs-only commit and is not the review subject.
The Git range from the predecessor contains two commits: docs-only packet v9
and the one material code/test commit. The path-filtered diff below intentionally
shows only that code/test change.

Acceptance does not authorize `seed`, `cleanup`, live benchmark, W10, release,
or any other portal write.

## Frozen inputs

- branch: `codex/bitrix24-client-benchmarks`;
- accepted predecessor: `3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5`;
- review subject: `8e03127621fa1892e629721d4b166b30846e755b`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

```bash
git worktree add --detach /tmp/b24api-human-review \
  8e03127621fa1892e629721d4b166b30846e755b
cd /tmp/b24api-human-review
uv sync --frozen --group dev
git status --short --branch
git diff 3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5..8e03127621fa1892e629721d4b166b30846e755b -- \
  tools/b24api_evidence/harness/live.py \
  tools/b24api_evidence/harness/live_test.py
```

## Live classification observation and fix

The two deliberately limited Bitrix24 webhooks returned the successful scope
envelope `{"result":[""]}`. The accepted predecessor classified that exact
portal sentinel as malformed data and exited with correctness code `4`. This
does not close the live path: both full-rights webhooks separately completed
read-only `plan` for both Tasks and CRM before this fix. The next trusted plan
checkpoint therefore uses a full-rights webhook after this review.

Those four full-rights runs were operator diagnostics on exact predecessor
`3947ac5`: `admin_full/tasks-task-v1`, `employee_full/tasks-task-v1`,
`admin_full/crm-deal-v1`, and `employee_full/crm-deal-v1` each returned exit
`0`. Their temporary bundles were deliberately discarded after the code change,
so this statement is not repository-attested evidence and does not authorize
admission or writes. The change under review is a classification fix for the
limited-role diagnostic, not the mechanism that opens the full-rights path.

Section 16.5 requires missing scope to be classified as unavailable rather than
strategy failure; section 16.2 assigns exit `3` to an unavailable or scope-gated
environment. The subject therefore maps only the exact list `[""]` to an empty
scope set. A required `task` or `crm` scope then produces
`LiveUnavailableError` and normative exit `3` before `app.info` and before any
artifact write. No SKIP artifact is written by `plan`; the named missing scope
is carried in the safe diagnostic and exit classification.

The change is deliberately narrow:

- `[""]` means no granted scopes;
- an empty string mixed with another scope remains malformed and exits `4`;
- `["", ""]` remains malformed through the same general list rule;
- capability dictionaries remain asymmetric by contract: empty keys such as
  `{"": true}` and `{"": false}` are malformed rather than an empty-scope
  sentinel;
- non-string values, non-boolean capability maps, and invalid build values keep
  the existing correctness classification;
- the added `PLR0912` suppression is local to the explicit wire-shape union and
  is required by the configured Ruff threshold.

## Reproduced results

Local exact-SHA gates:

```bash
uv sync --frozen --group dev
.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/ruff check . --no-fix
.venv/bin/ruff format --check .
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check 3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5..8e03127621fa1892e629721d4b166b30846e755b
git status --short --branch
```

```text
pytest: 469 passed in 195.49s
ruff check: PASS
ruff format --check: 41 files
mypy --strict b24api tools/b24api_evidence: PASS, 25 source files
git diff --check predecessor..subject: PASS
tracked status: clean
```

Independent clean-room review:

```text
Verdict: ACCEPT
Exact SHA: 8e03127621fa1892e629721d4b166b30846e755b
P1/P2/P3: 0/0/0
pytest: 469 passed in 242.79s
Ruff/format/strict mypy/diff/status: PASS
```

The diagnostic read-only portal replay used both limited roles against the
Tasks and CRM profiles. All four cases returned exit `3` with the exact missing
scope name and created zero files. This replay validates the observed wire
shape but is not yet admission evidence because the code change still requires
this human checkpoint. Before the fix, both full-rights roles also completed
read-only plans for both profiles, proving that the available credential model
does not deterministically block the live path.

## Required functional checklist

1. Confirm the diff is limited to the exact `[""]` sentinel and regression
   tests.
2. Confirm `[""]` becomes an empty scope set and a required scope produces
   `LiveUnavailableError` before `app.info`.
3. Confirm by code reading that the public CLI maps that result to exit `3` and
   reaches preflight before artifact persistence. The four real read-only CLI
   diagnostics additionally observed empty artifact directories; the committed
   unit test covers the harness boundary rather than spawning the public CLI.
4. Confirm `["", "task"]` and other malformed scope/build shapes still produce
   correctness exit `4` before artifact writes.
5. Confirm the list/dict asymmetry, `["", ""]` fallback, and local `PLR0912`
   suppression are intentional and bounded.
6. Run the exact complete gate commands above, not `mypy --strict .` and not
   only the two new tests. A fresh detached worktree must run the pinned
   `uv sync --frozen --group dev` first because `.venv` is intentionally not
   stored in Git.
7. Keep the review functional. Existing offline credential/error-boundary tests
   may be replayed, but no expanded cybersecurity investigation is requested.

## Copy-paste review prompt

```text
Проведи независимое read-only функциональное ревью exact SHA
8e03127621fa1892e629721d4b166b30846e755b в новом clean detached worktree.

Нормативный документ:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256:
2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7

Packet находится в более позднем docs-only commit:
git show codex/bitrix24-client-benchmarks:docs/bitrix24-client-2.0/w7-w9/review-packet-v10.md

Не используй live Bitrix24, сеть, webhook/credentials, W10 или release actions;
ничего не меняй и не читай user-owned untracked spec/session files.

Запусти точные команды:
uv sync --frozen --group dev
.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/ruff check . --no-fix
.venv/bin/ruff format --check .
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check 3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5..8e03127621fa1892e629721d4b166b30846e755b
git status --short --branch

Затем проверь diff
3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5..8e03127621fa1892e629721d4b166b30846e755b:

1. Exact scope result [""] означает empty scope set.
2. Required task/crm даёт LiveUnavailableError и CLI exit 3 до app.info. Связь
   с public CLI и отсутствие persistence подтвердить чтением call path; unit
   test находится на harness boundary, а zero files дополнительно наблюдались в
   четырёх read-only live diagnostics.
3. Mixed ["", "task"] и остальные malformed scope/build fixtures остаются
   LiveCorrectnessError / exit 4.
4. ["", ""] и dict с пустым ключом остаются malformed; это намеренная
   асимметрия exact list sentinel и capability map.
5. Existing live/api/contracts regression tests не регрессировали, локальный
   PLR0912 suppression обоснован.

Не углубляйся в cybersecurity; это функциональное ревью классификации portal
scope response. Верни P1/P2/P3 и exact file/line для любого finding.

Если actionable findings нет:
P1/P2/P3 = 0/0/0
ACCEPT 8e03127621fa1892e629721d4b166b30846e755b

Если findings есть:
REJECT 8e03127621fa1892e629721d4b166b30846e755b

После verdict остановись. Не выполняй seed/cleanup/live benchmark/W10/release.
```

## Required stop

After the human verdict, stop. Successful review authorizes only the next
read-only plan checkpoint. Every portal write still requires a separately
reviewed dataset plan and exact confirmation arguments.
