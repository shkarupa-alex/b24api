# W9 live preflight human review packet v10

## Blocking decision

Review exact implementation candidate
`8e03127621fa1892e629721d4b166b30846e755b` and either accept it or return
prioritized functional findings against that SHA.

The previously accepted implementation SHA was
`3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5`. This packet covers the single
material code change discovered by the first read-only live preflight. The
packet itself is a later docs-only commit and is not the review subject.

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
git status --short --branch
git diff 3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5..8e03127621fa1892e629721d4b166b30846e755b -- \
  tools/b24api_evidence/harness/live.py \
  tools/b24api_evidence/harness/live_test.py
```

## Live observation and remediation

Bitrix24 webhooks without application scopes returned the successful scope
envelope `{"result":[""]}`. The accepted predecessor classified that exact
portal sentinel as malformed data and exited with correctness code `4`.

Section 16.5 requires missing scope to be classified as unavailable rather than
strategy failure. The subject therefore maps only the exact list `[""]` to an
empty scope set. A required `task` or `crm` scope then produces
`LiveUnavailableError` and normative exit `3` before `app.info` and before any
artifact write.

The change is deliberately narrow:

- `[""]` means no granted scopes;
- an empty string mixed with another scope remains malformed and exits `4`;
- non-string values, non-boolean capability maps, and invalid build values keep
  the existing correctness classification.

## Reproduced results

Local exact-SHA gates:

```text
pytest: 469 passed in 195.23s
ruff check: PASS
ruff format --check: 41 files
mypy --strict: PASS, 25 source files
git diff --check: PASS
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
this human checkpoint.

## Required functional checklist

1. Confirm the diff is limited to the exact `[""]` sentinel and regression
   tests.
2. Confirm `[""]` becomes an empty scope set and a required scope produces
   `LiveUnavailableError` before `app.info`.
3. Confirm the public CLI maps that result to exit `3` and leaves the artifact
   directory empty.
4. Confirm `["", "task"]` and other malformed scope/build shapes still produce
   correctness exit `4` before artifact writes.
5. Run the complete static gates, not only the two new tests.
6. Keep the review functional. Existing offline credential/error-boundary tests
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

Запусти полный pytest, Ruff check, Ruff format --check, strict mypy,
git diff --check и проверь clean detached status. Затем проверь diff
3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5..8e03127621fa1892e629721d4b166b30846e755b:

1. Exact scope result [""] означает empty scope set.
2. Required task/crm даёт LiveUnavailableError и CLI exit 3 до app.info и без
   artifact writes.
3. Mixed ["", "task"] и остальные malformed scope/build fixtures остаются
   LiveCorrectnessError / exit 4.
4. Existing live/api/contracts regression tests не регрессировали.

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
