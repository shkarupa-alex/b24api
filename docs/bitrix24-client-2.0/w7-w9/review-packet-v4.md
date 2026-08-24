# W7-W9 final human review packet v4

## Blocking decision

Review the exact immutable implementation candidate
`3cb01757e5cab82c375cc5dffc5fea607939bba0` and either accept it or return
prioritized findings against that SHA.

This packet is a later documentation-only commit and is not the review
subject. Acceptance does not authorize webhook use, live portal access, seed,
cleanup, W10, profile/default publication, release actions, or performance
claims.

## Frozen inputs

- branch: `codex/bitrix24-client-benchmarks`;
- original rejected baseline: `e5ef566bcc1e3d3c3b8669170ee185a2c85e97b1`;
- review subject: `3cb01757e5cab82c375cc5dffc5fea607939bba0`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

Review in a clean detached worktree. Do not use live webhooks, external
network, or user-owned untracked specification/session files.

```bash
git worktree add --detach /tmp/b24api-human-review \
  3cb01757e5cab82c375cc5dffc5fea607939bba0
cd /tmp/b24api-human-review
git status --short --branch
git diff e5ef566bcc1e3d3c3b8669170ee185a2c85e97b1..3cb01757e5cab82c375cc5dffc5fea607939bba0 -- \
  b24api tests tools/b24api_evidence
```

## Remediation scope

The candidate closes the accumulated adversarial findings in one frozen
implementation SHA, including:

- shared counted traversal, remaining-budget admission after retries,
  transactional page reservations, exact compatibility controls, nested
  profile applicability, and same-candidate evidence provenance;
- failure-over-cancellation precedence across pagination, batch, item,
  reference, and facade cleanup/report windows;
- external human-review commit and plan-content binding, hostile scope/build
  validation, exact clean-SHA mutation guards, and resumable create/delete
  journal transitions;
- typed live absence, structured-error precedence, malformed transport
  classification, and credential redaction across Settings, HTTPX, and the
  live harness;
- exact-candidate checks around every evidence JSON publication and final
  bundle scan;
- transactional rollback of refused terminal evidence, including directory
  fsync failure after replace, restoration of a predecessor, and quarantine
  when cleanup is interrupted. Refused quarantine names are not recognized as
  `*-evidence.json` artifacts.

## Independent clean-room result

An independent reviewer audited the exact subject in a separate detached
worktree without edits, live access, credentials, or external network.

```text
Verdict: ACCEPT
Exact SHA: 3cb01757e5cab82c375cc5dffc5fea607939bba0
P1/P2/P3: 0/0/0
pytest: 432 passed
ruff check: PASS
ruff format --check: 41 files already formatted
mypy --strict: PASS, 25 source files
git diff --check: PASS
```

The reviewer also fault-injected clean-HEAD changes, drift immediately after
atomic replace, directory fsync failure, predecessor restoration,
`KeyboardInterrupt` during quarantine cleanup, HTTP statuses 600/999, and
credential-bearing transport failures.

## Human reviewer attack list

Verify the implementation rather than this summary. At minimum, test:

- forged approval, unrelated plan hashes, and non-commit review objects;
- exact build/scope types and refusal before entity I/O;
- dirty-tree and clean-HEAD drift before and after journal dispatch and before
  live create/delete;
- retry/resume transitions for create and delete, including ambiguous results;
- counted head retry budgets before emission, page-reservation cancellation,
  and failure/cancellation races during every cleanup/report window;
- explicit legacy defaults versus omission, requested page-size narrowing,
  nested filter/order paths, and mixed candidate provenance;
- Settings, HTTPX, and live-harness redaction in exception chains,
  serialization, and traceback locals;
- successful null point reads, typed `ERROR_NOT_FOUND`, non-200 structured
  errors, invalid gzip, and out-of-range HTTP statuses;
- clean-HEAD switches during evidence production and drift during atomic
  replace/final scan;
- refused terminal publication when replace or directory fsync fails, when a
  predecessor exists, and when quarantine cleanup raises `BaseException`;
- that hidden `.refused-*` files cannot qualify as terminal evidence.

## Copy-paste review prompt

```text
Проведи независимое read-only ревью exact SHA
3cb01757e5cab82c375cc5dffc5fea607939bba0 в отдельном clean detached worktree.

Нормативный документ:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256 документа:
2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7

Review packet (он находится в более позднем docs-only commit; прочитай через
`git show codex/bitrix24-client-benchmarks:docs/bitrix24-client-2.0/w7-w9/review-packet-v4.md`,
не добавляя его к review subject):
docs/bitrix24-client-2.0/w7-w9/review-packet-v4.md

Не используй сеть, live Bitrix24, вебхуки или credentials; ничего не меняй и
не читай user-owned untracked spec/session files. Сначала запусти полный pytest,
Ruff check, Ruff format --check, strict mypy и git diff --check. Затем проверь
весь attack list из packet и самостоятельно ищи расхождения со спецификацией.
Не останавливайся на первом finding: верни один полный итог.

Особенно воспроизведи exact-candidate drift во время evidence publication,
ошибку directory fsync после реального atomic replace, восстановление
предыдущего artifact, BaseException во время quarantine cleanup и проверь, что
canonical *-evidence.json не сохраняет новый PASS. Проверь HTTP 600/999 и
отсутствие webhook в cause/context/traceback locals.

Верни findings сначала: P1/P2/P3, exact file/line, минимальный reproducer и
нарушенный пункт спецификации. Если actionable findings нет, явно напиши:
P1/P2/P3 = 0/0/0
ACCEPT 3cb01757e5cab82c375cc5dffc5fea607939bba0

Если findings есть:
REJECT 3cb01757e5cab82c375cc5dffc5fea607939bba0

После verdict остановись. Не выполняй live/W10/release действия.
```

## Required stop

After the human verdict, stop. Live validation is a separate checkpoint and
requires explicit authorization plus a separately reviewed dataset plan.
