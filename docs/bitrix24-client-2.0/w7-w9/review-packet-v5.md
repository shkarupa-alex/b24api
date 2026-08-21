# W7-W9 final human review packet v5

## Blocking decision

Review the exact immutable implementation candidate
`d9cd6969415f571ce64ae16d7aab160b8a3a7d42` and either accept it or return
prioritized findings against that SHA.

This packet is a later documentation-only commit, not the review subject.
Acceptance does not authorize live Bitrix24 access, webhook use, seed, cleanup,
W10, publication, release actions, or performance claims.

## Frozen inputs

- branch: `codex/bitrix24-client-benchmarks`;
- rejected predecessor: `3cb01757e5cab82c375cc5dffc5fea607939bba0`;
- review subject: `d9cd6969415f571ce64ae16d7aab160b8a3a7d42`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

Review in a new clean detached worktree. Do not use network, live credentials,
or user-owned untracked specification/session files.

```bash
git worktree add --detach /tmp/b24api-human-review \
  d9cd6969415f571ce64ae16d7aab160b8a3a7d42
cd /tmp/b24api-human-review
git status --short --branch
git diff 3cb01757e5cab82c375cc5dffc5fea607939bba0..d9cd6969415f571ce64ae16d7aab160b8a3a7d42 -- \
  b24api tests tools/b24api_evidence
```

## Human findings addressed

- Live HTTP statuses outside `100..599`, including `600` and `999`, are
  correctness failures before structured `ERROR_NOT_FOUND` classification and
  can never prove entity absence.
- `HttpxTransport` and `LivePortal` retain only opaque handles. Raw webhooks
  live in process-local vaults removed on close and are not reachable through
  traceback `self`, wrapped arguments, cause/context, or nested locals.
- HTTPX constructor failure tests use deterministic fault injection instead of
  platform-dependent malformed proxy parsing.
- A primary traversal failure retains a `FAILED` report; concurrent
  cancellation is rearmed and interrupts the caller's next await across
  pagination, batch, reference scheduling/streaming, and facade boundaries.
- Cancellation-resistant cleanup uses `asyncio.wait`, preventing late
  shield-future exceptions from leaking arbitrary source text to the event
  loop error handler.
- Plan and benchmark multi-file dependencies use one rollback log before
  terminal publication. Refused quarantine is non-evidence and is cleaned by
  the next scan.
- The rollback transaction ends after terminal write and final scan. A later
  stdout failure cannot remove dependencies while leaving an orphan PASS.

## Independent clean-room result

An independent reviewer audited the exact subject in a separate detached
worktree without edits, live access, credentials, or external network.

```text
Verdict: ACCEPT
Exact SHA: d9cd6969415f571ce64ae16d7aab160b8a3a7d42
P1/P2/P3: 0/0/0
pytest: 437 passed
ruff check: PASS
ruff format --check: 41 files
mypy --strict: PASS, 25 source files
git diff --check: PASS
```

The reviewer independently replayed the full accumulated attack list. For the
last finding, a real completed benchmark followed by injected
`BrokenPipeError` retained `benchmark-evidence.json`, `model-matrix.json`, and
all 100 oracles; a subsequent independent bundle scan passed.

## Required attack list

Review the implementation rather than this summary, including:

- malformed `600/999` plus structured absence, successful null point reads,
  valid structured non-200 errors, and invalid gzip;
- recursive webhook reachability through exception chains, traceback locals,
  `self`, wrapped args, containers, slots, and constructor failures;
- primary-failure then replayed-cancellation behavior for every stream,
  scheduler, counted driver, and facade cleanup/report window;
- absence of event-loop exception contexts containing source error text;
- atomic replace/fsync failure, predecessor restoration, quarantine cleanup,
  exact clean-HEAD drift, and final-scan refusal;
- plan/benchmark all-dependency rollback before terminal commit;
- stdout failure after successful terminal commit without bundle corruption;
- approval binding, live journal transitions/resume, budgets, reservations,
  page caps, probes, nested applicability, candidate provenance, and facade
  delegation.

## Copy-paste review prompt

```text
Проведи независимое read-only ревью exact SHA
d9cd6969415f571ce64ae16d7aab160b8a3a7d42 в новом clean detached worktree.

Нормативный документ:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256:
2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7

Review packet находится в более позднем docs-only commit. Прочитай его через:
git show codex/bitrix24-client-benchmarks:docs/bitrix24-client-2.0/w7-w9/review-packet-v5.md

Не используй сеть, live Bitrix24, вебхуки или credentials; ничего не меняй и
не читай user-owned untracked spec/session files. Запусти полный pytest, Ruff
check, Ruff format --check, strict mypy и git diff --check. Затем воспроизведи
весь attack list packet и самостоятельно ищи расхождения со спецификацией.
Не останавливайся на первом finding: верни один полный итог.

Особенно проверь:
1. HTTP 600/999 + ERROR_NOT_FOUND не доказывает absence.
2. Webhook недостижим рекурсивно через traceback/self/args/cause/context.
3. Primary FAILED выходит первым, а cancellation срабатывает на следующем
   await без событий в loop exception handler.
4. Pre-terminal failure откатывает все файлы plan/benchmark bundle.
5. BrokenPipeError после terminal write/final scan не удаляет dependencies и
   повторный _scan_bundle проходит.

Верни findings сначала: P1/P2/P3, exact file/line, минимальный reproducer и
нарушенный пункт спецификации. Если actionable findings нет, напиши:
P1/P2/P3 = 0/0/0
ACCEPT d9cd6969415f571ce64ae16d7aab160b8a3a7d42

Если findings есть:
REJECT d9cd6969415f571ce64ae16d7aab160b8a3a7d42

После verdict остановись. Не выполняй live/W10/release действия.
```

## Required stop

After the human verdict, stop. Live validation is a separate checkpoint and
requires explicit authorization plus a separately reviewed dataset plan.
