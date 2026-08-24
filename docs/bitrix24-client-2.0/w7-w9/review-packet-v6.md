# W7-W9 final human review packet v6

## Blocking decision

Review the exact immutable implementation candidate
`2b31da1c8266154b362446ba208184fb67298d13` and either accept it or return
prioritized findings against that SHA.

This packet is a later documentation-only commit, not the review subject.
Acceptance does not authorize live Bitrix24 access, webhook use, seed, cleanup,
W10, publication, release actions, or performance claims.

## Frozen inputs

- branch: `codex/bitrix24-client-benchmarks`;
- rejected predecessor: `d9cd6969415f571ce64ae16d7aab160b8a3a7d42`;
- review subject: `2b31da1c8266154b362446ba208184fb67298d13`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

Review in a new clean detached worktree. Do not use network, live credentials,
or user-owned untracked specification/session files.

```bash
git worktree add --detach /tmp/b24api-human-review \
  2b31da1c8266154b362446ba208184fb67298d13
cd /tmp/b24api-human-review
git status --short --branch
git diff d9cd6969415f571ce64ae16d7aab160b8a3a7d42..2b31da1c8266154b362446ba208184fb67298d13 -- \
  b24api tests tools/b24api_evidence
```

## Findings addressed after v5

- Bundle scanning no longer deletes any broad `.*.refused-*` match. Caller-owned
  root and nested lookalikes survive unchanged.
- A refused artifact is moved to a unique sibling quarantine outside the
  evidence bundle before rollback. An interrupted cleanup cannot leave a
  terminal-looking PASS or nested quarantine inside the bounded bundle.
- Verify now publishes `oracle.json` and `verify-evidence.json` transactionally.
  A refused repeat restores both the predecessor oracle and terminal artifact;
  an independent subsequent bundle scan succeeds.
- Cancellation replay now requires a real pending cancellation on the current
  task. An internal cleanup `CancelledError` cannot cancel the caller.
- Rearming performs one `uncancel()` plus one `cancel()`, preserving the exact
  cancellation count for nested timeout/TaskGroup scopes.
- Core and live webhook vault entries contain no raw credential, and weakref
  finalizers bound their lifetime even when callers omit `close()`/`aclose()`.
- Invalid Settings input remains redacted while reporting the truthful safe
  diagnostic `Webhook URL is invalid`.

## Independent clean-room result

An independent reviewer audited the exact subject in a separate detached
worktree without edits, live access, credentials, external network, or reads of
user-owned untracked files.

```text
Verdict: ACCEPT
Exact SHA: 2b31da1c8266154b362446ba208184fb67298d13
P1/P2/P3: 0/0/0
pytest: 444 passed
ruff check: PASS
ruff format --check: 41 files
mypy --strict: PASS, 25 source files
git diff --check: PASS
```

The reviewer independently reproduced a valid predecessor verify bundle and a
refused replacement after the real terminal write: the prior oracle and
terminal artifact were restored and the next `_scan_bundle` passed. Internal
cleanup cancellation left the caller runnable with `cancelling() == 0`; two
external cancellations were replayed with `cancelling() == 2`. Forty core and
twenty live objects were garbage-collected without explicit close, leaving no
recoverable vault handles.

## Required attack list

Review the implementation rather than this summary, including:

- caller-owned root and nested refused-lookalikes, interrupted quarantine
  cleanup, and evidence-bundle file/byte accounting;
- verify predecessor restoration after dependency write, terminal replace,
  candidate drift, directory fsync failure, and final-scan refusal;
- internal cleanup `CancelledError` versus one and multiple genuine external
  cancellations for every stream, scheduler, counted driver, and facade;
- webhook reachability through module globals, traceback globals/locals,
  `self`, args, cause/context, constructor failures, GC without close, and
  malformed HTTP responses;
- invalid Settings URL redaction and diagnostic fidelity;
- malformed `600/999`, successful null reads, structured non-200 errors,
  invalid gzip, and hostile scope/build semantics;
- approval binding, mutation journal transitions/resume, clean-HEAD drift,
  atomic replace/fsync rollback, stdout failure after terminal commit,
  reservations, budgets, page caps, probes, nested applicability, candidate
  provenance, and facade delegation.

## Copy-paste review prompt

```text
Проведи независимое read-only ревью exact SHA
2b31da1c8266154b362446ba208184fb67298d13 в новом clean detached worktree.

Нормативный документ:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256:
2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7

Review packet находится в более позднем docs-only commit. Прочитай его через:
git show codex/bitrix24-client-benchmarks:docs/bitrix24-client-2.0/w7-w9/review-packet-v6.md

Не используй сеть, live Bitrix24, вебхуки или credentials; ничего не меняй и
не читай user-owned untracked spec/session files. Запусти полный pytest, Ruff
check, Ruff format --check, strict mypy и git diff --check. Затем воспроизведи
весь attack list packet и самостоятельно ищи расхождения со спецификацией.
Не останавливайся на первом finding: верни один полный итог.

Особенно проверь:
1. Scan не удаляет caller-owned refused-lookalikes на любом уровне.
2. Refused verify восстанавливает старые oracle + terminal и bundle сканируется.
3. Internal CancelledError не отменяет caller; N external cancel сохраняют N.
4. Webhook недостижим через globals/locals и удаляется GC без explicit close.
5. Invalid Settings URL редактируется с правдивой безопасной диагностикой.

Верни findings сначала: P1/P2/P3, exact file/line, минимальный reproducer и
нарушенный пункт спецификации. Если actionable findings нет, напиши:
P1/P2/P3 = 0/0/0
ACCEPT 2b31da1c8266154b362446ba208184fb67298d13

Если findings есть:
REJECT 2b31da1c8266154b362446ba208184fb67298d13

После verdict остановись. Не выполняй live/W10/release действия.
```

## Required stop

After the human verdict, stop. Live validation is a separate checkpoint and
requires explicit authorization plus a separately reviewed dataset plan.
