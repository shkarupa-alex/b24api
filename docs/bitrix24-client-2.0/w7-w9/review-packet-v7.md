# W7-W9 final human review packet v7

## Blocking decision

Review the exact immutable implementation candidate
`07b83c8e6572a2a891a2226eee991773838d467a` and either accept it or return
prioritized findings against that SHA.

This packet is a later documentation-only commit, not the review subject.
Acceptance does not authorize live Bitrix24 access, webhook use, seed, cleanup,
W10, publication, release actions, or performance claims.

## Frozen inputs

- branch: `codex/bitrix24-client-benchmarks`;
- rejected predecessor: `a804f3beb3f50c00ec1057177f90a4991d5d84b8`;
- review subject: `07b83c8e6572a2a891a2226eee991773838d467a`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

Review in a new clean detached worktree. Do not use network, live credentials,
or user-owned untracked specification/session files.

```bash
git worktree add --detach /tmp/b24api-human-review \
  07b83c8e6572a2a891a2226eee991773838d467a
cd /tmp/b24api-human-review
git status --short --branch
git diff a804f3beb3f50c00ec1057177f90a4991d5d84b8..07b83c8e6572a2a891a2226eee991773838d467a -- \
  b24api tests tools/b24api_evidence
```

## Findings addressed after v6

- Plan, benchmark, and verify publication now use one durable bundle-level
  transaction marker created before the first dependency mutation. A refused
  terminal publication rolls back the complete dependency set.
- An incomplete or failed rollback leaves the exact marker in the bundle, so an
  independent scan fails closed instead of accepting refused canonical files.
- Refused artifact quarantine remains on the same filesystem. Caller-owned
  lookalikes are never glob-deleted, and quarantine move/unlink failures are
  covered by fail-closed marker semantics.
- Rollback warnings in exception notes are emitted by both typed and generic CLI
  error boundaries after credential redaction.
- Transaction commit distinguishes a pre-unlink failure from an exception
  delivered after the marker was physically removed. The former rolls back or
  remains fail-closed; the latter is a crossed commit point and preserves the
  complete valid bundle without a false rollback warning.
- Simultaneous external caller cancellation and internal owned-task
  cancellation preserve the external payload and exact nested cancellation
  count.

## Independent clean-room result

An independent reviewer audited the exact subject in a separate detached
worktree without edits, live access, credentials, external network, or reads of
user-owned untracked files.

```text
Verdict: ACCEPT
Exact SHA: 07b83c8e6572a2a891a2226eee991773838d467a
P1/P2/P3: 0/0/0
pytest: 449 passed
ruff check: PASS
ruff format --check: 41 files
mypy --strict: PASS, 25 source files
git diff --check: PASS
```

The reviewer exercised real plan, benchmark with 100 oracle files, verify with
a predecessor, and standalone publication. Persistent pre-unlink failures
rolled back or left a marker that made scanning reject. Remove-then-raise kept
the complete committed bundle, left no marker or false fail-closed note, and an
independent scan passed. The complete accumulated attack list also passed.

## Required attack list

Review the implementation rather than this summary, including:

- transaction marker creation before every dependency mutation and failure to
  create any hypothetical late marker;
- marker unlink failure before the syscall versus remove-then-raise ambiguity
  after the commit point, on plan, benchmark, verify, and standalone flows;
- quarantine `EXDEV`/`EACCES`, unlink interruption, atomic replace/directory
  fsync failures, predecessor restoration, candidate drift, and final scan
  refusal;
- caller-owned root and nested refused-lookalikes and bundle file/byte limits;
- stdout failure after a successful terminal commit;
- internal owned-task cancellation combined with one or multiple genuine
  external cancellations, preserving payload, count, primary failure, and
  terminal reports for every stream/scheduler/facade boundary;
- webhook reachability through module globals, traceback globals/locals,
  `self`, args, cause/context, constructor failures, GC without close, and
  malformed HTTP responses;
- invalid Settings URL redaction and diagnostic fidelity;
- malformed `600/999`, successful null reads, structured non-200 errors,
  invalid gzip, and hostile scope/build semantics;
- approval binding, mutation journal transitions/resume, clean-HEAD drift,
  reservations, budgets, page caps, probes, nested applicability, candidate
  provenance, and facade delegation.

## Copy-paste review prompt

```text
Проведи независимое read-only ревью exact SHA
07b83c8e6572a2a891a2226eee991773838d467a в новом clean detached worktree.

Нормативный документ:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256:
2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7

Review packet находится в более позднем docs-only commit. Прочитай его через:
git show codex/bitrix24-client-benchmarks:docs/bitrix24-client-2.0/w7-w9/review-packet-v7.md

Не используй сеть, live Bitrix24, вебхуки или credentials; ничего не меняй и
не читай user-owned untracked spec/session files. Запусти полный pytest, Ruff
check, Ruff format --check, strict mypy и git diff --check. Затем воспроизведи
весь attack list packet и самостоятельно ищи расхождения со спецификацией.
Не останавливайся на первом finding: верни один полный итог.

Особенно проверь:
1. Один bundle marker существует до первой dependency mutation.
2. Pre-unlink failure откатывает/fail-closes, а remove-then-raise сохраняет
   полностью committed bundle без ложной rollback note.
3. Plan, benchmark (100 oracle), verify predecessor и standalone publication
   проходят независимый последующий bundle scan во всех допустимых исходах.
4. Quarantine EXDEV/EACCES/unlink faults не оставляют принимаемый refused PASS.
5. External cancellation выигрывает у owned internal cancellation, сохраняя
   payload и точный cancellation count.
6. Повтори весь накопленный security, lifecycle, pagination и provenance attack
   list, а не только последний diff.

Верни findings сначала: P1/P2/P3, exact file/line, минимальный reproducer и
нарушенный пункт спецификации. Если actionable findings нет, напиши:
P1/P2/P3 = 0/0/0
ACCEPT 07b83c8e6572a2a891a2226eee991773838d467a

Если findings есть:
REJECT 07b83c8e6572a2a891a2226eee991773838d467a

После verdict остановись. Не выполняй live/W10/release действия.
```

## Required stop

After the human verdict, stop. Live validation is a separate checkpoint and
requires explicit authorization plus a separately reviewed dataset plan.
