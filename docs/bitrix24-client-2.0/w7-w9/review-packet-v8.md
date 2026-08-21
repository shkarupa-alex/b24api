# W7-W9 final human review packet v8

## Blocking decision

Review the exact immutable implementation candidate
`ec931d89c2c56a7894c53bfc9cb4cf60ebc3bb63` and either accept it or return
prioritized findings against that SHA.

This packet is a later documentation-only commit, not the review subject.
Acceptance does not authorize live Bitrix24 access, webhook use, seed, cleanup,
W10, publication, release actions, or performance claims.

## Frozen inputs

- branch: `codex/bitrix24-client-benchmarks`;
- rejected predecessor: `07b83c8e6572a2a891a2226eee991773838d467a`;
- review subject: `ec931d89c2c56a7894c53bfc9cb4cf60ebc3bb63`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

Review in a new clean detached worktree. Do not use network, live credentials,
or user-owned untracked specification/session files.

```bash
git worktree add --detach /tmp/b24api-human-review \
  ec931d89c2c56a7894c53bfc9cb4cf60ebc3bb63
cd /tmp/b24api-human-review
git status --short --branch
git diff 07b83c8e6572a2a891a2226eee991773838d467a..ec931d89c2c56a7894c53bfc9cb4cf60ebc3bb63 -- \
  b24api tests tools/b24api_evidence docs/bitrix24-client-2.0/w9
```

## Findings addressed after v7

- Multi-file publication now holds one non-blocking OS lock for the complete
  artifact directory. Two concurrent publishers cannot both own or roll back
  canonical paths.
- The fixed v2 pending marker is a durable ownership journal written before
  each canonical mutation. Every entry binds the relative path, predecessor
  JSON, and exact SHA-256 of the new content.
- A released stale transaction is recovered automatically by the next
  bundle-bound command. Recovery restores only exact journal-owned content;
  foreign canonical content is preserved and the marker remains fail-closed.
- A persistent restore failure remains recoverable after its environmental
  cause is removed. Manual marker deletion is neither needed nor the documented
  recovery path.
- A different plan cannot replace a previously committed immutable plan bundle,
  whether the attempts overlap or run sequentially.
- Owned cleanup now returns both its failure and any concurrently delivered
  external cancellation. Cleanup failure remains the first exception, while
  the external payload and exact cancellation count are replayed at the next
  await.
- Executor checks replayed cancellation before any dispatch, including an
  otherwise fully synchronous transport path.
- Cleanup failure without a prior failure freezes a `FAILED` report carrying
  one blocking `cleanup_failure`; the raised exception carries that same report.

## Independent clean-room result

An independent reviewer audited the exact subject in a separate detached
worktree without edits, live access, credentials, external network, or reads of
user-owned untracked files.

```text
Verdict: ACCEPT
Exact SHA: ec931d89c2c56a7894c53bfc9cb4cf60ebc3bb63
P1/P2/P3: 0/0/0
pytest: 454 passed
focused accumulated suite: 334 passed
ruff check: PASS
ruff format --check: 41 files
mypy --strict: PASS, 25 source files
git diff --check: PASS
```

The reviewer ran ten real pairs of concurrent `plan` processes. Every pair
produced exit codes `[0, 2]`, exactly one completion, four canonical files plus
the accounted lock file, no pending marker, and a passing independent scan.
SIGKILL recovery, journal-before-write and journal-after-write windows,
foreign-content protection, persistent restore failure and retry, and real
plan-bundle bind recovery all passed.

For BatchStream, ReferenceStream/fan-out, and ItemStream cleanup, external
cancellation counts 1, 2, and 3 retained the `external-caller` payload. Cleanup
failure and its `FAILED` report arrived first; the next independent executor
call was cancelled before transport, with zero physical attempts and no loop
exception-handler events.

## Required attack list

Review the implementation rather than this summary, including:

- real concurrent and sequential different-plan publishers in one artifact
  directory; exclusive ownership must precede every mutation and rollback;
- SIGKILL during journal creation, before/after canonical replace, during
  rollback, and around terminal marker unlink;
- stale-marker automatic recovery through bundle binding used by cleanup and
  recover-manifest, with predecessor restoration and an independent scan;
- foreign canonical content, corrupted/legacy marker data, symlink/path escape,
  persistent permission/fsync failures, and retry after the fault is removed;
- lock and marker file/byte/secret accounting plus caller-owned lookalikes;
- plan, benchmark with 100 oracle files, verify predecessor, and standalone
  transaction commit/rollback paths;
- owned cleanup RuntimeError combined with external cancellation counts 1, 2,
  and 3 for BatchStream, ItemStream, ReferenceStream/fan-out, counted driver,
  and facade boundaries;
- immediate replay before a synchronous independent transport attempt, exact
  cancellation payload/count, terminal report identity, and loop diagnostics;
- webhook/settings/vault redaction and GC, malformed HTTP `600/999`, null reads,
  structured non-200 errors, invalid gzip, and hostile scope/build semantics;
- approval binding, mutation journal transitions/resume, clean-HEAD drift,
  reservations, budgets, page caps, probes, nested applicability, candidate
  provenance, facade delegation, and public API stability.

## Copy-paste review prompt

```text
Проведи независимое read-only ревью exact SHA
ec931d89c2c56a7894c53bfc9cb4cf60ebc3bb63 в новом clean detached worktree.

Нормативный документ:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256:
2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7

Review packet находится в более позднем docs-only commit. Прочитай его через:
git show codex/bitrix24-client-benchmarks:docs/bitrix24-client-2.0/w7-w9/review-packet-v8.md

Не используй сеть, live Bitrix24, вебхуки или credentials; ничего не меняй и
не читай user-owned untracked spec/session files. Запусти полный pytest, Ruff
check, Ruff format --check, strict mypy и git diff --check. Затем воспроизведи
весь attack list packet и самостоятельно ищи расхождения со спецификацией.
Не останавливайся на первом finding: верни один полный итог.

Особенно проверь:
1. Два реальных concurrent plan в один пустой artifact-dir дают одного winner;
   loser не изменяет и не откатывает committed bundle winner'а.
2. Retained v2 marker после SIGKILL автоматически и точно восстанавливает
   predecessor; foreign canonical остаётся неизменным и fail-closed.
3. Cleanup и recover-manifest могут пройти после безопасного stale recovery,
   без ручного удаления marker и без принятия refused content.
4. Cleanup RuntimeError выходит первым с FAILED/cleanup_failure report, затем
   external cancellation N=1/2/3 переигрывается с исходным payload.
5. Следующий synchronous Executor отменяется до transport: attempts == 0.
6. Повтори весь прежний security, lifecycle, pagination и provenance attack
   list, а не только последний diff.

Верни findings сначала: P1/P2/P3, exact file/line, минимальный reproducer и
нарушенный пункт спецификации. Если actionable findings нет, напиши:
P1/P2/P3 = 0/0/0
ACCEPT ec931d89c2c56a7894c53bfc9cb4cf60ebc3bb63

Если findings есть:
REJECT ec931d89c2c56a7894c53bfc9cb4cf60ebc3bb63

После verdict остановись. Не выполняй live/W10/release действия.
```

## Required stop

After the human verdict, stop. Live validation is a separate checkpoint and
requires explicit authorization plus a separately reviewed dataset plan.
