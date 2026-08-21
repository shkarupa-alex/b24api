# W7-W9 final human review packet v9

## Решение, которое требуется от reviewer

Провести независимое read-only ревью exact implementation candidate
`3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5` и вернуть `ACCEPT` либо
приоритизированные findings именно против этого SHA.

Сам packet находится в более позднем docs-only commit и не входит в subject.
Ревью не разрешает live Bitrix24, использование webhook, seed, cleanup, W10,
performance evidence, публикацию или release.

## Frozen inputs

- branch: `codex/bitrix24-client-benchmarks`;
- последний rejected candidate: `d79ea8a42d9b21faa3a6da5181d17ce3ff69e4ca`;
- review subject: `3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5`;
- нормативная спецификация:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- SHA-256 спецификации:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

Работать в новом clean detached worktree. Не читать user-owned untracked
spec/session files.

```bash
git worktree add --detach /tmp/b24api-human-review \
  3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5
cd /tmp/b24api-human-review
git status --short --branch
git diff d79ea8a42d9b21faa3a6da5181d17ce3ff69e4ca..3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5 -- \
  b24api tests tools/b24api_evidence docs/bitrix24-client-2.0/w9
```

## Что исправлено после v8

- Multi-file publication получает эксклюзивное ownership всего bundle до
  чтения predecessor и до первой canonical mutation.
- Два concurrent publisher не могут откатить или заменить успешно committed
  bundle друг друга. Standalone rollback восстанавливает predecessor,
  прочитанный только после получения lock.
- Persistent lock создаётся как regular single-link файл с mode `0600` и не
  следует filesystem links. Каталог, содержащий только корректный persistent
  lock после полного rollback, можно повторно использовать командой `plan`.
- Journal хранит lexical identity canonical path. Filesystem links в canonical
  path или его вложенном parent отклоняются до чтения и мутации.
- Reserved pending marker распознаётся по lexical directory entry до
  file-type filtering. Dangling/directory links и hardlink aliases остаются
  fail-closed и не заменяются следующей публикацией.
- Stale journal recovery восстанавливает только journal-owned exact content;
  чужое или изменённое canonical content сохраняется, а marker остаётся для
  повторяемого recovery.
- Counted traversal публикует `PaginationDriver.batch_report` до возможного
  cleanup exception, поэтому wrapper сохраняет наблюдаемый terminal report.

## Независимый clean-room результат

Exact subject был проверен другим reviewer в отдельном detached worktree без
изменений, сети, live-доступа, W10 и release actions.

```text
Verdict: ACCEPT
Exact SHA: 3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5
P1/P2/P3: 0/0/0
pytest: 467 passed in 204.24s
ruff check: PASS
ruff format --check: 41 files
mypy --strict: PASS, 25 source files
git diff --check: PASS
tracked status: clean
```

Clean-room отдельно подтвердил ownership-before-read для standalone/shared
transaction, сохранение intervening committed predecessor при отказе,
concurrent publisher refusal, interrupted recovery, marker/lock invariants и
terminal report cleanup. Actionable findings в согласованных функциональных
рамках не обнаружены.

## Обязательный функциональный checklist

Проверить реализацию, а не только этот summary:

1. Полные static gates проходят на exact SHA.
2. Два concurrent `plan` в одном новом artifact directory дают ровно одного
   winner; loser не меняет и не откатывает committed bundle.
3. После прерывания publication следующий bundle-bound command восстанавливает
   точный predecessor либо отказывает fail-closed, не принимая частичный набор.
4. Predecessor standalone/shared publication читается только под transaction
   ownership; rollback не затирает более новый committed predecessor.
5. После полного rollback lock-only каталог допускает повторный public `plan`;
   caller-owned lookalikes не игнорируются.
6. Reserved marker и canonical paths сохраняют lexical identity; необычные
   filesystem entries не превращаются в принятый terminal evidence.
7. Plan, benchmark с 100 oracle files, verify-with-predecessor и standalone
   publication имеют согласованные commit/rollback результаты и повторно
   проходят independent bundle scan.
8. Cleanup failure и cancellation сохраняют правильный `FAILED` report,
   payload/count cancellation и нулевой dispatch следующего запроса.
9. Counted budgets, page reservations, page-cap narrowing, probes, nested
   profile applicability, candidate provenance и facade delegation остаются
   согласованы со спецификацией.
10. Не расширять ревью в углублённый cybersecurity-аудит. Для credential и
    HTTP boundaries достаточно воспроизвести существующие offline regression
    tests; новые exploit-oriented сценарии не требуются.

## Copy-paste prompt

```text
Проведи независимое read-only ревью exact SHA
3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5 в новом clean detached worktree.

Нормативный документ:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256:
2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7

Review packet находится в более позднем docs-only commit:
git show codex/bitrix24-client-benchmarks:docs/bitrix24-client-2.0/w7-w9/review-packet-v9.md

Не используй сеть, live Bitrix24, webhook/credentials, W10 или release actions;
ничего не меняй и не читай user-owned untracked spec/session files.

Запусти полный pytest, Ruff check, Ruff format --check, strict mypy,
git diff --check и проверь clean detached status. Затем выполни функциональный
checklist packet: transaction ownership и predecessor ordering, concurrent
publisher, interrupted recovery, lock-only retry, marker/path identity,
multi-file commit/rollback, cleanup/cancellation report и pagination/profile
contracts. Не углубляйся в cybersecurity: существующих offline security
regression tests достаточно.

Не останавливайся на первом функциональном finding. Верни findings сначала:
P1/P2/P3, exact file/line, минимальное воспроизведение и нарушенный пункт
спецификации.

Если actionable findings нет:
P1/P2/P3 = 0/0/0
ACCEPT 3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5

Если findings есть:
REJECT 3947ac5dcd0b41aca234a14d0bdc2bb61bb4d3a5

После verdict остановись. Не выполняй live/W10/release действия.
```

## Required stop

После human verdict остановиться. Live validation — отдельный checkpoint с
отдельной явной авторизацией и отдельно reviewed dataset plan.
