# Спецификация доработок b24api: закрытие issues и аудит

> Дата: 2026-09-22. База `origin/develop` = `154ddbafb5821b7975fa5bb9e59d8c79797d109f`, b24api 2.2.0. Статус: архитектурный синтез утверждён пользователем; три раунда spec review завершились без принятия (6/10 и 5/10), после них выполнены ограниченные исправления для пользовательского ревью. Это план, не заявление о реализованных исправлениях.

## Цель и правила

Закрыть согласованные восемь [issues b24api #1–#8](https://github.com/shkarupa-alex/b24api/issues), все 19 пунктов [#6](https://github.com/shkarupa-alex/b24api/issues/6) и восемь наблюдений из [аудита](https://github.com/shkarupa-alex/b24api/issues/6#issuecomment-5772905794). Для сложных потоков разработчик должен использовать публичный ОО API клиента, а не вручную исправлять запросы, полноту, ошибки, pacing и scheduler. `examples/` доказывает это исполняемыми сценариями. Совместимость с API 2.2.0 не требуется. Нужны понятные migration notes, но старые сигнатуры не связывают архитектуру. Спецификация и результаты остаются локальными; корпоративный Specs Service не используется. Созданное позднее #9 про внешний `pumpe`/`sqlmodel` не входит в согласованный объём; примеры используют стандартный `sqlite3` и не зависят от этих пакетов.

Клиент владеет общими контрактами вызова, ограничителя, обхода и отчёта. Registry/версионированные method profiles владеют сведениями о конкретных REST методах, PHP slot ABI и доказанными особенностями портала. Приложение владеет прикладной БД, commit и checkpoint. Не создавать второй framework: расширять `contracts/`, `Request`/`WireRequest`, `RateCoordinator`, существующие drivers и reports малыми value objects с одной ответственностью. Методного каталога в ядре нет. Все публичные факты помечаются `SOURCE` (закреплённый исходник/SHA), `LIVE` (конкретный синтетический портал), `FIXTURE` (детерминированный тест), `UNVERIFIED`. LIVE не превращается в универсальную гарантию Bitrix.

## Комплект

- [Транспорт, маршруты и ошибки](transport-and-errors.md): #1, #7, #8, V3 и метрики.
- [Обходы и доказательство завершения](traversal-contracts.md): #3, #5, positional ABI, page index, sparse offset, counted tail.
- [Registry и методные предупреждения](registry-contracts.md): #2, #4; код меняется в отдельном `apidocs` repo.
- [19 исполняемых сценариев](examples-contracts.md): #6 и сквозное доказательство удобства.
- [Реестр решений и premortem](decision-ledger.md): принятые, отклонённые и отложенные решения, сигналы и меры.

Этот обзор является нормативной картой охвата; предметные документы конкретизируют API, invariant и тесты. При противоречии нормативны обзор и предметные контракты; [синтез консилиума](synthesis.md) сохраняет мотивировку, [реестр](decision-ledger.md) — историю решений. Всякий конфликт внутри нормативного комплекта блокирует реализацию до исправления.

## Решения и границы API

1. **Явный маршрут.** `Request.route` обязателен: `BARE`, `JSON`, `API_V3`, точный регистр метода и provenance сохраняются через все публичные пути. URL решается при dispatch; V3 может идти через клиент с classic base. Внутренние команды physical batch только BARE до отдельного доказательства. Неверный маршрут reject до I/O с correlated outcome. На портале JSON suffix работает direct/envelope, но не в inner cmd; V3 в classic batch способен тихо misdispatch.
2. **Защита credential.** Reference-counted filter ставится на реально emitting HTTPX INFO logger, до formatting; lock покрывает register/release/in-flight. Positive-control тест обязан доказать, что record вышел и был переписан. Собственные исключения redacted; чужой logger traffic не меняется. Тесты не содержат пользовательский webhook.
3. **Ошибки и throttle.** V1 scalar и V3 object errors типизированы. V3 dotted `validation[].field` сохраняется строкой. Original typed cause, normalized code, retryability и computed replay disposition проходят через batch, reference, traversal violation. `RateCoordinator.acquire(work_class: WorkClass, *, methods: frozenset[str], budget: DeadlineBudget | None = None)` соблюдает host header cooldown и per-method `operation_time_limit` с настраиваемым защитным default 120 сек. Никакого автоматического повторного запуска небезопасной batch пачки. `time.operating` лишь наблюдается до квалификации метода.
4. **Позиционный ABI.** `PositionalArguments` хранит точные top-level слоты и порядок, transactional writer проверяет casing, predicates, nested controls и encoded bytes. Direct JSON — начальный путь. Form/batch fail preflight, если пустые слоты теряются. `task.elapseditem.getlist` имеет LIVE четыре и пять валидных слотов с разным scope; сценарий 13 использует пять и проверяет семантику task scope, не только HTTP 200.
5. **Завершение обхода.** `PageStride` отделён от `ClosureRule`; единственный `CompletionGate` строит сильный terminal report после учёта всех bindings, scheduled/delivered/acknowledged pages, отрицательных/unknown/not-executed исходов, page rejection, caller stop, early close и cleanup. `COMPLETED_WITH_FAILURES` не имеет общего `exhausted=true`. Неподключённая traversal family не может выдать `COMPLETED`. Ограниченная память отчёта; structural completeness не обещает snapshot при мутирующем offset наборе.
6. **Страницы и курсоры.** Server page granularity отделена от wire limit и decoded cap. LIVE `start` у `crm.requisitelink.list`/`tasks.task.list` округляется к 50; продвижение 932 после 900 повторяет страницу. One-based page control продвигается на 1. Sparse offset требует квалифицированный raw bound, fixed stride и budget. `im.dialog.messages.get LAST_ID` — exclusive `id<c`; 0/null игнорируются и потому запрещены, valid positive empty закрывает диапазон. `FIRST_ID` не cursor для полного обхода.
7. **Bounded stop и keyset.** Page-boundary policy завершает отдельный binding с bounded-prefix assurance у cursor/offset/keyset/reference, где это возможно. Counted physical batch tail отвергает stop при construction; direct tail может поддерживать. Checkpoint только после durable commit; resume проверяет bounded overlap. Keyset завершает без пустого confirmation только при точном admitted upper boundary, enforced fence и квалифицированном method contract; отдельный probe row не считается witness.
8. **Counted tail.** Все scheduled offsets/windows и identities проверяются структурно, не выборочными двумя страницами. Старое сообщение 582/400 остаётся исследовательской гипотезой без воспроизводимого frozen fixture: LIVE при `filter[entityTypeId]=2`, шаге 50 и состоянии портала в первом консилиуме direct/batch дали 437/437; позднее независимый неотфильтрованный probe получил total=812, что не является противоречием этому фильтрованному числу. Если исходный >400 fixture доступен, сравнить первый расходящийся raw offset; обязательная приёмка #14 строится на structural page-plan coverage и независимом oracle конкретной fixture, а не на историческом числе 582. Если требуется direct tail, он явный; скрытой повторной выдачи строк нет.
9. **Registry и examples.** PHP `@restMethod` extraction покрывает exact-case `im.v2.*`, в том числе `im.v2.Chat.Message.CommentInfo.list`; portal `methods` enumeration не oracle существования (LIVE пропускает callable методы). Все 19 рецептов используют только публичные exports, независимые oracles, bounded resources, честный evidence class и opt-in live gates. Skipped live fixture не проходит как LIVE. Если batch recovery требует application-side классификации, добавить минимальный публичный bounded retry/resume plan без unsafe auto replay.

## Матрица приёмки

| Источник | Выход | Необходимое доказательство |
|---|---|---|
| [#1](https://github.com/shkarupa-alex/b24api/issues/1) | Credential-safe HTTPX logging | Emitting INFO record реально перехвачен; raw/formatted output, exceptions, concurrency и cleanup без sentinel |
| [#2](https://github.com/shkarupa-alex/b24api/issues/2) | Registry запрещает `FIRST_ID` как полный forward обход | Независимый ID oracle показывает пропуск середины, `LAST_ID` DESC даёт полный qualified диапазон |
| [#3](https://github.com/shkarupa-alex/b24api/issues/3) | Per-binding page stop | Один binding останавливается без следующего запроса, соседи продолжают, report = bounded prefix |
| [#4](https://github.com/shkarupa-alex/b24api/issues/4) | `im.v2.*` source extraction | Coverage всех `@restMethod` annotations, exact case, обязательная CommentInfo карточка; ручная одна карточка недостаточна |
| [#5](https://github.com/shkarupa-alex/b24api/issues/5) | Экономия финального keyset запроса | Exact admitted upper boundary и fence; unknown/changed boundary не создаёт ложный complete; sequential/auto/fast отдельно |
| [#6](https://github.com/shkarupa-alex/b24api/issues/6) | `examples/` 1–19 | Public API-only runnable recipes, independent offline oracles, required opt-in LIVE результаты; gap #1/#11/#12/#19 исправлены кодом |
| [#7](https://github.com/shkarupa-alex/b24api/issues/7) | Явный route через facade/transport/CLI | BARE/JSON/V3 matrix, direct, binary, traversal, batch preflight, exact case; live endpoint success отдельно от mock |
| [#8](https://github.com/shkarupa-alex/b24api/issues/8) | Typed batch throttle/cooldown | Причина и disposition доступны без extra call; fake-clock per-method 120 sec, host header и budget |
| Аудит 1 `.json` | #7 route | Без `.json.json`; physical inner batch boundary соблюдена |
| Аудит 2–3 high-water | Наблюдаемые peak metrics | Bounded inequality; exact peak только с deterministic barrier |
| Аудит 4 positional | Slot layout | Exact bytes, empty placeholders, correct semantic scope; отдельно для каждого legacy метода |
| Аудит 5 V3 | Typed object error | Valid success/4xx, flat dotted field, malformed `ProtocolError`, redacted bounded detail |
| Аудит 6 page index | `PageStride` one-based | Wire 1/2/3 и 10/3/0 строк с независимым oracle |
| Аудит 7 sparse offset | Raw-bound closure | Empty selected page между nonempty не завершает, неизвестный bound не создаёт complete |
| Аудит 8 direct tail | Counted structural closure | Все offsets и composite identities сверены; first-divergence probe на старом fixture |

## Порядок реализации и контрольные точки

1. Заморозить исходные SHA client, registry/PHP и fixtures. Подготовить synthetic sentinel и независимые oracles; не переносить неопубликованные `.json` коммиты автоматически. Нужные live probes read-only на предоставленном синтетическом портале; webhook не записывать в код, docs, тесты, CI или публичные логи.
2. Внести базовые `Request.route`, positional ABI, typed error/cause, logging shield, rate coordinator и версии профилей. Проверить реальные wire bytes, HTTPX emitting logger и silent wrong-scope cases. Рискованные комбинации fail preflight.
3. Внести `PageStride`, `ClosureRule`, `CompletionGate` и его event wiring для каждой traversal family. Positive-control тестами сломать каждый guard; проверить offset floor, sparse empty, counted tail, cursor domain, caller stop и checkpoint/resume. Только gate строит strong completion.
4. Обновить `apidocs` из PHP extraction и method warnings; зафиксировать coverage и exact case. Не менять существующий dirty checkout `apidocs` в рамках подготовки спеки. При реализации работу в отдельном безопасном checkout/пакете.
5. Реализовать `examples/` 1–19 и import-boundary тест. Каждый сценарий показывает методный профиль, публичный вызов, outcome/report, oracle, bounds, request count и доказанный класс. Если пример выявит недостающий публичный контракт, исправить клиент до повышения статуса.
6. Закрывать issue по строке матрицы, с тестом/примером/профилем и ссылкой на evidence. HTTP 200 без semantic oracle, чистый пустой ответ, skipped LIVE и локальный workaround не закрывают пункт.

## Проверки риска

- Протестировать gate на missing binding, not-executed outcome, rejected page, mismatched delivery/ack, cleanup failure; архитектурный тест запрещает terminal report construction в обход gate.
- Протестировать route provenance на `with_parameters`, `call_bytes`, direct fan-out, batch, traversal и CLI; запретить silent classic fallback V3.
- Протестировать logging shield положительным control и закрытием конкурентных transports; HTTPX upgrade требует повторного probe.
- Проверить positional semantic scope отдельно от wire equality: оба four/five slot запроса могут вернуть HTTP 200.
- Проверить bounded-prefix и snapshot assurance: mutable offset source не получает snapshot label из структурно полного обхода.
- Проверить рецепты на импорт только публичного API, объём деклараций, реальную экономию запросов и bounded память. Скрытая реализация scheduler в `examples/_support` считается провалом цели.

## Решения консилиума и открытые технические gates

Полный консилиум: два независимых судьи (Opus и Codex), предложения R0, взаимные проверки R1–R3, две доработки, отдельный premortem обоих. После трёх раундов строгой сходимости не было (4/7 и 5/7); утверждённый пользователем синтез выбрал решения по свидетельствам, а не объявил одно предложение победителем. Оба судьи получили разрешённый webhook и использовали только read-only диагностику синтетического портала. Аудитные артефакты сохраняют provenance; credential не помещается в эту спецификацию.

Технические gates: определить first-divergence на исходном >400 fixture; закрепить PHP slot layouts для прочих legacy методов; подтвердить raw total/стабильный order у `im.search.user`; подтвердить live fixtures для method-specific сценариев; квалифицировать `time.operating` перед optional meter policy. Если gate опровергает гипотезу, изменить method profile и recipe, не расширяя недоказанную сильную гарантию. Бизнес-вопросов, блокирующих эту спеку, нет.

## Разрешение замечаний трёх раундов spec review

Судьи оценили последнюю автоматически переработанную обзорную редакцию в 6/10 и 5/10 без принятия. Часть замечаний относилась к старому `spec/2026-09-22-b24api-issues/`, который не являлся приватным каталогом этой редакции. После лимита раундов выполнена целевая сверка, без заявления о новом судейском одобрении:

- `SR-01`, compatibility: обязательный route одинаково задан здесь и в [транспорте](transport-and-errors.md); ссылка на синтез разрешается в текущем комплекте.
- `SR-02`, completion/memory: [обходы](traversal-contracts.md) определяют события, переходы, `exhausted/partial`, точный внешний `IdentityStore` либо квалифицированный ordered invariant и finite budgets.
- `SR-03`, throttle: сохранён `WorkClass`, определены host/client lifetime, whole-batch wait, per-method map, eviction/cap и direct/batch observation в [транспорте](transport-and-errors.md).
- `SR-04`, keyset: [обходы](traversal-contracts.md) определяют bounded ID range, enforced fence, mutation boundary и слабую assurance при отсутствии snapshot.
- `SR-05`, интерфейсы: `PageStopPolicy`, `SparseRawBound`, `PageIndex`, `PositionalArguments`, `CompletionGate` и rejection cases определены в предметных контрактах; остающиеся portal qualification gates не объявлены реализованными.
- `SR-06`, traceability: [реестр](decision-ledger.md) включает adopted/rejected/deferred и premortem.
- Методные факты: [registry](registry-contracts.md) различает пустой `messageIds`, все неизвестные и смешанный валидный/невалидный набор, а [examples](examples-contracts.md) запрещает `next` у positional elapsed list. Числа 437 и 812 относятся к разным фильтрам/моментам; 582/400 — историческое наблюдение.

Эта целевая сверка исправляет текстовые блокеры, но не заменяет повторного независимого принятия окончательной редакции. Технические gates и live qualification остаются задачами реализации; пользовательская проверка комплекта — следующий шаг.
