# Реестр решений и проверка провала

> Пользователь утвердил архитектурный синтез консилиума 2026-09-22. R0–R3 не достигли строгой сходимости (4/7, 5/7); ниже зафиксирован выбор с основаниями. Этот файл не заменяет нормативные контракты.

| ID | Статус | Решение и отвергнутая альтернатива | Основание | Нормативная часть |
|---|---|---|---|---|
| D01 | adopted | ОО contracts/mechanics, методные профили вне ядра; отклонён hard-coded каталог | Пользователь и границы b24api | обзор §Цель; registry |
| D02 | adopted | `Request.route` обязателен; отклонён transport-wide `.json` switch и implicit BARE | Смешанные маршруты и отказ от совместимости | transport §Маршрут |
| D03 | adopted | Physical batch inner BARE-only; отклонён `.json`/V3 inner | LIVE misdispatch/failure | transport §Маршрут |
| D04 | adopted | Фильтр на actual HTTPX logger с positive control; отклонён глобальный record factory как достаточная защита | Порядок LogRecord extra/formatting | transport §Защита журналов |
| D05 | adopted | Typed cause и ReplayDisposition сквозь outcomes; отклонён diagnostic extra call | #8 и право caller решать replay | transport §Типизированные ошибки |
| D06 | adopted | Per-method reactive deadline + host header deadline; deferred proactive `time.operating` | LIVE meter замерзает/нуль | transport §Ограничитель |
| D07 | adopted | Позиционный slot ABI с semantic scope oracle, direct JSON первым; deferred form/batch | LIVE 4/5 slot оба 200, form теряет placeholders | traversal §Positional ABI |
| D08 | adopted | Единый CompletionGate и отдельные stride/closure; отклонена исходная неполная completion ladder | Premortem: missing binding/not-executed/ack | traversal §Архитектура/Исполнимый контракт |
| D09 | adopted | Server granularity/one-based page/sparse raw bound отдельны; отклонён `next`/visible count как универсальный cursor | LIVE floor 50, sparse fixture | traversal §Stride |
| D10 | adopted | `LAST_ID` exclusive range; отклонён `FIRST_ID` и `c+1` anchor probe | LIVE: silent loss, ignored 0/null | traversal §Cursor; registry |
| D11 | adopted | Keyset early closure лишь с admitted exact upper ID + fence; отклонён probe-only witness | #5 и mutation risk | traversal §Cursor/keyset |
| D12 | adopted | Per-binding caller stop и checkpoint after commit; отклонён global break как эквивалент | #3, #6/7 | traversal §Caller stop |
| D13 | adopted | Полная structural проверка counted tail; deferred generic batch decoder fix до first-divergence | LIVE direct/batch совпали при фильтре | traversal §Stride |
| D14 | adopted | `im.v2.*` из PHP annotations; отклонена portal `methods` enumeration как oracle | LIVE callable методы отсутствуют в списке | registry §im.v2 |
| D15 | adopted | 19 public API-only examples и независимые oracles; отклонены manual loops как поддержанный сценарий | Пользователь, #6 | examples |
| D16 | adopted | Совместимость 2.2.0 снята; миграция документируется | Пользователь | обзор; migration notes |
| D17 | adopted | Спека локальная, webhook доступен судьям/read-only probes, но credential редактируется из публикуемого пакета | Пользователь; защита credential | обзор; redaction manifest |
| D18 | deferred | Методные gates: первый divergence старого 582/400 fixture, другие positional layouts, sparse raw total/order, optional meter | Нет достаточного evidence для универсальной гарантии | обзор §Технические gates |
| D19 | out of scope | Issue #9 про внешние `pumpe`/`sqlmodel` создано после фиксации #1–#8; примеры используют стандартный SQLite, не импортируют эти пакеты | Явная граница согласованного объёма | обзор §Цель; examples |

## Premortem: обнаружение, реакция, проверка

| Сбой | Ранний сигнал | Контроль | Реакция |
|---|---|---|---|
| Тихая потеря страниц/неучтённого binding | Oracle расходится при HTTP 200; у gate непарный event | Positive-control нарушения на каждой traversal family; exact scheduled/ack ledger | `INCOMPLETE`, не повышать checkpoint, сохранить bounded trace |
| Wrong-scope positional HTTP 200 | Total/IDs не соответствуют заданному task | Semantic scope oracle, exact slot bytes, fail-closed encoding gate | Отвергнуть form/batch или профиль метода до квалификации |
| V3 route сброшен в classic | Ответ другого controller выглядит валидным | Сравнить request/wire provenance в dispatch; BARE-only inner batch | Preflight error с correlated negative outcome |
| Credential попал в INFO/DEBUG | Sentinel в raw/formatted logger test | Positive control HTTPX emitting logger, concurrency/upgrade test | Не выпускать transport; расширить shield/policy для нового logger |
| Пачка зациклилась после throttle | Повторные `OPERATION_TIME_LIMIT` без задержки | Fake-clock per-method deadline, shared coordinator across calls | Budgeted wait; typed failure/plan, без unsafe replay |
| Examples скрыли второй scheduler | Импорт внутренних модулей/большой `_support` | Public import-boundary и размер/requests budget | Исправить клиентский API, вернуть recipe к публичным операциям |
| Ошибка snapshot assurance при mutable offset | Повтор/пропуск IDs при структурно полном интервале | Независимый oracle и слабый label без snapshot evidence | Overlap/reconciliation, не объявлять identity-exact snapshot |
