# b24api 2.x

`b24api` is a thin asynchronous Bitrix24 REST client for Python 3.12+. It knows how to send
requests, split logical batches, traverse lists, retry safely, preserve caller correlation and
close resources. It does not contain a Tasks, CRM or IM method catalog and does not impose
application storage rules.

## Install and configure

```console
uv sync --frozen
export BITRIX24_API_WEBHOOK_URL='https://portal.example/rest/.../'
```

Keep the webhook out of source, logs and command arguments. Reuse one client for a related unit of
work so its HTTP/2 connection pool and rate state are reused. The URL has the form
`https://<portal>/rest/<user>/<token>/`, with a numeric user and a token of letters, digits, `_` and
`-`. `http` is accepted. Letter case is normalized only in the scheme and host, which are lowered: the
path keeps its case, so the token is sent as written and `/REST/` is refused. A default port is dropped
and any other kept, `.` and `..` segments are resolved, a missing trailing slash is added and
surrounding whitespace is dropped. Any other form is refused, one with a user name or password, a
query, a fragment or a percent-encoded path included, since the token is its only credential and is
registered for redaction as it is sent.

`Settings` reads two limits as well. `BITRIX24_API_HTTP_TIMEOUT` is each request's retry window in
seconds (30 by default, finite). `BITRIX24_API_MAX_CONCURRENT_REQUESTS` is how many requests one client
keeps in flight at once, all its calls, streams and retries together (10 by default). An operation's
own limits only narrow it, and which of them apply depends on the dispatch. A `DirectDispatch` sends at
most the smallest of its `concurrency`, `ExecutionPolicy.max_direct_concurrency`,
`ExecutionPolicy.max_active_references` and the client limit at once. A `BatchDispatch` keeps at most
the smallest of its `concurrency`, `max_active_references` and the client limit in flight;
`max_direct_concurrency` does not limit it. So `DirectDispatch(concurrency=40)` alone still runs 10
requests at a time. Raise the client limit only as far as the portal's rate limits allow.
`Bitrix24.from_webhook()` takes both limits as arguments:

<!-- tested: tests/settings_test.py::test_effective_concurrency_is_the_smallest_of_the_client_and_operation_limits -->
```python
import os

from b24api import Bitrix24, Request

async with Bitrix24.from_webhook(os.environ["BITRIX24_API_WEBHOOK_URL"], max_concurrent_requests=20) as client:
    users = await client.call(Request.bare("user.get", {"ID": 1}))
```

## Quickstart

<!-- tested: tests/readme_test.py::test_quickstart_runs_exactly_against_a_scripted_portal -->
```python
import os

from b24api import Bitrix24, Request

async with Bitrix24.from_webhook(os.environ["BITRIX24_API_WEBHOOK_URL"]) as client:
    deals = client.iter_list(Request.bare("crm.deal.list", {"select": ["ID", "TITLE"]}))
    async for deal in deals:
        print(deal["ID"], deal["TITLE"])
    print(deals.report.state)
```

Four things are at work:

- **The client.** `Bitrix24.from_webhook()` checks the URL and owns the connection pool it opens;
  `async with` closes it. `Bitrix24()` reads the same URL from the environment.
- **The request.** `Request.bare()` names a REST method and its parameters and sends them to the
  classic `/rest/` endpoint. The route is always explicit: `Request.v3()` targets the V3 API.
- **`iter_list()`.** It walks the list page by page and reads one more, empty, page to confirm
  the end.
- **The report.** `deals.report` says how the traversal ended. `completed` means every page was
  read; a traversal that stops early or fails records why.

A single call returns the decoded `result`:

<!-- tested: tests/readme_test.py::test_quickstart_runs_exactly_against_a_scripted_portal -->
```python
users = await client.call(Request.bare("user.get", {"ID": 1}))
```

The client owns the transport it creates. An injected transport remains caller-owned. `aclose()` is
idempotent and closes active streams before the owned transport. A closed client raises
`ClientClosedError` (from `b24api.errors`) for every call awaited and every stream created after
`aclose()` (the method that creates the stream raises it), and for the calls still waiting for a permit
when it closed. It is a `TransportError` of phase `not_dispatched`, so the attempt it stopped was not
sent, and it is never retried. It is a `RuntimeError` as well. The phase covers only that attempt: a
`SAFE` request that waited for the permit of a retry may already have run on an earlier attempt. To
prove that no row is missing or repeated, give the traversal an identity (see
[Choosing a list operation](#choosing-a-list-operation)).

## Direct calls

Use `call()` for detached decoded JSON and `call_response()` when you also need the immutable
response envelope: `result`, `total`, `next`, timing and bounded diagnostic evidence.

Use `call_bytes()` when a successful method response is a file rather than a Bitrix JSON envelope.
The operation is explicit and never hides malformed JSON by falling back to bytes.

<!-- tested: tests/client_findings_3_test.py::test_binary_call_returns_every_success_byte_without_json_sniffing -->
```python
from b24api import RouteKind
archive = await client.call_bytes(Request("example.export.download", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE))
payload = archive.body
```

`archive.filename` is the name decoded from `Content-Disposition` (`filename*` takes precedence
over `filename`), or `None`; `archive.evidence.filename_status` records whether that header was
absent, parsed or malformed. The name is caller-facing metadata, not a safe path: it is never
normalized, truncated or reduced to a basename, and it stays out of the representation and
diagnostics. Sanitize it before using it on a filesystem.

<!-- tested: tests/client_v2_test.py::test_call_and_call_response_have_stable_detached_types -->
```python
from b24api import RouteKind
from b24api import ReplaySafety

request = Request("example.item.get", {"id": 7}, ReplaySafety.SAFE, route=RouteKind.BARE)
decoded = await client.call(request)
response = await client.call_response(request)
```

### Replay safety

`Request.replay_safety` describes what the client may do when a connection fails after the request
may already have reached Bitrix:

| Value | Meaning | After possible dispatch |
|---|---|---|
| `SAFE` | Repeating the request cannot create a second business effect. Typical reads and explicitly idempotent operations belong here. | Automatic retry is allowed within policy budgets. |
| `UNSAFE` | Repeating the request is known to risk a duplicate effect, for example creating an entity without an idempotency key. | No automatic replay; the caller receives an ambiguous-execution error and reconciles state. It is retried only when the failure proves it did not run. |
| `UNKNOWN` | The caller has not established whether replay is safe. This is the default. | Same conservative behavior as `UNSAFE`, while diagnostics preserve that safety was unknown rather than known unsafe. |

A failure that proves the request did not run is retried whatever its safety: a transport failure
before dispatch, or a refusal listed in `AmbiguityPolicy`, which by default is an unstructured 423, 425
or 429 (`refusal_http_statuses`) or a `QUERY_LIMIT_EXCEEDED` / `OPERATION_TIME_LIMIT` /
REST 3.0 `BITRIX_REST_V3_EXCEPTION_RATELIMITEXCEPTION` refusal (`refusal_api_codes`). A code or
status added only to `RetryPolicy` is retried for `SAFE` work alone. A request that HTTPX could not
build was never sent: it fails at once with a `TransportError` of phase `not_dispatched`, never as
ambiguous. An exception from an injected client's event hook, auth flow, transport or response
stream may follow dispatch. An HTTPX request error (`httpx.RequestError`, such as a connection or read
error) follows the same phase rule as one from the network. A builtin `TimeoutError` counts as the
attempt's deadline passing after possible dispatch: `SAFE` work is retried, `UNSAFE` and `UNKNOWN`
work raises `AmbiguousExecutionError`. Any other `Exception` is not retried and counts as possibly
sent, with the same consequence for `UNSAFE` and `UNKNOWN` work; the failure itself is a
`TransportError` that names the exception's class and any HTTP status the client received
(`error.http_status`), without the exception, which would carry the webhook URL, on its chain. A
cancellation is re-raised with the same arguments, without the HTTPX frames it was raised in, and an
exception that is not an `Exception` (`KeyboardInterrupt`) passes through unchanged. A response the
transport refuses itself is reported as `ProtocolError` (a status outside 100-599) or
`ResponseTooLargeError` rather than `TransportError`.
A physical batch applies these rules to each command: after a failure, only the commands that
may run again are sent again, in a smaller batch, and an `UNSAFE` command of a batch that may have run
arrives as unknown. Replay rounds spend the same attempt and retry-time budget as the sends before
them. Method names never imply safety;
mark a request `SAFE` only when the operation's semantics justify it.

Use `ExecutionPolicy` to narrow attempts or resource budgets for one operation:

<!-- tested: tests/execution_test.py::test_ambiguous_dispatch_never_retries_unproven_request -->
```python
from b24api import ExecutionPolicy

one_attempt = ExecutionPolicy(max_attempts_per_request=1)
result = await client.call(request, policy=one_attempt)
```

A `policy=` argument replaces the client's default policy wholesale; fields are never merged. The
client default is `ExecutionPolicy.from_settings(settings)`: the library defaults with
`max_retry_elapsed_per_request` taken from `Settings.http_timeout` (30 s by default, while a bare
`ExecutionPolicy()` allows 120 s). To change one field and keep the configured timeout, derive the
per-call policy from that default:

<!-- tested: tests/settings_test.py::test_a_per_call_policy_replaces_the_client_default_without_merging -->
```python
import dataclasses

from b24api import ExecutionPolicy

one_attempt = dataclasses.replace(ExecutionPolicy.from_settings(settings), max_attempts_per_request=1)
result = await client.call(request, policy=one_attempt)
```

`max_retry_elapsed_per_request` (`Settings.http_timeout`) is each request's retry window. It opens
when the request's first send gets a permit from the client's shared limiter, so waiting in that
queue spends only the operation's `max_elapsed` (900 s by default; lower it to bound the queue, at
the cost of first sends that a long queue leaves only a sliver of it, and a known pause that outlasts
it fails at once). Every send, retry wait and later permit wait then comes out of the window. There
is no separate per-attempt timeout: each send may use what is left of the window. A retry waits the
longer of its own exponential backoff and the server's hint (`Retry-After` or
`X-Bitrix-RateLimit-Reset`, a date measured against the response's `Date`), so a hint of zero or a
past date never cancels the backoff. A retry of an `UNSAFE` or `UNKNOWN` request is sent only while
the window it would have left is at least as long as it waited (as planned, or longer when the wait
overran). Otherwise the request ends with `BudgetExceededError`, whose cause is the refusal that
proved the earlier send did not run, instead of a send whose wait took most of the window: under the
default 30 s window, a `Retry-After` of 15 s or more ends such a request without a retry even when its
first send took no time. `SAFE` requests are retried with whatever window is left, and so are the
`SAFE` commands of a classic batch replay round, whether the batch or its commands' entries were
refused. Each command is weighed against its own method's pause, as known when its round is planned:
a round waits only as long as its `UNSAFE` and `UNKNOWN` commands can afford, and a `SAFE` command
whose method is held longer is sent in a later round; one still waiting when
`max_attempts_per_request` is spent is not sent again. A pause that appears during a round's wait
stops the whole round at its permit when it outlasts the window. The commands not sent again keep
their own refusal, or end with `BudgetExceededError` caused by the batch refusal they followed. The
first send of a `batch()` or `batch_outcomes()` chunk weighs its commands the same way, by how much
longer their methods are held than the host: a command that cannot wait that long is not sent and ends
with `BudgetExceededError`, so that the others go without waiting for it; a chunk whose every command
can wait, or none can, and every chunk of a fail-fast `batch()`, which is never split, waits within
`max_elapsed` as a lone request does, and so do traversal pages and `fan_out()` waves. By
default `SAFE` requests are also retried after an unstructured 408, 500, 502, 503 or 504
(`RetryPolicy.transient_http_statuses`).

### Error codes

Every error of `b24api.errors` survives `pickle` and `copy` with its message and attributes, so it can
cross a process pool or a task queue. The exception is an error that holds a caller's correlation that
cannot be pickled: `BatchFailed` and `ReferenceFailed` keep their outcomes, each with its correlation,
so `pickle` and `copy.deepcopy()` fail on them with the error the correlation raises (`TypeError` for a
lock, for example); `copy.copy()` still works. A structured refusal raises `ApiResponseError`. Its
message, `wire_code`, `normalized_code`, `code` (lower case) and `to_safe_dict()` name the server's
code, so failures can be told apart:

```text
API error [BITRIX_REST_V3_EXCEPTION_ACCESSDENIEDEXCEPTION]: Access denied
API error [NOTE_EMPTY_UPDATE]: Nothing to update
```

A REST 3.0 `error.code` is shown verbatim whenever it is a snake-case identifier: upper case, such
as `BITRIX_REST_V3_EXCEPTION_INVALIDSELECTEXCEPTION` or `FEATURE_NOT_AVAILABLE_ON_CURRENT_PLAN`, or
lower case of letters only, such as `invalid_select_field`; the server's set is open, so no list is
kept. Error texts, classic
`{"error": ...}` codes and response previews keep the conservative redaction, so a long run of
letters and digits there still renders as `[REDACTED]`. A code that equals or contains your
webhook token or a credential of the request, in any letter case, is always `[REDACTED]`; one that
names a field of your request is shown as its `field#N` alias, or as `[REDACTED]` when only its
letter case differs.

Recognizing a code does not make it retryable. By default only the REST 3.0 load limits are retried,
after the server's `Retry-After`:

| Code | Status | Retried |
|---|---|---|
| `BITRIX_REST_V3_EXCEPTION_RATELIMITEXCEPTION` | 429 | Any direct request: the limiter refuses before the method runs. In a REST 3.0 batch envelope only `SAFE` commands, as for any structured envelope error. |
| `BITRIX_REST_V3_EXCEPTION_LIMITER_QUERYTIMELIMITEXCEPTION` | 503 | `SAFE` requests only: the method may have run. |

Every other REST 3.0 code is a deterministic refusal and is raised at once, including a 429 or 503
that a module declares for a longer-lived state (`LANDING_AI_SITE_MODERATION_BLOCK_LIMIT`,
`LANDING_AI_MODULE_UNAVAILABLE`). List more codes in `RetryPolicy.transient_api_codes` (and in
`AmbiguityPolicy.refusal_api_codes` when the code proves the method did not run). The request
faults `BITRIX_REST_V3_EXCEPTION_VALIDATION_REQUESTVALIDATIONEXCEPTION` and
`BITRIX_REST_V3_EXCEPTION_METHODNOTFOUNDEXCEPTION` in a REST 3.0 error envelope are never retried,
even when listed.

## Logical batch and correlation

`batch()` accepts an arbitrary-length synchronous or asynchronous command source. It consumes the
source incrementally and splits it into physical Bitrix batches of at most 50 commands; the full
input is never materialized.

`Command.correlation` is arbitrary caller-owned state. It is retained by reference, returned with
the outcome, never serialized to Bitrix and never included in safe diagnostics. This is useful for
matching a result to the object, file, chat or database row that produced its request.

<!-- tested: tests/client_v2_test.py::test_logical_batch_is_unbounded_ordered_and_correlation_is_strictly_off_wire -->
```python
from b24api import RouteKind
from b24api.contracts import Command, CommandSuccess

commands = (
    Command(
        Request("example.item.get", {"id": item_id}, ReplaySafety.SAFE, route=RouteKind.BARE),
        correlation=item_id,
    )
    for item_id in source_ids
)

async with client.batch(commands, batch_size=25) as stream:
    async for outcome in stream:
        assert isinstance(outcome, CommandSuccess)
        consume(outcome.correlation, outcome.result)
```

`batch()` is fail-fast. `batch_outcomes()` continues where safe and yields one of
`CommandSuccess`, `CommandFailure`, `CommandNotExecuted` or `CommandOutcomeUnknown` in input order.
It does not continue past its budgets: once `max_requests` refuses a send, it yields the outcomes of
the commands it has already read, those never sent as `CommandFailure` with `BudgetExceededError`,
then raises `BudgetExceededError`, reads no further input and closes the source, as when
`max_elapsed` runs out. Its report is `incomplete` and records the `budget_exhausted` violation.
`fan_out_outcomes()` and the reference traversals end the same way.

<!-- tested: tests/client_v2_test.py::test_batch_outcomes_retains_typed_failure_without_halting_later_commands -->
```python
from b24api.contracts import CommandFailure, CommandNotExecuted, CommandOutcomeUnknown

async with client.batch_outcomes(commands) as stream:
    async for outcome in stream:
        match outcome:
            case CommandSuccess() as success:
                consume(success.correlation, success.result)
            case CommandFailure() | CommandNotExecuted() | CommandOutcomeUnknown():
                handle(outcome)
```

Commands whose request uses `RouteKind.API_V3` travel in native REST 3.0 batch envelopes: one JSON
array of at most 50 commands, never mixed with classic commands and grouped into contiguous runs of
one replay safety (`UNSAFE` and `UNKNOWN` share a run). Envelopes go out in input order: a classic
command between two v3 commands travels in its own envelope between theirs. The transport must
advertise JSON-array bodies, as `HttpxTransport` does; otherwise such a command fails with
`CapabilityError` before any request. Each array element is checked against its own request's
embedded-error contract, so one failed command neither fails nor resends its successful neighbours.

<!-- tested: tests/v3_batch_test.py::test_a21_wire_four_safe_v3_reads_travel_in_one_envelope_in_input_order -->
```python
from b24api import RouteKind
from b24api.contracts import Command

v3_commands = (
    Command(Request("example.item.get", {"id": item_id}, ReplaySafety.SAFE, route=RouteKind.API_V3), item_id)
    for item_id in source_ids
)
async with client.batch_outcomes(v3_commands) as stream:
    async for outcome in stream:
        handle(outcome)
```

REST 3.0 does not roll a batch back. When a whole envelope fails after it may have run, `SAFE`
commands of a transient failure get one envelope retry within their attempt budget, and commands
still unresolved are sent individually (`RetryPolicy.isolate_safe_batch_abort`, enabled by default)
under the same attempt lineage; `UNSAFE` and `UNKNOWN` commands arrive
as `CommandOutcomeUnknown` without a resend. The report counts `ambiguous_physical_attempts` and
`batch_isolation_requests`. Fail-fast `batch()` finishes resolving the envelope it already sent and
then raises without reading further input.

For independently dispatchable commands, use `fan_out()` or `fan_out_outcomes()` with
`DirectDispatch` or `BatchDispatch`. Delivery order is explicitly `READY` or `INPUT`.

## Choosing a list operation

The unsuffixed operation is the basic strategy with the fewest endpoint assumptions. Faster or
more specialized mechanics have explicit names and explicit preconditions.

| Operation | Use it when | Network mechanics | Completion proof |
|---|---|---|---|
| `iter_list` | The method supports ordinary offset pagination. | Sequential requests follow server `next` (the next offset). Ordinary counted Bitrix list endpoints do server-side COUNT for `total` plus LIMIT/OFFSET page retrieval. | Continuation and empty terminal page; add identity for duplicate detection. |
| `iter_list_counted` | The first response provides an exact filtered `total` and stable offset pages. | Head page is direct; all known tail offsets are grouped into physical Bitrix batches. | Exact total, ranges and identities. |
| `iter_list_keyset` | The method may omit `total`, but reliably supports ordering and filtering by a unique integer identity. | Auto by default: it plans first, then selects boundary-only, sequential, range, or partitioned execution; runtime sends no diagnostic canaries. | Caller-asserted keyset contract, strict monotonic identity, active bound validation, and terminal empty confirmation. |
| `iter_list_cursor` | Each next request depends on a cursor from the previous response. | Sequential dependent cursor requests. | Strict unique monotonic cursor and empty terminal page. |
| `iter_list_continuation` | The next request is described by values on the current page, such as a response cursor object or a pair of controls. | Sequential requests; a pure caller rule returns the next values of declared controls or ends the source. | Rule-declared end; undeclared updates and repeated control vectors fail closed. |
| `iter_list_window` | A newest-first list resumes from an inclusive timestamp boundary and may also return pinned rows. | Sequential requests carrying the oldest paging timestamp of the previous page. | Strict boundary progress, overlap delivered once, and a response flag that closes the source. |
| `iter_cursors` | One or many parent-bound cursor traversals need correlation and shared batching. | Lazy per-parent drivers share the reference batch queue; each binding may have `start_cursor`. | Isolated strict cursor progress and terminal event per binding. |
| `iter_references` | The same list method must run for many parent parameter sets, such as comments per owner or messages per chat. | Bindings are scheduled with direct or physical-batch dispatch; each binding has its own traversal state. | Per-binding rows, completion/failure and caller correlation. |
| `iter_reference_sources` | Parents need different methods or traversals in one operation. | Each source brings its own request and traversal; compatible pages of all sources share one batch queue. | Per-source rows, terminal event and correlation. |

`page_size` is a local decoded-page cap. It is sent to Bitrix only when you provide the endpoint's
exact `limit_path`; the client never guesses method-specific parameter names.

### List traversal comparison

![List traversal comparison](https://raw.githubusercontent.com/shkarupa-alex/b24api/master/list-traversal-comparison.svg)

The animation replays traces executed against a scripted portal with 1,000 dense IDs at 50 rows
per page. `iter_list` sends 20 pages and one empty confirmation (21 HTTP). `iter_list_counted` sends
the head and one batch of 19 pages (2 HTTP). `iter_list_keyset` in auto mode selects range
execution: it reads both ends of the range in one batch, the 19 ranges between them in a second, and
confirms the end with one call for `ID > 1000` (3 HTTP). A `BoundedIdentityRange` with a qualified
upper ID needs no confirmation: sequential execution stops when it receives that ID. On a real portal
auto chooses from the observed geometry, so the plan and its request count can differ; the report's
`keyset_selection` says which plan ran.

### Sequential offset

This is the canonical default. It follows the `next` returned by the server and confirms the end
with an empty page. For ordinary counted Bitrix list endpoints, `next` is an offset for the next
LIMIT/OFFSET page, not a keyset cursor. Producing `total` involves a separate server-side count
query in addition to retrieving the page. These database operations are performed inside the same
REST request: the client does not issue an additional HTTP call just for the count. This distinction
matters for performance: not making a separate HTTP count call does **not** mean avoiding server-side
COUNT work. `iter_list` does not suppress that work; a returned `total` is observational and does not
control this strategy's completion. Exact database implementation is endpoint-specific.

<!-- tested: tests/client_v2_test.py::test_iter_list_is_sequential_mechanics_only_and_report_is_post_cleanup -->
```python
from b24api import RouteKind
from b24api import IdentityCoercion, IdentitySpec, ResultSelector

identity = IdentitySpec(
    item_path=("ID",),
    filter_key="ID",
    order_key="ID",
    coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
)

stream = client.iter_list(
    Request("example.item.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    selector=ResultSelector(("items",)),
    identity=identity,
)
async with stream:
    async for item in stream:
        consume(item)
```

Without `identity`, successful exhaustion is reported as `MECHANICS_ONLY`: pagination completed,
but the client cannot prove that the portal did not duplicate or substitute rows.

A page identical to an earlier page fails the traversal: rows alone cannot tell such a page from a
portal that ignored `start` and returned the same page again, and a changed `next` or `total` cannot
either, because an endpoint that ignores `start` can still compute them from it. A narrow `select`
whose rows repeat by content, such as `{"select": ["STAGE_ID"]}`, therefore needs a unique field in
the rows: add `ID` to the `select` and declare it as `identity`.

A row whose identity is missing or does not match its coercion raises `IdentityContractError`, a
`PaginationError`, as the cause of `IncompleteTraversalError` on every traversal; it names the path,
the coercion, the observed JSON type and the row offset, never the value. The offset counts rows of
the traversal; a fast keyset counts them within the lane that read the page. Identities and cursors
are read before a `page_adapter` runs, so an adapter never sees a row that breaks them.
`DECIMAL_STRING_INTEGER` accepts an integer or the canonical decimal string of one: ASCII digits
without a leading zero, with a minus only before a non-zero value (`"0"`, `"12"`, `"-7"`). `"012"`,
`" 12"`, `"1_2"` and `"+12"` are refused, so two different wire values never become one identity.

`OffsetContinuation.SERVER_NEXT` requires `TotalTermination.EXACT_QUALIFIED`. It never continues
without `next`, and the last Bitrix page carries rows but no `next`, so only an exact total can close
it; `OffsetSpec` refuses the combination without the total with `ValueError`. Without a qualified
total use the default `SERVER_NEXT_OR_OBSERVED_COUNT`, which confirms the end with an empty page.

An endpoint that honors `start` only at a fixed server window and signals its end only with a
shorter page (no usable `total`, no `next`) can declare that stop rule with
`OffsetSpec(short_page_termination=ShortPageTermination.DECLARED_TERMINAL)`; its successful
exhaustion is always `MECHANICS_ONLY`. See
[Declared short-page closure](https://github.com/shkarupa-alex/b24api/blob/master/docs/recipes.md#declared-short-page-closure).

Mapping-backed collections are explicit as well. `MAPPING_VALUES` yields values from a selected
mapping in insertion order; `MAPPING_VALUES_OR_EMPTY` additionally accepts only an empty terminal
sequence and records that degradation in the operation report.

<!-- tested: tests/client_findings_3_test.py::test_shape_rejection_is_retained_as_zero_admission_page_evidence -->
```python
from b24api import RouteKind
from b24api.contracts import ResultCollectionShape

stream = client.iter_list(
    Request("example.dictionary.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    selector=ResultSelector(("items",)),
    collection_shape=ResultCollectionShape.MAPPING_VALUES,
)
async with stream:
    async for value in stream:
        consume(value)
```

### Counted, physically batched tail

The first direct page must contain an exact filtered `total` and, when more rows exist, `next`.
The client derives all remaining offsets from the observed head width and sends tail pages through
bounded physical batches.

<!-- tested: tests/client_v2_test.py::test_counted_traversal_preserves_frozen_request_shape_and_exact_identity -->
```python
from b24api import RouteKind
stream = client.iter_list_counted(
    Request("example.item.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    selector=ResultSelector(("items",)),
    identity=identity,
    page_size=50,
    batch_size=50,
)
```

Use it only when `total` is exact for the supplied filter and offset pages are stable. Any missing
range, overlap, duplicate identity or total contradiction raises `IncompleteTraversalError`.

A filtered call that matches nothing may omit `total` entirely, as `user.get` does with
`result: []`. A first page with no rows, no `next` and no usable `total` (missing, `null`, or the
`-1` unknown sentinel) therefore completes as an observed empty source after that one request: the
report is `completed` and `exhausted`, but its assurance is `mechanics_only` (`identity_exact` with an
identity), never a count-matched claim, and no total is invented. A first page that reports
`total: 0` keeps the count-matched result. Rows without a usable total, a remaining `next`, a
positive total with no rows, a fixed step, or a `ConsistencyPolicy` whose `confirmation_policy` is
`QUALIFIED_TOTAL` (from `b24api.contracts.policy.ConfirmationPolicy`) stay strict and raise
`IncompleteTraversalError`.

Physical batching reduces HTTP exchanges, but does not suppress server-side COUNT in ordinary
counted list subrequests. Each command still performs its own offset page retrieval and associated
total calculation on the server. Do not confuse batching these commands with a no-count traversal.

### No-count keyset

Keyset traversal uses automatic execution by default. Omitting `execution` is equivalent to
`AutoKeysetExecution(StableIntegerKeysetContract())`: the client captures both ordered boundaries,
then selects boundary-only, sequential, range, or partitioned execution from the observed geometry
and available policy capacity. Planning completes before any row is emitted, so partial consumption
still pays that barrier cost. A sequential selection made by auto is a cost decision; a failed or
contradictory fast plan is never silently restarted as sequential.

By using the default, the caller asserts that the endpoint has a stable, unique integer key, honors
strict numeric bounds and ordering, and satisfies empty-confirmation completion. Concurrent mutation
outside the captured middle is handled by the finishing sweep; mutation inside it is outside this
assertion. Pass `SequentialKeysetExecution()` explicitly when an endpoint cannot satisfy the fast
contract or when the previous request-by-request behavior is required.

Static incompatibility with the auto contract raises `CapabilityError` from the
`iter_list_keyset(...)` call before iteration begins. A portal that accepts but contradicts the
declared controls fails before emission with `IncompleteTraversalError`; auto never restarts that
operation silently. Reference traversal can opt into the same selection for each binding; see
[Keyset references](#keyset-references). The
terminal report carries a compact `keyset_selection` (`requested_kind`, `selected_kind`, `reason`)
for every keyset traversal. A `page_stop` callback needs the ordered page stream, so auto reports
`AUTO`, `SEQUENTIAL`, `PAGE_STOP`; an explicit `SequentialKeysetExecution()` reports
`EXPLICIT_SEQUENTIAL`. The detailed `keyset_execution` report is present only when the fast path ran.

Use `await client.verify_keyset_capability(...)` as a development/CI/staging guard on a stable
representative fixture. It performs five strict-bound checks and returns only a `VERIFIED` report;
unsupported and inconclusive verdicts raise `KeysetCapabilityError`. The ordinary
`iter_list_keyset()` remains a separate caller-asserted operation with zero verifier canaries and
may emit a partial prefix before a late endpoint contradiction is detected.
Keep the guard beside the traversal, for example under
`if os.environ.get("ENV") != "PROD":`; set `ENV=PROD` only after qualifying the exact portal,
credentials, method, request/filter, identity, ordering representation, and page cap.

Every list operation also accepts an immutable `PageAdapter` strategy. The adapter synchronously
maps selected frozen items using sibling result metadata while preserving cardinality, order and
configured identities. The identity adapter is the default and preserves existing JSON output.

An advisory `total` may only raise an automatic cost estimate and never proves completion. Reports
record the selected strategy and reason: unbounded auto continuation has the same
`ordered_prefix_only` assurance as sequential traversal, while bounded plans additionally report
`caller_asserted_bounds`. Runtime traversal does not perform verifier canaries.

<!-- tested: tests/keyset_fast_test.py::test_omitted_execution_defaults_to_auto -->
```python
import os

from b24api import KeysetSpec, ParameterPath, ReplaySafety, Request, ResultSelector, RouteKind

request = Request("example.item.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)
selector = ResultSelector(("items",))
keyset = KeysetSpec(
    filter_path=ParameterPath(("filter",)),
    order_path=ParameterPath(("order",)),
)

if os.environ.get("ENV") != "PROD":
    # Accepting an ID filter does not prove strict bounds or ordering.
    await client.verify_keyset_capability(
        request,
        selector=selector,
        identity=identity,
        page_size=50,
        keyset=keyset,
    )

stream = client.iter_list_keyset(
    request,
    selector=selector,
    identity=identity,
    page_size=50,
    keyset=keyset,
)
```

### Dependent cursor

Use a cursor when the next boundary is returned or derived from the previous page, as with many
message-list methods.

<!-- tested: tests/client_v2_test.py::test_keyset_and_cursor_are_explicit_strict_alternatives -->
```python
from b24api import RouteKind
from b24api import CursorSpec, ParameterPath

stream = client.iter_list_cursor(
    Request("example.message.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    selector=ResultSelector(("items",)),
    cursor=CursorSpec(
        parameter_path=ParameterPath(("LAST_ID",)),
        item_path=("ID",),
        coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
        direction="ascending",
        take="last",
    ),
)
```

Cursor values must be unique and strictly monotonic. If an endpoint exposes only a non-unique
boundary, use an application-owned direct-call workflow or supply a unique tie-breaker.

Some message endpoints move the boundary toward older rows but return each page oldest-first.
Declare that page order separately with `page_order="ascending"` beside `direction="descending"` and
`take="first"`: rows must be strictly ordered inside the page and every value must lie beyond the
previous boundary. `continuation=ResponseFlagTerminal(ResultSelector(("hasNextPage",)))` closes the
traversal on a `False` flag, even on a non-empty page, instead of requesting an empty confirmation
page; a missing or non-boolean flag fails closed. An explicit global ascending or descending
`ConsistencyPolicy` order contradicting a reversed page order is refused before any request.

The cursor and the row identity are independent. Without `identity` the cursor value is the identity.
An `identity` on another field, or on the cursor field with another coercion, only has to be unique:
the cursor alone carries the order, so the identity's values may come in any order unless the
consistency policy declares an order. An explicit `ConsistencyPolicy(order_semantics=...)` then
applies to that identity, independently of the cursor's direction, and a page breaking it fails.

### Response-driven continuation

When the next request is fully described by the current page, `iter_list_continuation()` keeps the
request transaction, identity checks and terminal evidence in the client and asks a pure rule only
for the next control values. `ResponseCursorRule` carries a response cursor, whole and unchanged
(an object cursor keeps every field and its full precision), while a boolean response flag says more
pages exist:

<!-- tested: tests/continuation_test.py::test_a22_a_response_object_cursor_is_carried_whole_and_rule_ends_nonempty_page -->
```python
from b24api import ParameterPath, ResultSelector, RouteKind
from b24api.contracts import ContinuationTraversal, ResponseCursorRule, ResponseFlagTerminal

cursor_control = ParameterPath(("cursor",))
stream = client.iter_list_continuation(
    Request("example.event.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    traversal=ContinuationTraversal(
        selector=ResultSelector(("items",)),
        identity=identity,
        controls=(cursor_control,),
        rule=ResponseCursorRule(
            cursor_selector=ResultSelector(("cursor",)),
            parameter_path=cursor_control,
            terminal=ResponseFlagTerminal(ResultSelector(("hasMore",))),
        ),
    ),
)
async with stream:
    async for item in stream:
        consume(item)
```

A custom rule implements `evaluate(page)` and returns `ContinueWith(updates)` for exactly the
declared controls, or `EndOfSource()`. Rows of the page that ends the source are still delivered,
and the report closes with `rule_declared_end`. The client rejects updates to undeclared controls,
control vectors over 64 KiB and a repeated control vector with `ContinuationError`; such a failure
never reports the source as exhausted.

For newest-first lists that resume from an inclusive timestamp and may also return pinned rows,
`iter_list_window()` sends the oldest paging timestamp of each page unchanged, delivers rows
repeated by the overlap once and fails as non-progress, without another request, when the boundary
does not move. Its `page_stop` callback receives every validated page, empty closing page
included, and `PageBoundary.result` carries that page's server result for response-level fields. See
[Inclusive timestamp windows](https://github.com/shkarupa-alex/b24api/blob/master/docs/recipes.md#inclusive-timestamp-windows).

For multiple parent-bound cursor chains, `iter_cursors()` keeps cursor progress and correlation
isolated per binding while ready pages share the physical batch queue.

![Cursor batching across independent chats](https://raw.githubusercontent.com/shkarupa-alex/b24api/master/cursor-batching.svg)

See [architecture](https://github.com/shkarupa-alex/b24api/blob/master/docs/architecture.md), [migration](https://github.com/shkarupa-alex/b24api/blob/master/docs/migration.md),
[performance](https://github.com/shkarupa-alex/b24api/blob/master/docs/performance.md), and [endpoint recipes](https://github.com/shkarupa-alex/b24api/blob/master/docs/recipes.md) for the complete
contracts and selection guidance.

### One list method across many parent entities

`Binding` applies exact parameter updates to a base request and carries parent correlation. The
client remains unaware of entity types: a binding can represent a deal, lead, chat or any other
caller-defined parent.

![Reference batching across leads and deals](https://raw.githubusercontent.com/shkarupa-alex/b24api/master/references-batching.svg)

<!-- tested: tests/client_v2_test.py::test_bound_references_apply_nested_updates_off_wire_and_emit_exact_completion -->
```python
from b24api import RouteKind
from b24api import BatchDispatch, Binding, ParameterPath, ParameterUpdate, SequentialTraversal
from b24api.contracts import ReferenceComplete, ReferenceItem

bindings = (
    Binding(
        summary=f"owner {parent_id}",
        updates=(ParameterUpdate(ParameterPath(("filter", "OWNER_ID")), parent_id),),
        correlation=parent_id,
    )
    for parent_id in parent_ids
)

stream = client.iter_references(
    Request("example.comment.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    bindings,
    traversal=SequentialTraversal(selector=ResultSelector(("items",)), identity=identity),
    dispatch=BatchDispatch(batch_size=25, concurrency=2),
)
async with stream:
    async for event in stream:
        if isinstance(event, ReferenceItem):
            consume(event.correlation, event.item)
        elif isinstance(event, ReferenceComplete):
            record_completion(event.correlation, event.row_count)
```

With `KeysetTraversal`, a binding may also set simple constant fields directly inside the keyset
filter, such as `filter[=ownerId]`, while each binding keeps its own cursor; see
[Keyset references with per-owner filters](https://github.com/shkarupa-alex/b24api/blob/master/docs/recipes.md#keyset-references-with-per-owner-filters).

For messages across chats, use the same `iter_references()` shape: each binding updates the chat
parameter and carries the chat correlation; choose `CursorTraversal` when the message endpoint is
cursor-based. Identity tracking and completion remain scoped to each binding, so equal child IDs
under different parents are not conflated.

<!-- tested: tests/client_v2_test.py::test_bound_references_apply_nested_updates_off_wire_and_emit_exact_completion -->
```python
from b24api import RouteKind
from b24api import (
    Binding,
    CursorSpec,
    CursorTraversal,
    DirectDispatch,
    IdentityCoercion,
    ParameterPath,
    ParameterUpdate,
    ResultSelector,
)

chat_bindings = (
    Binding(
        summary=f"chat {chat_id}",
        updates=(ParameterUpdate(ParameterPath(("DIALOG_ID",)), chat_id),),
        correlation={"chat_id": chat_id},
    )
    for chat_id in chat_ids
)

messages = client.iter_references(
    Request("example.message.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
    chat_bindings,
    traversal=CursorTraversal(
        selector=ResultSelector(("items",)),
        cursor=CursorSpec(
            parameter_path=ParameterPath(("LAST_ID",)),
            item_path=("ID",),
            coercion=IdentityCoercion.DECIMAL_STRING_INTEGER,
            direction="ascending",
            take="last",
        ),
    ),
    dispatch=DirectDispatch(concurrency=4),
)
```

`iter_reference_outcomes()` additionally yields correlated `ReferenceFailure`,
`ReferenceNotExecuted` and `ReferenceOutcomeUnknown`. A malformed source object that is not a
`Binding` has no valid caller correlation, so it terminates the source with `InputSourceError`
rather than fabricating a reference outcome. Already accepted bindings retain their real outcomes.

With `DeliveryOrder.INPUT` the bindings are delivered in source order, and each still reads its pages
ahead while earlier bindings are being consumed, holding their rows within
`ExecutionPolicy.max_buffered_rows`, so physical batches stay well filled. They fill less than under
`READY`, whose consumer takes pages as they come: a sequential traversal of many bindings with several
pages each may send up to four or five times `READY`'s requests with the default pool, and up to twice
with a larger one; a counted traversal sends at most about twice as many, and often fewer. A binding
with a `page_stop` fetches its next page only once the consumer has received the page and the callback
has returned, so its pages travel in small batches.

### Keyset references

`KeysetTraversal` bindings run sequentially by default (`SequentialKeysetExecution()`), unlike
`iter_list_keyset()`, whose default is automatic. Pass `AutoKeysetExecution`, `RangeKeysetExecution`
or `PartitionedKeysetExecution` as the traversal's `execution` to opt in. A binding with automatic,
range or partitioned execution runs its own planning barrier and selection; its waves and its final
confirmation pages travel through the shared batch queue beside other bindings' pages, so a cohort
of small parents shares physical batches instead of confirming each parent with a direct request.
Its `ReferenceComplete` carries the binding's `keyset_selection` and, when the fast path ran, its
`keyset_execution`. From its first request to its terminal event the binding holds a share of
`ExecutionPolicy.max_buffered_rows` and plans every wave within it; once a sequential or
boundary-only plan is chosen, the binding keeps only the rows that plan can still hold and returns
the rest to the bindings waiting for a share. Under `INPUT` delivery row capacity is granted in
binding order: while a binding waits for its share, no later binding is granted a share or reserves
an ordinary page. A binding whose batch was lost in transit ends with `ReferenceOutcomeUnknown`.
Requests, pages, attempts and elapsed time stay on the one operation budget. An automatic binding
whose planning floor exceeds the whole row budget waits to run alone with all of it (under `INPUT`
delivery, once it is the next binding to deliver); a binding that cannot obtain its floor while no
other binding can release rows fails with `BudgetExceededError`, and waiting for a share never
outlasts `max_elapsed`.

Fast and automatic keyset references need `BatchDispatch`. With a `page_stop`, automatic execution
runs sequentially and reports `PAGE_STOP`; an explicit fast mode with a `page_stop` or
`DirectDispatch` is refused before any request. A `KeysetSpec.boundary` remains unavailable in
references.

### Counted references

`CountedTraversal` bindings read their head page and every known tail offset through the shared
batch queue, so one physical batch can carry pages of several bindings and the HTTP count drops
accordingly; each subrequest still performs its own server-side COUNT. Pages are committed in
offset order per binding. Counted references need `BatchDispatch` and do not accept a `page_stop`.

### Different methods in one operation

`iter_reference_sources()` accepts a lazy source of `ReferenceSource` values, each with its own
request template, traversal, binding and optional `page_stop`. All sources share one dispatcher,
row pool, `ExecutionPolicy` and terminal report, so compatible pages of different methods coalesce
into the same physical batch, and late sources start as capacity frees up rather than after earlier
sources finish. Identities and page state belong to each source: equal child IDs under different
sources are never conflated.

<!-- tested: tests/reference_sources_test.py::test_a28_different_templates_share_the_dispatcher_and_keep_separate_state -->
```python
from b24api import Binding, ParameterPath, ParameterUpdate, RouteKind, SequentialTraversal
from b24api.contracts import ReferenceItem, ReferenceSource


def owned(method, owner_id):
    return ReferenceSource(
        Request(method, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
        SequentialTraversal(selector=ResultSelector(("items",)), identity=identity),
        Binding(
            summary=f"{method} for owner {owner_id}",
            updates=(ParameterUpdate(ParameterPath(("filter", "OWNER_ID")), owner_id),),
            correlation=(method, owner_id),
        ),
    )


sources = (
    owned(method, owner_id)
    for owner_id in parent_ids
    for method in ("example.comment.list", "example.activity.list")
)
stream = client.iter_reference_sources(sources)
async with stream:
    async for event in stream:
        if isinstance(event, ReferenceItem):
            consume(event.correlation, event.item)
```

`iter_reference_source_outcomes()` additionally yields correlated failures. A valid source the
operation cannot run, such as a counted source under `DirectDispatch`, becomes a correlated
`ReferenceNotExecuted` before any request while the other sources continue.

## Streams, partial results and reports

Every multi-item operation returns an `OperationStream`. Prefer `async with`: a plain `break` does
not close an arbitrary async iterator. After cleanup, `stream.report` permanently exposes one
immutable `OperationReport`; before termination it is `None`.

<!-- tested: tests/client_v2_test.py::test_partial_helper_closes_without_claiming_completion -->
```python
first = await client.iter_list(request).first()
page = await client.iter_list(request).collect(limit=100)

assert first.report.partial
assert page.report.partial
```

Helpers do not pull an extra row just to prove exhaustion. Reaching a requested limit is therefore
`EARLY_CLOSED`, never a false `COMPLETED`. Cancellation and cleanup preserve the primary exception
and publish the same final report where the Python exception type permits it.

## Resource boundaries

`ExecutionPolicy` bounds requests, pages, elapsed time, attempts, decompressed response bytes,
buffered commands and rows, retained unordered identity keys, direct concurrency and active
references. The default response ceiling is 16 MiB and is enforced while streaming, before JSON
decoding.

A command's decoded rows are known only once it has run, so `max_buffered_rows` bounds what an operation
holds rather than what it sends. `batch()` and `batch_outcomes()` share the budget among a window's
results in input order, one row per outcome and a success's other rows while they fit; `fan_out()` and
`fan_out_outcomes()` give each command an equal share (50 rows under the default `BatchDispatch`, 250
under `DirectDispatch`). A result that does not fit is dropped: its command ends as a `CommandFailure`
with `ResultTooLargeError`, a `BudgetExceededError` that says the command ran and was answered, so it
is never replayed, and every other command keeps its own outcome. A page's wait for row capacity,
while the consumer has not yet taken the rows other pages hold, is bounded by `max_elapsed`: when it
runs out, the binding or `fan_out()` command fails unsent with `BudgetExceededError` ("row buffer wait
exceeded operation time budget"). A binding's wait for the consumer to take a page already handed to
it (under `READY` delivery, or with a `page_stop`) is not bounded: it lasts as long as the consumer
takes.

`max_elapsed` is checked at the operation's scheduling points: when a request, a page or a source item
is admitted, while a send waits for its permit (a pause included), while a page waits for row capacity
and while an async source is pulled. It does not bound the time the application spends while it holds
control: in a `page_stop` callback, or as a consumer that is slow between pulls of the stream. Nothing
interrupts the application when the budget runs out meanwhile; the next scheduling point ends the
operation with `BudgetExceededError` and an `incomplete` report, even when all that is left is to read
the end of the source.

Sequential, counted, and multi-reference exact traversal retain at most `max_identity_keys`
observed identities per operation in memory (100,000 by default). All active reference bindings
share that ceiling. A page that would exceed it is rejected atomically with typed budget evidence.
Set a larger finite ceiling when the expected aggregate cardinality is known, or pass
`identity_store=` to `iter_list`/`iter_list_counted` so a caller-owned `IdentityStore` (for example a
SQLite table keyed by `identity_store_key(...)`) proves uniqueness while in-process identity memory
stays bounded by one page; the client never closes that store. Repeated-page detection still keeps
one short fingerprint per page, so raise `max_pages` deliberately for very long traversals.
Strict keyset and cursor traversal retain only monotonic progression state when sufficient.

## CLI

The wheel installs `b24api`. Stdout contains only result data; list rows are JSONL. Reports and safe
errors go to stderr. Credentials come only from `Settings` and cannot be passed as CLI arguments.

<!-- tested-console: tests/cli_test.py::test_call_routes_replay_safety_and_keeps_success_data_on_stdout -->
```console
b24api call profile --route bare
b24api call example.item.get --route bare --params '{"id":7}' --raw --replay-safety safe
b24api list example.item.list --route bare --params @params.json
b24api list example.item.list --route bare --strategy counted --contract @counted-contract.json
```

The `--raw` CLI option selects the response envelope; it does not alter the Python API. Advanced
list strategies use closed JSON `version: 1` contracts. The entire contract is validated before
client construction. Run `b24api --help` and `b24api list --help` for the compact option surface.

Exit codes are `0` success, `2` usage/contract error, `3` unavailable configuration, `4`
remote/protocol/correctness/incomplete failure, `5` broken output consumer, `6` keyset bounds
unsupported and `7` keyset verification inconclusive (both from `verify-keyset`), and `130`
cancellation.
`5` takes priority whenever a write to stdout or stderr finds its reader gone, including a run whose
data was delivered but whose diagnostic hit a closed stderr. Any other failed write, such as to a full
disk, exits `4`.

## Correctness boundaries

The client fails closed on contradictory pagination, missing counted ranges, duplicate identities,
unsafe ambiguous replay, oversized responses and incomplete cleanup. It can prove only facts visible
through the transport contract: continuation, totals, identity, order, budgets and lifecycle.

It cannot generically prove that Bitrix honored the business meaning of a filter, choose an
application's composite storage key or reconcile an ambiguous write. Applications must validate
expected business sets and verify writes where needed.

## Performance and profiling

The current deterministic profile covers request counts, wall/CPU time, time to first row,
high-water counters, retained resources and optional Memray allocations:

```console
uv run python tools/b24api_evidence/profile_runtime.py --capability-suite
uv run python tools/b24api_evidence/profile_runtime.py --samples 7 --warmups 2
uv run --with memray python tools/b24api_evidence/profile_runtime.py \
  --case dense-10k --plan counted_batch --samples 7 --warmups 2 \
  --memray-output /tmp/b24api.bin
uv run --with memray memray stats /tmp/b24api.bin
```

These deterministic fixtures characterize local resources and network shape; they are not live
portal latency admission. See [docs/performance.md](https://github.com/shkarupa-alex/b24api/blob/master/docs/performance.md) for current measurements
and [docs/architecture.md](https://github.com/shkarupa-alex/b24api/blob/master/docs/architecture.md) for guarantees and ownership boundaries.

Projects moving from an earlier API surface can use [docs/migration.md](https://github.com/shkarupa-alex/b24api/blob/master/docs/migration.md).

## Verification

```console
uv sync --frozen
make qc
git diff --check
```

`make qc` runs the lint, type and default test checks that CI blocks on. It leaves out the internal
benches (the pytest marker `slow`: the evidence harness contracts and the 50k/100k-scale runs, which
take several minutes). `make bench` runs them, and so does the blocking CI job `slow`.

The wheel regression installs into an isolated environment, executes the `b24api` entry point and
checks that tests, live/evidence tooling and credentials are excluded.
