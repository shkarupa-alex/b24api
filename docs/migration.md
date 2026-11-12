# Migration guide

## `INPUT` delivery reads reference pages ahead

- **Physical batches fill up under `DeliveryOrder.INPUT`.** A reference binding of `iter_references()`,
  `iter_reference_outcomes()` and their sequential, cursor and counted variants, and each command of
  `fan_out()` and `fan_out_outcomes()`, waited for the consumer to leave its page before it fetched the
  next. Under `INPUT` delivery only the binding being delivered moved, so with `BatchDispatch` its pages
  left almost one per physical batch: 300 parents of two pages took 552 (counted) to 852 (sequential)
  requests and 30 to 46 s on a 50 ms wire, against 34 to 90 requests and 2.5 to 7 s under `READY`. A
  binding now reads its pages ahead into its own queue while the row pool has room, and the dispatcher
  waits for the rows the consumer frees on its own, so the same traversal takes 55 (sequential) and 33
  (counted) requests and 2 to 4 s. Batches still fill less than under `READY`, whose consumer takes
  pages as they come: in our deterministic model a sequential traversal sends 1 to 4.7 times `READY`'s
  requests with the default pool, the more pages per binding the more, and 1 to 2 times with a pool of
  10 000 rows; a counted traversal sends at most twice as many, and mostly fewer. Unless a budget stops
  the operation or the stream closes early (see below), the items, their order and the outcomes are
  unchanged; the number of physical requests, their grouping and the order the portal receives the
  commands change. `READY` delivery is unchanged, and so is a fast or automatic keyset binding, which
  hands over one row group at a time within its row lease.
- **`DirectDispatch` fetches ahead concurrently too.** Under `INPUT` delivery the bindings behind
  the one being delivered now send their pages while it is consumed, up to the dispatch concurrency,
  instead of standing still. A stream closed early has spent requests and pages on the pages read
  ahead and discarded.
- **Rows behind the binding being delivered are held until its turn.** They stay within
  `ExecutionPolicy.max_buffered_rows`; `buffered_rows_high_water` may come closer to it than before.
  The pool always keeps room for the binding being delivered and for the next pages of the bindings
  nearest it, so a small `max_buffered_rows` still completes and only reads less ahead.
- **Budgets are spent ahead of the consumer.** `max_requests`, `max_pages` and `max_elapsed` count
  the pages read ahead when they are fetched, not when they are delivered, so the binding a budget
  stops may be an earlier one than before. Closing a stream early discards the pages read ahead for
  bindings not delivered yet; the report counts their requests and pages.
- **A page stop still sees each page before its binding goes on.** A binding with a `page_stop`
  fetches its next page only once the consumer has received the page and the callback has returned,
  as documented under the page-stop contract below. Such a binding does not read ahead, so under
  `INPUT` delivery its pages still travel in small batches.

## Results beyond the row budget

One error is new; what happens to a command whose result does not fit `max_buffered_rows`, and how long
a binding waits for rows, have changed.

- **New: `b24api.errors.ResultTooLargeError`.** A `BudgetExceededError` for a command that ran and was
  answered, but whose decoded result holds more rows than `ExecutionPolicy.max_buffered_rows` leaves
  it. The client drops the result, and the command's outcome is a `CommandFailure` carrying this
  error: unlike a command a budget refused before its send, it ran, and it is never replayed. Its
  `rows` is how many rows the result decoded to (the length of a list result), its `limit` how many the
  budget left it. `except BudgetExceededError` still catches it.
- **Breaking: `batch()` and `batch_outcomes()` keep every outcome of a window whose results outgrow the
  row budget.** The budget was checked once a physical window had run and been decoded, and a window
  whose results held more rows than `max_buffered_rows` raised `BudgetExceededError` with no outcome
  at all, not even those of its commands that fit: with 5 `crm.deal.add` and 25 lists of 120 rows in
  one window, the deals were created and their ids lost, and the report counted the commands admitted
  and sent but none settled. Now every command of the window keeps its outcome, in input order. Each
  outcome holds one row of the budget, and a success holds the rest of its result's rows while they
  fit what the budget has left, in input order; a success whose result does not fit ends with
  `ResultTooLargeError` instead (in that example, the last 5 lists). `batch_outcomes()` yields every
  outcome and goes on with the next window, so the stream completes with failures where it raised;
  `batch()` yields the successes before the first such failure and raises `BatchFailed`, whose
  `outcomes` hold the rest, with a `budget_exhausted` violation. Code that caught
  `BudgetExceededError` from `batch_outcomes()` for this must look for `ResultTooLargeError` in the
  outcomes instead. To keep such results, raise `max_buffered_rows` or lower `batch_size`, so that
  fewer results share the budget.
- **A fan-out result longer than its share of the row budget reports that it ran.** `fan_out()` and
  `fan_out_outcomes()` reserve each command a share of `max_buffered_rows`: the budget divided among the
  results a dispatch may hold at once, 50 rows each under the default `BatchDispatch` and 250 under the
  default `DirectDispatch`. A list result longer than its share was a `CommandFailure` with a plain
  `BudgetExceededError`, the failure of a command never sent. It now carries `ResultTooLargeError`, since
  the command ran and was answered.
- **Waiting for row capacity ends with `max_elapsed`.** A page of a reference binding or a `fan_out()`
  command waiting for row capacity still held by results the consumer had not taken waited until the
  consumer read on, however long after `max_elapsed`, and its send was then refused at its admission.
  That wait now ends with the operation's elapsed budget, while the consumer still holds the rows: the
  command or binding fails unsent with `BudgetExceededError` ("row buffer wait exceeded operation time
  budget"), and the operation ends as the time budget ends it. Unchanged: a binding waiting for the
  consumer to take a page already handed to it (under `READY` delivery, or with a `page_stop`) waits
  without a bound, as long as the consumer takes; see the time-budget note under
  [Retry timing](#retry-timing).

## Batch budgets and held methods

Public signatures are unchanged; how an operation ends on its request budget, and how a batch's first
send meets a method pause, have changed.

- **Breaking: the request budget ends the operation.** Once `max_requests` was spent, `batch_outcomes()`,
  `fan_out_outcomes()` and the reference traversals (`iter_reference_outcomes()` and the operations
  built on it) went on reading their source: every further command or binding was refused before its
  send, so a source without end was read without end, each yielded as a failure with
  `BudgetExceededError`, and a finite source ended `completed_with_failures` with no violation. Now the
  first send that `max_requests` refuses ends the operation, as `max_elapsed` ends it. What was
  already read is still yielded: what was sent keeps its outcome, and a command never sent is a
  failure with `BudgetExceededError`, never sent and not eligible for replay. Then the stream raises
  `BudgetExceededError` ("physical request budget exhausted") with the report attached, reads no
  further input and closes the source, as on any other end. The report is `incomplete` and records a
  `budget_exhausted` violation. A retry or replay that the budget refuses ends the operation the same
  way. A budget that the last send the operation needs spends exactly refuses nothing, and the stream
  completes. A send that the spent budget will refuse is now refused before it queues for a permit, so
  it no longer waits out a cooldown first. Code that read such a stream to its end with `async for`
  must now catch `BudgetExceededError`, or give the operation enough `max_requests`.
- **Breaking: one held method no longer holds back or fails a whole `batch_outcomes()` chunk.** A batch
  envelope is admitted under every method it carries, so after an `OPERATION_TIME_LIMIT` (120 s by
  default) on one method, the first send of every later chunk that carried that method waited two
  minutes with all its other commands, or, when the operation had less time left than the pause, failed
  all of them with `BudgetExceededError`. The first send now weighs each command against its own
  method's pause, as a replay round already did: by how much longer its method is held than the portal
  as a whole (a `Retry-After` hint holds every command alike), against the retry window
  (`max_retry_elapsed_per_request`) or what the operation has left after the portal's pause, whichever
  is shorter. When some commands of a chunk can wait that long and others cannot, each command that
  cannot is not sent and ends at once with `BudgetExceededError` ("coordinator hold exceeds execution
  time budget"), never sent and not eligible for replay, and the others are sent after the longest pause
  among them. This is what breaks: a command whose own method is held longer than it may wait (by
  default, longer than the 30 s retry window), in a chunk whose other commands may go, used to wait with
  them and succeed when the operation had the time, and now fails unsent with `BudgetExceededError`.
  When every command of a chunk can wait, or none can, leaving some out would let no other command go,
  so nothing is left out: as for a lone `call()`, the chunk waits for its pauses within `max_elapsed`
  (and `batch_outcomes()` reads no further while it waits), or fails at once when a pause outlasts what
  the operation has left. A fail-fast `batch()` is never split either, since the failure of the command
  left out would fail the batch: its chunk is treated the same way, so it waits for the pause, as
  before, or fails at once when the pause outlasts what the operation has left. Unchanged in that last
  case: every later chunk of the held method fails the same way, so a source of only that method is
  still read, chunk by chunk, until `max_elapsed` ends the operation. This holds for classic and REST
  3.0 envelopes alike. To let such commands wait for a long pause again, raise the retry window above
  it: `http_timeout` sets it only under the client's default policy (`ExecutionPolicy.from_settings`);
  with an explicit `policy=`, raise its `max_retry_elapsed_per_request`. The pause must also fit in what
  is left of `max_elapsed`.
- **Unchanged: traversals and dispatched commands wait for a held method.** The pages of a list or
  reference traversal and the commands that `fan_out()` and `fan_out_outcomes()` gather into a
  `BatchDispatch` wave are never left out for their method's pause: a wave waits for it within
  `max_elapsed`, as the same requests sent one by one with `DirectDispatch` do. Failing a reference's
  page fails the whole reference, which is worse than waiting.

## Closed clients, copyable errors and the client's request limit

- **Breaking: a closed client raises `ClientClosedError`.** A closed client refused work with three
  unrelated types: `RuntimeError("client is closed")` for a call or stream started after
  `aclose()`, `CoordinatorClosedError` (a `RuntimeError` of `b24api.execution`, missing from
  `b24api.errors`) for a call still waiting for a permit when the client closed, and `TransportError`
  "transport is closed" or "HTTP client is closed" for a send after its transport or injected HTTPX
  client closed. Each now raises `b24api.errors.ClientClosedError`. It is a `TransportError` of phase
  `not_dispatched` and `retryable=False`: the attempt it stopped never left the process, and it is never
  retried. The phase covers that attempt alone. A `SAFE` request waiting for the permit of a retry may
  have run on an earlier attempt; an `UNSAFE` or `UNKNOWN` one is retried only after a failure that
  proved it did not run (a transport failure before dispatch, or a refusal listed in `AmbiguityPolicy`),
  so it stays proven not run. It also derives from `RuntimeError`, so
  `except RuntimeError` and `except TransportError` handlers keep catching what they caught, while
  `except B24ApiError` now catches a closed client too. `b24api.execution.CoordinatorClosedError` is
  removed: catch `ClientClosedError`.
- **Every error survives `pickle` and `copy`.** An error whose constructor needs more than its
  message (`ApiResponseError`, `BatchCommandError`, `AmbiguousExecutionError`, `ContinuationError`,
  `BatchFailed`, a bare `B24ApiError` and the others like them) failed
  `pickle.loads(pickle.dumps(error))`, `copy.copy()` and `copy.deepcopy()` with `TypeError`, since
  `Exception` rebuilds an error by calling its class with the message alone. Every class of
  `b24api.errors` now rebuilds without its constructor and keeps its message, attributes and notes, so
  an error can cross a process pool or a task queue. The exception is an error holding a caller's
  correlation that cannot be pickled: `BatchFailed` and `ReferenceFailed` keep their outcomes with
  their correlations, so `pickle` and `copy.deepcopy()` fail on them as on the correlation itself
  (`TypeError` for a lock, for example), while `copy.copy()` still works.
- **`Settings.http_timeout` must be finite.** `http_timeout=float("inf")` passed `Settings`, and
  `Bitrix24()` then failed with `ValueError` after it had already opened its HTTPX transport. It is
  now a `ValidationError` of `Settings` (and of `Bitrix24.from_webhook()`), as `nan` already was. The
  client also checks the rest of its configuration before it opens a transport, so a refused one
  opens nothing.
- **New: `Settings.max_concurrent_requests`.** The client's shared limiter let at most 10 requests be
  in flight at once, and no setting changed it: `DirectDispatch(concurrency=40)` under
  `ExecutionPolicy(max_direct_concurrency=40)` still ran 10 at a time. The limit is now
  `Settings.max_concurrent_requests` (environment `BITRIX24_API_MAX_CONCURRENT_REQUESTS`, or
  `Bitrix24.from_webhook(..., max_concurrent_requests=...)`), 10 by default as before. It bounds all
  the client's calls, streams and retries together, and an operation's own limits only narrow it, per
  dispatch: a `DirectDispatch` sends at most the smallest of its `concurrency`,
  `max_direct_concurrency`, `max_active_references` and the client limit at once; a `BatchDispatch`
  keeps at most the smallest of its `concurrency`, `max_active_references` and the client limit of
  physical batches in flight, and `max_direct_concurrency` does not limit it. The isolation of a REST
  3.0 envelope of `SAFE` commands that may have run sends its unresolved commands one by one, all at
  once, bounded only by the client limit. In `fan_out()` and `fan_out_outcomes()`,
  `BatchDispatch(batch_size=1)` sends one batch at a time whatever its `concurrency`, since each
  command's share of `max_buffered_rows` is sized for one batch; use `DirectDispatch` to send single
  commands concurrently. Raise the client limit only as far as the portal's rate limits allow.
- **A synchronous source keeps one context.** A synchronous iterator given to `batch()`,
  `fan_out()` or a reference traversal is pulled in a worker thread, and each pull ran in a fresh copy
  of the context, so a generator that set a `ContextVar` before a `yield` no longer saw it when it
  resumed, and resetting its token raised `ValueError`. Each source now runs every pull, and its
  close, in one `contextvars.Context`, copied from the caller when the call that receives the source
  is made: the generator sees what it set across its `yield`s, as in a plain `for` loop. What it sets
  stays in that context and never reaches the caller's.

## Retry timing

Public signatures are unchanged; when a retry waits and when it is sent have changed.

- **A throttle hint no longer cancels the backoff.** A retry after an answer that carries
  `Retry-After` or `X-Bitrix-RateLimit-Reset` waited only for that hint, so `Retry-After: 0`, a date
  in the past or a reset just before now sent every attempt at once. A retry now waits the longer of
  its exponential backoff and the hint, and the same holds for a method paused by
  `OPERATION_TIME_LIMIT`. A hint given as a date or an epoch is measured against the response's
  `Date` header when it has one, not the local clock, so a local clock running ahead no longer
  shortens it.
- **Breaking: a call may now wait up to `max_elapsed` for its first permit.** The retry window
  `max_retry_elapsed_per_request` (`Settings.http_timeout`, 30 s by default, for the client's default
  policy) used to start before the request queued for a permit of the client's shared limiter. A
  request that queued behind others was refused without a send, or sent with a sliver of time and
  reported unknown. The window now opens when the first send gets its permit, so queueing for that
  permit is bounded only by the operation's `max_elapsed`, 900 s by default, where it used to be
  bounded by `http_timeout`. Every send, retry wait and later permit wait spends the window, and an
  attempt still has no timeout of its own: it may use what is left of the window. A pause the client
  already knows about that outlasts what is left of `max_elapsed` (a `Retry-After` or an
  `OPERATION_TIME_LIMIT` cooldown on the request's methods) fails at once with
  `BudgetExceededError` instead of waiting the budget out. To bound the queue more tightly again,
  lower `max_elapsed` (see the example after this list).
- **A retry that may not be repeated is not sent into a sliver.** After a refusal proving that an
  `UNSAFE` or `UNKNOWN` request did not run (a quota error or a 429, for example), the retry was sent
  with whatever window the wait left, so a 0.95-second `Retry-After` in a 1-second window sent it with
  0.05 seconds, and a send cut that way was reported unknown. Such a retry is now sent only while the
  window left after its wait is at least as long as the wait, measured as the time it actually waited
  when that was longer than planned. Otherwise the request ends with `BudgetExceededError`, whose cause
  is the refusal, and its outcome stays proven not run: at once, without waiting, when the planned wait
  already does not fit, or after the wait, before the send, when the wait overran its plan (a late wake
  or a long permit queue). With the default 30-second window, a `Retry-After` of 15 seconds or more ends
  an `UNSAFE` or `UNKNOWN` request without a retry even when its first send took no time; each second
  the request has already spent lowers that bound by half a second. To let it wait longer, raise the
  retry window: `http_timeout` under the client's default policy, `max_retry_elapsed_per_request` with
  an explicit `policy=`. A `SAFE` retry is still sent with any window left: it may succeed, and
  repeating it is harmless.
- **The `SAFE` commands of a classic batch keep their replay.** The same rule applies to the replay
  rounds of a classic batch, whether its commands' own entries were refused or the whole batch was
  refused (a 429, or a quota error for the batch). When the window is too short for its `UNSAFE` or
  `UNKNOWN` commands, the round still sends its `SAFE` commands: after the same wait when the plan shows
  it, or at once, with the window left, when the send itself is refused after a wait that overran (the
  wait is not repeated; a pause the client still holds on their methods is). The other commands are not
  sent again. One whose own entry answered it keeps that answer, a `BatchCommandError` with its refusal;
  when the whole batch was refused, each ends with `BudgetExceededError` whose cause is that refusal,
  proven not run and not eligible for replay. Each command is weighed against its own method's pause
  too, as the client knows it when the round is planned (a method paused by `OPERATION_TIME_LIMIT`, for
  example), so such a pause does not take the replay away from the other commands: a command whose own
  pause outlasts the window is not sent again, and a round waits only as long as its `UNSAFE` and
  `UNKNOWN` commands can afford (less than half the window); a `SAFE` command whose method is held
  longer than that waits for a later round, after its pause. A pause that appears during a round's wait
  is met when the round asks for its permit, and stops the whole round when it outlasts the window. Only
  a round that is sent spends an attempt of the batch: a round stopped before its send (at its permit or
  at its admission) spends none, and a command carried to a later round spends none of its own, the
  round's send of the others spending the batch's attempt. Every round that carries a command plans to
  send another, so `max_attempts_per_request` bounds how often a command waits for a later round: one
  still waiting when the last attempt is spent is not sent again, and keeps its own refusal or ends with
  `BudgetExceededError` ("per-request attempt budget exhausted") caused by the batch refusal it
  followed. A command the budget stops before its round is sent ends with that stop caused by the
  failure it followed, never by the failure of a round it skipped, and it is reported possibly run only
  when a send it was in may have run. A fail-fast batch is never split: it is sent again whole or not at
  all, after the longest pause of its commands. A REST 3.0 batch keeps `SAFE` and other commands in
  separate envelopes, so each envelope is admitted or refused whole; one that was refused as a whole and
  whose replay the budget then stops (the window, or a pause or a full queue at its permit) now ends the
  same way, with `BudgetExceededError` whose cause is the refusal, where it used to keep the refusal
  itself.
- **408 and 504 are retried, 507 is not.** `RetryPolicy.transient_http_statuses` now defaults to
  408, 423, 425, 429, 500, 502, 503 and 504. An unstructured 408 or 504 is retried for `SAFE`
  requests and stays ambiguous for `UNSAFE` and `UNKNOWN` ones, which are never sent again after it.
  An unstructured 507 (insufficient storage) is now raised at once as `HTTPGatewayError` for a `SAFE`
  request; for an `UNSAFE` or `UNKNOWN` request it is still `AmbiguousExecutionError`, as every 5xx is.
  Set `RetryPolicy.transient_http_statuses` in the policy to keep the old set.
- **A time limit on a whole classic batch pauses its commands' methods.** An `OPERATION_TIME_LIMIT`
  answered for a classic batch envelope as a whole paused the method name `batch`, which no command
  is admitted under, so the retry did not wait for it. It now pauses each method of the envelope, as
  a REST 3.0 batch abort already did. The pause is host-wide: until it ends, every request of the
  client to one of those methods waits for it, direct calls included.

To restore a short bound on the wait for a permit, lower `max_elapsed` for one call or for the
client's default policy. For `call()` it bounds the whole call, queue included. A first send gets
whatever the queue left of `max_elapsed`, so a tight bound brings back the first sends cut to a sliver:
an `UNSAFE` or `UNKNOWN` one cut that way is reported unknown (`AmbiguousExecutionError`). Keep
`max_elapsed` well above the longest queue you expect plus one send:

```python
import dataclasses

from b24api import ExecutionPolicy, Settings

settings = Settings(webhook_url="https://example.bitrix24.com/rest/1/token/")
bounded = dataclasses.replace(ExecutionPolicy.from_settings(settings), max_elapsed=settings.http_timeout)
```

`max_elapsed` bounds the operation at its scheduling points: when a request, a page or a source item is
admitted, while a send waits for its permit (a pause included), while a page waits for row capacity and
while a source, synchronous or asynchronous, is pulled. A send in flight is cut at what is left of the
operation or of its retry window, whichever is shorter. It does not bound the time the application
spends while it holds control: in a `page_stop` callback, or as a consumer that is slow between pulls of
a stream, a binding waiting for the consumer to take its page included. Nothing interrupts the
application when the budget runs out meanwhile; the next scheduling point ends the operation, even when
all that is left is to read the end of the source. The report is then `incomplete`, with
`BudgetExceededError` as its terminal reason and a `budget_exhausted` violation. The stream raises
`BudgetExceededError`, except `fan_out()`, which raises `BatchFailed`, and `iter_references()`,
`iter_cursors()` and `iter_reference_sources()`, which raise `ReferenceFailed`.

## Compressed bodies are decoded whole

- **A compressed body cut short is a body read failure.** A `gzip` or `deflate` body (zlib-wrapped
  or raw) that ends before the end of its compressed stream, its gzip CRC-32 and size or its zlib
  Adler-32 included, was accepted as the prefix decoded so far, as HTTPX accepts it: `call_bytes()`
  returned a short body as a success, and `binary_digest` then hashed that prefix. It is now the
  transport failure every undecodable body already raises, `TransportError` "Transport failed while
  reading the response body" with phase `body_partially_received`: retried for `SAFE` requests, and
  `AmbiguousExecutionError` for `UNSAFE` and `UNKNOWN` requests. `gzip.decompress()` and
  `zlib.decompress()` refuse the same bytes.
- **Every gzip member is decoded.** A `gzip` body of several members returned only its first member
  as a success, as HTTPX returns it. Every member is now decoded, under one byte ceiling for them
  all, as `gzip.decompress()` decodes them. Zero bytes after a member are skipped as padding; any
  other byte must start a whole member, so bytes after a gzip stream that were dropped before are now
  the body read failure above, as `gzip.decompress()` refuses them.
- **Unchanged.** A body without a single byte under a compression coding (an empty `200` or `204`
  that still carries `Content-Encoding: gzip`) stays an empty body, as HTTPX reads it. Bytes after the
  end of a complete zlib or raw `deflate` stream are still dropped.
- **Not checked: a body read inside an injected HTTPX client.** When the client's transport returns a
  response it has already read (`httpx.Response(content=...)` in a `MockTransport`), a response event
  hook reads the body (`await response.aread()`), or the client's `httpx.Auth` declares
  `requires_response_body = True` (HTTPX then reads every response for it), HTTPX has decoded the body:
  without checking the end of a compressed stream, and keeping only the first gzip member.
  `HttpxTransport` accepts such a body as HTTPX decoded it, within the byte ceiling. The raw bytes may
  still sit behind the client's stream wrapper, but HTTPX's public API cannot tell an in-memory body
  read that way from a network stream already released to the connection pool, which cannot be read
  again, so the transport does not try. A transport that returns the raw bytes unread
  (`httpx.Response(..., stream=...)`, as HTTPX's own transports do), with hooks and auth that leave the
  body unread, gets every check above.

## Transport hygiene: webhook URLs, unsendable requests, error chains

- **One rule for webhook URLs, in `Settings` and `HttpxTransport` alike.** Both parse a webhook with
  the same function and accept exactly the classic base `http[s]://<host>[:<port>]/rest/<user>/<token>/`,
  read as HTTPX reads it to send and storable as pydantic's `HttpUrl`:
  - leading and trailing spaces and control characters are dropped, as URL parsers drop them; one
    inside the URL is refused. `Settings` also takes UTF-8 bytes, as pydantic did; `HttpxTransport` and
    `Bitrix24.from_webhook()` take only a string;
  - the scheme is `http` or `https`, and there is no user name or password: the token in the path is
    the webhook's only credential, and HTTPX never sent userinfo as Basic auth but logged it. An empty
    one (`https://@portal/...`, `https://:@portal/...`) is no userinfo and is dropped;
  - the host, as written, is labels of ASCII letters, digits, `_` and `-` (container service names
    such as `bitrix_web` included) or of non-ASCII letters, IDNA-encoded (`faß.de` is `xn--fa-hia.de`),
    a dotted-quad IPv4 address, or an IPv6 address without a zone index, and the port, if any, is ASCII digits for 1-65535. A host HTTPX cannot address
    is refused: one whose first label is punycode IDNA 2008 cannot decode (`xn--ls8h.la`; HTTPX decodes
    only a host that starts with `xn--`, so `portal.xn--ls8h.la` is accepted and works), and one
    pydantic's `HttpUrl` refuses (`xn---p1ai.example`, `portal.xn--p1.ai`);
  - the path is `/rest/<user>/<token>/`, with a numeric user and a token of letters, digits, `_` and
    `-`, the trailing slash optional, and no query or fragment, not even an empty `?` or `#`. A
    percent-encoded character anywhere in the path (`/%72est/`, `/rest%2F1/`, `/rest/%2E/`) is refused
    rather than decoded; literal `.` and `..` segments are resolved, as HTTPX and URL parsers resolve
    them;
  - the whole base is at most 2083 characters, the longest URL pydantic's `HttpUrl` stores.

  `Settings` refuses with a `ValidationError` whose error type is `webhook_url` (not a valid http or
  https URL, including one too long), `webhook_url_userinfo`, `webhook_url_host` (a host HTTPX cannot
  address or pydantic refuses) or `webhook_url_shape` (not the classic base); `HttpxTransport(...)`
  raises `ValueError` with the very message of the `Settings` error, where it raised `httpx.InvalidURL`
  or an `idna` error before. Neither quotes the URL, nor keeps it in the frame locals of its traceback,
  and neither does `Bitrix24.from_webhook()`. A host or port of none of the accepted forms is
  `webhook_url`, not `webhook_url_host`. What changes:
  - `Settings` accepted any URL pydantic's `HttpUrl` accepts, so a URL that is not a classic base
    (`https://portal/`, `.../rest/1/token/extra/`) failed only when `Bitrix24` built its transport,
    and a host HTTPX cannot address failed on every call. Both now fail in `Settings`. A `Settings`
    that only names the host of an injected transport needs a classic URL too.
  - Forms URL parsers repair are refused rather than repaired: IPv4 addresses that are not a dotted
    quad (`1.2.3`, `01.2.3.4`, `0x7f.0.0.1`, `2130706433`), an IPv6 zone index, a full stop other than
    `.` (`portal。example`), full-width hosts (`ｐｏｒｔａｌ.example`), a port that is empty, signed or
    not in ASCII digits (`:`, `:+443`, `:٨٠٨٠`), port 0, a scheme without `//`
    (`https:portal.example/...`), backslashes in place of slashes, and an empty query or fragment.
  - A doubled slash in the path (`https://portal//rest/1/<token>/`, `.../rest/1/<token>//`) was
    accepted and sent as written; it is not the classic base and is refused now.
  - `HttpxTransport` accepted `ws:` and `wss:` URLs, ports outside 1-65535, any path segments and
    hosts `Settings` refused; it refuses them now.
  - `Settings.webhook_url` stores the canonical base the transport sends: the scheme and host lower
    case, the host IDNA-encoded and an IPv6 address in pydantic's hexadecimal form (`::ffff:1.2.3.4`
    is `::ffff:102:304`), the default port (under any scheme case: `HTTPS://portal:443/...`) and an
    empty user name or password dropped, `.` and `..` segments resolved, and the trailing slash added.
    The canonical base parses back to itself.
  - `HttpxTransport.host` and `Bitrix24.host` are the host the rule yields: `xn--fa-hia.de`, where the
    transport reported the decoded `faß.de` and so failed `Bitrix24`'s injected-host check, and an
    IPv6 address without brackets (`::1`, where `Bitrix24.host` was `[::1]` and the transport's host
    never matched it). An injected transport may still report its IPv6 host with brackets.
  - The secret registered for redaction and with the HTTPX log shield is the token of the parsed
    webhook, the one that is sent. It was a segment matched in the raw string, which registered `1`
    for `/rest/./1/<token>/` and failed, quoting the URL in its frame locals, for `/%72est/1/<token>/`.
- **Method names are dot-separated names.** `Request` accepted a method with an empty segment (`..`,
  `.`, `.profile`, `profile.`, `crm..item`); `..` addressed the portal above the webhook token, and the
  failure surfaced only at dispatch. Such a method now raises `ValueError` when the `Request` is built.
- **Header values follow RFC 9110.** `RequestHeaders` accepted any string. A value outside the RFC 9110
  field-value grammar now raises `ValueError` when the headers are built, before anything is sent:
  only visible ASCII characters, with spaces or tabs between them and none around them. Leading or
  trailing whitespace used to be stripped and sent over HTTP/2, which HTTPX negotiates by default, and
  refused by h11 during the send over HTTP/1.1; both are now refused before dispatch. Control
  characters, some of which (`\x01`, DEL) were sent as they were, are refused too, and so are the
  grammar's obsolete non-ASCII characters: HTTPX encodes header values as ASCII and failed on them
  inside the send. A value over 8192 characters is refused with a message about its length.
- **A request that could not be built is not ambiguous.** When HTTPX could not build the request, the
  failure was reported as possibly sent: an `UNSAFE` or `UNKNOWN` request raised
  `AmbiguousExecutionError`. Nothing was sent, so it is now a non-retryable `TransportError` with
  phase `not_dispatched` (`possible_acceptance` is `False`) for every replay safety.
- **No HTTPX exception on the chain.** An exception raised inside an injected `httpx.AsyncClient`
  (an event hook's `raise_for_status()`, an auth flow, a custom transport or response stream) reached
  the caller as the `__cause__` of a `TransportError`, carrying the request and its webhook URL.
  `HttpxTransport` now raises a `TransportError` without `__cause__` or `__context__`. Its message names
  the exception's class and, when the exception carried a received response, its status, which is
  also `error.http_status`; never a URL or header. Code that read `error.__cause__` to get the hook's
  exception must keep what it needs in the hook itself. Retries follow one rule in every stage, as
  before: HTTPX's request errors (connection, read, write, protocol) are retried by the phase they may
  follow, and any other exception except a builtin `TimeoutError` (below) is not retried, since it
  would be raised again; it counts as possibly sent once the request was built.
- **Two failures inside the client are reported differently.** A `B24ApiError` raised inside the
  injected client, by a hook for example, was re-raised as is, with a traceback through the HTTPX
  frames that hold the request. It now arrives as the non-retryable `TransportError` above, naming its
  class, so its own retry and ambiguity classification no longer applies. A builtin `TimeoutError`
  raised there keeps the executor's deadline classification, as before, but arrives as a fresh
  `TimeoutError` without arguments under the executor's `TransportError`.
- **A fully read response survives a failing release.** HTTPX released a response stream itself after
  its last chunk. A failure there voided a complete response: an HTTPX transport error (`ReadError`,
  `CloseError`) was a body read failure, retried for `SAFE` requests; any other exception was a
  non-retryable `TransportError`; both raised `AmbiguousExecutionError` for `UNSAFE` and `UNKNOWN`
  requests. The transport now reads
  the stream directly and releases it itself once the outcome is decided, dropping a failure of the
  release: a complete response is returned, and a voided one keeps its own error.
- **What is guaranteed.** The webhook token appears in no rendering of an error that `HttpxTransport`
  raises itself, converts from an `Exception` raised inside the HTTP client, or raises while it is
  constructed: not in `str` or `repr` of the error, its chain, notes or arguments, not in a formatted
  traceback, not in the `repr` of the frame locals that `capture_locals` or an error tracker shows.
  In logs, the guarantee covers a record of such an error (the library writes no log records of its
  own), the records of the `httpx` logger, and the `hpack.hpack` and `hpack.table` records, which are
  dropped while a library HTTPX client is open (an HTTP/2 send is refused with `CapabilityError` before
  I/O when that filter was removed; see item 14 under
  [Upgrading from 2.3 to 3.0](#upgrading-from-23-to-30)). Records of other loggers are not filtered:
  `httpcore` DEBUG records carry the response headers, so a `Location` header that holds the token
  puts it in a record; see the HTTPX logging note in
  [Migrating to the issues architecture (2.3)](#migrating-to-the-issues-architecture-23). What is not
  converted passes through unchanged: an exception that is not an `Exception` (`KeyboardInterrupt`,
  `SystemExit`) raised by a hook, whose traceback runs through the HTTPX frames that hold the request,
  and the arguments a canceller gives a cancellation. One limitation remains: over HTTP/2, HTTPX's
  HPACK header table keeps the request path with the token. That is connection state of HTTPX,
  reachable only by walking the client's internal objects, not by rendering an error.

## Unknown outcomes follow what was sent

Public signatures are unchanged. Whether a sent command may have run is now one fact, recorded by the
executor and the batch dialects from the command's sends, and `batch_outcomes()`, `batch()`,
`fan_out_outcomes()`, `fan_out()` and `iter_reference_outcomes()` derive the outcome from it, whatever
the exception type. A command that was sent, was not refused, and got no conclusive answer arrives as
`CommandOutcomeUnknown` (`ReferenceOutcomeUnknown` for a reference): its `AmbiguousExecutionError`
never lets it be sent again, and its `__cause__` is the original failure. This holds for `SAFE`
commands too: a direct fan-out command or a direct reference whose sends were lost until the budget ran
out is unknown, no longer a failure with `BudgetExceededError`. A structured Bitrix error stays a
conclusive `CommandFailure` or `ReferenceFailure`, unless an earlier send of the same command may have
run. Then the outcome is unknown, and its reason and cause chain name that earlier send. For example, a
`SAFE` command whose first send lost its connection and whose retry was refused with `ACCESS_DENIED` is
now unknown with reason `CONNECTION_LOST_AFTER_DISPATCH` and a cause chain that reaches the
`TransportError`, whichever answer refused the retry:

- classic batch: the retried envelope answered 403, or it refused the command in `result_error`;
- a classic batch replay round: a `SAFE` command of a mixed batch, replayed after its first round was
  lost and then refused by the replay's envelope or in its `result_error`, is unknown like the
  `UNSAFE` command of the lost round;
- REST v3 batch: the retried envelope answered, and its element is the refusal the command's
  `ResultErrorSpec` finds; or isolation, where a `SAFE` command sent alone after an envelope that may
  have run (an abort, a malformed or a lost answer) is refused. When the budget leaves no isolated send,
  the reason and evidence are the envelope's, no longer `DEADLINE_AFTER_DISPATCH`. An isolated success
  is still returned;
- direct fan-out, and `iter_reference_outcomes()` with direct or batch dispatch. A batch reference
  whose lost envelope was answered 403 on retry was already unknown, but with reason
  `HTTP_STATUS_AFTER_DISPATCH`.

- **A success body that cannot be decoded is unknown.** A 2xx answer to a sent request whose body
  is not a Bitrix envelope (`{}`, truncated JSON or a non-finite number, for example) was a
  `CommandFailure` carrying `EnvelopeContractError` or `ProtocolError` in classic batch and
  direct fan-out, and a `ReferenceFailure` in `iter_reference_outcomes()` with direct or batch
  dispatch. It is now unknown with reason `HTTP_STATUS_AFTER_DISPATCH` and that error as its cause, as
  REST v3 batch already reported it. `call()` still raises the `EnvelopeContractError` or
  `ProtocolError` itself.
- **A command answer that cannot be decoded is unknown.** Malformed paging metadata of a classic
  batch command (a `result_total` that is not an integer, for example) and an embedded error that
  the command's `ResultErrorSpec` finds malformed were a `CommandFailure` with `ProtocolError` in
  classic batch, REST v3 batch and direct fan-out (with `EnvelopeContractError`, for malformed
  metadata in direct fan-out). They are now `CommandOutcomeUnknown` with reason
  `HTTP_STATUS_AFTER_DISPATCH` and that error as its cause on every path.
- **A classic batch command without exactly one entry is unknown.** A key in both `result` and
  `result_error` used to be read as its error, so a `QUERY_LIMIT_EXCEEDED` there made an `UNSAFE`
  command run again. A command missing from both maps was a `CommandFailure` with `ProtocolError`.
  Neither answer says whether the command ran, so both are now `CommandOutcomeUnknown` and are never
  sent again.
- **`HALTED` only where the command was proven not sent or the server halted before it.** A
  fail-fast `batch()` reports `CommandNotExecuted` with `NotExecutedReason.HALTED` for a command of a
  later envelope that fail-fast did not send, of a window refused before dispatch, or one that has no
  entry and comes after a command the server answered in `result_error`. A command the server halted
  before is unknown instead when an earlier round of the same batch may have run it. A failure found in a
  returned result by the command's own `ResultErrorSpec` does not halt the server, so a later command
  whose result came back keeps its own outcome; before, a later `ProtocolError` was reported `HALTED`.
- **The operation deadline bounds admission, not the send.** The operation deadline of
  `fan_out_outcomes()` and `iter_reference_outcomes()`, with direct or batch dispatch, ended a send
  that was already in flight and reported a failure with `BudgetExceededError`. The executor's own
  budgets now end that send, and the command or reference arrives as unknown with reason
  `DEADLINE_AFTER_DISPATCH` and a cause chain that reaches the `TransportError` of the cut send, as
  `batch_outcomes()` reports it; `call()` raises that ambiguity for an `UNSAFE` or `UNKNOWN`
  request.
- **`fan_out_outcomes()` keeps the replay disposition.** Its `CommandFailure` and
  `CommandOutcomeUnknown` carried `NOT_ELIGIBLE` whatever the evidence. They now carry the
  disposition that `batch_outcomes()` gives the same answer: a quota refusal in a `BatchDispatch`
  fan-out is `ELIGIBLE`.
- **An answer that arrives after the time budget is kept.** A success that arrived after the retry
  or operation time budget was replaced by `BudgetExceededError("transport completed after execution
  time budget")`: `call()` raised it, and a batch reported the command unknown. The success is now
  returned. A failure that arrives late meets the usual rules first. For example, a late 502 to an
  `UNSAFE` request raises `AmbiguousExecutionError`, and a late non-retryable error raises itself. The
  budget stops only the retry that such a failure would allow.
- **Fast keyset waves.** A wave whose physical batch answer cannot be decoded records its pages as
  `PageOutcome.UNKNOWN` with `PageRejectionCode.BATCH_ENVELOPE`, and the traversal raises the
  `AmbiguousExecutionError`, whose cause is the protocol failure. Before, the pages were `REJECTED`
  and the traversal raised `IncompleteTraversalError`. A wave command missing from the answer is
  `UNKNOWN` with `AMBIGUOUS_EXECUTION`.
- **Settlement agrees with the outcome.** A fan-out, reference or list page whose sends were all refused
  (for example 429 until the attempt budget ran out), or that the server answered conclusively
  (`ACCESS_DENIED`, for example), now settles as a failure, not as unknown. A page of a counted
  traversal's batch tail settles the same way: unknown when its batch may have run, a failure when it
  was answered conclusively; before, every failed tail page settled as a failure. A `SAFE` batch command
  whose every send the server answered with its own structured error until the budget ran out is a
  `CommandFailure`, not unknown. A command that was never sent settles as not executed even when it is
  reported as a `CommandFailure`: in `batch_outcomes()`, a command refused with `CapabilityError` before
  dispatch settled as a failure.
- **A failed page is traced the way it settles.** In the report of a fan-out, a reference or a list
  traversal, a page whose command failed is `PageOutcome.UNKNOWN` only when it may have run, with
  `PageRejectionCode.AMBIGUOUS_EXECUTION`, or `BATCH_ENVELOPE` when the answer of its physical batch
  cannot be decoded. A page refused conclusively (`ACCESS_DENIED`, for example) is `REJECTED` with
  `COMMAND_FAILURE`, as fast keyset waves record it. Before, every such page was `UNKNOWN`, and its code
  was `COMMAND_FAILURE` unless the failure was an `AmbiguousExecutionError`, so a page of a sequential
  or counted traversal that lost its answer was `COMMAND_FAILURE` too.
- **An abandoned batch page is not sent.** A reference page fetched with `BatchDispatch` whose fetch was
  cancelled, or whose admission deadline passed, while its wave was being assembled was still sent with
  that wave and its answer discarded. Its wave now leaves without it, and a wave left with no page is
  not sent at all.

## Closing a stream from inside its own pull or cleanup

- **Breaking: caller code a stream runs can no longer close that stream or its client.**
  `stream.aclose()` or `client.aclose()` now raises `RuntimeError` when it is called from code the
  stream itself runs: a page-stop `on_page`, an async command source of `batch()` or `fan_out()`,
  an async binding or reference source (its `finally` included, which the stream's own cleanup
  runs), an injected transport, or a task one of them started. The refusal changes nothing: the
  stream keeps reading and the client stays open.
- **What it fixes.** Such a close cancelled the stream's read or cleanup and then waited for it to
  end, while that read or cleanup was waiting for the code that asked for the close. The program
  hung for good, and no cancellation, `asyncio.wait_for` included, could end the wait. A task the
  caller code started hung the same way only when that code awaited the task.
- **What is refused, exactly.** A task started from that caller code inherits the stream's context,
  so its close is refused too while a read or the cleanup of that stream is running, even when
  nothing awaits the task. A close started with `asyncio.create_task(stream.aclose())` therefore
  works or is refused depending on when the task runs, and a refused one loses the stop it asked
  for. Between reads the close is allowed, except for `iter_references()`, `iter_cursors()`,
  `iter_reference_outcomes()`, `iter_reference_sources()`, `iter_reference_source_outcomes()`,
  `fan_out()` and `fan_out_outcomes()`, whose scheduler keeps running caller code in its own tasks
  until the stream ends.
- **How to stop instead.** Return `CallerStop` from `on_page` to stop a binding, end the source (return
  from the generator or raise `StopAsyncIteration`), or close from the code that iterates the
  stream. A task that must close the stream on its own can be started outside the stream's context:
  `asyncio.create_task(stream.aclose(), context=contextvars.Context())`. Closing another stream
  from caller code is still allowed.
- **How the refusal surfaces.** Left unhandled, the `RuntimeError` fails the stream as any failure of
  that caller code does: each family wraps it as it wraps that code's other failures (for example
  `ReferenceFailed`, `BatchFailed`, `InputSourceError` or `TransportError`), and the `RuntimeError`
  stays in the cause chain. Raised in a source's `finally` during the stream's cleanup, it is the
  cleanup failure: the close raises it and the report records `cleanup_failure`.
- **A cancelled close or cleanup gives up at the operation deadline.** Stream cleanup deferred a
  cancellation of its caller until it ended, however long that took. It now defers it only while
  the operation's `ExecutionPolicy.max_elapsed` lasts; cleanup still running then finishes in the
  background, the close raises `BudgetExceededError` and the cancellation is delivered at the next
  await, and the report carries a `cleanup_failure` violation with the terminal reason "stream
  cleanup failed". Without a cancellation, cleanup is still awaited to its end.

## CLI exit code for a closed reader

- **A write to a closed stdout or stderr reader exits `5`**, ahead of any other code. It exited `120`,
  `1`, or with unbuffered output `0` (help) or `2` (a parser error).
- **Any other failed write exits `4`**, `--help` to a full disk included, which exited `0` or `120`.

## Traversal contract checks

- **`SERVER_NEXT` needs an exact total.** `OffsetSpec(continuation=OffsetContinuation.SERVER_NEXT)`
  without `total_termination=TotalTermination.EXACT_QUALIFIED` now raises `ValueError` at
  construction. Such a traversal never continued without `next`, and with the total disabled only an
  empty page could close it: an empty first page, or one a `next` had led to. Bitrix omits `next` on
  its last page, which still holds rows, so every non-empty source failed with `PaginationError` on
  that page. Add `EXACT_QUALIFIED` when the
  filtered `total` is exact, or use the default `SERVER_NEXT_OR_OBSERVED_COUNT`.
- **A row breaking its identity raises `IdentityContractError` on every traversal.** A missing
  identity, or one of the wrong type, failed the traversal with a plain `PaginationError` (for example
  "identity path is missing") before the typed check could run, and the page was recorded as
  `range_contradiction`. Sequential, counted, keyset (sequential and fast: range, partitioned and
  automatic), cursor, continuation, window and reference traversals now all raise
  `IdentityContractError` with the path, coercion, observed type and row offset; the failure category
  and the page trace both say `identity_contract`. It is a subclass of `PaginationError`, so
  `except PaginationError` still catches it. The row offset counts rows of the traversal, except in
  a fast keyset, where it counts rows of the lane that read the page: of its range window, partition
  lane, boundary page or finishing sweep. In a cursor traversal without its own `identity`, a row
  whose cursor is missing or malformed now fails with this error too, since the cursor is its
  identity. Tokens are still read before the `page_adapter` runs, so an adapter never sees a page
  with a row breaking its identity or cursor; that page now raises the typed error. An adapter that
  removes or changes a valid identity or cursor value still fails with `PageAdaptationError`.
- **`DECIMAL_STRING_INTEGER` accepts only canonical decimal strings.** Strings were converted with
  `int()`, so `"012"`, `" 12"`, `"1_2"`, `"+12"` and non-ASCII digits all became `12`. Only ASCII
  digits without a leading zero are accepted now, with a minus before a non-zero value (`"0"`, `"12"`,
  `"-7"`); integers are accepted as before. A refused identity raises `IdentityContractError`. A
  refused cursor value raises `IdentityContractError` too when the cursor is the identity (no other
  `identity=` is declared), and the cursor's `PaginationError` ("cursor value does not satisfy
  cursor_coercion") beside a separate identity. A caller's start cursor is checked the same way: a
  `Binding(start_cursor=...)` of `iter_cursors()` with a non-canonical string fails that binding
  before it is sent, and a cursor control already in the request of an exclusive-range cursor
  (`CursorDomain.EXCLUSIVE_POSITIVE_INTEGER`) is refused with `CapabilityError` before any request.
- **A cursor identity on another field is no longer ordered.** `iter_list_cursor()` and
  `iter_cursors()` checked the order of `identity=` values as if they were the cursor, so an identity
  on another field (or on the cursor field with another coercion) failed with "page identities are
  not strictly ascending". The cursor carries the order; such an identity now only has to be unique,
  unless the consistency policy declares an order. An identity equal to the cursor keeps the cursor's
  order check. An explicit `ConsistencyPolicy(order_semantics=OrderSemantics.ASCENDING)` (or
  `DESCENDING`) now applies to such an identity on its own: beside a cursor of the opposite direction
  it used to be refused with `CapabilityError` before any request, and it is now admitted and fails
  the traversal with `PaginationError` as soon as the identity's values break that order.
- **A repeated page still fails without identity.** `iter_list()` without `identity` fails when a
  page equals an earlier one, for example under a narrow `select` whose rows repeat. That stays: such
  a page cannot be told apart from a portal that ignored `start`. The error message now says so; add a
  unique field such as `ID` to the `select` and declare it as `identity`.

## Budget accounting scales with the bindings

- **Long reference streams no longer slow down as they go.** Every page and row of
  `iter_references()`, `iter_reference_sources()` and `fan_out()` copied, sorted and validated the page count of
  every binding read so far, so CPU time grew with the square of the bindings: over an in-memory
  transport, 10,000 one-page bindings took about 74 s of CPU, against 8 s now. The operation's
  budget counters are now updated in place, and a binding's page count is dropped once the binding
  ends. Budgets, failures, events and reports are unchanged.
- **`BudgetCounters` is only a snapshot.** The value in `b24api.contracts.policy`, which is not
  exported, lost its `reserve_attempt`, `reserve_page` and `with_buffered_rows` methods; budget
  checks live in the operation's execution context. Its `pages_per_reference` in an execution
  snapshot lists only bindings that still hold pages: open bindings with a committed page, and a
  closed binding until its page in flight settles.

## Early close and completion evidence

- **A counted early exit publishes its report.** Leaving `iter_list_counted()` with `break`, or
  `collect(limit=...)`, while a tail page was being delivered raised `RuntimeError: completion gate
  requires stream terminal and cleanup outcome` and left `stream.report` empty. The stream now
  closes as `EARLY_CLOSED` with its report.
- **No false protocol violation on an early close.** A stream closed, or failed in a `page_stop`
  callback, while it delivered a page reported the blocking violation
  `completion_invalid_binding_terminal`. A binding that ends as failed or unknown now releases a page
  that was settled but never acknowledged, so that violation only reports a real protocol error, such
  as a page whose dispatch outcome was never settled or a caller stop over an unacknowledged page. A
  reference whose `page_stop` callback fails therefore ends its operation as
  `COMPLETED_WITH_FAILURES` with a `ReferenceFailure`, like any other failed binding, instead of
  `INCOMPLETE`.
- **A `BaseException` leaves a counted traversal as raised.** A `BaseException` that is not an
  `Exception`, such as an application's own shutdown signal, was wrapped in
  `IncompleteTraversalError`. It now propagates unchanged with a `FAILED` report, as from
  `iter_list()`; ordinary exceptions are wrapped as before.

## Keyset traversal

- **A caller order is refused.** A sequential keyset, flat or in references, accepted an `order`
  with another field beside the identity, for example `{"order": {"PRIO": "DESC"}}`. The traversal
  added its identity key after it, the server ordered by the caller's field first, and the cursor
  skipped rows while the report still claimed exact identity coverage. Such a request now fails
  with `CapabilityError` before any request is sent. A sequential keyset still accepts an order by
  the identity key alone; fast and automatic keyset refuse any caller order, as before.
- **Anchor rows are adapted.** A partitioned keyset, including the one `AutoKeysetExecution` picks
  for a large selection, published each lane's retained upper anchor row without passing it through
  the `page_adapter`. The anchor probe page is now adapted like every other page, so an adapter that
  raises on it fails the traversal instead of letting the row through unadapted. An adapter is
  therefore also invoked on the anchor probe pages of `AutoKeysetExecution` and
  `PartitionedKeysetExecution`, including when the automatic plan ends up sequential, and may be
  called more often than before.

## Cancellation is delivered once

- **Your cleanup after a cancelled read runs.** A stream, batch or reference operation cancelled
  while it waited raised the `CancelledError` and then armed it again, so the first `await` in the
  caller's own `finally` or `except asyncio.CancelledError:` block was interrupted. On Python 3.12 the
  first `await` after a handled `asyncio.timeout(...)` around a stream raised `CancelledError` too.
  A cancellation is now replayed only when stream cleanup absorbed it while another failure was
  raised in its place. On Python 3.12 that replay still survives a handled `asyncio.timeout(...)`
  around the stream when its cleanup fails, because `uncancel()` there does not disarm it; Python
  3.13 drops it.
- **Requests work in the cleanup of a cancelled task.** `call()` and the other request methods,
  started after the task received its cancellation and kept running (without `uncancel()`), raised
  `CancelledError` before sending, and a response that arrived after the task's first cancellation
  was dropped. They are now sent and return their result, so a compensating write or an unlock in
  cleanup takes effect. A cancellation requested while such a request runs still stops it, even
  when a transport suppresses it and returns a response or reports a failure: the request is not
  retried, and a batch sends no further envelope.

## Request encoding

- **Enum members send their value everywhere.** A `str`, `int` or `float` subclass in request
  parameters, such as a member of `class Stage(str, Enum)` or an `IntEnum`, is now stored as its
  plain value, keys included. A direct JSON call already sent `"NEW"`, but a classic batch command
  rendered the member through its own `__str__` and sent `Stage.NEW`, so a counted traversal sent its
  head and its tail with different filters. `Request` equality and hashing now also treat a member
  and its value alike.
- **Classic batch refuses an empty array or object.** PHP bracket encoding names leaves only, so
  `{"fields": {"UF_MULTI": []}}` reached the server as no field at all, and an update meant to clear
  a field succeeded without clearing it. Each command a classic batch would send is now checked before
  any request:
  - `batch_outcomes()`, `fan_out_outcomes()` and batched references fail that command or binding
    locally with `CapabilityError` (a `CommandFailure` or a `ReferenceFailure`), and the others still
    run; a binding that replaces an empty placeholder of its template is sent as usual.
  - Fail-fast `batch()` sends nothing for a window that holds such a command and raises `BatchFailed`.
  - `iter_list_counted()` (its tail is batched), fast and automatic keyset traversal and
    `verify_keyset_capability()` refuse such a base request before their first request. A common case
    is an empty `filter: {}`: omit it, or use `iter_list()` or `SequentialKeysetExecution`.
  - A `FORM_URLENCODED` request with an empty container is refused when it is built.

  Native REST v3 batch and every direct JSON request still send empty containers. `None` is still
  omitted from bracket encoding, so `{"UF_MULTI": [None]}` also sends no field there. The new
  read-only `Request.has_empty_container` tells whether a request holds such a value.

  **Warning: `None` in a classic batch is a no-op, not "clear this field".** A direct JSON call sends
  `None` as `null` (`{"fields": {"TITLE": null}}`); what the method does with `null` is its own. The
  bracket encoding of a classic batch command, and of a `FORM_URLENCODED` request, drops every `None`
  instead, with nothing in its place and no error: `{"fields": {"TITLE": None}}` sends no `TITLE`, so
  the update leaves the field as it was; a mapping left holding only `None` values is not sent at all;
  and `{"fields": {"UF_MULTI": [None, 5]}}` sends only `fields[UF_MULTI][1]=5`, the later items keeping
  their index. `Request.has_empty_container` does not flag a `None`. To clear a field in a batch, send
  the value the method itself expects for clearing it, never `None`.

## Page-level response fields

- **`PageBoundary.result`.** A `page_stop` callback now receives the page's immutable server result
  as `boundary.result`, the same snapshot a page adapter, a window classifier and a continuation rule
  see as `PageView.result`. Window and continuation traversals offer every page, including an empty
  or overlap-only closing page, so a field that the endpoint returns beside the rows on every response
  can be merged there instead of through a manual response walk or an impure page adapter. The field
  is keyword-only and `None` for a `PageBoundary` built by application code, so existing
  constructions keep working. Only its top-level type is checked: anything but a JSON scalar, a
  tuple or a `FrozenMapping` (a dict or a list, for example) is rejected with `TypeError`.
- **`thaw_json` is public.** `b24api.contracts.thaw_json` returns a plain mutable copy of a frozen
  value (dicts and lists), for example to store a field of `boundary.result`. It replaces the private
  `b24api.contracts.json._thaw_json`.

## Fast keyset references: shared final confirmations

Public signatures are unchanged; what changes is how a fast or automatic keyset binding on the shared
reference queue spends physical requests and how it reports a failed or lost command.

- **Final confirmations are batched.** A binding's sequential finish pages, including the final empty
  confirmation, are now commands of the shared batch queue instead of direct requests. Their
  `PageRecord.dispatch` is `BATCH` instead of `DIRECT`, and the operation report's `batch_requests`
  and `batch_commands` count them with the other commands of the shared batches. A flat
  `iter_list_keyset()` still sends its finish page directly.
- **Batching trades request count for waiting.** A finish page now waits for a free batch slot
  instead of running beside the batch in flight. Under `INPUT` delivery a binding confirms its end
  only once it is the frontier, so with many small parents and the default
  `BatchDispatch(concurrency=1)` the confirmations queue one after another behind the batches in
  flight. How much longer depends on how many pages `max_buffered_rows` holds: 100 parents of 3
  rows each, at 10-100 ms per request, took from about 1.1 times as long (default budget, page size
  10) through 1.5 times (default budget, page size 50) to 1.7 times (300 rows, page size 10), while
  sending fewer requests. Where the cost appears, `BatchDispatch(concurrency=2)` brings wall time
  back to what it was. Under
  `READY` delivery the dispatcher now also waits up to `coalesce_wait` for a fast binding that is
  about to send its next command, so far fewer requests are sent; at very low latency this waiting
  can cost more wall time than the saved requests.
- **A refused finish page fails like a refused wave command.** Before, the server's refusal of the
  direct finish request was the binding's error itself (an `ApiResponseError`), and the page trace
  had no record of that page. Now the binding ends with an `IncompleteTraversalError` whose `error`
  is the command's `BatchCommandError` (an `ApiResponseError` subclass) and which carries its replay
  disposition, and the page is recorded as `BATCH`, `REJECTED`, `COMMAND_FAILURE`. Code that matched
  `ApiResponseError` on such a binding should look at `IncompleteTraversalError.error`. A malformed
  batch envelope now fails every binding with a command in that batch, and a lost one leaves all of
  them unknown, as it already did for wave commands, instead of affecting only the binding whose
  finish page it was.
- **A command lost in transit leaves its binding unknown.** When the batch carrying a fast binding's
  wave or finish command is ambiguous (for example, the connection drops after it was sent), the
  binding now ends with `ReferenceOutcomeUnknown` carrying the `AmbiguousExecutionError` and is
  counted in the report's `unknown`, as a binding whose direct page was lost already was. Before, a
  wave command ended it with a `ReferenceFailure` wrapping that error in an
  `IncompleteTraversalError`. Likewise, a flat range, partitioned or automatic `iter_list_keyset()`
  whose wave is lost now raises the `AmbiguousExecutionError` itself and ends `FAILED`, as a flat
  sequential stream whose page is lost already did, instead of raising `IncompleteTraversalError`
  and ending `INCOMPLETE`.
- **`INPUT` hands rows out in binding order.** Under `DeliveryOrder.INPUT`, while a fast or automatic
  binding waits for its row lease, no later binding is granted a lease or reserves an ordinary page,
  and the next frontier waits for a lease that its binding's plan has not settled yet. A tight
  `max_buffered_rows` that used to fail a frontier with `BudgetExceededError`, depending on timing,
  now lets it start, also when keyset, ordinary and automatic sources are mixed. Later bindings
  prefetch less while an earlier one waits.
- **Small plans give rows back.** Once a binding selects a sequential or boundary-only plan, it trims
  its row lease to the rows that plan can still hold, so the bindings waiting for a lease start
  sooner. The plan itself is still chosen at the full lease, so selection and geometry are as before.
  A lease never grows, so a dense binding admitted right after a cohort of small ones is sized while
  their leases are still active and can get a smaller share than before: 20 one-row parents followed
  by a 5000-row parent send about 15% more requests (428 instead of 373 under `READY`), and a tight
  pool can make it up to 40%. Across mixed cohorts the total request count still drops.

## REST 3.0 error codes

- **Codes are shown verbatim.** A REST 3.0 `error.code` of snake-case identifier shape now appears
  as is in the `ApiResponseError` message, `wire_code`, `normalized_code`, `code` and
  `to_safe_dict()`. Before, all but two such codes of 16 or more characters with a digit in them
  (such as `BITRIX_REST_V3_EXCEPTION_ACCESSDENIEDEXCEPTION`) rendered as `[REDACTED]`. Code that matched
  `[REDACTED]` to detect a REST 3.0 failure should match the code instead. A structured code that
  contains a registered or request secret in any letter case is now `[REDACTED]` as a whole.
- **Two REST 3.0 load limits are retried.** `RetryPolicy.transient_api_codes` now also lists
  `BITRIX_REST_V3_EXCEPTION_RATELIMITEXCEPTION` and
  `BITRIX_REST_V3_EXCEPTION_LIMITER_QUERYTIMELIMITEXCEPTION`, and
  `AmbiguityPolicy.refusal_api_codes` lists the rate limit, which the server refuses before the
  method runs. A policy built with its own code sets keeps exactly those sets. Every other code keeps
  its previous retry behavior.
- **`b24api.contracts.v3_codes`.** `KNOWN_V3_ERROR_CODES` now also holds the two load limit codes and
  is no longer the non-retryable set; use `NON_RETRYABLE_V3_ERROR_CODES` for that.

## Declared short-page closure and keyset binding filters

Both changes are additive: existing calls keep their behavior, and the `b24api` root exports are
unchanged. The new public names live in `b24api.contracts`.

- **Declared short-page closure.** `OffsetSpec(short_page_termination=ShortPageTermination.DECLARED_TERMINAL)`
  lets a qualified fixed-step endpoint close on a page shorter than its window. The default
  `ShortPageTermination.DISABLED` keeps the fail-closed fixed-step behavior, including ignoring `next`.
  A traversal that closes this way reports `exhausted=True` with `mechanics_only` assurance; see
  [Declared short-page closure](recipes.md#declared-short-page-closure) for the strict preconditions.
- **Completion evidence.** `BindingClosure` gains `DECLARED_SHORT_PAGE` and `BindingTerminal` gains
  `declared_short_page_width` (default `None`); exhaustive matches over `BindingClosure` need one more
  arm.
- **Per-binding closure.** `ReferenceComplete` gains a last field, `closure`, naming the witness that
  ended each successful binding (`SOURCE_EMPTY`, `QUALIFIED_TOTAL`, `DECLARED_SHORT_PAGE`,
  `CALLER_STOP` and so on). It is excluded from equality and hashing, so existing comparisons keep
  their results; `dataclasses.fields()`, `asdict()` and `repr()` now include it, and a value built
  without it holds `None`.
- **Keyset binding filters.** A `KeysetTraversal` binding may now set a simple field directly inside
  `KeysetSpec.filter_path`, such as `filter[=ownerId]`; such bindings previously ended as
  `ReferenceNotExecuted(LOCAL_VALIDATION_FAILED)` and now run. Updates that name the identity field,
  reach deeper into the filter, or carry a mapping or nested list still fail locally, and every other
  control path stays exclusive; see
  [Keyset references with per-owner filters](recipes.md#keyset-references-with-per-owner-filters).
  `traversal_control_paths()` is unchanged and still names the whole filter container.

## Upgrading from 2.3 to 3.0

3.0.0 changes only what the list below names. Work through it in order; each item links to the
details.

1. **Root imports.** The `b24api` root exports 51 names; 115 others moved to `b24api.contracts`,
   `b24api.errors`, `b24api.transport` or `b24api.completion`. Old imports fail with `ImportError`
   or `AttributeError`, and type checkers flag them. `from b24api import *` follows `__all__` and
   no longer brings the moved names; replace it with explicit imports. Run
   `python -m b24api.migration src/ tests/` to list both; see [Root imports in 3.0](#root-imports-in-30).
2. **Removed report vocabulary.** Values no code path produced are gone from `KeysetExecutionReport`,
   `KeysetAssuranceSource`, `ReplayDisposition`, `CompletionAssurance`, `SnapshotState` and
   `NotExecutedReason`, and `PageValidated` has no `identity_digest`. Drop those arms from exhaustive
   matches; see [Removed intentionally](#removed-intentionally).
3. **`EnvelopeContractError` is a `ProtocolError`.** `except ProtocolError` now also catches a 2xx
   response without the result envelope; see
   [Important semantic corrections](#important-semantic-corrections). When such a response answers
   the physical batch of a fast keyset wave, `page_trace` records `PageRejectionCode.BATCH_ENVELOPE`
   for its pages (previously `COMMAND_FAILURE`). A 2xx body with a top-level `error` that is not
   strict JSON (invalid UTF-8, `NaN` or `Infinity`, a duplicate batch correlation key) is now an
   `EnvelopeContractError` too, where 2.3 reported the embedded error as `ApiResponseError`.
4. **A logical batch closed early says so.** Closing `batch()` or `batch_outcomes()` before the input
   is exhausted gives an `EARLY_CLOSED` report whose `terminal_reason` is
   `"stream closed before exhaustion"` (previously `"GeneratorExit"`).
5. **Reports name the public failure.** A fail-fast batch reports `terminal_reason="BatchCommandError"`
   with the violation `batch_command_failure`, instead of a private carrier class and
   `internal_failure`. Code that matched the old strings must match the new ones.
6. **Fixed step refuses at once.** `OffsetContinuation.FIXED_STEP` without an exact qualified total
   raises right after a short page instead of first requesting an unusable confirmation page, so such
   a traversal sends one request fewer.
7. **Stream lifecycle.** Every stream family now terminates through one lifecycle owner:
   - reading a stream after `aclose()` ends the iteration with `StopAsyncIteration` and sends
     nothing (previously `RuntimeError("stream was closed before exhaustion")`);
   - `aclose()` during an in-flight read reports `EARLY_CLOSED` (previously `CANCELLED`): the close
     owns termination, even though it cancels the read;
   - a report is final once published, after cleanup; a later `aclose()` never raises a cleanup error
     or changes it;
   - when cleanup fails after the source was exhausted, the report records a `cleanup_failure`
     violation and `CleanupState.FAILURE`, and the cleanup error is raised;
   - a failure during cleanup that follows another failure is kept as a secondary `cleanup_failure`
     violation (for example `reference cleanup also failed (RuntimeError)`), so a report can carry
     more than one.
   - a stream closed before its first read, by `aclose()` or on leaving `async with`, still closes
     the iterator it took from your command or reference source, once, without reading from it; a
     failing close is raised and recorded as a `cleanup_failure` on an `EARLY_CLOSED` report;
   - a stream raises its terminal failure once: a later read, before or after `aclose()`, ends the
     iteration with `StopAsyncIteration` and sends nothing (previously the same exception was raised
     again). Keep the exception from the read that raised it if you need it later; it carries the
     published report;
   - a cancellation during the cleanup on leaving `async with` no longer replaces the exception raised
     in its body: that exception propagates and the cancellation is raised at the next `await`.

8. **Compressed responses.** Only identity, `gzip` and `deflate` bodies are decoded; `br`, `zstd`,
   stacked and unknown codings are refused as a transport failure, and so is a compressed body that
   ends before the end of its stream. Library-owned requests send
   `Accept-Encoding: gzip, deflate`. If you inject an HTTPX client that asks for `br` or `zstd`,
   remove that header.
9. **Batch replay is decided per command.** A failed physical batch is not retried or abandoned as
   a whole. Each `SAFE` command is sent again, in a smaller physical batch, after any transient
   failure. An `UNSAFE` or `UNKNOWN` command is sent again only when the failure proves it did not
   run: a transport failure before dispatch, an unstructured 423, 425 or 429, or a
   `QUERY_LIMIT_EXCEEDED` / `OPERATION_TIME_LIMIT` refusal of the batch or of that command in
   `result_error`. When the batch may have run (a transport failure after dispatch, or a 408 or 5xx
   status from `AmbiguityPolicy.ambiguous_unstructured_statuses`), its `UNSAFE` and `UNKNOWN`
   commands arrive as `CommandOutcomeUnknown`; reconcile them as you would an ambiguous direct call.
   In 2.3 the whole batch was retried or not by its least safe command. Replays share the request's
   attempt and time budgets. A command keeps its last outcome when the budget stops a replay before
   it is sent; a replay that was sent reports the command as possibly executed, unless its last
   answer was a listed refusal. A direct `UNSAFE`
   or `UNKNOWN` request is now also retried after such a proven refusal. The refusals come from
   `AmbiguityPolicy.refusal_http_statuses` and `AmbiguityPolicy.refusal_api_codes`. A code or status
   you add to `RetryPolicy` alone retries `SAFE` work only; add it to the refusal sets too if it
   proves that Bitrix did not run the method. A fail-fast `batch()` is not split.
10. **Error text.** Error descriptions show field names from your request as `field#N`, known V3
    codes verbatim, and distinct hidden mapping keys as `[REDACTED#1]`, `[REDACTED#2]`, … Code that
    parses error strings must accept these forms.
11. **Mid-collection start with an exact total.** An offset traversal whose initial `start` differs
    from the plan's first offset raises `CapabilityError` before any request when
    `TotalTermination.EXACT_QUALIFIED` closes it. Start from the beginning, or use
    `TotalTermination.DISABLED` to walk a suffix.
12. **Keyset verification.** When a boundary read is answered wrongly, `verify_keyset_capability()`
    raises `KeysetCapabilityError` with an `UNSUPPORTED` report (`error.report`) instead of a raw
    `PaginationError` (order, page cap) or `CapabilityError` (identity shape). Code that caught
    `PaginationError` there must catch `KeysetCapabilityError` or `CapabilityError`.
13. **Keyset selection reasons.** `KeysetSelectionReason` gains `EXPLICIT_SEQUENTIAL` and
    `PAGE_STOP`; handle them in exhaustive matches.
14. **HTTP/2 and hpack logging.** While a library HTTPX client is open, `hpack.hpack` and
    `hpack.table` records are dropped. Removing that filter makes the next HTTP/2 send raise
    `CapabilityError` before I/O.
15. **Permanent transport refusals.** A `TransportError` with `retryable=False` is raised after one
    send instead of being retried until `BudgetExceededError`. Code that caught
    `BudgetExceededError` for such a failure must catch `TransportError`.
16. **Exceptions from an injected transport.** An arbitrary exception raised inside a custom
    `Transport.send` or `send_wire` becomes `TransportError(phase=DISPATCH_STARTED, retryable=False)`
    with your exception as `__cause__`. A direct `SAFE` request raises that `TransportError` after one
    send; a direct `UNKNOWN` or `UNSAFE` request raises `AmbiguousExecutionError`; every admitted
    command of a physical batch arrives as `CommandOutcomeUnknown`. Catch these instead of your own
    exception class, and read `__cause__` for the original. A closed `HttpxTransport` raises
    `TransportError(phase=NOT_DISPATCHED, retryable=False)` instead of `RuntimeError`: nothing was
    sent, so no command is reported as possibly executed.
17. **Oversized responses.** A response larger than `ExecutionPolicy.max_response_bytes` is refused
    before decoding. An injected transport is now held to the same ceiling as the bundled one: a
    direct `SAFE` request raises `ResponseTooLargeError`, and a direct `UNKNOWN` or `UNSAFE` request
    raises `AmbiguousExecutionError`. For a physical batch the change applies to every transport,
    the bundled `HttpxTransport` included: each command arrives as `CommandOutcomeUnknown` with the
    `ResponseTooLargeError` as the cause, where 2.3 gave a `CommandFailure`, and a keyset page trace
    records the page as an ambiguous execution rather than a command failure. Code that retried
    such commands with a smaller `select` must now treat them as possibly executed.

Additions that need no change: `Request.bare()` and `Request.v3()`, `Bitrix24.from_webhook()`,
`HttpxTransport` in the root, `ExecutionPolicy.from_settings()` and
`OperationReport.keyset_selection`.

The sections below describe the full current contract, including the 2.3 changes (a required
`route=`, positional arguments, page stops) for code upgrading from earlier releases.

## Migrating to the issues architecture (2.3)

The route is now a required part of every `Request` and closed request mapping. Existing callers
must choose `RouteKind.BARE`, `RouteKind.JSON` or `RouteKind.API_V3`; `with_parameters()` preserves
that choice. The CLI likewise requires `--route bare|json|api_v3` for `call`, `list` and
`verify-keyset`. A classic webhook base must have the form `/rest/<user>/<token>/`. The transport
resolves bare and `.json` suffixes or `/rest/api/<user>/<token>/` at dispatch. Classic physical
batches carry only `BARE` commands; `JSON` commands need direct dispatch. `API_V3` commands travel in
native REST 3.0 batch envelopes (one JSON array per contiguous run of `SAFE` commands or of non-`SAFE`
commands, so `UNSAFE` and `UNKNOWN` share a run) on a transport that advertises JSON-array bodies,
such as `HttpxTransport`; on any other transport they are refused with `CapabilityError` before any
request. Before this native envelope existed, every non-`BARE` batch command was refused, so code
that relied on that refusal for `API_V3` commands now sends them; `UNSAFE` and `UNKNOWN` commands of
an envelope that may have run arrive as unknown and are never resent. V3 accepts a JSON
body and reports object-valued API errors with typed validation details. A transport serves only the
routes it declares in `TransportCapabilities.routes`, which defaults to `BARE`; `HttpxTransport`
declares all three. A custom send-only transport, which cannot declare routes, serves only `BARE`.
An undeclared route is refused with `CapabilityError` before any request, instead of silently
reaching the classic handler.

```python
from b24api import Request, RouteKind

request = Request("profile", route=RouteKind.BARE)
v3_request = Request("tasks.task.result.list", route=RouteKind.API_V3)
```

HTTPX INFO records for requests owned by `HttpxTransport` have their registered webhook URL
redacted before logging handlers format them. This applies to an injected `httpx.AsyncClient` while
it is used through that transport, including its redirect hops. Any other request through the same
client, even one sent from its event hook or auth flow while an owned request is in flight, is logged
unchanged except that the registered webhook secret itself is never logged.
An injected client's `auth` still runs on owned requests when it authorizes the request in place, as
`httpx.BasicAuth`, `httpx.DigestAuth` and header-setting flows do. A flow that yields any other
`Request` (a clone, a credential rotation or an unrelated request) is refused before that request is
sent, with a non-retryable `TransportError` whose phase records whether the owned request had
already been answered: a substitute would make it undecidable which records carry the webhook.
Direct use of a caller-owned client after the transport closes is outside that shield. An
application enabling the separate `httpcore` DEBUG logger needs its own logging policy and test;
this guarantee covers the emitting `httpx` INFO logger. `httpcore` DEBUG records are not filtered:
they include the response headers, so a redirect whose `Location` header carries the webhook token
writes the token into a record.
The supported HTTPX range is `>=0.28.1,<0.29`, with `h2>=4.3.0,<4.5` and `hpack>=4.1.0,<4.3` as
direct requirements, because the shield filters the `hpack` logger names verified on those lines. An
environment pinned below them must upgrade; raising any of these upper bounds requires rerunning the
positive logger controls against the newly admitted version.

Direct access to `RateCoordinator.acquire()` now requires a non-empty `methods` frozenset. A
physical batch passes every inner method as one admission unit. `Retry-After` pauses the portal
host; `OPERATION_TIME_LIMIT` pauses only its method, with a configurable 120-second default.
The coordinator binds the portal host of every transport it serves, `HttpxTransport` or custom,
through the `Transport.host` property, and rejects attempts to share it across different hosts; a
custom transport without `host` is refused with `TypeError` when the `Executor` is built. An unsafe
request or batch failure records the throttle without automatically replaying the request. Callers
can pass an absolute monotonic `DeadlineBudget` to bound permit waits and handle typed budget,
closed, and capacity errors.

Qualified PHP methods that take positional arguments now use `PositionalArguments` with an
explicit `PositionalLayout`. Pass that value as the second argument to `Request`. Slots are
represented by `Present`, `EmptyObject`, `EmptyArray`, `Null`, or a trailing `Omitted`. A layout
declares exact arity, each slot's shape, fixed slots, and case-sensitive writable control paths.
`write_control()` returns a new value and rejects an undeclared or missing parent path. A declared
final mapping leaf may be created when every parent container already exists. Positional requests
use a top-level JSON array; form encoding and physical batch reject them before I/O. A traversal
control the slots cannot accept raises `CapabilityError` before I/O; its message names a value-free
reason and its cause is the positional control error carrying a
`b24api.contracts.positional.PositionalControlFault`.

`OperationReport.exhausted` now records whether every binding reached qualified full-source
closure. A successful page-boundary stop therefore has `state=COMPLETED`, `exhausted=false`, and
`partial=true`. Failures, unknown outcomes, early close, and cleanup failure also cannot claim
exhaustion. Applications that previously treated successful state as a complete checkpoint must
gate that checkpoint on `report.exhausted`.

The public `page_stop` callback runs after the validated page has been delivered. Return only after
the page and its checkpoint are durably committed; the client emits `PageAcknowledged` after the
callback succeeds and before scheduling the next page. A callback exception is a traversal failure.
Counted physical-batch tails reject page-stop construction because already scheduled sibling pages
cannot be withdrawn safely.

Bounded keyset execution now needs a qualified admitted upper boundary, an enforced fence, and the
declared method contract in `BoundedIdentityRange`. `SequentialKeysetExecution` consumes that
boundary, closes only after the exact admitted upper ID is witnessed, and reports
`BOUNDED_RANGE_OBSERVED`; it does not need a trailing empty confirmation. Fast
`RangeKeysetExecution`, `PartitionedKeysetExecution`, and auto execution reject a boundary before
I/O, and so does a reference `KeysetTraversal`, because one filter-bound range cannot be shared by
bindings that rewrite parameters. `CALLER_ASSERTED_BOUNDS` describes the source of fast-execution bounds and does not assert a
stable snapshot of a mutating source.

Logical-batch `CommandFailure` and `CommandOutcomeUnknown`, reference failures and unknown outcomes,
blocking violations, and `IncompleteTraversalError` now expose the kernel's
`replay_disposition`. Retry only when it is `ReplayDisposition.ELIGIBLE`; replay safety and
retryability remain inputs to that closed decision.

A per-reference `IncompleteTraversalError` (the `error` of a `ReferenceFailure`, including those in
`ReferenceFailed.outcomes`) now carries `report=None`: only the whole stream has an operation report,
available as `stream.report` and `ReferenceFailed.report`. Read the binding's typed cause from the
error's `error` attribute and its delivered prefix from `ReferenceFailure.partial_rows`.

For one-based page controls, pass `OffsetSpec(parameter_path=path, page_index=PageIndex(path,
initial=1, increment=1, max_rows=10))` to `iter_list(..., page_size=10)`. The wire control
advances by one even when a page selects fewer than ten rows. An empty page is the terminal
witness. `PageStride` records a method's qualified server offset granularity separately from its
decoded row cap and rejects increments that would alias a rounded server page. For a sparse
selected result, `SparseRawBound` adds an exact raw total path, fixed stride, finite page budget,
and stable-order contract; empty selected pages remain traversable until the raw range is covered.
This is structural coverage, so its report uses `RAW_RANGE_COVERED` rather than claiming a
snapshot of a mutable source. A sparse traversal starts at offset zero and is available only
through `iter_list`: a resumed nonzero start, an offset off the server granularity, and a reference
traversal with `SparseRawBound` are each refused with `CapabilityError` before any request.

`OffsetContinuation.FIXED_STEP` without `TotalTermination.EXACT_QUALIFIED` no longer accepts an
empty page after a short page as closure. Such a traversal now raises `IncompleteTraversalError`
(cause `PaginationError`) and reports `exhausted=False`; full pages followed by an empty page still
complete with `mechanics_only` assurance. Qualify an exact total, switch to `OBSERVED_COUNT` or
`PageIndex` when the endpoint supports them, or use `SparseRawBound`; see
[endpoint recipes](recipes.md#fixed-server-stride). Assurance names what a completed traversal
proved: a report that did not complete carries at most `mechanics_only`, whatever its plan declared
and even when a caller stopped one of its bindings; `bounded_prefix` appears only on a completed
report. `completed_with_failures` keeps the declared assurance, which describes the bindings that
completed.

`SparseRawBound.total_path` also accepts `RawTotalSource.ENVELOPE` when the qualified raw extent is
the response envelope `total` rather than a field inside `result`; a missing or negative envelope
total fails closed like a missing result field.

Exact traversal now has an in-memory `ExecutionPolicy.max_identity_keys` budget (100,000 keys by
default); exceeding it fails the operation closed. `iter_list` and
`iter_list_counted` accept `identity_store=`, a caller-owned `IdentityStore` whose
`add_if_absent(identity_store_key(value)) -> bool` records each identity after the rest of the page
validates; `False` is a duplicate, and any exception rejects the page. The client does not close the
store. Under `DuplicatePolicy.REPORT`, an observed duplicate now withdraws identity strength: the
report carries `mechanics_only` instead of `identity_exact` or `identity_and_count_matched`.

When a stride's decoded row cap differs from its wire increment and the request owns a limit
control, set `requested_wire_limit` explicitly to at least the wire increment. Construction now
rejects an omitted or smaller value before I/O instead of leaving part of each wire window
unrequested. For ordinary traversal, `max_decoded_rows` must equal the wire increment, which
prevents both skipped subwindows and overlapping windows from claiming completion. Sparse raw-bound
traversal keeps its separate raw-range closure contract.

The earlier 2.x keyset migration notes below remain as historical guidance for that API.

## Root imports in 3.0

The `b24api` root now exports only the names a typical application needs: the client and its
settings, requests and routes, policies, traversal and dispatch specifications, the report and the
main errors, and the transports, including `HttpxTransport`. The other 115 names moved to one of the
public packages `b24api.contracts`, `b24api.errors`, `b24api.transport` and `b24api.completion`,
which export the same objects. No object was removed or renamed; only the root path is gone.

The old root paths do not resolve in 3.0 and there is no deprecation period:
`from b24api import CommandSuccess` raises `ImportError: cannot import name 'CommandSuccess' from
'b24api'`, and `b24api.CommandSuccess` raises `AttributeError: module 'b24api' has no attribute
'CommandSuccess'; it moved to b24api.contracts.CommandSuccess in 3.0`. Mypy and pyright report each
old root import as `attr-defined`, which points at every line to change. A wildcard import follows
`__all__`, which lists only the 51 root names, so a moved name used after `from b24api import *`
raises `NameError`. The scanner reports every such import; replace it with
explicit imports of the names the module uses. Leaf modules such as
`b24api.contracts.request` or `b24api.transport.base` are not public paths; import from the packages
above.

To list every old root import in your code, run the scanner. It rewrites nothing and exits with 1
when it finds any:

```console
$ python -m b24api.migration src/ tests/
src/app.py:3 b24api.CommandSuccess -> b24api.contracts.CommandSuccess
```

<!-- ROOT_MOVES table: generated by b24api.migration.migration_table(); do not edit by hand -->
| Old import | New import |
|---|---|
| `from b24api import CompletionGate` | `from b24api.completion import CompletionGate` |
| `from b24api import AdaptedPage` | `from b24api.contracts import AdaptedPage` |
| `from b24api import AmbiguityReason` | `from b24api.contracts import AmbiguityReason` |
| `from b24api import BinaryEvidence` | `from b24api.contracts import BinaryEvidence` |
| `from b24api import BinaryResponse` | `from b24api.contracts import BinaryResponse` |
| `from b24api import BindingAdmitted` | `from b24api.contracts import BindingAdmitted` |
| `from b24api import BindingClosure` | `from b24api.contracts import BindingClosure` |
| `from b24api import BindingTerminal` | `from b24api.contracts import BindingTerminal` |
| `from b24api import BodyEncoding` | `from b24api.contracts import BodyEncoding` |
| `from b24api import CallerStop` | `from b24api.contracts import CallerStop` |
| `from b24api import CleanupOutcome` | `from b24api.contracts import CleanupOutcome` |
| `from b24api import CleanupState` | `from b24api.contracts import CleanupState` |
| `from b24api import ClosureWitness` | `from b24api.contracts import ClosureWitness` |
| `from b24api import Command` | `from b24api.contracts import Command` |
| `from b24api import CommandFailure` | `from b24api.contracts import CommandFailure` |
| `from b24api import CommandNotExecuted` | `from b24api.contracts import CommandNotExecuted` |
| `from b24api import CommandOutcome` | `from b24api.contracts import CommandOutcome` |
| `from b24api import CommandOutcomeUnknown` | `from b24api.contracts import CommandOutcomeUnknown` |
| `from b24api import CommandSettlement` | `from b24api.contracts import CommandSettlement` |
| `from b24api import CommandSuccess` | `from b24api.contracts import CommandSuccess` |
| `from b24api import CompletionEvent` | `from b24api.contracts import CompletionEvent` |
| `from b24api import CompositeIdentitySpec` | `from b24api.contracts import CompositeIdentitySpec` |
| `from b24api import ConsistencyPolicy` | `from b24api.contracts import ConsistencyPolicy` |
| `from b24api import ContinuePage` | `from b24api.contracts import ContinuePage` |
| `from b24api import CursorDomain` | `from b24api.contracts import CursorDomain` |
| `from b24api import DeliveryOrder` | `from b24api.contracts import DeliveryOrder` |
| `from b24api import DuplicatePolicy` | `from b24api.contracts import DuplicatePolicy` |
| `from b24api import EmptyArray` | `from b24api.contracts import EmptyArray` |
| `from b24api import EmptyObject` | `from b24api.contracts import EmptyObject` |
| `from b24api import FrozenJson` | `from b24api.contracts import FrozenJson` |
| `from b24api import FrozenMapping` | `from b24api.contracts import FrozenMapping` |
| `from b24api import identity_store_key` | `from b24api.contracts import identity_store_key` |
| `from b24api import IdentityComponent` | `from b24api.contracts import IdentityComponent` |
| `from b24api import IdentityPageAdapter` | `from b24api.contracts import IdentityPageAdapter` |
| `from b24api import IdentityStore` | `from b24api.contracts import IdentityStore` |
| `from b24api import KeysetAssuranceSource` | `from b24api.contracts import KeysetAssuranceSource` |
| `from b24api import KeysetCapabilityCheckName` | `from b24api.contracts import KeysetCapabilityCheckName` |
| `from b24api import KeysetCapabilityCheckOutcome` | `from b24api.contracts import KeysetCapabilityCheckOutcome` |
| `from b24api import KeysetCapabilityCheckResult` | `from b24api.contracts import KeysetCapabilityCheckResult` |
| `from b24api import KeysetCapabilityReport` | `from b24api.contracts import KeysetCapabilityReport` |
| `from b24api import KeysetCapabilityVerdict` | `from b24api.contracts import KeysetCapabilityVerdict` |
| `from b24api import KeysetExecution` | `from b24api.contracts import KeysetExecution` |
| `from b24api import KeysetExecutionKind` | `from b24api.contracts import KeysetExecutionKind` |
| `from b24api import KeysetExecutionReport` | `from b24api.contracts import KeysetExecutionReport` |
| `from b24api import KeysetInconclusiveReason` | `from b24api.contracts import KeysetInconclusiveReason` |
| `from b24api import KeysetPageCompletion` | `from b24api.contracts import KeysetPageCompletion` |
| `from b24api import KeysetPhase` | `from b24api.contracts import KeysetPhase` |
| `from b24api import KeysetSelectionReason` | `from b24api.contracts import KeysetSelectionReason` |
| `from b24api import MembershipRecheck` | `from b24api.contracts import MembershipRecheck` |
| `from b24api import NotExecutedReason` | `from b24api.contracts import NotExecutedReason` |
| `from b24api import Null` | `from b24api.contracts import Null` |
| `from b24api import Omitted` | `from b24api.contracts import Omitted` |
| `from b24api import OperationStream` | `from b24api.contracts import OperationStream` |
| `from b24api import PageAcknowledged` | `from b24api.contracts import PageAcknowledged` |
| `from b24api import PageBoundary` | `from b24api.contracts import PageBoundary` |
| `from b24api import PageCommandOutcome` | `from b24api.contracts import PageCommandOutcome` |
| `from b24api import PageDelivered` | `from b24api.contracts import PageDelivered` |
| `from b24api import PageDispatch` | `from b24api.contracts import PageDispatch` |
| `from b24api import PageIndex` | `from b24api.contracts import PageIndex` |
| `from b24api import PageOutcome` | `from b24api.contracts import PageOutcome` |
| `from b24api import PageRecord` | `from b24api.contracts import PageRecord` |
| `from b24api import PageRejected` | `from b24api.contracts import PageRejected` |
| `from b24api import PageRejectionCode` | `from b24api.contracts import PageRejectionCode` |
| `from b24api import PageScheduled` | `from b24api.contracts import PageScheduled` |
| `from b24api import PageStride` | `from b24api.contracts import PageStride` |
| `from b24api import PageValidated` | `from b24api.contracts import PageValidated` |
| `from b24api import PageView` | `from b24api.contracts import PageView` |
| `from b24api import PartialResult` | `from b24api.contracts import PartialResult` |
| `from b24api import partition_command_outcomes` | `from b24api.contracts import partition_command_outcomes` |
| `from b24api import partition_reference_outcomes` | `from b24api.contracts import partition_reference_outcomes` |
| `from b24api import PositionalArguments` | `from b24api.contracts import PositionalArguments` |
| `from b24api import PositionalLayout` | `from b24api.contracts import PositionalLayout` |
| `from b24api import Present` | `from b24api.contracts import Present` |
| `from b24api import RawTotalSource` | `from b24api.contracts import RawTotalSource` |
| `from b24api import ReferenceComplete` | `from b24api.contracts import ReferenceComplete` |
| `from b24api import ReferenceEvent` | `from b24api.contracts import ReferenceEvent` |
| `from b24api import ReferenceFailure` | `from b24api.contracts import ReferenceFailure` |
| `from b24api import ReferenceItem` | `from b24api.contracts import ReferenceItem` |
| `from b24api import ReferenceNotExecuted` | `from b24api.contracts import ReferenceNotExecuted` |
| `from b24api import ReferenceOutcome` | `from b24api.contracts import ReferenceOutcome` |
| `from b24api import ReferenceOutcomeUnknown` | `from b24api.contracts import ReferenceOutcomeUnknown` |
| `from b24api import ReplayDisposition` | `from b24api.contracts import ReplayDisposition` |
| `from b24api import RequestHeaders` | `from b24api.contracts import RequestHeaders` |
| `from b24api import RequestSummary` | `from b24api.contracts import RequestSummary` |
| `from b24api import Response` | `from b24api.contracts import Response` |
| `from b24api import ResultCollectionShape` | `from b24api.contracts import ResultCollectionShape` |
| `from b24api import ResultErrorShape` | `from b24api.contracts import ResultErrorShape` |
| `from b24api import ResultErrorSpec` | `from b24api.contracts import ResultErrorSpec` |
| `from b24api import SlotContract` | `from b24api.contracts import SlotContract` |
| `from b24api import SlotShape` | `from b24api.contracts import SlotShape` |
| `from b24api import SparseRawBound` | `from b24api.contracts import SparseRawBound` |
| `from b24api import SplitOrderSpec` | `from b24api.contracts import SplitOrderSpec` |
| `from b24api import StreamClosure` | `from b24api.contracts import StreamClosure` |
| `from b24api import StreamTerminal` | `from b24api.contracts import StreamTerminal` |
| `from b24api import TotalHintMode` | `from b24api.contracts import TotalHintMode` |
| `from b24api import TraceClass` | `from b24api.contracts import TraceClass` |
| `from b24api import traversal_control_paths` | `from b24api.contracts import traversal_control_paths` |
| `from b24api import TraversalIdentity` | `from b24api.contracts import TraversalIdentity` |
| `from b24api import UnknownRequestAudit` | `from b24api.contracts import UnknownRequestAudit` |
| `from b24api import UnknownRequestCollector` | `from b24api.contracts import UnknownRequestCollector` |
| `from b24api import Violation` | `from b24api.contracts import Violation` |
| `from b24api import ViolationSeverity` | `from b24api.contracts import ViolationSeverity` |
| `from b24api import BatchCommandError` | `from b24api.errors import BatchCommandError` |
| `from b24api import EnvelopeContractError` | `from b24api.errors import EnvelopeContractError` |
| `from b24api import HTTPGatewayError` | `from b24api.errors import HTTPGatewayError` |
| `from b24api import IdentityContractError` | `from b24api.errors import IdentityContractError` |
| `from b24api import InputSourceError` | `from b24api.errors import InputSourceError` |
| `from b24api import PageAdaptationError` | `from b24api.errors import PageAdaptationError` |
| `from b24api import PageAdaptationViolation` | `from b24api.errors import PageAdaptationViolation` |
| `from b24api import ResponseTooLargeError` | `from b24api.errors import ResponseTooLargeError` |
| `from b24api import ResultShapeError` | `from b24api.errors import ResultShapeError` |
| `from b24api import ValidationIssue` | `from b24api.errors import ValidationIssue` |
| `from b24api import TransportCapabilities` | `from b24api.transport import TransportCapabilities` |
| `from b24api import WireRequest` | `from b24api.transport import WireRequest` |
| `from b24api import WireResponse` | `from b24api.transport import WireResponse` |
<!-- end of ROOT_MOVES table -->

## Keyset verification, cursor fan-out, and page adaptation

Normal `iter_list_keyset()` no longer sends the five diagnostic canary commands. Range and
partitioned reports use `KeysetAssuranceSource.CALLER_ASSERTED_BOUNDS`. 3.0.0 removes the legacy
`KeysetAssuranceSource.CANARY_VERIFIED_BOUNDS` and the always-zero `canary_requests`,
`canary_commands` and `canary_rows` fields of `KeysetExecutionReport`; `KeysetPhase.CANARY` remains
because `verify_keyset_capability()` still records its canary wave under that phase.
This reduces every bounded runtime estimate by the former canary waves, but a broken endpoint can
now emit a partial prefix before later bound validation raises typed `IncompleteTraversalError`.

Capability-report identity evidence is recursively immutable (`FrozenJson`) in Python. Consumers
that need mutable or directly serializable containers should use `KeysetCapabilityReport.to_dict()`,
which returns a detached ordinary JSON tree. Fast-keyset adapter failures are identified in page
traces by the additive `PageRejectionCode.PAGE_ADAPTATION` enum member.

Qualify the exact portal, credentials, request/filter, identity, ordering representation and page
cap on a stable development or staging fixture. Keep the guard immediately beside the production
call; verification does not switch runtime mode and is not cached:

```python
import os

if os.environ.get("ENV") != "PROD":
    # Accepting an ID filter does not prove strict bounds or ordering.
    await api.verify_keyset_capability(
        request,
        selector=selector,
        identity=identity,
        keyset=keyset,
    )

stream = api.iter_list_keyset(
    request,
    selector=selector,
    identity=identity,
    keyset=keyset,
)
```

`UNSUPPORTED` proves a shape, order, cap, or out-of-interval defect. `INCONCLUSIVE` is also
fail-closed, but means the fixture is sparse or changed during verification. Re-run after portal,
permission, request shape, filter, or server changes. Do not use fast keyset for
`crm.contact.userfield.list` or deprecated `crm.productsection.list`, which were observed to ignore
strict ID bounds; use `iter_list()` / `iter_list_counted()` instead. Prefer
`catalog.section.list` over the deprecated product-section method. These observations are guidance,
not a built-in endpoint registry or a guarantee for other portals.

For one or many independent parents, replace manual cursor batching or the verbose
`iter_references(..., traversal=CursorTraversal(...))` form with `iter_cursors()`:

```python
bindings = [
    Binding(
        "chat 42",
        (ParameterUpdate(ParameterPath(("DIALOG_ID",)), "chat42"),),
        correlation={"chat_id": 42},
        start_cursor=7300,
    ),
]

async with api.iter_cursors(
    request,
    bindings,
    selector=ResultSelector(("messages",)),
    cursor=cursor,
) as stream:
    async for event in stream:
        consume(event)
```

A one-element binding source is the canonical single-parent correlated form. The existing
`iter_list_cursor()` is not removed or renamed: it remains the simpler raw-row API for one request.
`iter_cursors()` is finite and fail-fast; tolerant processing remains
`iter_reference_outcomes(..., traversal=CursorTraversal(...))`. The application owns the sync/async
binding source and any database/session lifetime; `b24api` never owns or imports storage machinery.

`Binding.start_cursor` is an application-owned checkpoint. Persist the identity of the last item
successfully committed downstream, then recreate the binding after restart. Delivery is
at-least-once across the gap between stream delivery and durable commit, so downstream writes must
be idempotent. Each parent advances independently and still requires an empty confirmation page.
When `CursorSpec.allow_create_controls=False`, the request must already contain the complete cursor
control path including its leaf. `start_cursor` may replace that leaf, but never creates it.

All list APIs now accept a synchronous object strategy `page_adapter`. It sees one immutable
`PageView` containing the full frozen result and the selected frozen items, and returns an
`AdaptedPage` with the same cardinality, order and configured identity/cursor values. For example,
an application-owned adapter may enrich `messages` from sibling `users` and `files` nodes:

```python
class ImMessagePageAdapter:
    def adapt(self, page: PageView, /) -> AdaptedPage:
        users = page.result["users"]
        return AdaptedPage(
            {**message, "author": users[str(message["author_id"])]}
            for message in page.items
        )
```

Adapters must be pure, reentrant and synchronous: no I/O, `await`, filtering, fan-out, aggregation
or reordering. Violations raise `PageAdaptationError`; inspect its closed
`PageAdaptationViolation`, while raw rows, sibling metadata, correlation and application exception
text remain absent from safe diagnostics.

`BatchDispatch` now defaults to `coalesce_wait=0.020`: an underfilled physical wave may wait up to
20 ms for another capacity-eligible producer. A full batch or zero producer potential is sent
immediately, and the absolute deadline is per wave, not per operation. Set `coalesce_wait=0` for
latency-oriented workloads. The dispatcher shutdown remains cancellation-based; the old unreachable
`None` queue sentinel has been removed.

## Important semantic corrections

- Unstructured status 408 and 5xx responses are ambiguous for `UNSAFE` and `UNKNOWN` requests;
  they are never replayed automatically. Structured Bitrix errors remain conclusive.
- API error text preserves the portal's wire spelling and shows the normalized spelling only when
  it differs. Match `.normalized_code`, not rendered text.
- Boolean values in form and physical-batch bracket encoding are now `1` and `0`; the old `False`
  text was truthy to PHP.
- `None` values are omitted from form and physical-batch bracket encoding, matching PHP query conventions;
  JSON encoding continues to send them as `null`.
- A 2xx response missing the canonical result envelope raises `EnvelopeContractError`. It is a
  subclass of both `HTTPGatewayError` and `ProtocolError`, so `except ProtocolError` now catches it
  too; its origin stays `http_gateway`, it is never retried, and reports still classify it as
  `envelope_contract`. Malformed non-empty JSON remains a plain `ProtocolError`.
- `ExecutionPolicy.from_settings(settings)` returns the client's default policy (the library
  defaults with `max_retry_elapsed_per_request = settings.http_timeout`). A `policy=` argument still
  replaces that default wholesale; derive it with `dataclasses.replace` to keep the timeout.
- Counted identity is optional. Without it, matching a qualified total yields count-only assurance;
  with it, the report records identity-and-count assurance.
- `iter_list_counted()` no longer fails when the first page has no rows, no `next`, and no usable
  `total` (missing, `null`, or `-1`). It completes and is exhausted after that one request, with
  `mechanics_only` assurance (`identity_exact` with an identity) and the terminal reason
  `empty source observed without a total`. Callers that must see an exact total set
  `ConsistencyPolicy.confirmation_policy` to `b24api.contracts.policy.ConfirmationPolicy.QUALIFIED_TOTAL`,
  which keeps such a page incomplete.
- Final 1xx and 3xx responses are classified as `HTTPGatewayError` with their actual status before
  envelope decoding.
- `RequestSummary.to_dict()` now includes bounded `encoding` and normalized `header_names` fields.
- `WireResponse.__repr__` is intentionally value-free; do not parse or snapshot its former body text.

Two construction/identity details can affect callers that used public values as low-level building blocks:

- `AmbiguousExecutionError` construction now requires explicit `reason` and `declared_unsafe` keywords;
- `Request` equality and hashing are structural and type-sensitive, so equivalent immutable request trees
  compare equal and can be used safely as dictionary or set keys.
- Canonical JSON nesting deeper than 256 levels is rejected at construction with `ValueError` for request,
  response, parameter-update, batch, and reference values; an over-deep server response surfaces as
  `ProtocolError`. This rejects the value before equality or hashing can encounter the recursion limit.

Use `UnknownRequestCollector` as the client's `unknown_request_audit` hook to inventory requests
that still rely on the default replay classification. The hook receives value-free summaries only.

Physical batches cannot represent per-command form bodies or scoped headers. Dispatch those requests
directly. Use `call_bytes()` explicitly for binary success responses. Fixed-step, split-order,
mapping-shape, and composite-identity configurations are shown in [endpoint recipes](recipes.md).

This release retains the compatibility removals already made in 2.x. Migrate by capability rather than by
preserving names or return-shaping flags.

| Earlier capability | Current 2.x operation | Important difference |
|---|---|---|
| Decoded `call()` | `call()` | Returns detached decoded JSON. |
| Raw/envelope call | `call_response()` | Always returns the immutable `Response` type. |
| Disable retries | `ExecutionPolicy(max_attempts_per_request=1)` | Replay safety is separately declared on `Request`. |
| Finite batch | `batch(Command(...))` | The logical source may be arbitrarily long; physical chunks remain bounded. |
| Payload tuples / `with_payload` | `Command.correlation` | Correlation stays off-wire and is present on typed outcomes. |
| Tolerant batch | `batch_outcomes()` | Handle the closed success/failure/not-executed/unknown union. |
| Sequential offset list | `iter_list()` | Conservative default; follows server continuation sequentially. |
| Counted batched list | `iter_list_counted()` | Direct head plus physically batched tail; requires an exact total; identity is optional but strengthens assurance. |
| No-count/keyset list | `iter_list_keyset()` | Auto is now the default and may select boundary-only, sequential, range, or partitioned execution after a planning barrier. |
| Cursor wrappers | `iter_list_cursor()` / `iter_cursors()` | Raw single traversal, or correlated one/many-parent scheduling with optional per-binding seed. |
| Independent request wrappers | `fan_out()` / `fan_out_outcomes()` | Explicit direct or batch dispatch and delivery order. |
| Per-parent/reference wrappers | `Binding` + `iter_references()` / `iter_reference_outcomes()` | Parent correlation and traversal state are explicit and isolated. |

## Removed intentionally

- return-type-changing Python flags;
- tuple-shaped payload conventions;
- automatic endpoint profiles and method-name inference;
- permissive cursor de-duplication that could hide missing rows;
- automatic unsafe direct fallback;
- public low-level execution plans and compatibility data models.
- report values no code path produced (3.0.0): `ReplayDisposition.REPLAYED_DIRECT` and
  `DIRECT_REPLAY_FAILED` (a physical batch is never replayed as direct calls),
  `CompletionAssurance.ORACLE_VERIFIED`, `SnapshotState.VERIFIED` and `SnapshotState.CHANGED`
  (no snapshot oracle exists; a required snapshot reports `UNVERIFIED`), and
  `NotExecutedReason.SCHEDULER_STOPPED`. Drop those arms from exhaustive matches; the enum inputs
  `SnapshotRequirement` and `ConfirmationPolicy` keep every member.
- `PageValidated.identity_digest` (3.0.0). The gate only ever checked that the digest was non-empty,
  so every recorder paid a SHA-256 per page for no guarantee. The gate still checks event order and
  `row_count`; a custom recorder or test that builds `PageValidated` drops the argument.

There is no assumption-free fast no-count shortcut. Direct `Bitrix24.iter_list_keyset()` calls and
CLI keyset contracts that omit `execution` now assert the default `StableIntegerKeysetContract` and
use auto planning, so verify that the endpoint honors a unique integer identity, strict numeric
bounds, ordering, and empty-confirmation completion. `KeysetTraversal` used by reference traversal
remains explicitly sequential because reference keysets do not support fast execution. Account for
the pre-emission planning cost. Fast keyset totals remain advisory, and the application still owns
mutation and business-filter reconciliation.

Static ineligibility raises `CapabilityError` synchronously from the `iter_list_keyset(...)` call,
before iteration starts. Typical causes include a non-integer identity, caller-supplied order,
start, or strict-bound controls, incompatible consistency requirements, and insufficient policy
capacity. If the endpoint accepts the controls but violates the declared ordering or bounds, the
traversal fails closed with `IncompleteTraversalError` (for example, `range_contradiction`), possibly
after a partial prefix. Auto never restarts such a failed traversal as sequential.

If auto is ineligible for an endpoint, exposes a portal incompatibility, or the old request-by-request
behavior is required, opt out per call. This performs the original sequential keyset traversal and
does not run the auto planning barrier:

```python
import os

from b24api import SequentialKeysetExecution

if os.environ.get("ENV") != "PROD":
    # Accepting an ID filter does not prove strict bounds or ordering.
    await client.verify_keyset_capability(
        request,
        selector=selector,
        identity=identity,
        keyset=keyset,
    )

stream = client.iter_list_keyset(
    request,
    selector=selector,
    identity=identity,
    keyset=keyset,
    execution=SequentialKeysetExecution(),
)
```

For CLI keyset contracts, use `"execution": {"kind": "sequential"}`. Omitting `execution` (or
passing an empty execution object) selects auto. Use `iter_list_counted()` only when an endpoint
supplies an exact filtered total. CLI reports now include a `keyset_selection` object
(`requested_kind`, `selected_kind`, `reason`) for every keyset traversal, and a detailed
`keyset_execution` object only when the fast path ran; a `page_stop` traversal with the default auto
execution reports `auto`, `sequential`, `page_stop`. Report consumers should treat both additive
fields as selected-plan evidence.

## Practical migration order

1. Replace direct calls and choose `call()` versus `call_response()` by required return type.
2. Mark replay safety explicitly for operations whose semantics are known.
3. Replace payload tuples with `Command.correlation` or `Binding.correlation`.
4. Choose list traversal from the table in README based on evidence the endpoint actually exposes.
5. Consume terminal reports and tolerant outcome unions exhaustively.
6. Add application checks for business filters and composite identities; the generic client cannot
   infer them from method names.

Signed-upload support is not exported because its acceptance gate was not established by the
available evidence. Counted-tail direct recovery is likewise not enabled: the observed failure was
an identity-contract defect, not a proven batch-only transport failure. Both omissions are deliberate
release-gate outcomes rather than silent fallbacks.
