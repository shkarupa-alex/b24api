# Migration guide

## Actions outside a JSON or form POST

- **Breaking for code that matches or enumerates `BodyEncoding`: it gains `QUERY_STRING`.** Such a request sends its
  parameters in the URL's query, in the PHP encoding of form bodies, and refuses at build time what that
  encoding would drop, as a form request does, but without advising JSON encoding, which a GET-only action
  cannot take. Without content it is a GET, which `crm.site.fileuploader.preview`,
  `crm.site.fileuploader.download`, `call.Track.download` and `crm.timeline.ai.mergefields` require.
- **New `RawContent(data, content_type)` and `Request(content=...)`** (also on `Request.bare()`, in the
  `RequestSpec` mapping, kept by `with_parameters()`). A `QUERY_STRING` request with content is a POST of
  those bytes as they are, under the declared content type, as `crm.site.fileuploader.upload` reads a chunk.
  Content with another encoding is refused when the request is built. The content type is a media type with
  optional parameters, as RFC 9110 writes it in visible ASCII (`text/plain; charset=utf-8`), at most 256 characters;
  `HttpxTransport` checks it again before it sends. Its representation shows the type and length, never
  the bytes.
- **Breaking: a transport that declares `QUERY_STRING` in `TransportCapabilities.encodings` must honor
  `content`.** `HttpxTransport` and `ScriptedTransport` declare it; a send-only transport, one that does not declare it and
  a classic batch refuse such a request before any send. The `CapabilityError` for an undeclared encoding now
  reads "transport does not support <encoding> requests" (it said "request bodies").
  A scripted exchange matches raw content by its bytes and type. A third-party transport that declares its
  encodings as `frozenset(BodyEncoding)` now declares `QUERY_STRING` too, with no change of its own: the
  client sends it query-string requests, and `run_transport_conformance` fails it. List the encodings it builds instead.
- **Breaking for code that matches or enumerates `ConformanceCase`, or counts a report's outcomes: it gains
  `QUERY_STRING_GET` and `QUERY_STRING_RAW_CONTENT`.** Both are skipped for a transport that does not declare `QUERY_STRING`. They
  decode the query as PHP does (`+` and `%20` are both a space, the order of distinct keys does not matter,
  a blank value or a bare key is a parameter) and require exactly the pairs the client encodes, so a transport
  that sends a list item as `[]`, repeats a key or adds an empty one fails though PHP would read it alike.
  The GET must carry no body and no content type; the raw POST must carry exactly the declared bytes and the
  declared content type exactly as written, so a transport that rewrites it, even to an equivalent form, fails.
  An injected HTTPX client's own `params` join the query, as they join a body request's URL; on a shared key
  the request's value wins.
- **Breaking for a transport whose requests HTTP/1.1 cannot parse: the conformance responder reads a request
  with `h11`**, the HTTP/1.1 parser under HTTPX, now a declared dependency. Every case decodes a body sent
  chunked as one sent with `Content-Length`, so a transport that streams its body no longer fails the body
  cases, and a GET whose body goes out chunked fails `QUERY_STRING_GET`. Header names are matched in any
  letter case. A request HTTP/1.1 cannot parse, such as one without `Host` or with a space before a header's
  colon, which a server must refuse, fails the case that sent it with the detail `UnparseableRequest` when
  the responder has read it by the time the transport ends, whether the transport returns, raises or is
  cancelled, so always for a transport that reads the answer. A transport that ends right after it writes may
  end before the responder has even accepted its connection; closing the responder then drops the request
  unread, the case keeps its own outcome or the transport's error, and a case that does not wait for a request
  may even pass. A header line folded onto the next is joined to it with one space, keeping any whitespace that
  ended the first line, as HTTP/1.1 lets a server do, so the raw case reads a folded content type as joined;
  a portal's web server may refuse it instead.
- **HTTPX log records of the client's own requests drop the URL's query** and keep its path, as they never
  show a body. The query still reaches the portal's web server and any proxy, which may log it.

## The reference row pool and its batches

- **A fan-out result longer than its share grows into free rows.** `fan_out()` and `fan_out_outcomes()`
  reserve each command a share of `max_buffered_rows` before it is sent; a list result longer than that
  share was dropped with `ResultTooLargeError`, so a lookup returning 51 rows failed under the default
  `BatchDispatch`, although the budget had room for it. The answer is decoded already, so it now takes the
  rest from the free rows. While it waits for them, the rows that come back are its first: no other command
  reserves rows (under `INPUT` delivery, none but the frontier's), and the commands reserved before it are
  still sent. It waits while the rows it lacks may all come back without it: a command already reserved
  lands, or the consumer takes rows (its own command's earlier pages and, under `READY` delivery, any). Under `INPUT` delivery a result behind the frontier waits for the
  frontier to reach it: the rows the commands before it hold come back by then, those waiting to grow as well,
  while the rows read ahead for the commands behind it come back only after it. While a result waits, its decoded rows are held beside `max_buffered_rows`, not in it: at most one
  result per command in flight, each no longer than the budget. A result ends with `ResultTooLargeError`, at
  once, only when it is longer than the whole budget, or when the rows it lacks cannot all come back: they are
  held by results waiting to grow as well or, under `INPUT`, read ahead behind it, or when `max_elapsed` runs
  out while it waits (the operation then ends with its time budget). The error's `limit` is then the rows it
  could hold. A paged reference page never grows: one with more rows than its
  page size fails its reference: the response exceeded the declared page cap (`PaginationError`).
- **Source-order (`INPUT`) batched references send fuller batches.** Each binding ahead of the frontier
  kept room for its next page however many rows its page in flight already held, so under the default
  budget half the pool stayed free while pages waited for rows: 50,000 bindings took twice the requests of
  `READY` delivery. A binding now keeps only the part of its room its pages in flight do not hold, the
  dispatcher asks whether a page may still come by the pool's own rule, and an underfilled wave whose
  pages wait for the rows of a wave in flight waits for that wave to land instead of for the coalescing
  timer. `concurrency=4` no longer splits the pool between half-empty waves. Without a rate limit that
  costs wall time, since a send slot stays idle while the rows it could use are in flight (5.5 s against
  4.7 s for 2,000 one-row commands at `batch_size=20, concurrency=4` and 100 ms per batch);
  `coalesce_wait=0` sends every wave at once, in more requests. The figures are in `docs/performance.md`. Two small read-ahead shapes send more requests than before (35 instead of 32, and
  59 instead of 54, in `tests/reference_read_ahead_test.py`): that is the price of keeping only the unheld
  part of a room, which on large sources sends far fewer (about 120 instead of 193 for 5,000 bindings of one
  and two pages).
- **A lone fast keyset binding costs what the same traversal costs directly.** A fast or automatic keyset
  binding is granted its row lease when it is admitted, while the source may still hold bindings that
  would share the pool, so a single binding planned on half of `max_buffered_rows`: over 20,000 dense
  rows Auto took 23 requests in a binding against 14 directly, range windows 20 against 11, partitioning
  290 against 227. The lease now grows as its binding plans, once its boundary wave has landed, to the
  share the facts then known give: a source read out by then leaves a lone binding the whole pool, and
  the binding costs what the direct traversal costs. A source whose next binding is still being read
  keeps the half.
- **A counted tail fills its waves whatever `max_active_references`.** The pages waiting for a wave were
  bounded by the active bindings, so with `max_active_references=1` and no coalescing a counted tail of 116
  pages took 116 requests. They are bounded by the rows they reserve, and the same tail takes 8.

## Load limits, holds and attempt timeouts

- **Breaking: `Settings.http_timeout` bounds each attempt, and the client's default policy is
  `ExecutionPolicy()`.** `http_timeout` was each request's retry window under the client's default
  policy, `ExecutionPolicy.from_settings(settings)`, and an attempt had no timeout of its own: one that
  hung (a server that accepts TCP and never answers the TLS handshake, say) spent the whole window, and
  was reported sent, so an `UNSAFE` request ended unknown though no byte of it left, and a `SAFE` one
  was never retried. A `policy=` replaced the default wholesale, so
  `Bitrix24(Settings(http_timeout=2.0), policy=ExecutionPolicy(max_requests=7))` silently got a
  120-second window. `http_timeout` (30 s by default) now bounds each attempt of the client's own HTTPX
  transport: each phase of it (connecting, TLS included, waiting for a pooled connection, writing the
  request, each read of the answer) times out after it, and earlier when the retry window has less
  left. An attempt that times out before anything was sent is proven not sent and retried whatever its
  safety. `ExecutionPolicy.from_settings()` is removed and `Settings` puts nothing in a policy: the
  client's default policy is `ExecutionPolicy()`, whose retry window is 120 s, and a `policy=` keeps the
  configured attempt timeout. To keep the old 30-second window, pass
  `ExecutionPolicy(max_retry_elapsed_per_request=30.0)`. `HttpxTransport` takes `timeout=` (30 s by
  default) and reports it as `timeout`; a transport you inject keeps its own timeouts. The window's end
  cuts an attempt of the client's transport where the transport knows how far it got: a transport that
  ends each attempt by its `attempt_timeout` itself and classifies that timeout by its phase declares
  `TransportCapabilities(bounds_attempts=True)`, as `HttpxTransport` does, and the executor then sets no
  deadline of its own around the attempt. Any other transport is cut by the executor, which cannot tell
  how far the attempt got, so a send it cuts may have run, as before.
- **Breaking: a quota refusal slows every request to the portal.** A `QUERY_LIMIT_EXCEEDED`, a REST 3.0
  rate limit or a 429 without a `Retry-After` (the portal sends none: it answers a burst with HTTP 503
  `QUERY_LIMIT_EXCEEDED`) only delayed the refused request's own retry: every other request of the client
  went on at full rate and was refused in turn. Such a refusal now paces the whole portal: the client's
  requests leave one gap apart, a request waiting for the slot the refused one frees included, and the
  portal is held for that gap. The gap starts at the retry backoff's initial delay (0.5 s by default)
  and grows by its factor, up to its maximum delay (30 s), with each refusal of a request sent after it
  last grew; the refusals of one burst, sent before it, do not grow it. Each answer about anything but
  the load (a success, or a failure with no limit's code, no 429 and no 503) to a request sent after
  that quickens the pace a step, until the client sends at full speed again; time alone does not, so an
  idle client keeps its gap until its next requests are answered. A `Retry-After` or
  `X-Bitrix-RateLimit-Reset` sets the pause instead, as before, and paces nothing.
  `CoordinatorSnapshot.pace_gap` shows the gap.
- **Breaking: `OPERATION_TIME_LIMIT` holds its method as long as the portal says.** It held the method
  for a fixed 120 s, which no setting changed. It now holds it for the answer's hint, else until the
  method's operating time resets, as its latest answer said (`time.operating_reset_at`), else for the
  backoff of that method's refusals in a row. A classic batch command's `result_time` is now decoded
  (`CommandSuccess.response.time` was always `None`) and counts the same. Its `Retry-After` now holds only
  the refused method, where it held every request to the portal too.
- **Breaking: a throttle hint holds the portal only on an answer about its load.** A `Retry-After` on
  any failed answer held every request to the portal, for up to an hour: a 401 `expired_token` with
  `Retry-After: 3600` refused every later call at once. A hint now holds the portal only on a 429, a
  503 or an answer with a load limit's code; on any other failure it is ignored.
- **Breaking: a request held longer than its window ends with the portal's refusal.** A request whose
  retry the portal's hold kept past its window ended with `BudgetExceededError` ("retry delay would
  exceed elapsed budget"), the portal's code only in its `__cause__`. When the hold alone, whatever the
  backoff, outlasts what the window allows (for an `UNSAFE` or `UNKNOWN` request, half of what is left),
  the request now ends with the refusal itself: `ApiResponseError` with `normalized_code`
  `operation_time_limit` or `query_limit_exceeded`, or the 429 or 503 as `HTTPGatewayError`. A batch
  command that followed its envelope's refusal, and a REST 3.0 group, end the same way; a command its
  own entry answered keeps that answer, as before. When the backoff is what does not fit, the request
  still ends with `BudgetExceededError` caused by the refusal. Code that caught `BudgetExceededError`
  for a held request must catch the refusal too.
- **Breaking: a limit refusal declared inside a result is a refusal.** A request whose
  `ResultErrorSpec` finds `QUERY_LIMIT_EXCEEDED` or `OPERATION_TIME_LIMIT` inside a 200 answer
  raised it at once. It is now classified within the attempt, as a refusal of the whole answer is:
  it holds the portal or the method, and the request is retried after the hold, an `UNSAFE` one
  included, since the refusal proves it did not run. So is any other code
  `RetryPolicy.transient_api_codes` lists, a code you added included: found inside a 200 answer, it is
  retried as the same refusal of the whole answer would be.
- **Breaking: a held command goes in a later envelope.** The first send of a `batch()` or
  `batch_outcomes()` chunk left out a command whose method was held longer than the retry window when
  another command could go, and that command failed unsent with `BudgetExceededError` ("coordinator
  hold exceeds execution time budget"): after one `OPERATION_TIME_LIMIT` on `crm.deal.update`, a chunk
  of 49 updates and one get sent the get and failed the updates. The command now goes in a later
  envelope of its chunk, once its hold ends; only a command whose hold outlasts what the operation has
  left (`max_elapsed`, after the portal's own hold) is refused unsent. That later envelope goes after
  the chunk's other envelopes, so a held update sent before its comment now goes after it; outcomes
  still come back in input order. A chunk is still sent whole when every command can go at once, or
  none can.
- **Breaking: retries take turns with first sends.** A retry, whose window runs while it queues,
  got one permit in six from the client's shared limiter, behind every other class of work.
  Retries and first sends now alternate: a retry waits behind the retries queued before it and at most
  one first send per retry granted.
- **New: a cancelled batch stream tells in flight from never sent.** A `batch_outcomes()` or `batch()`
  stream ended before it delivered every outcome (cancelled mid-envelope, say) reported `unknown=0`,
  though up to an envelope of commands may have run. `OperationReport.undelivered_possibly_run` counts
  the admitted commands never delivered that a send may have run (in flight, or sent without an answer),
  and `undelivered_not_sent` those never sent; the rest of `admitted - emitted` were answered.
- **Breaking: `cooldown_seconds` counts the time the portal was held.** Concurrent refusals each added their
  whole hint: ten 429s at once with a 0.2-second hint reported 2 s of cooldown for 0.2 s of pause. Each
  now adds only how far it extended the hold. `RateCoordinator.observe_throttle` returns that extension,
  not the hold left: a hint the current hold already covers returns `0.0`.
- **The backoff saturates.** A large `RetryPolicy.backoff` or many attempts raised `OverflowError`
  computing the delay (`backoff=1e200` after three sends, `backoff=2.0` after 1,025). The delay now
  saturates at `maximum_delay`.
- **Breaking: the rate coordinator's limit API.** `RateCoordinator(operation_time_limit_delay=...)`,
  `observe_operation_time_limit()` and `observe_api_throttle()` are removed. A method is held with
  `hold_method(method, seconds)` and its reset recorded with `observe_reset(method, seconds)`;
  `RateCoordinator` takes `sleep=` to wait out a hold on its `clock`'s time.

## Stream lifecycle, cancellation and closing

- **Breaking: a read ended by a close from another task raises `StreamClosedError`.** A stream or client
  closed from another task while a read was in flight made that read raise `CancelledError`, although nobody
  cancelled the reader's task: in a `TaskGroup` the code after the loop was skipped silently, and a parent
  awaiting the reader took it for its own cancellation. The read now raises `b24api.errors.StreamClosedError`
  with the stream's report as `.report` (or the close's failure, when the close failed). A reader whose own
  task is cancelled still gets its `CancelledError`, unless an interrupt ends the stream within the one second
  its read waits for the close: then it gets the interrupt, which the close raises too. A read that starts
  after the stream ended still raises `StopAsyncIteration`.
- **Breaking: a cancellation is deferred for one second, not for the rest of `max_elapsed`.** A cancelled
  read, `aclose()` or `async with` exit waited for cleanup that ignored the cancellation (a synchronous
  source blocked in `next()`, a source close that waits) up to the operation's time left: by default up to
  15 minutes of `task.cancel()` or Ctrl-C doing nothing, and `client.aclose()` had no bound at all. Each now
  waits at most one second, then raises the cancellation; an `async with` exit
  whose body the cancellation ended (Ctrl-C while the body processes an item) is a cancelled exit, and the
  exits one cancellation unwinds (the stream's, then the client's) share that one second, as does a second
  cancellation that arrives within it. One an exit replays behind another exception (the body's own, or a
  failed read or close) is a new delivery: each exit it then reaches waits at most one second more. Ctrl-C under
  `asyncio.run` ends with `KeyboardInterrupt`, an `asyncio.timeout` with `TimeoutError`. Cleanup still
  running goes on in the background, the report records it as a cleanup failure behind the cancellation
  ("stream cleanup also failed (CleanupOverrunError)"), and a later `client.aclose()` waits for it. A close
  nobody cancelled still waits for its cleanup as before, including an exit whose body ended with a
  `CancelledError` its task was never asked for (one an awaited task that was cancelled raised).
- **Breaking: the operation's page budget ends the operation.** Once `max_pages` refused a page, every binding read
  after it got its own `BudgetExceededError` and the source was drained to its end, with its side effects;
  the operation ended `completed_with_failures`. It now ends as when `max_requests` refuses a send: the
  source is not read further, the report is `incomplete` with a `budget_exhausted` violation, and the
  stream raises the budget's error. This holds for keyset traversal too, whose waves reserve their pages at
  once: a wave the pages already read leave no room for is that refusal. A wave that fits only once pages in
  flight are released waits for them, where it failed at once. `max_pages_per_reference` still refuses only
  its reference.
- **`collect()` and `first()` keep what they read before a failure.** The items a helper had read were lost
  with its return value when the stream then failed or its close failed, though the report counted them
  in `emitted`. The raised error now carries them as `partial_values` (a tuple).
- **Breaking: a close from another task while a failed read cleans up returns quietly.** `aclose()` (or the client's
  close) raised the read's failure (`BatchFailed`, `ReferenceFailed`, a `TransportError`) and the report was
  `early_closed` with a cleanup failure and without the read's failure, in a shape that differed by family.
  The failure is now the read's alone: the report is `failed` with it as the terminal reason, and the close
  returns. A source's failure is the same: a read that waited for work when the source failed raises it.
- **Breaking: a close no longer raises a source failure no read reached.** When the source had failed in the
  read-ahead of `fan_out()`, `fan_out_outcomes()` or a reference stream after the items the reader took,
  `aclose()` (or the `async with` exit after `break`) raised it as a cleanup failure, and the report was
  `early_closed` with a `cleanup_failure` violation. The close now ends the stream before that failure, as
  before a binding's failure the reader did not reach: it returns, and the report is `early_closed` with no
  violation. A failure the source raised while being closed is still the close's.
- **`async with client` keeps a cancellation that arrives while the body's exception closes it.** The body's
  exception left and the cancellation was lost, so under `asyncio.run` the first Ctrl-C did nothing. The
  body's exception still leaves, and the cancellation lands at the next `await`, as for a stream's
  `async with`. **Breaking:** a close that fails behind the body's exception is no longer set as its
  `__cause__`, which replaced the body's own cause; it is noted on it, "client close also failed (X)".
- **A cancellation re-armed after a failed body no longer leaks on Python 3.12.** After `async with stream`
  whose body raised, a timeout that fired during the close made the next unrelated `await` after a handled
  `asyncio.timeout`, `wait_for` or `TaskGroup` raise `CancelledError` on Python 3.12 (not on 3.13). It now
  ends within the scope that owns it on both. Python 3.12 cannot withdraw a cancellation once it is armed
  (`Task.uncancel` does not disarm it), so there the replay is decided where the task next waits: a task
  that catches the body's exception and returns without another `await` ends with what it returned, not
  cancelled. On Python 3.13 and later the replay is armed at once, and such a task ends cancelled.
- **A read coroutine closed while it waits ends the stream.** A `__anext__()` coroutine closed during its
  wait (by hand, or by the garbage collector for an unreferenced task) left the stream in cleanup forever,
  and `stream.aclose()` and `client.aclose()` then hung. The stream now ends early-closed and closes as any
  other.
- **A started stream the application drops stays the client's.** It fell out of the client's weak set:
  its tasks were destroyed by the garbage collector ("Task was destroyed but it is pending"), it got no
  report, and `client.aclose()` did not know of it. The client now keeps a started stream until it ends and
  closes it with itself.
- **A close requested from a stream's own work is refused wherever it comes from.** A close of the stream
  or its client from a callback during the handoff of an item to the reader, or from a synchronous
  source's worker thread, was not refused and could wait for itself. Both are now refused as from the
  stream's other work. `client.aclose()` decides once whether its caller is such work, then runs outside
  it, so a stream it closes no longer refuses it later.
- **A cancellation nobody requested ends what it reached.** A transport or caller code that raised
  `CancelledError` while its task was not cancelled (a shared future it awaited was cancelled) hung the
  operation forever. `call()` now raises a `TransportError`, and in a stream the command or binding it
  reached gets an outcome and the stream ends.
- **Breaking: a client serves the one event loop it was first used in.** Used from a second loop (two `asyncio.run`
  calls in turn), a client could hang after a server pause, its wake task left on the closed loop. It now
  raises `RuntimeError` at once: a call, a stream (on its first read, before it reads its source or sends) or
  a close, which leaves the client open. Create a client per event loop, and close it there.
- **Breaking: an early close always gives its reason.** A `fan_out()`, `fan_out_outcomes()` or `iter_list()` stream
  closed before its first read, or a close whose cleanup a cancellation cut at the grace, reported the
  `terminal_reason` `"early_closed"`, the state's name, because its source recorded no end. Every early
  close now reports `"stream closed before exhaustion"`, as a logical batch's did.

## Offset traversal ends at the page without `next`

- **Breaking: `iter_list()` ends at the page without `next`.** It followed `next` and then asked for
  one more page at `start=<rows read>`, expecting it empty. `crm.lead.list`, `crm.deal.list`,
  `crm.activity.list` and `tasks.task.list` round `start` down to a multiple of 50 and answer that
  request with their last page again, and `department.get` ignores `start`: the traversal yielded
  every row and then raised `IncompleteTraversalError` (repeated page), on any row count that is not
  a multiple of 50. Every Bitrix list page with more rows carries `next` and the last one carries
  none, so that page now ends the traversal and nothing is requested past it: one request fewer, the
  terminal reason "page without continuation ended the source", and a reference binding's
  `ReferenceComplete.closure` is the new `BindingClosure.NO_CONTINUATION` where it was `SOURCE_EMPTY`.
  Sequential reference bindings end by the same rule.
- **Breaking: `OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT` is removed; `SERVER_NEXT` is the
  default.** Its fallback, "no `next`, so advance by the rows read", is what sent the request past the
  end. An endpoint that honors `start` exactly and sends no `next` declares
  `OffsetContinuation.OBSERVED_COUNT`, which still ends at an empty page. `SERVER_NEXT` no longer
  requires `TotalTermination.EXACT_QUALIFIED`. For hand-built plans, `OffsetTerminalRule` gains
  `NO_CONTINUATION`, the default end of an `OffsetSequentialPlan`, and a server-next plan must close
  with it and not with `EMPTY_PAGE`.
- **Breaking: rows that do not reach the reported total never complete.** A traversal under
  `SERVER_NEXT` that started at the first page compares the `total` of its last page (or of the latest
  page that had one) with the rows it read; when they differ it raises `IncompleteTraversalError`
  caused by a `PaginationError` whose `rejection_code` is `total_drift`, and the report is
  `incomplete`. It completed before, with 50 rows of 120 when the source ended early. An endpoint
  whose `total` is not an exact count declares `ConsistencyPolicy(total_semantics=TotalSemantics.ADVISORY)`
  and gets the `advisory_total_mismatch` warning instead. Under `OBSERVED_COUNT` and `FIXED_STEP`
  the `total` is still read only when the caller qualifies it.
- **Breaking: `iter_list_counted()` fails as `iter_list()` does.** It wrapped every failure after the
  first request in `IncompleteTraversalError` and reported `incomplete`. Now only a broken pagination
  invariant (`PaginationError`) is wrapped; an API error, a missing `total` (`CapabilityError`), a
  result of the wrong shape (`ResultShapeError`), a spent budget (`BudgetExceededError`), a closed
  client (`ClientClosedError`) or a lost batch (`AmbiguousExecutionError`) is raised as it is, and the report says `failed`
  (`incomplete` for a budget), as `iter_list()` and the fast keyset already did. Code that caught
  `IncompleteTraversalError` for every counted failure catches the specific error or `B24ApiError`.
  A counted page, the first one included, that contradicts its planned range (its length, its `total`, its `next`), a page that
  reaches the total while `next` remains, and a tail that ends short of the total now raise
  `PaginationError` (`range_contradiction` or `total_drift`) where they raised `CapabilityError`.
- **Breaking: a counted head without `next` short of its total fails with `total_drift`.** It failed
  with `CapabilityError` ("requires a positive in-band stride"), and a sequential counted reference
  path advanced by the rows read. A head that holds the whole range (`total` equal to its rows, no
  `next`) now completes under `SERVER_NEXT`, now the default, where an explicit `SERVER_NEXT` refused it.
- **A counted range the budget cannot hold is refused at its traced head.** The page, request and
  row-buffer checks made from the head's exact total raise `BudgetExceededError` (they raised
  `CapabilityError`), the in-memory `max_identity_keys` ceiling is checked there too (it failed only
  at the page that crossed it, after yielding the rows before), and the head is traced as a rejected
  page with `budget_exhausted` (it was not traced).
- **Breaking: page rejection codes say what refused the page.** `PageRejectionCode` gains
  `BUDGET_EXHAUSTED`, `CAPABILITY_CONTRACT`, `IDENTITY_STORE` (the caller's `IdentityStore` failed or
  answered with a non-boolean) and `INTERNAL_FAILURE`. Each was `RANGE_CONTRADICTION`, which now
  means only a pagination invariant without a narrower code. `NOT_EXECUTED`, never produced, is
  removed.
- **Breaking: refused-only members are removed.** `ConfirmationPolicy.BOUNDARY_ID_SEEN`,
  `ConfirmationPolicy.INDEPENDENT_ORACLE` and `KeysetTerminalRule.BOUNDARY_ID_SEEN` were refused on
  every traversal; they no longer exist.
- **`EMPTY_AFTER_BOUNDARY` under `SERVER_NEXT` is refused before any request.** It asks for an empty
  page past the end, which a server-next traversal never requests; the combination was accepted and
  failed at the end. It raises `CapabilityError` without a request now.
- **An exact-total `iter_list()` over an empty source completes.** A first page with no rows, no
  `next` and no usable `total` (as `user.get` answers a filter that matches nothing) completes as an
  observed empty source under `TotalTermination.EXACT_QUALIFIED`, as it already did in
  `iter_list_counted()`; it raised a bare `CapabilityError`.
- **Breaking: `CursorSpec(take="first")` requires a reversed page order.** On a page in cursor order
  the first row is the one nearest the previous boundary, so the next request read the rest of the
  page again and the traversal failed with "identity order did not advance". `take="first"` with
  `page_order=None` or equal to `direction` now raises `ValueError`.
- **An empty page carrying `next` is refused only where `next` is followed.** A cursor, continuation
  or window traversal, an observed count and a fixed step ignore the envelope `next`, so such a page
  now ends them normally; under `SERVER_NEXT` it is still refused (`range_contradiction`).
- **A selector that finds nothing raises `ResultShapeError`.** `ResultSelector(("items",))` over
  `{"result": []}` raised `CapabilityError`; it now raises `ResultShapeError` naming the selector, the
  expected shape and the observed JSON type, and the page is traced as `shape_contract`.
- **A continuation control repeated in another spelling is a cycle.** `1`, `"1"` and `True` reach the
  server as the same value, so a rule that alternates between them now fails with
  `ContinuationError(CONTINUATION_CYCLE)` at the repetition instead of running until a budget ends it.

## What a classic batch command can carry

- **Breaking: a classic batch command holding `None` is refused before any request.** A direct JSON
  call sends `None` as `null`, which Bitrix reads as an empty value (`{"filter": {"COMPANY_ID": None}}`
  selects the deals without a company). The query string of a classic batch command dropped every
  `None` with no error, so the same filter selected every deal and `{"fields": {"TITLE": None}}`
  updated nothing. Every path that sends classic batch commands now refuses such a command, as it
  already refused an empty array or object: `batch_outcomes()`, `fan_out_outcomes()` and batched
  references fail that command or binding alone with `CapabilityError`, `batch()` raises
  `BatchFailed`, and `iter_list_counted()`, explicit fast keyset execution and
  `verify_keyset_capability()` raise `CapabilityError` before their first request. The message names
  the value and points to direct dispatch. Send such a command with `client.call()`, `DirectDispatch`
  or REST 3.0.
- **Breaking: text holding a batch reference pattern is refused in a classic batch.** The batch
  handler replaces `$result[...]` (and `$result_error[`, `$result_total[`, `$result_next[`,
  `$result_time[`, in any letter case) inside any value and drops the rest of the value, so a title
  `"probe-$result[a][0][ID]-end"` was stored as `probe-1`. The library has no batch reference
  feature and the pattern cannot be escaped, so such a command is refused as `None` is. A direct call
  sends the text as written.
- **Breaking: a key PHP would rename is refused in a classic batch.** An empty key, a key holding `[`,
  `]` or NUL, and a top-level key holding `.` or a space (PHP turns those into `_`) are refused there.
- **Breaking: a `FORM_URLENCODED` request refuses `None` and such keys when it is built**, as it
  already refused an empty container, with "use JSON encoding" in the message.
- **Automatic keyset execution sends its pages directly** when the request cannot travel in a classic
  batch. Its report says `requested_kind=AUTO`, `selected_kind=SEQUENTIAL`,
  `reason=BATCH_UNREPRESENTABLE`. Breaking for an exhaustive match: `KeysetSelectionReason` gains that member.
- `True` and `False` are still sent as `1` and `0` in a classic batch, as PHP's `http_build_query`
  writes them.
- The new read-only `Request.classic_batch_obstacle` names what keeps a request out of a classic
  batch, or is `None`.

## Request values are checked when they are built

- **Breaking: a value no encoding can send is refused when the `Request` is built.** A string with a
  lone surrogate (from `os.fsdecode()` of a non-UTF-8 file name, for example) or an integer with more
  decimal digits than the interpreter allows in `str()` was accepted and failed later, inside the send,
  as a transport failure. `Request`, `ParameterUpdate`, a positional `Present` value and
  `Binding.start_cursor` now raise `ValueError` at construction, before any request.
- **Breaking: a bare string where a sequence (of path parts or codes) is expected raises `TypeError`.**
  `ParameterPath("OWNER")` was read as the path `("O", "W", "N", "E", "R")`. `ParameterPath`,
  `ResultSelector`, `IdentitySpec.item_path`, `IdentityComponent.item_path`, `CompositeIdentitySpec.components`, the `ResultErrorSpec`
  paths, `CursorSpec.item_path`, `InclusiveWindowTraversal.timestamp_path`, positional control paths
  (declared or written), and
  `RetryPolicy.transient_api_codes` and `AmbiguityPolicy.refusal_api_codes` now refuse `str` and
  `bytes`. Pass a tuple or list: `ParameterPath(("OWNER",))`.
- **`FrozenMapping` freezes what it holds** when it is built directly, as it already did inside a
  `Request`: nested lists become tuples and nested mappings become `FrozenMapping`, so a later change
  to the caller's value no longer reaches it and it can be hashed. `Binding.start_cursor` is frozen the
  same way.

## Settings and the CLI

- **Breaking: `Settings` no longer reads a `.env` file.** It read `.env` from the current directory,
  so a library call could pick up a credential from whatever directory the process ran in. It now
  reads only the process environment. The `b24api` CLI still reads `.env` from its working directory
  (the environment wins). A program that relied on `.env` should load it itself, for example with
  `python-dotenv`, or export the variables.
- **Breaking: `Settings` cannot be pickled.** A pickled copy held `[REDACTED]` in place of the webhook
  and failed far from the cause, in `multiprocessing` for example. `pickle.dumps(settings)` now raises
  `TypeError`; build `Settings` in each process.
- **`b24api list --strategy counted` works.** It refused every run, because the contract it built had
  no exact total.
- **CLI exit codes at the process boundary.** A run started with stdout or stderr already closed exits
  `5`, as a reader gone during the run does. `SIGTERM` and `SIGHUP` end a run as Ctrl-C does, closing
  a `list` stream and writing its report, and exit `128 + signal` (`143`, `129`). Local input that is
  not UTF-8 or not valid JSON (a number with too many digits, nesting too deep) exits `2`, as other
  usage errors do. Output that a stream cannot encode is written as JSON with `\u` escapes.
- **`--contract -` reads the contract from stdin**, as the help said. `--contract -` with
  `--params -` is a usage error.
- **`cli.main()` writes help and usage errors to the streams it is given.** `main(stdout=..., stderr=...)`
  wrote `--help` to `sys.stdout` and a usage error to `sys.stderr` whatever streams it was passed; both
  now reach the given streams, and a usage error never falls back to stdout.

## Test helpers and logs

- **`ScriptedTransport` serves what an HTTP transport serves and names a call nothing matches.** It
  now accepts form-encoded requests and scoped headers, as `HttpxTransport` does. A call no exchange
  matches still fails its send (the client reports a transport failure, which a stream may absorb),
  and is now also kept in `unexpected`; `assert_exhausted()` fails on it even when every exchange was
  used. A fixture that passed only because such a failure was absorbed now fails: script that call.
  Matching still compares parameters as JSON values, whatever their key order; `calls` keeps each
  request's keys in the order sent, for a test that checks the sort priority of `order`.
- **httpcore's DEBUG records are scrubbed.** With the `httpcore` loggers at `DEBUG`, a redirect's
  `Location` header carried the webhook token into the log. The URL filter now covers every
  `httpcore` logger as it covers `httpx`.

## One verdict per send

What the failure that ended a send proves about whether its request ran is now one value, read by one rule
from the whole checked answer, for `call()`, direct and batch fan-out, references, classic and REST 3.0 batch
alike (see the execution facts in `docs/architecture.md`).

- **Breaking: an ambiguous status (408, 5xx) proves nothing, structured or not.** A structured error with a
  status the ambiguity policy holds ambiguous (408 or any 5xx by default), such as `INTERNAL_SERVER_ERROR`
  with 500 or the REST 3.0 `BITRIX_REST_V3_EXCEPTION_LIMITER_QUERYTIMELIMITEXCEPTION` with 503, was the
  request's conclusive answer: `call()` raised the `ApiResponseError` and the outcome was a `CommandFailure`.
  It now leaves the request possibly run, as an unstructured one does: `call()` of `UNSAFE` or `UNKNOWN` work raises
  `AmbiguousExecutionError` whose cause is the `ApiResponseError`, and the outcome is `CommandOutcomeUnknown`.
  A `SAFE` request still raises the `ApiResponseError`, and is retried only when its code is transient. A
  listed refusal code (`QUERY_LIMIT_EXCEEDED`, which the portal sends with 503, or `OPERATION_TIME_LIMIT`)
  still proves the request did not run, and it is still sent again whatever its safety. A classic batch
  envelope answered so was a `CommandFailure` for each of its commands; each is now unknown and is not
  replayed unless `SAFE` and transient, so a write that ran before the server failed is never reported failed.
- **Breaking: `call()` of a write whose answer cannot be read raises `AmbiguousExecutionError`.** A 2xx answer
  that cannot be decoded (truncated JSON, `{}`, a `total` or `next` that is not an integer) already made the
  outcome of a batch or fan-out command unknown, but `call()` raised the `EnvelopeContractError` or
  `ProtocolError` itself. For `UNSAFE` and `UNKNOWN` work it now raises `AmbiguousExecutionError` with that
  error as its cause, as for a lost answer. So does a malformed error the request's own `ResultErrorSpec`
  finds. `SAFE` work still raises the decoder's error.
- **Breaking: a REST 3.0 envelope refused for its credentials fails its commands without isolation.** A 401 on
  a REST 3.0 batch envelope (`expired_token`, `invalid_token`) was read as an abort that may have run: `SAFE`
  commands were each sent alone and `UNSAFE` ones were unknown, so a thousand reads cost a thousand refused
  requests. No command can pass credentials the envelope failed, so a 401 now answers every command, as on a
  classic envelope: each is a `CommandFailure` with that error, never sent alone. Any other structured error on
  a REST 3.0 envelope, a 403 or 404 included, may come from the one command that aborted it after earlier ones
  ran, so it still proves nothing about them, as a refusal code of the REST 3.0 load limiter does not. Nor does
  an error body on a REST 3.0 envelope that cannot be read or contradicts itself, at any status but a 401: only
  a gateway's page (not JSON, or JSON without a top-level `error` and without a repeated top-level member) at a
  listed refusal status (423, 425, 429) still proves the envelope was refused.
- **An answer that contradicts itself proves nothing.** A body that is both a `result` and an `error` (for
  example `{"result": 42, "error": "QUERY_LIMIT_EXCEEDED"}`) was read as the error, so a quota code there
  sent an `UNSAFE` creation again. It is now a `ProtocolError` that proves nothing: the request may have run.
  Outside 2xx the status decides, as for any error body the client cannot read, but such a body never proves
  a refusal: a 429 that also carries a result is the request's answer, not a gateway's refusal.
- **A repeated control member makes the answer unreadable.** A member name repeated where the answer is
  decided kept only its last value, so a repeated command key in `result_error` could turn a server error into
  a quota refusal and replay a write. Now:
  - a body that repeats a top-level member or a member of a REST 3.0 `error` object is malformed
    (`EnvelopeContractError` for a 2xx answer, `ProtocolError` at any other status). At 2xx or an ambiguous
    status (408, 5xx) the request may have run; at any other status the status answers it, and at a refusal
    status (423, 425, 429) it proves no refusal, with or without an `error` member: a gateway's page repeats
    none of its top-level members, so the body is taken for the server's own answer;
  - a classic batch envelope that repeats one of its members or a command key leaves each of its commands
    unknown, caused by a `ProtocolError`;
  - a command's error entry, or the error a request's `ResultErrorSpec` finds in its result, that repeats a
    member leaves that command unknown; its neighbours keep their answers. So does a repeat on the way to that
    error, its code or its description (`{"error": {"code": "ERR", "code": ""}}`), which would otherwise hide
    the error or change it.

  A malformed classic envelope is not transient, so it is not sent again, `SAFE` or not; `SAFE` commands of a
  REST 3.0 envelope that cannot be read are still sent alone, as after any abort. A name repeated in a result's
  own data still keeps its last value, a direct result whose members happen to be named `result` or
  `result_error` included: the keyset paths no longer have a stricter mode of their own, and
  `Executor.execute()` no longer takes the `strict_json_members` keyword that selected it.
- **A malformed classic error entry is unknown.** A `result_error` entry that is not an object with a scalar
  `error` code (`null`, `[]`, `{}`, `{"error": []}`) was a `CommandFailure` with a `BatchCommandError` of code
  `malformed_batch_error`, as if the server had answered it. It is now `CommandOutcomeUnknown` caused by a
  `ProtocolError`, and under fail-fast it no longer proves that the server halted the commands after it.
- **A body nested more than 256 levels below a top-level member is a malformed answer, at any status.** It
  escaped as a bare `RecursionError` where the parser could not follow it (`call()` raised it, a tolerant
  `batch_outcomes()` ended with it, and a fan-out command was a `CommandFailure` that said it was never sent),
  and was decoded where it could, so the same bytes meant different things on different interpreters. The
  decoder now refuses it at a fixed depth, the 256 levels a member such as `result` holds once frozen, and it
  is read like any other unreadable body: at 2xx, 408 or 5xx the request may have run (unknown,
  `AmbiguousExecutionError` from `call()` of a write); at a listed refusal status (423, 425, 429) it is a
  gateway's page that proves a refusal; at any other status the status answers it. A failure an outcome
  carries in a `CapabilityError` (a caller's exception) now keeps what was recorded about whether its command
  ran.
- **A leading byte order mark is ignored.** A body that began with a UTF-8 byte order mark was malformed, so a
  sent write was unknown; the mark is now skipped, as RFC 8259 allows, and the answer is read.
- **Unreadable server timing no longer drops the answer.** A `time` block that could not be read (a negative
  `duration` after a server clock step, numbers as strings) failed the whole response with `ProtocolError`. The
  timing is diagnostic: `Response.time` is now `None` and the result is returned.
- **A request its transport could not build is not a physical request.** A `TransportError` of phase
  `not_dispatched` that is not retryable (a request HTTPX could not build, a lone surrogate in a parameter for
  example) was counted in `physical_requests` and recorded as answered, so its settlement was a failure. It is
  now a stop before the send, as a closed client's `ClientClosedError` is: not counted, settled not executed. A
  retryable failure of that phase (a refused connection) is still a send that is retried.
- **A spent retry budget names its request and cause.** The `BudgetExceededError` that ends a retried request
  ("per-request attempt budget exhausted", for example) now carries the request's summary, and the CLI's error
  output adds the failure it followed as `cause` (its type, code and HTTP status).
- **A reference binding with an unknown page stays unknown.** A later page of the same binding that failed
  conclusively no longer clears the mark, so the binding closes `UNKNOWN`, not `FAILURE`.

## The retry path

- **A REST 3.0 isolation waits and is admitted as a retry is, and sends one command at a time.** When
  a REST 3.0 envelope of `SAFE` commands failed after it may have run and its one envelope retry was
  used (or its failure is not retried), its commands were sent alone all at once, without the retry's
  backoff or the host's throttle hint, and beyond the `BatchDispatch` concurrency (bounded only by the
  client limit). The isolation is now a replay of the envelope's commands, planned as their envelope
  retry is: it waits the longer of the backoff and the host's hint, within the attempt budget, the
  retry window and the deadline (the report's `retries` counts that wait, as it counts the envelope
  retry's), and then sends the commands one at a time, so a `BatchDispatch` never has more requests in
  flight than its `concurrency`. An isolation refused because the operation's
  deadline has passed now fails with "execution time budget exhausted before the retry", as any retry
  does, and still ends the stream with `BudgetExceededError`. One refused for its own request alone (no
  attempt left, its retry window closed, or a wait longer than the window left) sends nothing and records
  nothing. Either refusal, and one of the envelope retry, now ends each command as a refused retry of a
  classic batch does: unknown, with the refusal (`BudgetExceededError`) caused by the envelope's failure,
  where it kept the envelope's failure alone; a group held longer than its window after the portal's
  answer about its load keeps that answer, as a lone request does (see "Load limits, holds and attempt timeouts"). An envelope retry the budget refused is no longer followed
  by an isolation, which the same budget would refuse.
- **Breaking: a retry stopped before its send ends with that stop, caused by what it followed.** When
  a request or a batch was sent again after a failure and that retry was stopped before its send by
  something other than the budget (the client closed meanwhile, for example), the stop was reported
  without the failure it followed, or not at all. A lone request raised the bare `ClientClosedError`. A
  classic command proven not run (its envelope refused whole) ended with the bare `ClientClosedError`,
  and one whose send may have run ended unknown (`AmbiguousExecutionError`) caused by that send's
  failure, the stop dropped; a REST 3.0 command, after its envelope retry or isolated send was stopped,
  ended unknown caused by the envelope's failure, the stop dropped. Every retry stopped before its send
  now ends as a budget stop already ended it: with the stop, which is what ended it, caused by the
  failure it followed, and reported possibly run only when a send it was in may have run (a batch
  command then ends `AmbiguousExecutionError` whose cause is that stop; a lone request raises the stop,
  its execution facts saying so). Its ambiguity reason and evidence name the latest send that may have
  run, whatever stopped the retry: after a send answered `502` or cut by the timeout, a retry a closed
  client or transport stopped reported `connection_lost_after_dispatch` without evidence, as if the stop
  were that send. A send its transport refused because it is closed (a closed `HttpxTransport`
  or injected HTTPX client, or any transport raising `ClientClosedError`) is such a stop too: it ended a
  lone request with the bare `ClientClosedError` and a batch command unknown caused by that refusal, the
  failure it followed dropped, and was counted in `physical_requests` and `batch_isolation_requests`
  though nothing was sent. It now ends as a stop at the permit does, and is counted in neither. The same
  holds now for a REST 3.0 envelope retry after an abort or a
  lost answer that the budget stops at its permit, which kept the envelope's failure. A command its own
  entry or element answered keeps that answer whatever stops its replay: a classic command its own
  entry answered with a transient error (`QUERY_LIMIT_EXCEEDED`, for example), whose replay such a
  non-budget stop kept from being sent, ended with that stop (`ClientClosedError`) and now keeps its
  `BatchCommandError`, as it already did when the budget stopped its replay and as a REST 3.0 command
  its element answered did. The commands of a classic round a stop ended no longer share one error
  object: each gets its own copy of the stop, with the stop's traceback and the failure that command
  followed as its cause.
- **A stale `Date` no longer stretches a dated throttle hint.** A `Retry-After` given as an HTTP date,
  or an `X-Bitrix-RateLimit-Reset` given as a date or an epoch, was measured against the response's
  `Date` whatever that `Date` said, so an answer whose `Date` was an hour old (from a cache or a relay,
  or a server clock set back) held every request to the portal for the hour cap. The `Date` is now used
  only when it is at most a minute behind the local clock; otherwise the local clock measures the hint,
  as it does without a `Date`. A local clock running ahead of the server by less than a minute still
  cannot shorten a hint; one further ahead now can, which costs at worst an early retry that the server
  throttles again. A delta in seconds is unchanged, and every hint is still capped at an hour.

## Traversals that end with the budget, and dropped results

- **Breaking: a keyset traversal the budget ends reports the budget, not a failed command or a protocol
  violation.**
  When `max_requests` refused a wave or the final confirmation of `iter_list_keyset()` under
  `RangeKeysetExecution`, `PartitionedKeysetExecution` or an automatic choice (sequential or
  partitioned), the report recorded a `command_failure` for each command the budget refused, or, beside
  `budget_exhausted`, the protocol violations `completion_rejection_before_settlement` and
  `completion_invalid_binding_terminal`: explicit range and partitioned runs recorded them as automatic
  ones did, under `max_requests` and under `max_elapsed`. Such a run now ends as an explicit
  `SequentialKeysetExecution` ends on its budget: the report is `incomplete`, with
  `BudgetExceededError` as its terminal reason and `budget_exhausted` as its only violation.
  `iter_list_keyset()` still raises `IncompleteTraversalError` whose cause is the `BudgetExceededError`.
  Keyset bindings of the reference streams
  (`iter_references()`, `iter_reference_outcomes()` and the reference source streams) under the same
  strategies changed the same way, under `max_requests` and under `max_elapsed`: a wave or final
  confirmation the budget refused no longer records a `command_failure`, so the report holds
  `budget_exhausted` and a `reference_failure` for each binding the budget ended. Once the budget has
  refused work, every later refusal by it is that same end, whatever work it names (an admission, a
  page's rows, a send's permit); a command refused by its own limits (its attempts, its retry window)
  still fails as a command. A fast keyset wave whose batch answer was ambiguous (a command's answer
  missing or repeated) no longer records `completion_unknown_binding_claimed_known`: its binding
  closes as unknown.
- **A page the budget kept from its send is not traced.** A page of a counted traversal's batch tail
  (`iter_list_counted()` and counted reference bindings), and a fast keyset wave or final confirmation,
  that the budget kept from its send was traced in `page_trace` as `REJECTED` with `COMMAND_FAILURE`,
  though no request carried it. A page that never executed because it was never sent (traversals never
  halt a batch) now leaves no trace record on every path, as a direct or batch fan-out page the budget
  refused already did. The binding's outcome, its settlement and the report's violations are unchanged.
- **A range, partitioned or automatic keyset run that ends while a page is unsettled records no protocol
  violation.** When the final confirmation of `iter_list_keyset()` under these strategies failed (an
  error answer, a lost answer), or the caller cancelled the stream while a wave was in flight, the report
  recorded `completion_rejection_before_settlement` and `completion_invalid_binding_terminal` beside the
  failure, and so did an interrupt; the end of the elapsed budget or a transport failure while a wave was
  in flight recorded `completion_unknown_binding_claimed_known`. Such a page now settles from what its own command did: a wave the end interrupts settles each
  page from its command's answer (a success, an answered failure or refusal), as unknown only when the
  command's send may have run without an answer (a cancellation while the wave is on the wire), and as
  not executed when it was never sent; a finish page settles from the facts of its failure. A reference
  binding's page cancelled after its wave was sent was unknown even when its command had been answered;
  it now keeps that answer, a success settling as a success as on a flat traversal, and is unknown only
  while its wave may still send it. So does each page of a batch that failed as a whole with an error
  that is not the client's own (an answer nested too deep to decode, for example): every page of it was
  unknown, also one its command's answer in an earlier round settled; it now settles, and its failed page
  is traced in the report's `page_trace`, from what its own command did: one whose command was answered
  is rejected, not unknown, as `transaction_aborted` when that answer was a success. The binding's outcome
  and its error are unchanged.
- **A counted tail page larger than its share is an answered failure.** When a tail page of
  `iter_list_counted()` decoded to more rows than `max_buffered_rows` left it, the traversal raised
  `IncompleteTraversalError` caused by `BudgetExceededError` ("buffered row budget exhausted"), and the
  pages of that window that did fit were lost: neither yielded nor in the page trace, though they had
  run. The tail window is now fitted as a `batch()` window is: the pages before the oversized one are
  yielded and committed, and that page is rejected with `ResultTooLargeError`, recorded as answered. The
  stream still raises `IncompleteTraversalError`, now caused by that `ResultTooLargeError` (still a
  `BudgetExceededError`), and the report's terminal reason is `IncompleteTraversalError`.
- **`ResultTooLargeError` keeps the answer's evidence.** Its `http_status` and `request_id` were
  `None`, although the command it reports was answered. They are now those of the response whose result
  was dropped.
- **An empty batch result counts its one row.** `batch()` and `batch_outcomes()` already fitted every
  outcome as at least one row of `max_buffered_rows`, but an empty list result was then charged no
  row, so the report's `buffered_rows_high_water` could stay below what the window held. It now counts one row,
  as it was fitted.

## One model for a source's failure and a failed close

- **A failed close is reported the same way in every family.** The stream families that read a
  caller's source described the same failed close differently: `batch()` and `batch_outcomes()`
  recorded "batch cleanup failed (X)" plus an `internal_failure` "stream cleanup failed (X)", while
  `fan_out()`, `fan_out_outcomes()` and the reference families took the error's class name as the
  terminal reason and recorded "reference cleanup also failed (X)" even when the close was the only
  failure, plus an `internal_failure` "fan_out failed (X)". All nine now build one report: when
  nothing failed before the close, the terminal reason is "stream cleanup failed" and the report
  carries one `cleanup_failure` violation "stream cleanup failed (X)", nothing else; behind an
  earlier failure, that failure stays the terminal reason and the close adds a `cleanup_failure`
  "stream cleanup also failed (X)". X is `InputSourceError` for a close that raised (next item) and
  `CleanupOverrunError` for one that overran its window. The earlier failure's violation comes first
  and the close's after it (a family that stops at the first failure listed the close first). `iter_list_keyset()` on its
  fast path reports a failed close the same way: an early close whose cleanup failed has the terminal
  reason "stream cleanup failed" (it was "stream closed before exhaustion"), and a failure it ends with
  no longer carries "stream cleanup also failed (X)" as a note. Code that matched the old strings must
  match these. A library error raised behind an earlier failure no longer carries that line as a note
  (only `batch_outcomes()` added one); read it from the report the error carries (`error.report`). Your
  own exception raised inside `async with stream`, which a failed close does not replace, carries one
  note: the report's `cleanup_failure` violation word for word (it was "stream cleanup also failed (X)",
  with X the raised class, `BatchFailed` for `batch()` and `fan_out()`; it is now "stream cleanup
  failed (InputSourceError)" or "stream cleanup failed (CleanupOverrunError)"). A `KeyboardInterrupt` or
  `SystemExit`, from a close or anywhere else in the operation, is never behind anything: it is what the
  stream raises, whatever ended the stream before it (a failure, the reader's cancellation, a timeout, a
  close from another task), and the report is `failed` with the terminal reason "operation interrupted":
  the violations recorded before it stay (an earlier failure's first) and one `interrupted` violation,
  "operation interrupted (X)" with its class as X, comes last. A close's interrupt is no longer recorded
  as the close's failure.
- **Breaking: a close that raises is the source failing.** A source's close that raised, from its
  `aclose()`, its `close()` or a generator's `finally`, surfaced as the raw exception on `break` from a
  synchronous source and wrapped from an asynchronous one. Every family now raises it as it raises a
  failed pull: `InputSourceError` (`batch()` and `fan_out()`: `BatchFailed`, whose `__cause__` is that
  `InputSourceError`), with the exception the close raised as its `__cause__`. Catch
  `InputSourceError` (or `BatchFailed`) where you caught your own close's exception, and read it from
  `__cause__`. A close that overran its window still raises `CleanupOverrunError`; a cancellation, an
  interrupt or any other `BaseException` that is not an `Exception` is not wrapped.
- **Breaking: anything a batch source raises is a source failure.** A source of `batch()` or
  `batch_outcomes()` that raised a library error of its own (for example a `BudgetExceededError` it
  caught elsewhere and let through) ended the stream as if that error were the operation's; the
  reference families already wrapped it. Every `Exception` a source raises, from a pull or from its
  close, is now `InputSourceError` chained to it in every family.
- **The reference scheduler always closes its source.** When `fan_out()`, `fan_out_outcomes()` or a
  reference traversal ended while its own tasks were still running past the cleanup window, it left
  the caller's source unclosed. The source now gets its close in every case; the overrun is
  recorded as "stream cleanup failed (CleanupOverrunError)", and the error's message names the work that
  overran. With `DirectDispatch` the reference tasks send the commands: "reference task cleanup exceeded
  operation time budget". With `BatchDispatch` the dispatcher's workers send them: "batch dispatcher
  cleanup exceeded operation time budget" (and the first message when only the reference tasks overran,
  the workers not).
- **A close while a read is in flight is an early close like any other.** When another task closed
  a stream while its reader waited, the read raised `CancelledError` and a failed close showed only as
  a report violation. Neither a caller's cancellation, the budget nor a failure ended the stream, so
  the close is an early close: the report is `early_closed`, and when the close failed, both the
  close and the read it ended raise its failure (`InputSourceError` or `BatchFailed`, or
  `CleanupOverrunError`), with the terminal reason "stream cleanup failed". When the close succeeds,
  the read still raises `CancelledError` and the close returns. A read in flight when the client
  closes raises the source's failure the same way.
- **A rejected page chains to the public `PaginationError`.** Fail-fast `iter_references()` and
  `fan_out()` raised `ReferenceFailed` whose `__cause__` was a private `PaginationError` subclass,
  and a budget end could chain to a private window error. A foreign item a `batch()` or
  `batch_outcomes()` source yielded chained `InputSourceError` to a private `TypeError` subclass; it
  is now a plain `TypeError`, as in every other family. Public errors now chain only to public
  errors or to your own exception. `PaginationError` has a `rejection_code` attribute
  (`PageRejectionCode`, `None` unless a page check rejected a page), the code the rejected page's
  `PageRecord` carries.
- **`report.emitted` counts the items the reader got.** A reader cancelled just as a read produced
  its item never got it, but the report counted it as emitted. An item now counts once the reader
  has it.
- **No library task ends in a failure at shutdown.** When the event loop shut down while a stream
  ended (for example during the cleanup grace after the budget's end, or while a read waited in the
  source), `asyncio.run()` reported a library task that ended in the source's failure: a source's pull
  or close task, or a stream's read task. Every task the library starts around your code (a source's
  pull and close, a reference traversal's binding tasks and page fetches, a stream's read and
  cleanup, the client's close) now returns how it ended to its owner, which raises
  the failure; only your own task receives it. That includes a `KeyboardInterrupt` or `SystemExit` your
  transport raises under a reference family, which stopped the event loop from inside a library task:
  it now reaches the read in flight, which raises it with the report. Raised between your reads, it
  leaves the event loop at once, as before, and the stream's end follows: a later read raises
  `StopAsyncIteration` and a close returns, the report attached to the interrupt as `.report`. Under
  `asyncio.run` the interrupt stops the loop before that end begins: the end is the close the shutdown
  runs as it cancels your tasks (an `async with stream` or the client's `async with`), which closes the
  source and reports the interrupt. A read or close cancelled while it runs the end still lets it publish
  ("stream cleanup also failed (CancelledError)"). With nothing to close the stream (you never close it
  nor its client, or `asyncio.run` was already shutting down when the interrupt was raised) it gets no
  end: its source stays open and there is no report.
- **Breaking: only an `Exception` is a failure.** Any other `BaseException` that is neither an interrupt nor
  a cancellation is no longer treated as an interrupt, which asyncio's loop only logged and dropped between
  reads, nor as a failure: no family wraps it, makes it an outcome or ranks it. It propagates as Python
  propagates it: you get it as it is from your read, whether your transport raised it in that read or
  between reads, or your source's pull or close did; raised by your source's close it replaces whatever
  ended the stream, as from a `finally`. It is never lost silently: what no read got, the close raises,
  once; one the library could no longer deliver to a read or a close (raised after the stream ended, by
  work that outlived its cleanup window, by the cleanup of a close or a read whose coroutine was closed while it ran the end,
  or in a stream nothing ever ends, once it is collected) is
  reported to the event loop's exception handler, once. Your source is closed once and nothing hangs. Its
  report and its rank behind other ends are not promised. Code that caught it as the `__cause__` of
  `InputSourceError`, `BatchFailed` or `ReferenceFailed`, or read it from a failure outcome, catches it
  directly.
- **An outcome's failure from outside the library keeps its cause.** In the `iter_*_outcomes()` families,
  a failure that is not the library's error (the exception your `page_stop` callback raises, say) was
  carried as a `CapabilityError` ("reference failed") that dropped it. That `CapabilityError` now has it as
  `__cause__`, as `ReferenceFailed` does in the fail-fast families; a traversal's error
  (`IncompleteTraversalError`) keeps it in its chain.
- **A cancelled stream gives the cancellation's reason.** A stream read to its end and cancelled
  while its source closed reported `cancelled` with the terminal reason "input exhausted" (or
  "reference input exhausted"). Every family now gives "iteration cancelled" whenever the reader's
  cancellation ends the stream.
- **A stream of yours read under a closed read is cancelled.** A stream your code read inside work an
  outer stream's read started (a transport, for example), cancelled because the outer stream was
  closed, reported `early_closed` as if closed itself. It now reports `cancelled`.
- **Breaking: an asynchronous source keeps one context, copied when the stream is created.** Each
  pull of an asynchronous source ran in a fresh copy of the reading task's context, so an async
  generator that set a `ContextVar` before a `yield` could not reset its token after it (`ValueError`,
  and the stream failed with the source). As a synchronous source already did, each source now runs
  every pull and its close in one `contextvars.Context`, copied when the call that creates the stream
  (`batch()`, `fan_out()`, a reference traversal) is made; the reading task drives each step, so
  nothing else about the source's scheduling changes. A context variable set after that call is no
  longer seen by the source: for example, a tracing span or request id set around the `async for`
  loop. Set it before creating the stream.
- **An interrupt through a send keeps no credential.** A `KeyboardInterrupt`, `SystemExit` or
  `GeneratorExit` passing through `HttpxTransport` kept the transport's and HTTPX's frames in its
  traceback, whose locals hold the request with the webhook token in its URL. The transport now
  drops those frames, and the exception's chain, before re-raising it unchanged otherwise.

## The end of `max_elapsed` is not a cleanup failure

- **Cleanup gets a grace past the budget.** Every stream but `iter_list()` waited for its source's
  close only for what was left of `max_elapsed`, which is nothing when the budget's end is what ends
  the operation. So nearly every operation the budget ended also reported a blocking
  `cleanup_failure` ("reference source cleanup exceeded operation time budget"), even for a list or
  a plain generator, and `fan_out_outcomes()`, `iter_reference_outcomes()` and
  `iter_reference_source_outcomes()` raised that cleanup error instead of the budget's. Each cleanup
  step now waits for what is left of the budget, but at least 0.1 s: a close that ends at once is no
  longer a failure, and the report records `budget_exhausted` alone. A close that blocks longer still
  goes on in the background and is recorded as a `cleanup_failure`, but never replaces the error that
  ended the stream. A cancelled close or cleanup likewise defers the cancellation for at least the
  grace.
- **A retry the deadline refuses ends the operation too.** A retry, a batch replay round or a batch's
  isolation phase planned when no time was left was refused as that request alone ("retry delay would
  exceed elapsed budget"), so a tolerant stream completed with failures. When the operation's deadline
  has passed, it is now refused as every other work is ("execution time budget exhausted before the
  retry", recorded), and the stream raises `BudgetExceededError`. A retry whose delay is merely longer
  than the time left is still refused alone, as is one whose own retry window closed first: that request
  fails with `BudgetExceededError`, nothing is recorded, and a tolerant stream goes on and completes with
  failures (`completed_with_failures`, with no `budget_exhausted`).
- **Which window refused a request is in the message.** A request refused because its own retry window
  (`max_retry_elapsed_per_request`) closed before the operation's deadline fails with
  "per-request retry time budget exhausted", wherever it was refused (its permit wait, its dispatch,
  its attempt or its retry); one the operation's deadline refused names the work refused and the
  execution or operation time budget.
- **Tolerant reference streams end with the deadline.** `fan_out_outcomes()` and the tolerant
  reference traversals read their source ahead. When they had read it out before `max_elapsed` ran
  out, the commands or bindings the deadline then refused became failures and the stream completed
  with failures; only the cleanup failure above made it raise. They now end as the documentation
  says and as `max_requests` ends them: they yield those failures, then raise `BudgetExceededError`,
  whose message names the first work the deadline refused, with an `incomplete` report, also when the
  deadline passes while a send waits for its permit or as it is granted. A deadline that passes once
  all the work was admitted, while the consumer is slow, still ends nothing.
- **A deadline while a batch window fills ends the batch with the budget.** When `max_elapsed` ran out
  while `batch()` or `batch_outcomes()` waited for its source to fill a window, the stream failed with
  `InputSourceError` (`source_failure`), and the commands already read became `CommandNotExecuted`
  with `SOURCE_FAILED`. Those commands are now sent as any window is: the budget refuses each, so each
  is a `CommandFailure` with `BudgetExceededError`, nothing reaches the transport, and the stream
  raises `BudgetExceededError` (`batch()`: `BatchFailed`) with an `incomplete` report.
- **A batch stream reads its source's end once.** `batch()` and `batch_outcomes()` pulled their source
  again after it had raised `StopAsyncIteration`, so a deadline that passed while the consumer held the
  last outcome ended a stream whose every command had run with `BudgetExceededError` and an `incomplete`
  report. A source's end is now read once, as the other families read it: such a stream completes.
- **A counted list's tail ends with the budget.** When `max_requests` or `max_elapsed` refused a page
  of the batched tail of `iter_list_counted()`, the report's terminal reason was
  `IncompleteTraversalError`, as for a page that failed. That refusal ends the operation, so the
  terminal reason is now `BudgetExceededError` and the `budget_exhausted` violation reads
  "iter_list_counted failed (BudgetExceededError)", as when the budget stops the head or another
  traversal. The traversal still raises `IncompleteTraversalError` whose `__cause__` is that
  `BudgetExceededError`, with an `incomplete` report. A page stopped for its request alone (its
  attempts, its retry window or a retry delay longer than the time left) is still a failed page: the
  terminal reason stays `IncompleteTraversalError`.
- **A cancellation outranks a close that fails.** A consumer cancelled while its stream was running,
  whose source close then blocked past `max_elapsed`, received `BudgetExceededError` ("... cleanup
  exceeded operation time budget") and a `budget_exhausted` report. One whose source close raised
  received the close's failure, with a `cancelled` report when the cancellation landed between reads and
  a `failed` one when it landed while a read waited. In every family, the fast `iter_list_keyset()`
  included, and whenever the cancellation lands, it now receives its `CancelledError`; the report is
  `cancelled` with the terminal reason "iteration cancelled", and the close's failure is a
  `cleanup_failure` "stream cleanup also failed (X)"; the `CancelledError` carries that failure as its
  `__cause__` and the report as `.report`. An interrupt is the exception (first item). This holds only for a cancellation that ended the stream: a consumer cancelled after the
  budget (or a failure) ended it, while its cleanup waits, receives that error with an `incomplete` (or
  `failed`) report, and the cancellation is delivered at its next `await`.
- **A cleanup overrun has its own public error and is reported as a failed close.** A close that
  overran its window raised `BudgetExceededError` from `aclose()` or the `async with` exit; on a
  stream read to its end, the report also took it for the budget's end (`incomplete`, with a
  `budget_exhausted` violation), though all the work had been admitted. It now raises
  `b24api.errors.CleanupOverrunError`, a subclass, so `except BudgetExceededError` still catches it.
  It refused no work, so the report is the one a close that raised any other error gets, with no
  `budget_exhausted`: after `break` or an early `aclose()`, `early_closed` with the terminal reason
  "stream cleanup failed"; on a stream read to its end, `failed`. Either way its `cleanup_failure`
  violation names `CleanupOverrunError`.
- **A close before the first pull is bounded too.** A stream closed before it was read waited for its
  source's close without bound; it now waits as any cleanup step does.
- **A read the client closes raises a public error.** A read in flight when the stream's client closed
  could raise a private exception class (for example `_BindingSourceError`) when the source had failed
  meanwhile. It now raises the public error the stream raises for that failure otherwise:
  `InputSourceError` (`fan_out()`: `BatchFailed`; a failed reference window: `ReferenceFailed`). A close
  after the source failed in the read-ahead raises nothing (see "Stream lifecycle, cancellation and
  closing").
- **A source failure chains from the source's own exception.** The `__cause__` of the
  `InputSourceError` a failed source raises is now the exception the source raised (`BatchFailed`
  chains to it through that `InputSourceError`); for `fan_out()`, `fan_out_outcomes()` and the
  reference families a private carrier class stood in between.
- **`ExecutionContext.remaining_time()` is gone, and `time_left()` is not a drop-in for it.** The
  execution layer asks for the time left through `ExecutionContext.time_left()`, or waits under it
  with `ExecutionContext.deadline()`. When no time is left, both record the refusal on the context
  before raising `BudgetExceededError`, and a recorded refusal ends the operation: catching the error
  does not undo it, and the stream still ends with `BudgetExceededError` and an `incomplete` report.
  Call them only where running out of time should end the operation. A read that refuses nothing is
  `ExecutionContext.coalescing_time_left()`: the time left, negative once the deadline has passed,
  recording nothing. A cleanup step bounds its wait with `ExecutionContext.cleanup_window()` instead.
- **`ExecutionContext.reserve_attempt()` no longer takes `retry_started`.** It reserves the request
  and attempt budgets only; the time check is the send's own, made just before it with `time_left()`.
  The new `ExecutionContext.release_attempt()` gives back an attempt reserved for a send its transport
  then refused because it is closed: that send is no physical request. Call it only for such a send.
- **A synchronous source is never closed during its pull.** When the operation ended while a pull of
  a synchronous source still ran in its worker thread, its close could start before the pull ended
  (for example once the loop, shutting down, cancelled the tasks left) and failed in the background
  with `RuntimeError: cannot enter context ... is already entered`. The close now waits for the pull,
  then runs in the source's one `contextvars.Context`, as documented.

## Keyset boundary pages are checked at both ends

- **Breaking: a descending boundary page below the ascending one fails the traversal at the boundary.** A
  fast or automatic keyset reads the first page of its selection in each direction first: the ascending
  page starts at the least identity, the descending page at the greatest. The two pages were checked
  against each other only at the top: a descending page whose greatest identity was below the ascending
  page's greatest failed. A descending page holding an identity below the ascending page's least passed,
  so a portal whose two views of the selection disagreed (one view missing rows the other has) could
  complete an ascending traversal without those rows, or fail a descending one only at its last page,
  after delivering them. Such boundary pages now fail the traversal before any row is delivered or any
  further page is sent: `IncompleteTraversalError` with the blocking `direction_contradiction` violation
  ("descending boundary passes head"), or, in a reference traversal, that binding's failure.
- **Breaking: fast and automatic keyset references refuse `BatchDispatch(batch_size=1)` before any
  request.** Their boundary wave is two commands and every wave travels in one physical batch, so such
  a binding never sent a request and failed with an internal `ValueError` ("fast keyset wave must
  contain 1..50 commands"), as a `ReferenceFailed` or a `ReferenceFailure`. The operation now raises
  `CapabilityError` when it is created, as it does for `DirectDispatch`; in a heterogeneous operation the
  source is a correlated local non-execution.

## Auto keyset plans priced on the waves the runtime sends

- **Breaking: `AutoKeysetExecution.max_range_waves` is removed**, and so is the `max_range_waves` key
  of the CLI's auto keyset execution JSON. Auto took range windows only when they needed no more body
  waves than that (2 by default), so a larger selection went to partitioning. Output is strictly
  ordered: the rows of a lane behind the first open one stay in the row buffer until the lanes before
  it close, so behind the default 2,500 buffered rows a partitioned plan reads its lanes almost one at
  a time. 20,000, 50,000 and 100,000 dense rows at page 50 took 227, 790 and 1,790 requests. Auto now
  prices range windows as it prices every plan and takes them whenever they pay off: the same
  selections take 14, 27 and 48 requests (an explicit `RangeKeysetExecution`: 11, 24 and 45).
  Passing `max_range_waves` raises `TypeError`, and the CLI refuses the key as a usage error. The
  selection reason `range_within_wave_budget` keeps its value and now means that Auto took range
  windows before probing as the cheapest plan that pays off.
- **Auto prices a plan by the waves the runtime will send.** The estimate runs the runtime's own
  body-wave rule over lanes of estimated rows: a wave advances the first open lanes in output order,
  as many as `batch_capacity` carries and the row buffer left beside the rows already held can take
  a page of. A plan the buffer reduces to near-sequential traversal is priced as such. A lane whose
  last row sits right below its upper fence closes on that page; otherwise an empty page confirms
  its end. The estimate counts that page at the chance the lane's top value holds no row, its share
  of empty values, so on rows spread at random as densely as the pages Auto measured,
  `selected_requests_estimate` follows the plan's real requests (within 25% across 18 such
  fixtures, 12,000 rows over two, three and ten times as many values). The model steps through every
  wave of the plan, so it adds no error of its own; it runs in a worker thread, off the event loop, and
  takes about 0.3 µs per page the plans it prices read (about 2.5 s for a plan of 353,000
  requests over 10.8 million pages, before the probes and again after them, and a third time when a
  requested advisory total is plausible), which the first row waits for. Rows on a fixed step can put
  a row right below every window's fence, or below none, and then take fewer or more requests than
  estimated. The estimate took every lane group to advance one page per wave and so was optimistic
  behind a small buffer: 20,000 dense rows were estimated at 21 requests and took 227.
- **Anchor probes read a page each and measure the lanes.** A probe read one row, its anchor. It
  now reads a page from its starting point: the first row is the anchor and the rest, discarded,
  measure the density of the lane above it, up to its last row; a page short of the cap measures
  up to the probe's upper fence only under `KeysetPageCompletion.SHORT_PAGE_EXHAUSTS`, since under
  `EMPTY_CONFIRMATION` a server may cap its pages below the requested size. The first probe starts
  at the lower boundary page, in either direction, so the values between that page and the row it
  finds hold nothing, and that lane closes without a request. After the probes Auto prices
  partitioning, range windows and sequential traversal over the measured lanes and takes the
  cheapest plan that pays off (`post_probe_range_preferred` for range windows,
  `post_probe_gain_lost` for sequential traversal). A probe page counts as a logical page as
  before, and the rows it discards count in `probe_rows_discarded`.
- **Auto sizes partition lanes to the row buffer.** It probed `target_lanes` anchors (20 by default)
  whatever `ExecutionPolicy.max_buffered_rows` allowed. It now also prices finer lanes, up to two
  waves of probes (twice the batch capacity) and no more anchors than the buffer retains beside one
  page, and takes them when they are cheaper and their estimate fits what is left of `max_pages` and
  `max_requests`. The report's `anchor_probe_commands` shows how many anchors were probed;
  `target_lanes` still reports the configured value.
- **Breaking: under `TotalHintMode.IGNORE` a total the server sends anyway steers no plan.** Auto priced its
  plans with any consistent boundary total within the boundary pages' bound, asked for or not. It now
  weighs only a total `REQUEST_ADVISORY` asked for; an unasked one is still reported as
  `total_hint_observed`, with `total_hint_plausible` and `total_hint_used` false.
- **Auto refuses fast plans whose minimum cost exceeds the operation budget.** Auto did not consult
  `max_requests` or `max_pages` when choosing. It now refuses range or partitioned execution when
  the plan's minimum cost (what it spends whatever the interior holds) exceeds what is left of
  either, checked before choosing and again after the anchor probes, and takes the other fast plan
  if it fits and pays off, or traverses sequentially (`insufficient_predicted_gain`, or
  `post_probe_gain_lost` after probing). The minimum cost does not extrapolate the interior:
  - range: one page per window plus the finish page, and one wave per batch capacity of windows plus
    the finish request;
  - partitioning, before probing: one page per probe, one lane page and the finish page, and the
    probe waves, one lane wave and the finish request;
  - partitioning, after probing: one page per lane and the finish page, and one wave per batch
    capacity of lanes plus the finish request.

  A plan whose estimate does not fit what is left of the budget is taken only while the estimate of
  sequential traversal does not fit either. The budget rule is Auto's: an explicit
  `RangeKeysetExecution` or `PartitionedKeysetExecution` takes the plan it names whatever the budget.
- **Clustered data stays near sequential; a dense middle between sparse ends costs more than in
  2.6.2.** Probes see only the pages they read: rows packed into a narrow cluster the probes step
  over are priced from the density the probes did see, and Auto mostly settles on sequential
  traversal there, as it did. Rows sparse at both ends and dense in the middle are overestimated by
  the probes that land in the middle, each extrapolating its page to its whole lane; a partitioned
  plan that would take about four fifths of sequential traversal's requests does not pay off by
  Auto's margin (three fifths), so Auto probes and then traverses sequentially. 2,000 such rows
  (200 sparse rows at each end) take 203 requests at page 10, where sequential traversal takes 201
  and 2.6.2 took 164, and 42 at page 50, where 2.6.2 took 36. Likewise 20,000 rows at page 50
  with 95% in a block at the head take 402 requests, where 2.6.2 took 384: the block lies inside the
  first lane the probes measure. Pass `PartitionedKeysetExecution` explicitly for such data. An explicit `PartitionedKeysetExecution` over lanes deeper than the buffer still reads them
  almost one at a time (the 227, 790 and 1,790 requests above); pass `RangeKeysetExecution` for
  large dense selections.
- **Under a tight budget, Auto can fail where the previous release fitted.** Auto chooses its plan
  from estimates extrapolated from the boundary and probe pages; none of these choices is
  guaranteed to fit a `max_pages` or `max_requests` set close to the need, and a choice that
  differs from the previous release's can fail where that one fitted. Such a run ends on the
  exhausted budget as any traversal does (`iter_list_keyset()` raises `IncompleteTraversalError`);
  rows already yielded are a correct prefix and the buffer and budget bounds hold. Across 1,512
  runs on a fixture (2,000, 8,000 and 20,000 rows; pages of 10 and 50; 600, 2,500 and 10,000
  buffered rows; six data shapes; `max_pages` or `max_requests` from 0.3 to 1.3 times the pages of
  sequential traversal), Auto failed in 33 runs that 2.6.2 completed and completed 64 that 2.6.2
  failed; without a budget it took a third fewer requests (32,011 against 47,578). The default
  budgets are no exception: 497,000 rows at page 50 with 200 identities 1,000 apart at each end,
  the top ones a hundred million values up, take 96 anchor probes and then sequential traversal,
  which runs out of the default 10,000 pages after 495,150 rows; 2.6.2 completed them in 9,962
  pages. To get a predictable page and request need under a tight budget, pass an explicit
  strategy: `SequentialKeysetExecution()` or `RangeKeysetExecution`.
- **Breaking: explicit range windows follow the measured density under empty confirmation too.**
  Without a `window_width`, `RangeKeysetExecution` under `KeysetPageCompletion.EMPTY_CONFIRMATION`
  opened windows one page of identity values wide whatever the density; on sparse data each window
  read a few rows and the windows ran out of the default `max_pages`: 5,000 rows 100 identities
  apart, at page 50, took 274 requests and returned 3,358 rows before `IncompleteTraversalError`.
  A window now spans a page of rows at the density the boundary pages measured, as under
  short-page completion: the same rows take 5 requests. A dense selection keeps windows a page of
  values wide. An explicit `window_width` is unchanged; pass the page size as `window_width` to
  keep the old windows.
- **Explicit partitioning sends nothing for the values its first probe found empty.** The first
  anchor probe starts at the lower boundary page, in either direction, so no identity lies between
  that page and the anchor it finds; that lane closes without a request, and so does a lane whose
  fences leave no identity value between them. A partitioned traversal sends one empty page fewer
  per such lane.
- **Breaking: `KeysetSelectionReason.PLANNING_INCOMPLETE` is new.** An Auto traversal whose
  boundary wave failed, or that the budget stopped before that wave, reported
  `insufficient_predicted_gain`, a cost decision it never made. It now reports
  `planning_incomplete`. One whose probe wave failed or was stopped had made its first decision:
  it reports that reason (`wide_span_partitioning`) and no `final_selection_reason`, as before.
  Code matching every member of the closed enum adds this one.
- **After the anchor probes the report carries the estimates the plan was chosen on.**
  `sequential_requests_estimate`, `interior_rows_estimate` and `total_rows_estimate` kept the values
  extrapolated from the boundary pages, beside a `selected_requests_estimate` priced on what the
  probes measured: 2,005 rows with five outliers a million identities up, at page 5, reported
  200,001 sequential requests for a traversal of 405. After probing they now come from the probes,
  as the selected estimate does. Without probes they are the boundary estimates, as before.
- **Breaking: `anchor_count` counts the anchors of a partitioned plan only.** When Auto probed
  anchors and then chose range windows or sequential traversal, the report counted the probed
  anchors as the plan's; they are discarded rows, and `anchor_count` is now 0.
- **A fast traversal's finish page runs in the traversal work class.** It was admitted as
  interactive direct work, competing with the caller's own direct calls; it is now traversal work,
  as every page of a sequential keyset traversal is.
- **Every keyset execution accepts the controls sequential traversal writes.** A named request that
  orders by the identity in the traversal's direction, suppresses `start` with `-1`, or sets the
  limit to the page size passed `SequentialKeysetExecution()` and was refused by range, partitioned
  and automatic execution with `CapabilityError`. Every execution now applies sequential traversal's
  rule: leave a control out or set it to the value sequential traversal writes; a fast command writes
  its own value there. A value sequential traversal would overwrite still raises `CapabilityError`.
- **The keyset verifier compares identities across a decimal width even when none was observed.**
  When no two observed boundary identities differ in width, the two-row checks now reach down to the
  widest value one digit shorter, so a portal that compares identities as text fails them instead of
  passing; `cross_digit` reports whether a check crossed a width. The canaries of fast keyset
  planning follow the same rule.
- **Traversal controls a request lacks are written in a fixed order.** A traversal that adds several
  controls the request did not carry wrote them in an order that depended on the interpreter's hash
  seed, so the wire form of the same request (the parameter order of a classic batch command or a
  query string) changed between runs; a fixture matching the wire form could fail intermittently.
  They are now written in the order the traversal lists them.

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
- **A fan-out's row share follows its concurrency.** A command's share was `max_buffered_rows` divided
  by `batch_size` alone, sized for one batch in flight, so the row budget, not the dispatch, decided how
  many batches were sent at once: `BatchDispatch(batch_size=1, concurrency=8)` gave one command the whole
  budget and sent one batch at a time (200 commands took 200 sequential requests), and
  `BatchDispatch(batch_size=5, concurrency=4)` fit five commands in all, in batches of about one. The
  budget is now divided among the results the dispatch may have in flight at once: `batch_size` times
  the smallest of its `concurrency`, `max_active_references` and `Settings.max_concurrent_requests` for
  a `BatchDispatch`, the smallest of its `concurrency`, `max_direct_concurrency`,
  `max_active_references` and the client limit for a `DirectDispatch`, at most `max_active_references`
  either way. A `BatchDispatch` share never drops below a full 50-row portal page while the budget holds
  one per command of a batch: no more commands are counted than the budget has pages (50 of the default
  2,500 rows), and the row budget holds back the commands beyond them, which coalesce into fewer,
  fuller batches. Below that budget the share is the old one-batch share. A `DirectDispatch` share
  has no such floor: its requests do not coalesce, so a floor would only cut its requests in flight;
  it keeps the old rule, now bounded by the client limit too. Shares of the default 2,500 rows:

  | Dispatch | Before | Now | In flight at once now |
  | --- | --- | --- | --- |
  | `BatchDispatch()` | 50 | 50 | 1 batch of 50 |
  | `DirectDispatch()` | 250 | 250 | 10 requests |
  | `BatchDispatch(batch_size=1, concurrency=8)` | 2,500 | 312 | 8 batches of 1 (was 1) |
  | `BatchDispatch(batch_size=5, concurrency=4)` | 500 | 125 | 4 batches of 5 (was 5 commands) |
  | `BatchDispatch(batch_size=20, concurrency=4)` | 125 | 50 | 50 commands (was 20) |
  | `BatchDispatch(concurrency=2)` or `(concurrency=4)` | 50 | 50 | 50 commands |

  A `BatchDispatch` with `concurrency` above 1 and a small `batch_size` now gives each command less,
  since more results share the budget, and a result longer than that now ends with
  `ResultTooLargeError`. In exchange the batches are really in flight together. A batch dispatch wider
  than the budget's page count keeps a full page per command, so ordinary 50-row pages still fit, and
  has at most as many commands in flight as the budget has pages. A client limit below the dispatch's
  `concurrency` now counts too and gives each command more: under `max_concurrent_requests=2` the
  default `DirectDispatch` share is 1,250 rows (was 250). To keep a longer result, raise
  `max_buffered_rows` or lower `concurrency`.
- **Waiting for row capacity ends with `max_elapsed`.** A page of a reference binding or a `fan_out()`
  command waiting for row capacity still held by results the consumer had not taken waited until the
  consumer read on, however long after `max_elapsed`, and its send was then refused at its admission.
  That wait now ends with the operation's elapsed budget, while the consumer still holds the rows: the
  command or binding fails with its waiting page unsent, with `BudgetExceededError` ("row buffer wait
  exceeded operation time budget"), and the operation ends as the time budget ends it. Unchanged: a
  binding waiting for the consumer to take a page already handed to it (under `READY` delivery, or with
  a `page_stop`) waits without a bound, as long as the consumer takes; see the time-budget note under
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
  as a whole (a `Retry-After` hint holds every command alike). The least held commands go at once, a
  command held longer goes in a later envelope of its chunk once its pause ends, and only a command
  whose pause outlasts what the operation has left after the portal's pause is not sent: it ends at
  once with `BudgetExceededError` ("coordinator hold exceeds execution time budget"), never sent and
  not eligible for replay. This is what breaks: a chunk of a held method and others takes more than one
  envelope, so more requests, and its commands are sent in a different order. When every command of a
  chunk can go at once, or none can, leaving some out would let no other command go, so nothing is left
  out: as for a lone `call()`, the chunk waits for its pauses within `max_elapsed` (and
  `batch_outcomes()` reads no further while it waits), or fails at once when a pause outlasts what the
  operation has left. A fail-fast `batch()` is never split either, since the failure of the command
  left out would fail the batch: its chunk is treated the same way, so it waits for the pause, as
  before, or fails at once when the pause outlasts what the operation has left. Unchanged in that last
  case: every later chunk of the held method fails the same way, so a source of only that method is
  still read, chunk by chunk, until `max_elapsed` ends the operation. This holds for classic and REST
  3.0 envelopes alike.
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
  retried. A custom transport that refuses a send because it is closed raises it too: the executor then
  counts no physical request for that send; any other failure before dispatch stays a `TransportError`
  and its send counts. The phase covers that attempt alone. A `SAFE` request waiting for the permit of a retry may
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
  correlation: `BatchFailed` and `ReferenceFailed` keep their outcomes with their correlations, so
  `pickle` fails on them when the correlation cannot be pickled, and `copy.deepcopy()` when it cannot be
  deep-copied, as on the correlation itself (`TypeError` for a lock, for example), while `copy.copy()`
  still works.
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
  physical batches in flight, and `max_direct_concurrency` does not limit it. A reference traversal
  over a `BatchDispatch` (`iter_references()`, `iter_cursors()`, `iter_reference_sources()` and their
  outcome forms) now plans its waves for that smallest limit too, so under a client limit below the
  dispatch's `concurrency` it reads the same rows, but its waves can group commands into different
  physical batches and take a different number of requests than before. The isolation of a REST
  3.0 envelope of `SAFE` commands that may have run sends its unresolved commands one at a time, within
  the batch it isolates (see "The retry path"). In `fan_out()` and `fan_out_outcomes()`, a `BatchDispatch`
  keeps as many batches in flight as these limits allow, whatever its `batch_size`: see "A fan-out's
  row share follows its concurrency" under "Results beyond the row budget". Raise the client limit
  only as far as the portal's rate limits allow.
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
  shortens it (unless that `Date` is more than a minute behind the local clock: see "The retry path").
- **Breaking: a call may now wait up to `max_elapsed` for its first permit.** The retry window
  `max_retry_elapsed_per_request` (120 s by default; see "Load limits, holds and attempt timeouts" for
  `Settings.http_timeout`, which no longer sets it) used to start before the request queued for a permit
  of the client's shared limiter. A
  request that queued behind others was refused without a send, or sent with a sliver of time and
  reported unknown. The window now opens when the first send gets its permit, so queueing for that
  permit is bounded only by the operation's `max_elapsed`, 900 s by default, where it used to be
  bounded by the window. Every send, retry wait and later permit wait spends the window, and each
  attempt is also bounded by its transport's timeout (see "Load limits, holds and attempt timeouts"). A pause the client
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
  is the refusal, or with the refusal itself when the portal's hold alone does not fit (see "Load limits, holds and attempt timeouts"), and
  its outcome stays proven not run: at once, without waiting, when the planned wait already does not
  fit, or after the wait, before the send, when the wait overran its plan (a late wake or a long permit
  queue). With the default 120-second window, a `Retry-After` of 60 seconds or more ends an `UNSAFE` or
  `UNKNOWN` request without a retry even when its first send took no time; each second the request has
  already spent lowers that bound by half a second. To let it wait longer, raise
  `max_retry_elapsed_per_request`. A `SAFE` retry is still sent with any window left: it may succeed,
  and repeating it is harmless.
- **The `SAFE` commands of a classic batch keep their replay.** The same rule applies to the replay
  rounds of a classic batch, whether its commands' own entries were refused or the whole batch was
  refused (a 429, or a quota error for the batch). When the window is too short for its `UNSAFE` or
  `UNKNOWN` commands, the round still sends its `SAFE` commands: after the same wait when the plan shows
  it, or at once, with the window left, when the send itself is refused after a wait that overran (the
  wait is not repeated; a pause the client still holds on their methods is). The other commands are not
  sent again. One whose own entry answered it keeps that answer, a `BatchCommandError` with its refusal;
  when the whole batch was refused, each ends with `BudgetExceededError` whose cause is that refusal,
  proven not run and not eligible for replay, or with that refusal itself when its own hold alone
  outlasts the window. Each command is weighed against its own method's pause
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
  itself; it keeps the refusal when its hold alone outlasts the window.
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
client (`policy=`). For `call()` it bounds the whole call, queue included. A first send gets
whatever the queue left of `max_elapsed`, so a tight bound brings back the first sends cut to a sliver:
an `UNSAFE` or `UNKNOWN` one cut that way is reported unknown (`AmbiguousExecutionError`). Keep
`max_elapsed` well above the longest queue you expect plus one send:

```python
from b24api import ExecutionPolicy

bounded = ExecutionPolicy(max_elapsed=30.0)
```

**What `max_elapsed` bounds.** It bounds the operation at its scheduling points: when a request, a page
or a source item is admitted, while a send waits for its permit (a pause included), while a page waits
for row capacity and while a source, synchronous or asynchronous, is pulled. A send in flight is cut at
what is left of the operation or of its retry window, whichever is shorter. It does not bound the time
the application spends while it holds control: in a `page_stop` callback, or as a consumer that is slow
between pulls of a stream, a binding waiting for the consumer to take its page included. Nothing
interrupts the application when the budget runs out meanwhile; the next scheduling point ends the
operation, even when all that is left is to read the end of the source. The report is then `incomplete`,
with `BudgetExceededError` as its terminal reason and a `budget_exhausted` violation. The stream raises
a plain `BudgetExceededError`, except that a fail-fast stream whose stop failed a command or a binding
raises its wrapper (`BatchFailed` or `ReferenceFailed`, whose report carries that reason); a stop at a
source pull or the end-of-source check fails none, except that commands a batch window already read are
sent and refused, so `batch()` raises `BatchFailed`. So for a fail-fast stream it depends on where the
budget stopped the stream, not on the operation (a tolerant stream raises the plain error): handle both,
or read the report. The cleanup no longer stops a stream with the budget: it gets a grace past
`max_elapsed`, and a close that overruns it raises `CleanupOverrunError` (see "The end of `max_elapsed`
is not a cleanup failure"). A send cut in flight is another case: its command may have run, so it is
unknown (`AmbiguousExecutionError`), and a fail-fast stream may end on that ambiguity instead.

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
    is `::ffff:102:304`), the default port (under any scheme case: `HTTPS://portal:443/...`) and
    an empty userinfo dropped, `.` and `..` segments resolved, and the trailing slash added.
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
  [Upgrading from 2.3 to 2.4](#upgrading-from-23-to-24)). Records of other loggers are not filtered:
  `httpcore` DEBUG records carry the response headers, so a `Location` header that holds the token
  puts it in a record; see the HTTPX logging note in
  [Migrating to the issues architecture (2.3)](#migrating-to-the-issues-architecture-23). What is not
  converted passes through as it is: an exception that is not an `Exception` (`KeyboardInterrupt`,
  `SystemExit`) raised by a hook, which leaves without the transport's and HTTPX's frames that hold the
  request and with its `__cause__` and `__context__` cleared, and the arguments a canceller gives a
  cancellation. One limitation remains: over HTTP/2, HTTPX's
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
  REST v3 batch already reported it. `call()` still raised the `EnvelopeContractError` or
  `ProtocolError` itself until [One verdict per send](#one-verdict-per-send).
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
- **A flat counted tail page's `batch_index` is its index in its own batch.** In the report of
  `iter_list_counted()`, a tail page's `PageRecord.batch_index` was its ordinal in the whole tail (the
  fourth tail page was 3 whatever the batch size), unlike every other batch page, whose index is its
  command's in the batch it was sent with. It is now that index, as on the reference path.
- **A keyset reference page's `batch_index` is its index in the shared batch.** In the report of
  `iter_reference_outcomes()` with a fast keyset traversal, a wave page's `PageRecord.batch_index` was
  its index in its own binding's wave and a finish page's was `None`, though both are commands of a
  physical batch shared with other bindings' commands. Each is now its command's index in that batch.
  A page replayed alone keeps the index of the batch it was first sent with: it is the command's key
  in every send of it.
- **A page sent with a batch that failed as a whole is traced.** When a physical batch fails as a whole,
  each page sent with it is traced the way it settles, with its index in that batch: a fast keyset wave or
  finish page, of `iter_list_keyset()` or of a reference binding, and the counted tail page the traversal
  stops at, flat or reference. Before, only an envelope failure (a structured error on the envelope, or a
  lost answer) was traced; a batch whose answer could not be decoded left no trace, nor did a reference
  counted tail page whose replay's answer could not be decoded, and the flat counted tail traced its page with its
  index in the whole tail. A direct keyset finish request that fails is traced too, with no `batch_index`.
  The errors raised are unchanged.
- **A cancelled request's pages are traced as possibly run.** When `iter_list_counted()` is cancelled while
  a tail batch is on the wire, the page the traversal stops at, the batch's first, is traced `UNKNOWN` with
  `AMBIGUOUS_EXECUTION`; when `iter_list_keyset()` is cancelled while a keyset wave or its direct finish
  request is on the wire, each page of it is, as a direct page cancelled on the wire already was. Before,
  they were not traced. A reference operation's report still holds no page of a binding its cancelled
  consumer did not read.
- **A rejected page is retired with its trace's code.** In the completion evidence of a list or reference
  traversal, a page whose own command succeeded but which the traversal rejects is `PageRejected` with the
  code its trace records (`transaction_aborted`, `identity_contract`, ...), on every path. Before, a counted
  tail page the traversal stopped at, when the batch around it failed, had no `PageRejected`; a fast keyset
  page was `PageRejected` with the reason `rejected` whatever its code, the pages of a flat keyset wave that
  failed as a whole with `failed`, and those of a reference binding's wave that failed as a whole were not
  rejected at all.
- **A flat counted tail page the traversal stops before settles as its command did.** In the
  completion evidence of `iter_list_counted()`, every tail page read but not reached when the traversal
  stopped (at a failed page, at the budget, cancelled, or when the consumer broke out of the loop)
  settled as unknown, so the binding closed `UNKNOWN`. Each now settles from what its command did: one answered with a success as a success,
  rejected as aborted; one never sent as not executed; only one that may have run as unknown. The
  binding closes `FAILURE` unless one of its pages may have run.
- **A keyset page read but never consumed is rejected as aborted.** In the completion evidence of
  `iter_list_keyset()` or of a keyset reference binding, a page the traversal read and committed but whose
  rows never reached the caller (the caller stopped, or the traversal failed past it) is `PageRejected` with
  `transaction_aborted`, as a counted page is. Before, its reason was the traversal's end state
  (`cancelled`, `failed`, ...). Its trace still records it committed: the trace says what the traversal read.
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
  source's failed close: the close raises it as `InputSourceError` (`batch()`, `fan_out()`:
  `BatchFailed`) and the report records `cleanup_failure`.
- **A cancelled close or cleanup gives up at the operation deadline.** Stream cleanup deferred a
  cancellation of its caller until it ended, however long that took. It now defers it only while
  the operation's `ExecutionPolicy.max_elapsed` lasts; cleanup still running then finishes in the
  background, the close raises `BudgetExceededError` and the cancellation is delivered at the next
  await, and the report carries a `cleanup_failure` violation with the terminal reason "stream
  cleanup failed". Without a cancellation, cleanup is still awaited to its end. A read or close that
  runs the end an interrupt between reads left is the exception: that end has no deadline of its own, so
  the read or close defers its cancellation until the end is over, which the cleanup windows of its steps
  bound (the operation's time left, at least the cleanup grace), not the operation's deadline.

## CLI exit code for a closed reader

- **A write to a closed stdout or stderr reader exits `5`**, ahead of any other code. It exited `120`,
  `1`, or with unbuffered output `0` (help) or `2` (a parser error).
- **Any other failed write exits `4`**, `--help` to a full disk included, which exited `0` or `120`.

## Traversal contract checks

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
  `IncompleteTraversalError`. It now propagates unchanged, as from `iter_list()`; ordinary exceptions are
  wrapped as before.

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

  Native REST v3 batch and every direct JSON request still send empty containers. The new
  read-only `Request.has_empty_container` tells whether a request holds such a value. Classic batch
  now refuses `None` the same way; see
  [What a classic batch command can carry](#what-a-classic-batch-command-can-carry).

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
  A lease did not grow then (it now grows once as its binding plans, see "The reference row pool and its
  batches"), so a dense binding admitted right after a cohort of small ones is sized while
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

## Upgrading from 2.3 to 2.4

2.4.0 changes only what the list below names. Work through it in order; each item links to the
details.

1. **Root imports.** The `b24api` root exports 51 names; 115 others moved to `b24api.contracts`,
   `b24api.errors`, `b24api.transport` or `b24api.completion`. Old imports fail with `ImportError`
   or `AttributeError`, and type checkers flag them. `from b24api import *` follows `__all__` and
   no longer brings the moved names; replace it with explicit imports. Run
   `python -m b24api.migration src/ tests/` to list both; see [Root imports in 2.4](#root-imports-in-24).
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
     violation (for example `stream cleanup also failed (InputSourceError)`), so a report can carry
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

## Root imports in 2.4

The `b24api` root now exports only the names a typical application needs: the client and its
settings, requests and routes, policies, traversal and dispatch specifications, the report and the
main errors, and the transports, including `HttpxTransport`. The other 115 names moved to one of the
public packages `b24api.contracts`, `b24api.errors`, `b24api.transport` and `b24api.completion`,
which export the same objects. No object was removed or renamed; only the root path is gone.

The old root paths do not resolve since 2.4 and there is no deprecation period:
`from b24api import CommandSuccess` raises `ImportError: cannot import name 'CommandSuccess' from
'b24api'`, and `b24api.CommandSuccess` raises `AttributeError: module 'b24api' has no attribute
'CommandSuccess'; it moved to b24api.contracts.CommandSuccess`. Mypy and pyright report each
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
partitioned reports use `KeysetAssuranceSource.CALLER_ASSERTED_BOUNDS`. 2.4.0 removed the legacy
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
- `None` values were omitted from form and physical-batch bracket encoding; JSON encoding sends them as
  `null`. Since then such a request is refused instead (see
  [What a classic batch command can carry](#what-a-classic-batch-command-can-carry)).
- A 2xx response missing the canonical result envelope raises `EnvelopeContractError`. It is a
  subclass of both `HTTPGatewayError` and `ProtocolError`, so `except ProtocolError` now catches it
  too; its origin stays `http_gateway`, it is never retried, and reports still classify it as
  `envelope_contract`. Malformed non-empty JSON remains a plain `ProtocolError`.
- `ExecutionPolicy.from_settings(settings)` returns the client's default policy (the library
  defaults with `max_retry_elapsed_per_request = settings.http_timeout`). A `policy=` argument still
  replaces that default wholesale; derive it with `dataclasses.replace` to keep the timeout. (Removed
  since: see "Load limits, holds and attempt timeouts".)
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

| Earlier capability | Current operation | Important difference |
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
- report values no code path produced (2.4.0): `ReplayDisposition.REPLAYED_DIRECT` and
  `DIRECT_REPLAY_FAILED` (a physical batch is never replayed as direct calls),
  `CompletionAssurance.ORACLE_VERIFIED`, `SnapshotState.VERIFIED` and `SnapshotState.CHANGED`
  (no snapshot oracle exists; a required snapshot reports `UNVERIFIED`), and
  `NotExecutedReason.SCHEDULER_STOPPED`. Drop those arms from exhaustive matches; the enum inputs
  `SnapshotRequirement` and `ConfirmationPolicy` keep every member.
- `PageValidated.identity_digest` (2.4.0). The gate only ever checked that the digest was non-empty,
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
