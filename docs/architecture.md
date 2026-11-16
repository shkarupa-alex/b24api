# Architecture and correctness guarantees

`b24api` is a method-agnostic asynchronous transport client. Its runtime is split into contracts,
transport, execution, batching, traversal, reference scheduling and a thin public facade. The
client does not contain endpoint catalogs, persistence, business reconciliation or recipe logic.

## Public capability surface

| Capability | Public operation | Guarantee |
|---|---|---|
| Decoded direct request | `call()` | Detached decoded JSON with explicit replay policy. |
| Response envelope | `call_response()` | Immutable result, pagination, timing and bounded evidence. |
| Binary response | `call_bytes()` | Every bounded successful body byte returned without JSON sniffing. |
| Arbitrary logical batch | `batch()` / `batch_outcomes()` | Lazy input, bounded physical batches, off-wire correlation and typed outcomes. |
| Independent command fan-out | `fan_out()` / `fan_out_outcomes()` | Explicit dispatch mode, concurrency and delivery order. |
| Sequential offset list | `iter_list()` | Server-continuation validation and terminal empty-page confirmation. |
| Counted list | `iter_list_counted()` | Direct head, batched tail and exact total/range/identity validation. |
| No-count keyset | `iter_list_keyset()` | Deterministic auto default plus explicit sequential, range, or partitioned execution for caller-asserted stable integer keysets. |
| Dependent cursor | `iter_list_cursor()` | Strict unique monotonic cursor progress. |
| Response-driven continuation | `iter_list_continuation()` | Pure caller rule over declared controls, cycle detection and a rule-declared end. |
| Inclusive timestamp window | `iter_list_window()` | Classified paging rows, strict boundary progress and overlap delivered once. |
| Correlated cursor set | `iter_cursors()` | Lazy one/many-parent cursor traversal with isolated seeds and shared batching. |
| Keyset qualification | `verify_keyset_capability()` | Explicit fail-closed strict-bound report for one portal and request shape. |
| Per-parent traversal | `iter_references()` / `iter_reference_outcomes()` | Isolated traversal state and correlation for every accepted binding. |
| Heterogeneous references | `iter_reference_sources()` / `iter_reference_source_outcomes()` | Per-source request, traversal and state over one shared dispatcher, row pool and report. |
| Shell access | `b24api call` / `b24api list` | JSON/JSONL stdout and diagnostics on stderr. |

## Failure ownership

The client fails closed when transport or pagination evidence cannot prove the requested mechanical
result. Contradictory continuation, missing counted ranges, duplicates, total mismatch, response
size overflow and unsafe ambiguous replay never become ordinary success.

The application still owns facts that a generic REST client cannot infer:

- whether Bitrix honored the business meaning of a filter;
- the correct composite storage identity for application data;
- endpoint-specific applicability of total, offset, keyset and cursor mechanics;
- reconciliation of a write whose result is ambiguous after possible dispatch.

Whether a command may have run is one execution fact, recorded on its failure by the code that sent or
withheld it: the executor for its own sends, the classic and REST v3 batch dialects for the commands of
an envelope they decode, and the batch page dispatcher of references and list traversal for a page
cancelled after its wave was sent. A page whose fetch ended before its wave left, cancelled or past its
admission deadline, is never sent, so it stays proven not run. A caller whose wait for a running chunk
ends early (a cancelled page, or a fast keyset wave the traversal's end interrupts), and every page of a
chunk that fails as a whole (it reports the chunk's error), reads what each of its commands did from the chunk's `BatchProgress`, which the batch engine keeps by the same rule: a
command answered, by its entry or with its envelope, was answered, and one answered with a success
succeeded; one whose latest send, or one before it, may have run without an answer may have run; one
never sent was not sent. Until the chunk ends, a command without a final outcome may still be sent (a replay
it waits for), so a page cancelled meanwhile may have run; a REST v3 group keeps its state to itself, so its
commands in progress may have run. A fast keyset wave engine, flat or shared by reference bindings, settles
each page it was handed by its command id from what that command did, also when its call raises; the page
ledger (`FastPageLedger`) settles a page never handed to an engine as not executed, which retires it, the one
rule on every path. A page answered with a success settles as a success on every path, a page the traversal
stops before included: the facts recorded on a cancelled reference page keep that its command succeeded
(`facts.settlement`), and a counted tail, reference or flat, settles each page it read but will not consume
from what its command did, the flat one from the state of the batch it joined
(`_BatchOutcomeStream.settlement`): its outcome once answered, its `BatchProgress` while sent, not executed
while only read, and not executed when its pull was cut short before it joined one: answered with a success, a
success, rejected as `transaction_aborted`; never sent, not executed; unknown only when it may have run. One
rule, `facts.settled_as`, gives a command's settlement from what it is known to have done and whether it
succeeded; its callers only read those two facts from what they hold: a failure's recorded facts
(`facts.settlement`), a final batch outcome (`batch.outcome.settlement_of`), a command of a running chunk
(`BatchProgress.settlement`) and a shared wave's dispatch task. `b24api.execution.facts` holds the one
mapping from that fact to the public outcome variant (`ambiguity_of`) and to the completion settlement
(`Execution.settlement`), and `b24api.traversal.page_failure` holds the one rule of a failed page's
trace, from its settlement alone: a page that never executed because it was never sent (traversals never
halt a batch, so no sent command is withheld) leaves no request to record and is not traced, whatever kept
it from its send (the budget, a closed client, a cancellation); the settlement decides the code of every
other failed page the traversal records. That page is unknown only when it may have run, otherwise
rejected; a page whose own command succeeded is rejected as `transaction_aborted`, the code of a page
dropped with the unit around it, not as a `command_failure`, and its completion record retires it with the same
code (`PageRejected`), as it retires a page the traversal read but never consumed. One function couples the
two (`page_failure.rejection_reason`): the driver and the keyset runtime both retire a page they trace as
rejected with the trace's code. Whatever ended the page
decides nothing: a page cancelled on the wire may have run and is traced unknown, as one whose answer was lost.
A traversal records the failed page it stops
at, not every page its failure leaves behind: a counted tail stops at a binding's first failed page in
offset order, and the binding's later pages, sent or not, are settled and released without a trace record.
A page's trace and its settlement read the same fact, the page's own: for a page of a chunk that failed as
a whole, its command's in the chunk's `BatchProgress`. Every failure the batch page dispatcher gives a page
it handed to a batch carries those facts (`_FailedPage`: what it reports, how it settles, and the command's
own failure when the batch answered or ended it), and every caller reads a failed page through one function
(`_failed_page`), which falls back to the facts recorded on any other failure: the binding's failure, a page's
settlement and trace, and a wave or a counted tail that takes a command's own failure as its outcome.
`page_failure.failed_page` alone decides whether a failed page is traced and how, for `record_failed_page` and
for keyset's `classify_rejection`. Both counted tails, reference and flat, hand the tail's one trace site a
receipt for every failed page they reach, sent or never sent, with its command's own failure or its whole
chunk's, and that site traces it by `failed_page` alone. A page sent with a chunk that failed as a whole (its
batch failed, or its answer could not be decoded) is traced as its settlement says, with its position in that
chunk as its `batch_index`, on every path that sends one: the reference dispatcher's `_FailedPage`, the flat
tail's stream (`failed_chunk`: the traversal stops at that chunk's first page, the one it traces) and a keyset
engine whose wave or finish call failed (`failed_pages`: each page's settlement and index, which the keyset
transaction traces by `failed_page`, a direct finish page with no index). Each of them remembers every call
that raised, a cancelled one included, and none filters by the exception's type. A reference binding's trace
reaches the operation's report only through the events its consumer reads: when the consumer is cancelled, it
reads no more, so a reference binding's pages, settled as their commands did, are not traced, whatever path
they took (sequential, counted or keyset). A page's `batch_index` is
its command's index in the physical batch it was submitted with (sent with, or handed to), as its command's
outcome carries it, on every path: a keyset wave or finish page of a reference binding shares that batch with
other bindings' commands, so its index in its own binding's wave is not one. The index is also the command's
key in that batch's envelope, which every replay send of the command keeps: a page replayed alone, or with
fewer commands, keeps the index of the batch it was first submitted with, whichever send's answer settled it.
A page with no batch command to point to, such as a direct page, has none. Classic and REST v3 batch,
`call()`, direct and batch fan-out, references and list traversal all use them, and none of them chooses from
the exception type, which names the cause of a failure and never proves that a command did not run. A sent
command is unknown unless a listed refusal proves it did not run or a conclusive answer settles it. A
conclusive answer is a structured Bitrix error, top-level or found in the result by the command's own
contract, or any status outside 2xx that the ambiguity policy does not hold ambiguous, whatever its body, so a
malformed error body on such a status is conclusive too. A REST v3 batch envelope is the one exception: a
top-level structured error on it is an abort, which may come after some of its commands ran, so it settles
none of them; a command is answered only by its own element or its own isolated send, or proven not run by a
transport failure that proves the envelope was not accepted or a listed refusal status of the envelope. A 2xx
answer the client cannot decode settles nothing, whichever part is malformed: the envelope, a command's paging
metadata or its embedded error.

A command keeps one send lineage across executor retries, classic batch replay rounds and REST v3
isolation: what the sends it was in left open. The commands a REST v3 envelope group still resolves are
always a subset of those of its envelope before, so the group's lineage is the lineage of each of its
pending commands, and an isolated send continues it on a per-command copy. A classic batch round may
carry a `SAFE` command past a round, so the classic engine keeps each command's lineage and state
(answered by its own entry, or following its envelope's failure) itself: a failure a round's commands
share records what that round's send alone left open, and each command adds what its own earlier sends
left open. The lineage is an explicit state: nothing sent yet, or the latest
send answered or possibly run (with the failure that left it so), and whether a send before it may have
run. When a later conclusive answer ends a command after an earlier send may have run (the retried
envelope, the command's own entry in it, a REST v3 element or an isolated send), the outcome is unknown
and its reason and cause name that earlier send; the conclusive answer that ended it does not appear in
the cause chain. `HALTED` means the command was proven not sent and no earlier send of it may have run:
the server's own halt in a classic fail-fast batch, a later envelope that fail-fast did not send, or a
window refused before dispatch. A success that arrives after the time budget is returned, not discarded.
The operation deadline of direct and batch fan-out and references bounds admission only: once a send
left, the executor's own budgets end it, so a deadline that passes mid-send gives the same evidence on
every path. The executor ends a send only by cancelling it, so a transport that resists cancellation
holds the operation for as long as it resists, on every path. A list traversal that fetches a page
directly raises the page's own failure, as `call()` does, rather than an ambiguity; the page's
settlement and trace still come from the fact. One settlement does not come from the fact: when
`iter_list_counted()` stops at a failed page, a page of its batch tail whose outcome it never read
settles as unknown.

## Correlation and source failures

`Command.correlation` and `Binding.correlation` are caller-owned objects retained by reference and
excluded from wire data and safe diagnostics. Every accepted command or binding keeps that
correlation through its terminal outcome.

A malformed source object that is not a `Command` or `Binding` has no valid correlation. The source
therefore terminates with `InputSourceError`; the runtime does not fabricate an outcome identity.
Items accepted before that source error retain their actual success, failure or non-execution
outcome.

## Resource and lifecycle model

- Logical sources are consumed incrementally; physical Bitrix batches contain at most 50 commands.
- Response bytes, buffered rows and commands, retained unordered identity keys, request/page
  budgets, concurrency and elapsed time are bounded by `ExecutionPolicy`. Concurrency has one more,
  client-wide ceiling: the permits of the client's shared rate coordinator,
  `Settings.max_concurrent_requests`. An operation's limits only narrow it.
- The row budget bounds what an operation holds, not what it sends: a result's rows are known only
  after its command ran. A logical batch and a counted traversal's tail fit a window's results into
  `max_buffered_rows` in input order (`_within_row_budget`), and the reference scheduler fits each
  page into the rows reserved for it before the page is accepted. A result that does not fit is dropped, and its command fails with
  `ResultTooLargeError`, recorded answered by `b24api.execution.facts.dropped_result`: a failure that
  ran, never one proven not run. A page's wait for row capacity (`_RowBuffer.reserve`) is bounded by
  the operation's elapsed budget, as every other dispatch wait is. A binding's wait for the consumer is
  not: under `READY` delivery, or with a page stop, `_deliver_page` puts the page on the output queue and
  awaits its acknowledgement without a deadline, since the consumer holds control there. The budget
  ends the operation at its next scheduling point after the consumer takes the page.
- Budgets are charged to one operation-local ledger of plain counters updated in place, so a
  reservation, its settlement and a row-buffer change cost the same however many bindings the
  operation has read. Per-binding page counts are kept only while a binding is open; the validated,
  immutable counters are built only for a snapshot or the terminal report.
- A request's retry window (`max_retry_elapsed_per_request`) opens when its first send is granted
  a coordinator permit; queueing before that is bounded by the operation's `max_elapsed`, and a known
  coordinator hold that outlasts the budget left fails at once. A retry waits the longer of its own
  backoff and the coordinator's hold on its methods (a host throttle hint or a method cooldown). A
  retry that may not be repeated is planned only with more than twice its wait left, and starts only
  with at least as much window left as it actually waited since then (more than the planned wait
  when the wait overran), so a wait never takes more of the window than the send after it gets; a
  send that the window still cuts is reported unknown.
  `Executor.plan_replay` plans the executor's own retries of a direct request and every replay round
  of a physical batch; each round's caller decides it: the executor for a direct request, the REST
  3.0 resolution for its envelopes (one safety class each, admitted or refused whole), and the
  classic batch engine for a classic envelope, which the executor then sends once per call. The
  engine plans each command on its own safety and its own method's hold as known when planning: a
  round waits only as long as its `UNSAFE` and `UNKNOWN` commands can afford, carries a `SAFE` command
  held longer to a later round (an attempt is spent per send of the envelope, when `_RequestAttempts`
  records it once its transport returned or failed: a round that is sent spends one, a round stopped
  before its send, at its permit, its admission or its transport, spends none, and a carried command
  spends none of its own;
  every round that carries a command plans to send another, so the attempt budget bounds the carrying:
  a command still carried when the last attempt is spent ends as the attempt budget stops it), and
  still sends the others when the window or a held method stops some, unless it is fail-fast, which is
  sent whole or not at all. A hold that appears during the wait is met at the permit and stops the whole round
  when it outlasts the window. A stop before a send that follows a failure, by the budget or by any
  other failure (a closed client), is chained to the failure of the latest send in one place, where the
  executor stops it (`_RequestAttempts.chain_stop`, at the permit, at the admission and at the transport),
  for a lone request's retry, a REST 3.0 envelope retry or isolated send and a classic round alike.
  Each send's failure is classified into the request's lineage where the send ended, so the failure that
  ends a request after its sends, a stop or a cancellation included, is recorded with what the lineage left
  open (`record_open`), by the executor for a lone request as by the batch dialects for their own (a REST 3.0
  group's envelopes, and the isolated sends of the command sent alone when it ended; a classic envelope's
  rounds), never told by its exception type: a command a stop ended is explained by the latest send that may
  have run (its reason and evidence), whatever stopped it: the budget, a closed coordinator or a closed
  transport. With nothing sent, `record_open` records nothing, so a failure before the first send, a stop or a
  cancellation, stays proven not run. A transport refuses a
  send before it leaves the process with `ClientClosedError` only because it is closed (a closed
  `HttpxTransport` or injected HTTPX client, or any transport): the executor releases the attempt it
  reserved and records nothing sent (`Executor._dispatch`), so that send is no physical request and no
  isolated send, and stops it as the permit does. Until its transport returns, a send holds its
  reservation, so a concurrent send the spent request budget refused meanwhile stays refused even when
  that reservation is then released. Any other failure before dispatch (a credential unavailable, a
  connection pool timeout, a refused connection) is a `TransportError` and its send counts. A batch command
  that followed its envelope's failure ends with that stop; a classic round may carry a command past a
  round, so its engine gives each command a copy of the stop caused by the failure that command
  followed. A command its own entry or element answered keeps that answer, whatever stops its replay.
  The REST 3.0 isolation of an unresolved `SAFE` envelope is
  a replay round of its group like its one envelope retry: planned whole by `plan_replay` and waited out
  by `replay_after` (the longer of the backoff and the coordinator's hold, within the attempt budget,
  the retry window and the deadline), then its commands are sent one at a time, inside the dispatch
  slot the envelope held, so a `BatchDispatch` never has more requests in flight than its concurrency.
  Each isolated send is an ordinary request on its command's copy of the lineage, which the executor
  admits at its permit and paces through its own retries. A replay its plan refuses, by the deadline
  (recorded) or while time is left by the attempts, the window or a wait it cannot carry, sends nothing
  more and ends each command with that refusal caused by the envelope's failure, in both dialects, as a
  lone request's refused retry ends. An envelope's first send is admitted under all its methods, so for a user's
  logical batch (`batch()`, `batch_outcomes()`) `Executor.plan_first_send` weighs its commands with
  the same `_Pause.admits` and `_plan_round`, with no backoff, by how much longer each command's own
  method is held than the host, against the window its send opens or what the operation has left after
  the host hold: when some commands can wait and others cannot, the engine refuses those unsent and
  sends the rest; otherwise the envelope goes whole to its permit. A fail-fast envelope, traversal
  pages and dispatcher waves (`BatchExecutor.execute_requests`) are never split: leaving a command out
  of them lets no other command go.
- The request budget and the deadline end an operation: `ExecutionContext.admit_request` refuses a
  send once `max_requests` is spent, before its permit and again at its reservation, and records the
  refusal (`requests_refused`), after which no send can be admitted. Time refuses work only through
  `ExecutionContext.time_left`, which returns the time left or refuses when none is, and
  `ExecutionContext.deadline`, which waits under it (a send's permit and its dispatch,
  the plan of a retry, a batch replay round (a REST 3.0 isolation included) or an envelope's first send, a
  page reservation, a row-capacity wait, a dispatcher admission, a fast keyset lease, a source pull).
  Both record the refusal when the operation's deadline is what closed, and record nothing when a
  request's own retry window closed first, which refuses that request alone with "per-request retry
  time budget exhausted". The time left is read otherwise only by `coalescing_time_left`, at the batch
  dispatcher's coalescing wait, which ends in a send, never a refusal, and by `cleanup_window`, at the
  cleanup steps below. Each site that refuses work at the deadline has a behavioural test showing the
  refusal recorded and the operation ended with it; those tests are what hold the rule.
  `tests/internal/deadline_refusal_test.py` adds a cheap static tripwire for the usual idioms (the
  `ExecutionContext` members that read its clock, the readers that refuse nothing, the timeouts armed
  through `asyncio`, every `BudgetExceededError` constructed), which a site written otherwise passes. The refusals of one request while time is left, for a
  coordinator hold or a retry delay its window cannot carry, or a send that may not be repeated left
  less window than its wait, record nothing. `ExecutionContext.refusal` keeps the first recorded refusal's message.
  Whether an error is the operation's refusal, which ends it, is told by its mark, not by its text or its type:
  the mark is a value unique to the context, so it survives a copy or a pickle round trip of the error, and
  the context marks every error it raises as such (`operation_refusal`, from `_refuse`, `admit_request`,
  `raise_if_refused` and the source pull; a replay round's stopped commands end with that refusal or a copy
  of it, which keeps its mark), and `ExecutionContext.ended_by` is the one check, used by the
  reference scheduler's source pull, the batch window's pull, the counted tail and the fast keyset
  commands. Every later work the budget refuses names its own work but is that same end; a request's own
  refusal or a dropped result is not marked and fails its work alone.
  `AsyncIteratorController.get`, the one source pull of the batch streams, refuses further input once
  a send was refused or the deadline has passed, so the operation ends with `BudgetExceededError`; the
  commands a batch window read before such a pull are sent as any window is, the budget refusing each,
  and the stream then raises the pull's refusal. A pull the budget refuses ends the reference
  scheduler's source as a read-out source ends (lease waiters are woken, and the source no longer
  counts as one that may still bring a binding), without its end having been read. Its task ends
  without error, so a fail-fast window's failure stays the operation's only error and cleanup records
  nothing. The consumer decides from `refusal` itself, not from how the source ended (the producer
  reads ahead and may read a finite source out before a refusal), and raises `BudgetExceededError`
  with that message once the bindings already read are settled. A budget the last needed send spends
  exactly, or a deadline that passes once nothing is left to admit, ends nothing.
- Cleanup is bounded by `ExecutionContext.cleanup_window()`: what is left of `max_elapsed`, but at
  least `CLEANUP_GRACE_SECONDS` (0.1 s), since cleanup typically starts when the deadline has passed.
  The source close (`AsyncIteratorController.aclose`), the reference scheduler's wait for its
  cancelled tasks and the batch dispatcher's wait for its workers each wait that long; what is still
  running then finishes in the background and is reported as a `cleanup_failure`
  (`b24api.errors.CleanupOverrunError`, a public `BudgetExceededError` that records no refusal and
  that `classify_failure` classifies as any failed close, never as `budget_exhausted`),
  secondary to the error that ended
  the operation; behind a caller's cancellation, the cancellation is raised and the stream is
  cancelled. A stream owns its source through its `AsyncIteratorController` from the start, so a close
  before the first pull is bounded the same way. `OwnedSource` runs a synchronous source's pulls and
  close one at a time in its one `contextvars.Context`, under a lock its worker threads take, so a
  close that the end of the operation starts while a cut pull still runs waits for that pull instead
  of entering the context twice. An asynchronous source's steps run in its own context too, driven by
  the reading task (`_InContext` enters the context on each resumption), and its `failure` mapper
  turns any exception a pull or the close raises into the family's source failure. `OwnedSource.rejected`
  keeps what its own admission (`accept`) raised, so a batch tells an item it refused from the source's
  own failure without a private exception class.
- A failed close is reported by one rule, built by `OperationRunner` (`cleanup_failure` and
  `cleanup_failure_violation`), for every family and every kernel stream, the fast keyset one
  (`KeysetFastStream`) included: "stream cleanup failed (X)" as the terminal failure when nothing failed
  before it, "stream cleanup also failed (X)" as a secondary otherwise, recorded in the report. Behind
  the reader's cancellation it is secondary too (`cancellation_outranks`): the reader gets the
  cancellation and the stream is cancelled whenever the cancellation landed, and `OperationStream`
  demotes its own cleanup failure behind a cancelled source the same way
  (`with_cleanup_failure_behind`). It does so behind an interrupt as well (`closed_behind_its_end`): a
  source it closes records a failed close as what failed it, not knowing that an interrupt ended the
  operation first, so a close failing behind an interrupt reads the same whether the interrupt came in
  a read or between reads. An interrupt (`is_interrupt`: a `KeyboardInterrupt` or `SystemExit`, what
  the event loop raises out of `asyncio.run` from a callback, where it only logs and drops anything else)
  is never secondary. Only an `Exception` is a failure (`InputSourceError`, `BatchFailed`,
  `ReferenceFailed` or a failure outcome, whose error carries it by one rule, `outcome_error`): every catch
  site that decides a failure catches `Exception`. Any other `BaseException` but an interrupt or a
  cancellation is no failure and nothing classifies, wraps or ranks it; it propagates as Python propagates
  it. The library only delivers it: to the reader as it is, from a read, from work between reads or from
  the source's pull or close, the source closed once, no hang and nothing for the event loop, and it is
  never lost: what no read raised, the close raises. The task it ends returns it (`Settled`), and the cleanup
  collectors pass any `BaseException` on to their owner, so a cleanup ends in it; that replaces whatever
  ended the stream, as a `finally` does (`OperationRunner._terminate`). A reference binding that ends in one
  ends its task in it and puts `_EndedEvent` in its queue (`ReferenceScheduler._end_binding`), only so that a
  read reaches the binding's place instead of waiting for an end that never comes; it raises the task's end
  there (`_finish_task`), and a close that comes first raises it from the tasks it collects. A read a
  concurrent close ended never raises it (that close does, from the read it settles): the read ends as the
  close's cancellation of it would. A coroutine closed by hand or at collection (`GeneratorExit`) ends no
  binding. One piece of state carries an
  interrupt: each operation has one
  `InterruptLatch`, the isolated runner's, which its pull and cleanup tasks publish through `INTERRUPTS`
  so the tasks they start inherit it (an inline kernel uses the latch of the runner it serves). Every
  place where library code first catches a caller's `BaseException` latches the first interrupt and goes
  on as it would: `settle`, so every `spawn_settled` task; the reference scheduler's binding tasks; the
  dispatcher's senders; and `OperationRunner.__anext__` where a read ends, which is where an inline
  kernel (`batch()`, `iter_list*`, the fast keyset) first catches the interrupt its own send raised, its
  end then raising what the kernel ends with (a failed cleanup, say). Whatever later handles that
  failure (a scheduler's close, say) may drop it freely. The latch's state is explicit
  (`LatchState`): `ARMED` while the operation runs, when the first interrupt wakes the owner; `ENDING`
  once cleanup starts, when an interrupt is latched for the end and wakes nothing (an inline kernel's
  cleanup runs in the very read a wake would cancel); `CLOSED` once the interrupt is out. The wake
  (`OperationRunner._interrupted`) decides delivery at once. A read in flight is cancelled and raises
  the interrupt (a read whose own task latched it and still returned an item raises it instead of the
  item). With no read in flight, between reads, the wake closes the latch and hands the interrupt to
  the event loop then and there (`InterruptLatch.hand_on`, the path of a late interrupt): nothing
  later has to run for it to leave. The loop raises it once the callbacks already due in that loop turn
  have run: a reader those wake (one waiting on what the interrupted work did, say) resumes first, and a
  read or close it makes then finds the operation ended and runs its end; every other reader resumes
  after the raise. The operation's
  end (cleanup and publication) is decoupled from that delivery: the phase turns `INTERRUPTED` (ended,
  its end not begun) and whoever comes first runs the end (`OperationRunner._end_between_reads`): a
  read or close, or the task the wake starts once the interrupt is handed on, which a read or close
  never waits for. The end raises no interrupt, whoever runs it; it attaches the report to the
  interrupt (`.report`). A read or close that finds an end some other caller runs (`CLOSING`, claimed;
  `CLEANING`, its cleanup begun) waits for the phase to turn `CLOSED` (`_published`): such an end has begun
  in a caller that runs it to its publication, and a read in flight that a concurrent close ended waits
  the same way. Only a cleanup that began settles the end: one cancelled before its first step (as a
  loop's shutdown cancels every task it finds) closed nothing, so the claimant starts it again
  (`OperationRunner._end`), and the cancellation is the claimant's, raised, never recorded. The operation's
  own work cannot close it from the wake on (`inside_own_work`): that close would run or wait for an
  end that waits for the work. Once ended, a read raises `StopAsyncIteration` and a close returns.
  On a loop that runs on, the end's task runs it if nobody came first. A loop the interrupt stops does
  so before that task's first step, since the task is started after the raise is due: under
  `asyncio.run` the shutdown cancels the task before it begins, and the end is the close the shutdown
  runs as it unwinds the tasks it cancels (`async with stream`, the client's `async with`); that
  cancellation has already landed, so the close closes the source and the report is the interrupt's
  alone. A read that resumed ahead of the raise and claimed the end has its cleanup cancelled by the
  shutdown before it begins, and starts it again, past the shutdown's cancellations: the source is closed
  once, and the report is the interrupt's alone as well. An end that a cancellation meets while its cleanup runs (a read or close running it is
  cancelled, or the shutdown cancels a task running it) still publishes, the cancellation recorded as
  cleanup's ("stream cleanup also failed (CancelledError)"). There is no report, and the source is not
  closed, when no end ever begins: the stream is never read or closed again and the loop stops before
  the end's task runs (a stream nobody closes, or an interrupt the loop raises out of `asyncio.run`'s
  own shutdown, which then stops), or an end that has begun never runs again (its loop stopped while
  it waited and is never run again). The interrupt leaves `asyncio.run` in every case.
  `OperationRunner._raise_latched` is the one raise point of an interrupt a read or close sees: after
  the report is published it closes the latch and raises its interrupt, with the report attached,
  whatever ended the stream, from every caller that sees the end (the read or close that ended it, a
  read a concurrent close ended); an interrupt handed to the event loop between reads is not raised
  again, since no read or close ends that operation.
  The reader's own cancellation that its read absorbed before cleanup began (handed to
  the read, which ended in its own failure, an interrupt say) is raised as the end, but never recorded
  as a failed close: only a cancellation that arrived while cleanup ran is. The report has one shape for it, applied by
  the runner to whichever report it publishes, the family's or the fallback after a failed finalization
  (`LifecycleHooks.interrupted`, `with_interrupt`: failed, `INTERRUPTED_REASON`, the earlier violations
  kept, one `interrupted` violation last). `close_owned_resources` raises the client's close's first
  interrupt the same way with a latch of its own. A closed latch keeps nothing: an interrupt caught
  after the operation ended (a task outlasting the cleanup window raises it, say) is handed to the event
  loop, which raises it out of `asyncio.run` as it does any task's, once. The
  failure is primary: `finalize_failure` puts its violation ahead of the cleanup failures behind it.
  A library error never carries it as a note, a failed finalization's error included: it carries the
  report (`error.report`). The caller's own
  exception raised inside `async with stream`, which the failed close does not replace, carries one
  note: the report's `cleanup_failure` violation, word for word (`__aexit__` reads it from the published
  report). A close while a read is in flight is an early close: the read it ends raises the close's
  failure (an interrupt included), after the close has run (only an inline kernel consults the closed read's marker; an isolated runner's own close is its CLOSING phase). A
  stream its reader's cancellation ended gives `CANCELLED_REASON`, whatever its source reached. The
  report counts an item as emitted only once `LifecycleHooks.delivered` says the
  reader has it. Every task the library starts around caller code (a source's pull and close, the
  worker-thread hop, the reference scheduler's producer and binding tasks, a wave's or a counted tail's
  page fetch, a stream's read and cleanup, the client's close) runs through
  `spawn_settled`: it returns how it ended (`Settled`) and its owner raises the failure. The batch
  dispatcher's senders, which send many pages' commands at once, hand any failure, an interrupt's too,
  to those pages' futures instead, so the fetch that awaits one raises it. So no task ends in a
  failure, which `asyncio.run` would report at shutdown whether or not the owner
  retrieved it. That includes `KeyboardInterrupt` and `SystemExit`: a task ending in one would stop the
  loop from inside that task and leave its owner pending, so the interrupt travels to the caller's own
  task instead; a worker-thread call a cancellation cut is such a task too, run in the operation's context, so
  its interrupt is latched even though the cancelled call's result is dropped.
- The default response ceiling is enforced while streaming, before JSON decoding.
- Exact sequential, counted, and multi-reference traversal shares one operation-wide
  `max_identity_keys` ceiling and rejects an overflowing page atomically. Sequential and counted
  offset traversal may instead record identities in a caller-owned `IdentityStore`, keeping
  in-process identity memory bounded by one page (per-page fingerprints remain bounded by
  `max_pages`); a reported duplicate withdraws identity assurance.
- Streams publish one immutable terminal report after cleanup. Early close and cancellation never
  claim completion.
- A cancellation is delivered once. Owned cleanup finishes under cancellation protection and
  replays a cancellation it absorbed only when another failure leaves in its place. A request treats
  as pending only a cancellation requested after it began, so a call made in the cleanup of a
  cancelled task is sent, while a later cancellation still ends it, even when a foreign transport
  suppressed it into a response or a failure. An isolated pull that absorbs its reader's
  cancellation by failing in cleanup hands the cancellation back to the reader's next await. A
  stream's close and cleanup defer a cancellation only for the operation's cleanup window (what is
  left of `max_elapsed`, at least the cleanup grace); cleanup still running then finishes in the
  background and the report records it as failed.
- Caller code a stream's read or cleanup runs (a page-stop callback, an async command, binding or
  reference source, an injected transport, or a task one of them starts) cannot close that stream
  or its client while the read or cleanup is in progress, nor at all on a reference-scheduler
  stream, which runs such code between reads too: the close could wait for the code that requested
  it, so it raises `RuntimeError` and changes nothing. Stop a binding with `CallerStop`, end the
  source, or close from the code that iterates.
- The client owns its default transport and active streams; injected transports remain caller-owned.

## Reference scheduling

Every reference source, homogeneous or not, is lowered once into a per-source program before its
binding is admitted; a single pure compatibility check refuses what the operation cannot run as a
correlated local non-execution before any request. One scheduler holds at most one pending pull from
the source, one dispatcher coalesces ready pages of every binding into physical batches whose
commands share decoding options, and one completion gate builds the terminal report after cleanup.

Ordinary and counted bindings reserve rows per page from one shared row pool and release them when
the consumer acknowledges the rows. Under `INPUT` delivery a binding reads its pages ahead: a
validated page waits in the binding's own queue, its rows held in the pool, while the binding goes
on to its next page, so the bindings behind the frontier keep the batches full. Only a binding with a
page stop waits for the consumer to leave each page, as its callback must see a delivered page. Rows
behind the frontier stay held until it advances, so the pool keeps the frontier's next page free, or
the pages its counted tail may still put in flight at once: rows the frontier already holds count
toward that reserve, and the reserve covers the largest page of any admitted binding. A page of a
binding behind the frontier also leaves free the next page of every producing binding between the
frontier and it, as the consumer drains the nearer bindings first; otherwise later bindings would
fill the pool and each page a nearer binding then needs would travel alone. The frontier is never
held back by that room, so it always progresses: either its page fits, or it holds rows the
consumer drains. The pool counts the rows each binding holds, and those of its queued pages, per
binding, and sums the producers' rooms in binding order, so a check scans no reservation. A page that
cannot reserve waits in binding order; a change of the pool wakes only the waiting pages that fit,
nearest the frontier first, and stops at the first page behind the frontier that does not fit, since
every later page keeps more room free. Under `READY` delivery a page's check is a count, and a change
wakes the waiting pages that fit in the order they came. Under both, a page is woken only for rows free
beyond those promised to the pages already woken and not yet resumed, so a release wakes the pages its
rows serve, not every waiting page. Promised rows are the woken page's own: no page or lease asking
later takes them, and a woken page that no longer fits when it resumes (the frontier's reserve grew)
waits again and wakes the waiting pages its promise serves. A counted binding reads its head and its known tail offsets as ordinary pages of
that pool and commits them in offset order. A fast or automatic keyset binding instead holds one row
lease from activation to its
terminal event. Its runtime plans and fits every wave against a view of the operation policy that
differs only in `max_buffered_rows`, equal to the lease; requests, pages, attempts, elapsed time and
response bytes are charged to the single operation context, and each buffered-row change is applied
once to the shared counter. Its waves and its sequential finish pages are commands of the shared
dispatcher, and under `READY` delivery a binding awaiting the consumer's acknowledgement that has
not reached its terminal, and so will hand over another row group or send its next command, is a
pending continuation the dispatcher waits for, as an ordinary binding's page is. A command whose
batch was lost in transit leaves its binding unknown, as a direct page does. The lease never grows:
a binding is granted at least its planning floor and at most the larger of that floor and an equal
share of the pool among active fast bindings, rows kept for ordinary pages excepted. Once the plan
is chosen at the granted lease, a sequential or boundary-only plan, which only drains the rows it
holds and then holds one finish page at a time, trims the lease to that bound and gives the rest
back to the pool; a lane plan keeps it whole. Under `INPUT` delivery the pool hands rows out in
binding order: while a binding waits for its lease, no later binding is granted a lease or reserves
an ordinary page, and the frontier waits for a lease that has not been settled by its binding's plan
yet, since that lease may still shrink. An automatic binding whose floor exceeds the whole pool
waits to run alone with it (under `INPUT` delivery only as the frontier, since rows behind the
frontier cannot be released), a binding that cannot obtain its floor while no other binding can
release rows fails finitely, and every lease wait is bounded by the operation's elapsed budget.

## Package boundaries

Architecture tests enforce one-way imports, absence of removed compatibility modules and module-size
ceilings. The wheel contains only the runtime library and its console entry point; evidence tooling,
tests and credentials are excluded.

The client deliberately has no automatic endpoint profiles, shape-changing Python flags, tuple
payload conventions, public low-level execution plans or mutable global registries.

## Wire, traversal, and evidence boundaries

`Request` owns an explicit route, immutable body encoding, scoped headers, replay safety, and an optional declarative
embedded-result error contract. Legacy transports continue to support ordinary JSON requests;
advanced request representations require an advertised `WireTransport` capability and fail before
I/O otherwise. No ordinary wire value can name an absolute destination. Native REST 3.0 batch commands travel as
an internal JSON-array envelope that the transport must advertise; one physical-attempt classifier
decides, before any retry, whether an envelope failure proves non-execution, attributes per-command
results, or leaves the envelope unresolved for bounded `SAFE` isolation. Isolation, a planned replay of
the envelope's group, sends each `SAFE` command alone, one at a time, and returns its success; a failure stays unknown, because the envelope may have run the
command, and is explained by that envelope like any other earlier send that may have run. When the
budget leaves no isolated send, the command ends unknown with that refusal, caused by the envelope's
failure.

Traversal contracts separately declare progression (`server next`, observed width, or fixed step),
completion (empty confirmation or caller-qualified exact total), collection shape, and identity.
A fixed step without a qualified total fails closed after a short page; a sparse raw bound reads its
qualified extent from a result path or the envelope `total`.
Page evidence contains only bounded counters and enum classifications—never rows, identities,
parameters, headers, URLs, or body fragments.

Fast keyset execution has a planning barrier before emission. Boundary-only and automatic
sequential continuation rely on the same ordered-prefix assurance as sequential traversal; numeric
range and partition plans are caller-asserted and retain positive bound validation on every page.
The separate verifier owns diagnostic canaries and never changes runtime mode. Advisory totals are isolated from
completion and admission. A planner (`keyset_plan.py`) owns boundary analysis, automatic selection
and anchor probing, and freezes a plan of kind, reason and bounds; the pure integer geometry behind
it lives in `keyset_geometry.py`. One runtime (`keyset_runtime.py`) owns wave correlation, ordered
admission, row-buffer deltas, finishing continuation, cleanup, and the immutable selection report.
The planner and the stateless transaction functions reach the runtime only through the
`KeysetTransactionHost` protocol and define no persistent state of their own.

Selection and validation operate on the response's existing frozen tree. A synchronous `PageAdapter`
is invoked once at the two canonical row funnels (driver and fast-keyset receipt) before commit;
published rows alone are thawed. Reference cursor drivers share a capacity-aware dispatcher whose
absolute per-wave coalescing deadline defaults to 20 ms and is skipped when no eligible producer can
add a command. A pulled binding is a producer from the moment it leaves the source, before its task
first runs, until it publishes its terminal event: it stops counting once its cleanup has cancelled
the pages it abandons, before that event may wait for room. A binding refused as a local
non-execution is never a producer. A command whose task has started but has not reached the queue
yet, such as a counted tail page or a fast keyset wave command, counts as one about to arrive, under
either delivery order, until it is queued or its task ends, since its producer may already have left
the runnable set; one that must still reserve its logical page counts only while the page budget
admits it. A counted tail whose next page waits only for its consumer to move past a receipt it
holds is a pending refill: its consumer frees window slots one by one, and those pages coalesce.
Unlike a pending continuation, its own pages already in the batch do not exclude it, the rows the
held receipt returns count toward the next page's reservation (a window may hold the whole row
pool). Under `INPUT` delivery the dispatcher waits for what the consumer frees on its own: it drains
the queued pages of the frontier and of every binding that adds no page any more, up to the first
one that still does, and each binding it passes frees an admission slot for the next binding from
the source. A producer whose next page fits once those rows return counts, under either order, as do
a binding being admitted, a source pull in flight and every pending refill. The consumer draining a
queued page changes none of that projection, as the drained rows count already: it wakes a waiting
wave without the dispatcher projecting again.
