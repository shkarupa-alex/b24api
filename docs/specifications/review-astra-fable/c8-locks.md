# C8: locks in the rate coordinator and the execution ledger

Design record for decision C8 of the astra-fable review. It gives the outcome for each lock in
`b24api/execution/rate.py` and `b24api/execution/context.py`. Like every record in this archive, it
describes the tree at the time it was written.

## Rule

The spec allowed a lock to go only after checking two things: no guarded section awaits, and nothing
mutates the same state concurrently. A lock that guards a real race stays, with a race test.

## Why both locks were inert

- Both objects live on one event loop, and no thread reaches them. The only `asyncio.to_thread` in the
  package runs a caller's synchronous iterator (`_sources.py`) and never touches either object.
- No guarded section contained an `await`. The waits sat outside the lock: `asyncio.sleep` in the
  coordinator's wake task, and `_page_changed.wait()` in `reserve_page`.
- The coordinator's `asyncio.Condition` was never waited on or notified, so it acted as a plain lock.
- A lock that is never held across a suspension can never be found taken. `asyncio.Lock.acquire`
  then always takes its fast path, which returns without yielding. So the lock added no exclusion
  and no cancellation point.
- `_Permit.release_now`, `commit_page` and `release_page` already mutated the same state with no lock.
  They were correct for the same reason, and the removal relies on nothing more than they did.

## Outcomes

| Lock | Guarded methods | Outcome |
|---|---|---|
| `RateCoordinator._condition` | `acquire` (enqueue, and the cancel/timeout rollback), `observe_operation_time_limit`, `observe_throttle`, `close`, `snapshot`, `_wake_after_cooldown` | Removed. The `_locked` suffix is dropped from the helper names, since no lock exists. |
| `ExecutionContext._lock` | `start`, `reserve_attempt`, `record_retry`, `record_cooldown`, `reserve_page`, `reserve_pages`, `set_buffered_rows`, `adjust_buffered_rows`, `snapshot` | Removed. |

The public methods stay `async`, so callers see no change. Both class docstrings now state the
invariant: every state change is synchronous code between awaits, and a mutator must never await in
the middle of an update.

## Evidence

`tests/internal/lock_free_state_test.py`:

- Each formerly locked mutator on both objects runs to completion in one coroutine step, so no other
  task can interleave with it. An `await` added in the middle of an update fails this test.
- 60 concurrent admissions across every work class, with 20 random cancellations, never hold more
  permits than the capacity and leave no permit or queue entry behind.
- 40 concurrent page reservations with random commit or release stay inside the page budget, and
  waiters are woken as capacity returns.

Six cancellation-race regressions used to hold the ledger lock to park a task at a chosen await: two
each in batch, pagination and references. They now use `tests/ledger_hold.py`, which does the same
thing on one instance: while held, every async ledger call waits. The tests now wait until the task
is actually parked, instead of spinning a fixed number of loop ticks.
