# W3/W4 review findings v1 — resolution

Rejected subject: `8f280728fc9ff1a9287cb77109d863e8a63ccfef`.

Replacement implementation subject:
`5bec599ef1e47e375ada56f67c856f4c11963719`.

## Resolutions

| Finding | Resolution | Regression |
|---|---|---|
| Granted permit leaked when the waiter was cancelled before resuming. | `RateCoordinator.acquire()` distinguishes a queued cancellation from an already granted future. A granted cancellation decrements `_active` under the coordinator lock and immediately runs fair re-admission. | Release the held permit, cancel the newly granted task before it resumes, assert `active_permits == 0`, then acquire and release a replacement permit under `max_concurrency=1`. |
| PHP `result: []` destroyed all per-command errors. | Both `result` and `result_error` use the same PHP-map decoder: an empty JSON list becomes an empty map, an object remains a map, and a non-empty sequential list is malformed. | A fully failed chunk returns `result: []` plus three typed errors; tolerant mode retains all codes, fail-fast raises the original `BatchCommandError`, and direct fallback remains eligible through the same shape. |
| Missing HTTPX trace events could classify read/write/unknown errors as pre-dispatch. | Write, read, remote-protocol, and otherwise unclassified `httpx.TransportError` failures are clamped to at least `DISPATCH_STARTED`. Only connect/pool failures retain a pre-dispatch classification without send evidence. | A custom external `AsyncBaseTransport` ignores the trace extension and raises after delivery. `UNSAFE` execution performs one attempt and raises `AmbiguousExecutionError`. Separate cases cover read, write, remote-protocol, and base transport errors. |
| `total=-1` was rejected before total semantics could interpret it. | Canonical `Response.total` now accepts exactly `-1` or a non-negative integer. Lower totals are rejected as typed HTTP/protocol evidence errors rather than leaking a raw constructor failure. | `im.recent.list`-shaped `total=-1` is preserved; `total=-2` raises `HTTPGatewayError` carrying status 200 evidence. |
| Early `BatchStream.aclose()` did not close its original input iterator. | The sync/async source adapter retains the actual iterator and calls its `close()`/`aclose()` in `finally`. | Infinite sync and async generators set their own `finally` flags after one emitted result and early stream close. |
| Attempt accounting could increment before a second elapsed check rejected dispatch. | The final elapsed check now occurs before attempt reservation. A successful reservation flows directly into the bounded transport coroutine invocation with no intervening budget rejection. | Permit-wait expiry still records zero attempts; dispatched cancellation still records one. |
| `halt` used an unreviewed JSON boolean shape. | Batch requests encode `halt` as exact integer `1` for fail-fast and `0` for tolerant execution. | Callback transport asserts `type(halt) is int`; fail-fast and tolerant tests assert `1` and `0` respectively. |
| Error text could say post-dispatch while the recorded phase said `not_dispatched`. | Post-dispatch-capable exception classes now share the same clamped phase and matching “possible dispatch” wording. | No-trace exception-class matrix asserts `DISPATCH_STARTED` and `possible_acceptance=True`. |
| The packet attributed overlay gates to the clean SHA. | The v2 packet reports clean-checkout and protected-overlay results separately. Clean full pytest and scoped Ruff/mypy are immutable-SHA evidence. The inherited clean-baseline full-tree Ruff/mypy findings are reported, not hidden. Overlay results are explicitly supplemental and are bound only by the seven W0 hashes. | A detached worktree at the replacement SHA reproduces 115 full tests, 31 focused tests, scoped Ruff/mypy success, and the exact inherited full-tree static findings. |

## Non-impact determinations

- No facade, root export, pagination, profile, or live-evidence code changed.
- No endpoint, webhook, credential, principal, or live response was added.
- The seven protected user files remain byte-identical to their W0 hashes.
- The W9 review obligations remain open.
