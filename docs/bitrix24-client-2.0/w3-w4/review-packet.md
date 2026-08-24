# W3/W4 review packet — execution instrumentation and batch correlation

> **Superseded after review.** The immutable subject below was rejected. The
> gate table also incorrectly mixed results from that clean SHA with the
> protected working-tree overlay. See `review-findings-v1-resolution.md` and
> `review-packet-v2.md` for the corrected subject and clean-checkout evidence.

## Decision requested

Review immutable implementation SHA
`8f280728fc9ff1a9287cb77109d863e8a63ccfef` and either accept it as the
instrumentation foundation for W5/W6, or return findings against that exact
SHA.

No retry, rate, or batch measurement may authorize a strategy or default until
this checkpoint is accepted. This packet contains no live-performance claim.

## Immutable subject

- W3/W4 implementation SHA: `8f280728fc9ff1a9287cb77109d863e8a63ccfef`
- Parent / W2 acceptance SHA: `82f2d9ddafa50e5229c4191b66b0ad6db4ad4600`
- W2 implementation SHA: `abe42e097b81052fdca5cd6873ef065a1e422d2f`
- W1 implementation SHA: `7fc7b221b91636e13231540d062f824398c80421`
- W0 approved packet HEAD: `90a449e9ba6f7cbc88ee577ef7d996db498cba9b`
- Original baseline SHA: `08277c4d921b83b9252177b3e72a21a4c0c86109`
- Branch: `codex/bitrix24-client-benchmarks`

The subject diff is exactly:

```text
b24api/batch.py          | 521 lines added
b24api/batch_test.py     | 291 lines added
b24api/error.py          |  43 lines added
b24api/execution.py      | 673 lines added
b24api/execution_test.py | 303 lines added
b24api/models.py         |  64 lines changed
6 files changed, 1,893 insertions(+), 2 deletions(-)
```

Review command:

```bash
git diff 82f2d9ddafa50e5229c4191b66b0ad6db4ad4600..8f280728fc9ff1a9287cb77109d863e8a63ccfef
```

## Symbol inventory

W3 adds or materially changes:

- `FailurePhase` and phase-bearing `TransportError`;
- `WorkClass`, `CoordinatorState`, `WireResponse`, and the `Transport`
  protocol;
- `HttpxTransport` with httpcore trace-driven lifecycle classification;
- `CoordinatorSnapshot`, `RateCoordinator`, `_Permit`;
- `ExecutionSnapshot`, `ExecutionContext`, and `Executor`;
- strict success-envelope, server-time, retry-hint, and bounded safe-evidence
  decoders.

W4 adds or materially changes:

- `ReplayDisposition`;
- deeply immutable `BatchSuccess.result` and validated `BatchFailure`
  correlation/replay fields;
- `BatchInput`, `BatchSource`, `FailFastItem`, and `BatchStreamItem` aliases;
- `BatchExecutor` and lazy async/context-managed `BatchStream`;
- PHP-aware batch-envelope decoding, per-command error decoding, total shared
  failure synthesis, and replay-safe direct fallback.

These symbols are module-public but deliberately are not root-exported or wired
into the legacy facade yet. W7 owns compatibility signatures and root exports.

## Semantics under review

### Transport and retry

- Lifecycle phases are `not_dispatched`, `connection_established`,
  `dispatch_started`, `headers_received`, `body_partially_received`, and
  `response_complete`.
- HTTPX trace events provide the last conclusive phase. Unknown transport
  exceptions default conservatively to possible post-dispatch execution.
- `Request.replay_safety is None` is treated as `UNKNOWN`.
- `SAFE` may retry configured transient transport, HTTP, and structured API
  failures within all budgets.
- `UNKNOWN` and `UNSAFE` may retry only when no request bytes could have been
  sent. A failure at or after dispatch raises `AmbiguousExecutionError` after
  exactly one attempt.
- A permit wait is bounded by both operation and per-request elapsed budgets.
  The physical-attempt counter is incremented after permit acquisition and
  immediately before I/O, so a cancelled or timed-out queue wait is not called
  a physical attempt.
- A dispatched attempt remains counted when cancellation or response failure
  follows.

### Rate coordination

- Direct, traversal, batch, and retry work share one coordinator.
- Explicit throttle hints merge by the latest bounded monotonic deadline.
- The coordinator exposes `OPEN`, `COOLDOWN`, and `CLOSED` snapshots.
- Weighted round-robin gives each non-empty class service within a complete
  cycle; a retry queue cannot consume all permits while interactive work waits.
- Proactive governance from `time.operating` is intentionally absent.

### Batch execution

- Construction and context entry perform no I/O; the first item request starts
  bounded input consumption.
- Fail-fast execution sends `halt=true`; tolerant execution sends `halt=false`.
- Both synchronous and asynchronous inputs are consumed one chunk at a time,
  with the portal command cap fixed at 50.
- Stable global command keys preserve input order and payload correlation.
- Tolerant execution returns exactly one `BatchSuccess` or `BatchFailure` for
  every submitted command. A chunk-level error is copied into a separately
  correlated failure for every unresolved command.
- In accordance with Bitrix/PHP serialization, an explicitly present empty
  JSON array in `result_error` means no command errors. A populated associative
  PHP array must arrive as an object. A missing key, non-empty JSON list, or
  other shape is malformed and cannot silently pass.
- `fallback_failed="direct"` is allowed only for an explicitly `SAFE` request
  and a policy-transient or profile-marked fallback-eligible command error.
  Authorization, validation, unknown, and unsafe failures are not rerun.
- Directly recovered successes record `REPLAYED_DIRECT`; failed direct recovery
  records `DIRECT_REPLAY_FAILED`.

## Tests and counterexamples

The 22 focused W3/W4 tests include:

- safe and unknown pre-dispatch retry versus unsafe/unknown post-dispatch
  ambiguity;
- safe partial-body replay with exact physical-attempt/retry counters;
- attempt, retry-delay, total elapsed, permit-wait, and cancellation budgets;
- structured throttle cooldown, latest-deadline merging, class fairness, and
  cancelled-waiter permit cleanup;
- real loopback sockets for connection refusal, post-dispatch close, truncated
  response body, and cancellation after dispatch;
- lazy bounded synchronous and asynchronous batch input;
- fail-fast ordering and payload correlation;
- PHP `result_error: []` acceptance versus missing/non-empty-array rejection;
- per-command errors, missing result keys, and chunk-level total failure
  synthesis;
- mixed safe/unsafe/unknown chunk ambiguity without replay;
- direct fallback eligibility and immutable recovered result values;
- invalid batch size and mapping shape refusal before I/O.

Current static and deterministic gates:

| Check | Result |
|---|---|
| CPython 3.12.10 pytest | 125/125 passed in 2.46s. |
| Focused W3/W4 tests | 22/22 passed. |
| Ruff 0.15.12 full tree, no fix | Passed. |
| mypy 1.20.2 `b24api` | Passed for 23 source files. |
| `git diff --check` | Passed. |

## Sanitization and protected tree

- Socket tests bind only to loopback ephemeral ports and use the literal path
  `test-endpoint`; they contain no portal hostname, webhook, token, principal,
  or response row.
- All protocol bodies are synthetic and bounded.
- No supplied live webhook was used for W3/W4 and none is committed.
- The seven remaining W0-protected user files are byte-identical to their
  recorded SHA-256 hashes:
  `README.md`, `b24api/__init__.py`, `b24api/api.py`,
  `b24api/api_test.py`, `b24api/entity.py`, `b24api/helper.py`, and
  `b24api/entity_types.py`.

## Assumptions

1. A completed connect/TLS event with no send event proves no request bytes
   were sent; it is therefore replayable for every safety class.
2. Any send event or later phase is potentially accepted by the server.
3. A complete HTTP response is authoritative for retry classification only
   through configured statuses/codes; `time.operating` alone is observational.
4. Bitrix batch command keys are opaque strings and retain exact correlation in
   `result` / `result_error` maps.
5. PHP serializes an empty error array as `[]` and a populated associative
   error array as a JSON object; a populated sequential list is not a valid
   Bitrix command-error map.
6. A profile may explicitly add a batch-hostile fallback code later, but W4
   does not infer one from a failure observed once.

## Known gaps and non-authorizations

- The canonical executor and batch stream are not yet connected to
  `Bitrix24.call`, `Bitrix24.batch`, root exports, settings migration, or legacy
  `list_method` compatibility. That is W7 work and intentionally avoids the
  protected dirty facade in this checkpoint.
- HTTPX tracing classifies a response-body receive event conservatively as
  `BODY_PARTIALLY_RECEIVED`; byte-level replay safety does not depend on whether
  zero or several body bytes arrived because dispatch already occurred.
- Chunk shrinking after an operating/time-limit response is not inferred or
  promoted here. It requires reviewed profile evidence.
- No proactive rate limit, automatic dispatch, prefetch, pagination, or
  reference strategy is admitted by this SHA.
- No live portal timing, retry, throttle, or batch artifact was generated.
- The W9 obligations in `docs/bitrix24-client-2.0/w9/review-obligations.md`
  remain open and are not weakened by this packet.

## Dependent decisions

Acceptance of this checkpoint authorizes only:

- W5 to build traversal lifecycle and state machines on `ExecutionContext` and
  `Executor`;
- W6 to build bounded reference scheduling on the reviewed coordinator and
  total batch outcomes;
- W7 to adapt these canonical modules behind compatibility wrappers;
- later evidence harnesses to record retry/rate/batch behavior using this exact
  instrumentation lineage.

It does not authorize a strategy default. Each W5/W6/W10 strategy still needs
its own correctness evidence and required review checkpoint.
