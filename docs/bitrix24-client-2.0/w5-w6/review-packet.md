> **Superseded.** The subject `0b9dfe9...` was rejected by human review.
> Use `review-packet-v2.md`, which requests review of the replacement code SHA.

# W5/W6 review packet — bounded traversal foundation

## Decision requested

Review implementation SHA
`0b9dfe9c1e80753c09184f259a021b198ca8d9b1` and either accept it as the
W5/W6 traversal foundation for W7 integration, or return findings against that
exact SHA.

This checkpoint authorizes only the explicit sequential traversal engine and
bounded reference scheduler. It does not authorize a default strategy,
automatic dispatch, prefetch, partitioned traversal, or benchmark admission.

## Immutable subject and lineage

- final implementation SHA: `0b9dfe9c1e80753c09184f259a021b198ca8d9b1`
- W5 initial implementation: `bc75c33`
- W6 initial implementation: `1f2ebe9`
- remediation lineage: `5e2c8a1`, `91b918b`, `6a26c73`, `bf6adca`,
  `e8c9b7b`, `0b9dfe9`
- accepted W3/W4 dependency: `521f0eb7cb107ec948c693496154f94e57dbf7c9`
- branch: `codex/bitrix24-client-benchmarks`

The packet is a later documentation commit. All code, test, and clean-room
claims below refer to the immutable implementation SHA, not to the packet
commit or the dirty protected overlay in the main worktree.

## Implemented boundary

The subject adds:

- explicit single-response, counted-offset, sequential-offset, and keyset
  pagination drivers;
- immutable selector, identity, continuation, consistency, budget, cursor, and
  report contracts;
- transactional page reservations: an HTTP response commits exactly one
  logical page, while pre-response failure or cancellation rolls it back;
- exact duplicate, ordering, total, snapshot, and delivered-prefix accounting;
- bounded direct and batch reference dispatch with input- or ready-order
  output, total tolerant correlation, head-of-line progress, and row
  backpressure;
- lazy/context-managed W4–W6 streams with deterministic owned-resource
  cleanup, stable terminal report identity, and retained late cleanup errors;
- PHP-aware batch envelopes: an empty PHP array is accepted as an empty map,
  while a non-empty JSON array is malformed and cannot silently pass.

No compatibility facade or public default is added here; W7 owns that surface.

## Clean-room review history

Each remediation was frozen as a new SHA and reviewed from a new detached,
clean worktree without the main-tree overlay. Rejected candidates were not
presented for human acceptance.

| Candidate | Clean-room result | Material findings closed by the next candidate |
|---|---|---|
| `1f2ebe9` | Rejected | eager source admission blocked ready output; dispatcher worker cancellation; post-response budget checks; required-snapshot PASS; cursor/identity ambiguity; control collisions after I/O; incomplete W6 uniqueness evidence |
| `5e2c8a1` | Rejected | provisional page counts were not rolled back; cleanup could exceed the operation deadline; delivered-prefix evidence could be lost |
| `91b918b` | Rejected | W5 prefix uniqueness; decoded-response commit race; cancellation-resistant worker ownership; interruptible reservation rollback; blocking source close; affected W4 lifecycle/report gaps |
| `6a26c73` | Rejected | cancellation-resistant input ownership; blocking synchronous pulls; suppressed cleanup errors; partial tolerant-batch correlation; admitted W6 outcome loss; repeated-cancellation report identity |
| `bf6adca` | Rejected | late cleanup observability; cancellation during failed finalization; simultaneous admission/source-error race; post-terminal direct page commit; controller exhaustion/cancellation race |
| `e8c9b7b` | Rejected | a secondary W4/W6 cleanup failure could replace the primary typed iteration failure |
| `0b9dfe9` | **Admitted** | full v1–v6 replay and fresh adversarial audit found no blocking or nonblocking code finding |

The final remediation preserves the primary typed error and the same final
`stream.report`. A secondary cleanup failure is recorded as one blocking
`cleanup_failure`; retained source cleanup errors remain observable from later
`aclose()` calls.

## Final clean immutable-SHA gates

Executed with CPython 3.12.10 in a detached clean worktree at exactly
`0b9dfe9c1e80753c09184f259a021b198ca8d9b1`:

| Check | Result |
|---|---|
| Full clean pytest | 207/207 passed |
| Focused W4–W6 pytest | 97/97 passed |
| Scoped Ruff, six affected source files | Passed |
| Scoped strict mypy, six affected source files | Passed |
| `git diff --check` and `git status --porcelain` | Clean |
| Targeted primary/cleanup regressions with asyncio debug and warnings as errors | 2/2 passed |

The clean repository-wide baseline still has exactly the disclosed inherited
W2 debt in untouched `b24api/api_test.py`: three Ruff findings and 24 strict
mypy `union-attr` findings. The W5/W6 scoped files add none.

The supplemental protected overlay in the main worktree passes 226/226 tests,
97/97 focused W4–W6 tests, repository-wide Ruff, and strict mypy for 27 source
files. Those numbers are not attributed to the immutable SHA.

## Adversarial evidence replayed on the final SHA

The independent review replayed every prior finding and specifically verified:

- 500/500 simultaneous admission/source-error races in both READY and INPUT
  ordering;
- cancellation during failed finalization and repeated cancellation with the
  same frozen report on W4, W5, and W6;
- no raw `StopAsyncIteration`, PEP 479 conversion, unretrieved task/future, or
  post-terminal logical-page mutation;
- exact tolerant correlation for partial batch input and already-admitted
  references;
- bounded synchronous and asynchronous source pulls and cleanup;
- cancellation-resistant batch-worker and late direct-response ownership;
- delivered-prefix uniqueness when cancellation occurs inside a page;
- primary `ProtocolError` precedence over a secondary cleanup failure in both
  W4 and W6, with cleanup evidence retained.

The exact external v5/v6 repro scripts also passed under
`PYTHONASYNCIODEBUG=1` and `-W error`.

## Known contract boundary

A custom `Transport` that both ignores its supplied `attempt_timeout` and
suppresses cancellation can hold a public await beyond `max_elapsed`. This is a
nonconforming transport boundary, not an authorization for unbounded built-in
I/O: `HttpxTransport` is cooperative, and the transport contract receives the
remaining attempt deadline. W7 documentation must state this requirement for
injected transports.

## Non-authorizations and open obligations

- No live webhook, portal data, or write operation was used for W5/W6 review.
- `total=-1` remains preserved protocol data; W5/W7 must not assign endpoint
  semantics without provenance.
- Early close owns and closes the supplied iterator; W7 must document the
  compatibility consequence.
- No optimization, profile, benchmark threshold, or default is admitted.
- W7 compatibility work and all W9 evidence, seed, manifest, cleanup, and leak
  obligations remain open.
- The binding follow-ups remain in
  `docs/bitrix24-client-2.0/w5-w7/review-obligations.md` and
  `docs/bitrix24-client-2.0/w9/review-obligations.md`.
