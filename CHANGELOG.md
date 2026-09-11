# Release notes

## Unreleased

- Added `Bitrix24.iter_cursors()` with lazy one/many-parent scheduling, per-binding
  `Binding.start_cursor`, strict seed progression, shared physical batching and existing
  fail-fast/tolerant reference semantics.
- Added immutable `PageView` / `AdaptedPage` and the synchronous `PageAdapter` strategy to every
  list traversal. Contract violations now raise value-free `PageAdaptationError`.
- Added the strict, standalone `verify_keyset_capability()` Python API and `b24api verify-keyset`
  CLI. Unsupported and inconclusive reports use distinct CLI exit codes 6 and 7.
- Normal range and partitioned `iter_list_keyset()` execution no longer sends runtime canaries.
  It reports the new `KeysetAssuranceSource.CALLER_ASSERTED_BOUNDS`; consumers using exhaustive enum
  matching must handle that additive member. A broken endpoint may now yield a partial prefix before
  active page validation raises a typed incomplete traversal.
- AUTO estimates no longer include canary waves, and the canary-prefix-only small-selection branch
  was removed. `KeysetSelectionReason` is unchanged and `SMALL_SELECTION` remains a cost result.
- `BatchDispatch.coalesce_wait` defaults to an absolute, capacity-aware 20 ms deadline for each
  underfilled physical wave. Set it to zero for low latency. Parallel workers preserve aggregate
  work but do not promise an exact split. The unreachable `None` queue sentinel was removed; live
  shutdown still cancels workers and pending futures.
- Selection, shape checks, identity validation and fingerprints now use the response's existing
  frozen JSON tree; only published rows are thawed.
- Legacy `KeysetAssuranceSource.CANARY_VERIFIED_BOUNDS`, `KeysetPhase.CANARY`, and canary report
  fields remain source/deserialization compatible. Normal runtime leaves their counters at zero.
