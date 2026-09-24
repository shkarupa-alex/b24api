# Release notes

## Unreleased

- **Breaking (3.0.0):** the `b24api` root exports 51 names instead of 165 (C7). The other 115 moved
  to `b24api.contracts`, `b24api.errors`, `b24api.transport` or `b24api.completion`, which export
  the same objects; nothing was removed or renamed. The old root paths keep working for all of 3.x
  with a `DeprecationWarning` naming the new path, but type checkers report them as `attr-defined`.
  `python -m b24api.migration PATH...` lists every old root import, and the migration guide has the
  full table, generated from `b24api.migration.ROOT_MOVES`. `KeysetSelectionSummary` is exported
  only by `b24api.contracts`.
- `HttpxTransport` is exported from the root.
- Development: `pytest-httpx` is no longer a dev dependency. Tests share one offline responder
  transport and build clients through the public constructor, and the README, recipe and migration
  examples are bound against the real `Bitrix24` signatures (B13, B15).
- `Request.bare(...)` and `Request.v3(...)` build a request with the route fixed; they take the
  constructor's other arguments unchanged, and `v3` keeps the V3 contract (a mapping, JSON only). The
  route stays explicit and `Request(...)` is unchanged (C1, C13).
- `Bitrix24.from_webhook(url, *, http_timeout=None, policy=None)` validates the URL through
  `Settings` and owns the transport and coordinator it creates; `http_timeout=None` keeps the
  `Settings` default.
- The README opens with a quickstart on `crm.deal.list` and `user.get` that needs four concepts:
  the client, a request, `iter_list()` and the report. It runs in a doc-test on the real client (B26).
- **Breaking (3.0.0):** removed never-produced report vocabulary: the `canary_requests`,
  `canary_commands` and `canary_rows` fields of `KeysetExecutionReport`,
  `KeysetAssuranceSource.CANARY_VERIFIED_BOUNDS`, `ReplayDisposition.REPLAYED_DIRECT` and
  `DIRECT_REPLAY_FAILED`, `CompletionAssurance.ORACLE_VERIFIED`, `SnapshotState.VERIFIED` and
  `CHANGED`, and `NotExecutedReason.SCHEDULER_STOPPED` (B11). See the migration guide.
- **Breaking (3.0.0):** `PageValidated` no longer carries `identity_digest`, and the recorders no
  longer hash identities per page; the gate checks event order and `row_count` as before (B12).
- A JSON success body is parsed once, strictly, instead of once by the error codec and again by the
  envelope decoder. Structured errors and malformed bodies keep their previous classification; a
  16 MiB call decodes about 15% faster (B8).
- A public operation stream's report now always carries its own cleanup result: a cleanup failure
  that the underlying kernel did not record is added as a `cleanup_failure` violation, by the same
  §3.1 rules as the kernel reports. Each of the five stream families has a barrier-driven test for
  every row of the §3.1 transition table.
- The rate coordinator and the execution ledger no longer take locks; every state change is
  synchronous between awaits, which a test pins (C8).
- Internal kernel copies of public values carry a `Kernel` prefix (`KernelReferenceItem`,
  `KernelReferenceFailure`, `KernelBatchDispatch`, `KernelDirectDispatch`), so every public class name
  has one definition; an architecture test keeps it that way (D30).
- The PyPI release workflow accepts only canonical stable tags `MAJOR.MINOR.PATCH` (for example
  `2.2.1`, with no `fix-` or `v` prefix) and rejects any other tag before the build backend runs. It
  then requires exactly one sdist and one wheel whose filenames and metadata carry that version, and
  passes `twine check --strict`, before upload; publication still depends on the build job. The
  checkout, setup-python, upload-artifact, and download-artifact actions move to their Node 24 `v7`
  majors.
- A positional request whose slots cannot accept a declared traversal control still raises
  `CapabilityError` with the original positional error as its cause; the message now appends a
  value-free reason (undeclared path, absent slot, missing parent, or near-match casing), and the
  cause is a `ValueError` subclass carrying the closed
  `b24api.contracts.positional.PositionalControlFault`.
- Documented the five-slot positional `task.elapseditem.getlist` route and the optional,
  offline-tested sequential positional keyset profile.
- `iter_list_counted()` accepts a first page with no rows, no `next`, and a missing, `null`, or `-1`
  `total` as an observed empty source. The report completes and is exhausted with `mechanics_only`
  assurance (`identity_exact` with an identity) and never claims a count; `total: 0` keeps its
  count-matched result, and an explicit `ConsistencyPolicy` confirmation of `QUALIFIED_TOTAL` keeps such a page strict.
- Added `Bitrix24.iter_cursors()` with lazy one/many-parent scheduling, per-binding
  `Binding.start_cursor`, strict seed progression, shared physical batching and existing
  fail-fast/tolerant reference semantics. When cursor-control creation is disabled, the complete
  control path including its leaf must already exist; a seed authorizes replacement, not creation.
- Added immutable `PageView` / `AdaptedPage` and the synchronous `PageAdapter` strategy to every
  list traversal. Contract violations now raise value-free `PageAdaptationError`; rejected fast
  keyset pages use the additive `PageRejectionCode.PAGE_ADAPTATION` trace category.
- Added the strict, standalone `verify_keyset_capability()` Python API and `b24api verify-keyset`
  CLI. Unsupported and inconclusive reports use distinct CLI exit codes 6 and 7. Capability
  evidence is stored as deeply immutable `FrozenJson`; `to_dict()` returns detached ordinary JSON.
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
