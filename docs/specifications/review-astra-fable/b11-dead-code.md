# B11: dead code found by vulture

Design record for finding B11 of the astra-fable review. It lists every `vulture b24api
--min-confidence 60` finding and what happened to it. Like every record in this archive, it describes
the tree at the time it was written.

## Rule

- Remove internal state that is written and never read, and functions that nothing calls.
- Remove a public output value (a report field or an enum member) when no code path can produce it.
  Such a value is a false promise: a caller who matches on it writes a branch that can never run.
- Keep every input member (a value a caller passes in), even one the library itself never passes.
- Keep names that vulture cannot see being used: public client methods, pydantic hooks, dataclass
  fields read by callers or through reflection, and test seams.

## Removed

Internal state and code (part 1, and the second sweep):

| Finding | Why it was dead |
|---|---|
| `KernelReport.plan_id`, `KernelReport.dispatch_id` | written by every family, read by none |
| dispatch `_accounted` | bookkeeping set that no branch consulted |
| `work_index` on `_KernelReferenceComplete` and `_KernelFanOutSuccess` | never read; outcomes carry `binding_index` |
| `LaneSpec.owns_output`, `LaneState.retained` | set at construction, never read |
| `LaneReceipt.page_full`, `LaneReceipt.last_identity` | never read |
| `PageObservation.page_full` and the `effective_page_cap` parameter of `page_observation` | only fed `page_full` |
| `OrderedAdmissionState.last_identity`, `AdmissionCommit.unique_rows` | never read |
| fast-trace `phase_empty`, fast-stream `phase_commands` and `phase_rows` | counters no report exposes |
| `AnchorFacts.probe_commands` | never read |
| `CANARY_COMMANDS` and the `canary_commands=` parameter of `estimates()` | the fast path sends no canaries; `planning_waves` is `max(1, ceil_div(target_lanes, capacity))` |
| fast-runtime canary branches in `keyset_transactions`, `page_validation`, `keyset_capability`, `keyset_observation` and the fast trace classifier | the fast scheduler never plans a canary phase |
| `DiagnosticContext.alias` | no caller; `alias_text` is the used entry point |
| `RateCoordinator._release` | no caller; `_release_now` is the used path |
| `_BatchPageDispatcher._worker` | written, never read; `_workers` owns the tasks |
| `_ProducerState.source_terminal` | written by the scheduler, read only by two test assertions |
| `Bitrix24._settings` | written in both constructors, never read; the policy is resolved once at construction |
| `b24api.references.stream.fan_out` | no production caller (the client uses the fan-out family); moved into `tests/references_test.py` as a local helper |

Public output vocabulary (part 2, breaking, documented in the migration guide):

| Finding | Why no code path produced it |
|---|---|
| `KeysetExecutionReport.canary_requests`, `canary_commands`, `canary_rows` | always zero since the fast path stopped sending canaries |
| `KeysetAssuranceSource.CANARY_VERIFIED_BOUNDS` | range and partitioned reports use `CALLER_ASSERTED_BOUNDS` |
| `ReplayDisposition.REPLAYED_DIRECT`, `DIRECT_REPLAY_FAILED` | a physical batch is never replayed as direct calls |
| `CompletionAssurance.ORACLE_VERIFIED` | no oracle implementation exists |
| `SnapshotState.VERIFIED`, `SnapshotState.CHANGED` | no snapshot oracle exists; a required snapshot reports `UNVERIFIED` |
| `NotExecutedReason.SCHEDULER_STOPPED` | no scheduler stop path records it |

A caller who wants the old guarantee that normal keyset traversal sends no canaries can check that
no `page_trace` record has phase `KeysetPhase.CANARY`. The test suite asserts exactly that.

## Kept

| Finding | Reason |
|---|---|
| `Bitrix24.call_bytes`, `batch_outcomes`, `fan_out`, `fan_out_outcomes`, `iter_cursors`, `iter_reference_outcomes` | public client API |
| `Bitrix24._from_executor` | test construction seam over an injected executor |
| `CompletionDecision.bindings_admitted`, `bindings_terminal`, `pages_scheduled`, `pages_acknowledged` | decision evidence that tests assert |
| `ContinuePage.CONTINUE` | page-stop input value a caller's policy returns |
| `UnknownRequestCollector.summaries` | public observation property |
| `SnapshotRequirement.FROZEN_MANIFEST`, `INDEPENDENT_PRE_POST_ORACLE`, `ConfirmationPolicy.INDEPENDENT_ORACLE` | input members; a request for them is refused explicitly |
| `binding_index`, `stop_reason`, `completions` on reference outcomes | public outcome fields |
| `ResultErrorShape.SEQUENCE_ITEMS` | chosen by the `else` branch of the result-error classifier |
| `ResponseTime.date_start`, `ResponseTime.date_finish` | pydantic model fields parsed from the envelope |
| `Response.list_items` | public helper |
| `CoordinatorSnapshot` fields | public snapshot read by callers |
| `KeysetPhase.CANARY` and `canary_commands()` | used by `verify_keyset_capability()` |
| `Settings.model_config`, `_sanitize_invalid_webhook_input`, `_serialize_webhook_url` | pydantic configuration and hooks |
| `ScriptedTransport.calls`, `assert_exhausted` | public testing API |
| `__suppress_context__` | standard exception attribute set to chain a tail failure |
