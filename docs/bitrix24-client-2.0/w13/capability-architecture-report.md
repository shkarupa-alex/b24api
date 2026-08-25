# b24api 2.x capability and architecture implementation report

Date: 2026-08-25

Runtime candidate: `a07b104b90d6c7a69455c34f72d0aa7dd18c87dc`

Frozen pre-refactor comparator: `e5fd427eddb28b7079b75bfef6443c04aa6350d1`

Frozen 1.0.1 structural comparator: `08277c4d`

Scope: correctness-first client release. This report does not claim live latency admission, a fast
generic no-count traversal, or application-specific filter correctness.

Final blocking lists:

```text
missing=[]
regressed=[]
```

## Five-way business capability matrix

| Business gate | Baseline capability or risk | Required v2 outcome | Concrete evidence | Missing/regressed disposition |
|---|---|---|---|---|
| Old failure prevention | Partial batch, unsafe replay, control desynchronization, counted overlap and identity-path mismatch | Every sampled mechanical failure is prevented, typed or failed closed; application-only failures are not overclaimed | `tests/issues_test.py`, nine exact issue tests; full correctness suite | Complete; `missing=[]`, `regressed=[]` |
| Capability preservation | Direct/response, batch, generators, offset/count/keyset/cursor and reference workflows were spread over canonical and compatibility APIs | Every useful generic capability has one typed operation or an explicit conservative replacement | Preservation ledger below, `tests/client_v2_test.py`, executable README examples | Complete; `missing=[]`, `regressed=[]` |
| Correlation | Payload support was shape-dependent and could disappear on failures | `Command[C]` and `Binding[C]` correlation remains off-wire and survives every correlated terminal state | `test_command_outcome_partition_retains_every_closed_variant`, `test_reference_partition_retains_items_empty_completion_and_negative_variants`, B1/B4 tests | Complete; `missing=[]`, `regressed=[]` |
| Legacy removal | Compatibility wrappers, profiles, duplicate routes and aliases created a second API | One typed public surface; automatic profiles and compatibility scaffolding absent | Root-export snapshot, `tests/architecture_test.py`, wheel-content gate, dependency/symbol searches | Complete; `missing=[]`, `regressed=[]` |
| Architecture and operations | State machines were concentrated behind low-level plans and there was no installed CLI | Thin facade, typed mechanics, bounded streams/transport, explicit ownership and installed JSON/JSONL CLI | Import/module-size gates, lifecycle tests, runtime profile, CLI suite and wheel test | Complete; `missing=[]`, `regressed=[]` |

## Detailed capability-preservation ledger

The disposition vocabulary is the closed set from specification §32.9. Every evidence entry below
is executable or points to the reproducible runtime profile. No planned evidence remains.

| Capability | v1/current form | Final v2 form | Concrete evidence | Disposition | Missing/regressed? |
|---|---|---|---|---|---|
| Decoded direct call | `call()` | `call()` | `test_call_and_call_response_have_stable_detached_types`; README Direct calls | preserved directly | No |
| Immutable response envelope | `call(raw=True)` | `call_response()` | same test; `test_response_is_deeply_immutable_and_selectors_are_exact` | preserved under a different primitive | No |
| Replay control | `retry` boolean and unsafe historical behavior | `Request.replay_safety` plus typed policy | B2; `test_safe_and_unknown_retry_only_when_replay_is_proven`; ambiguous-dispatch tests | preserved under a different primitive | No |
| Finite physical batch | `batch()` | logical `batch()` over `Command[C]` | `test_logical_batch_is_unbounded_ordered_and_correlation_is_strictly_off_wire`; physical batch suite | preserved directly | No |
| Unbounded logical batch | kernel capability | canonical `batch()` | `test_public_logical_batch_accepts_100k_generator_without_input_materialization`; capability profile | preserved directly | No |
| Tolerant batch | `errors="yield"` / outcome path | `batch_outcomes()` four-state union | B1; `test_batch_outcomes_retains_typed_failure_without_halting_later_commands` | preserved under a different primitive | No |
| Sync generator input | tuple/request conventions | `Iterable[Command[C] | Binding[C]]` | source success/failure/cancel/early-close tests in batch/reference suites | preserved under a different primitive | No |
| Async generator input | unevenly exposed | `AsyncIterable[Command[C] | Binding[C]]` | B4; bounded-admission and exact source-close tests | preserved directly | No |
| Off-wire payload | tuple plus `with_payload` | generic `correlation` | B1/B4; wire assertions in `tests/client_v2_test.py` and capability profile | preserved under a different primitive | No |
| Sequential offset list | `list_sequential` | `iter_list` | `test_iter_list_is_sequential_mechanics_only_and_report_is_post_cleanup`; pagination contradiction suite | preserved under a different primitive | No |
| Counted list | `list_batched` | `iter_list_counted` | B5; frozen request/identity test; `1/2/5` runtime profile | preserved under a different primitive | No |
| No-count keyset | `list_batched_no_count` / `list_keyset` | `iter_list_keyset` | C1b; exact keyset progress and empty-confirmation tests | preserved under a different primitive | No |
| Dependent cursor | cursor wrappers | `iter_list_cursor` | strict monotonic, coercion, repeated-boundary and empty-confirmation tests | preserved under a different primitive | No |
| Permissive repeated cursor boundary | endpoint-specific dedup | no permissive generic traversal | C5 fail-closed regression; README limitation | intentionally removed as unsafe | No |
| Independent fan-out | reference/batch wrappers | `fan_out()` / `fan_out_outcomes()` | ready/input ordering, direct and multi-window batch tests | preserved under a different primitive | No |
| Bound reference traversal | `reference_*` wrappers | `Binding[C]` plus `iter_references()` | B4; nested update, parent correlation, partial rows and completion tests | preserved under a different primitive | No |
| Empty reference success | conflated with no emitted rows | `ReferenceComplete(row_count=0)` | `test_bound_references_apply_nested_updates_off_wire_and_emit_exact_completion` | preserved under a different primitive | No |
| Response selector | `result_key` and plan selectors | `ResultSelector` relative to `Response.result` | B4; selector and detached response tests | preserved under a different primitive | No |
| Mapping-shaped collections | implicit shape handling | explicit `ResultCollectionShape.MAPPING_VALUES` | `test_mapping_values_shape_preserves_mapping_insertion_order` | preserved under a different primitive | No |
| Page-size control | wrapper-specific `list_size` / `LIMIT` | explicit `limit_path`, `page_size` cap and conflict validation | B3; control injection, reverse counted-cap and reference-local-cap tests | preserved under a different primitive | No |
| Early close and cancellation | shared kernel behavior | `OperationStream` terminal contract | context break, client close, cancellation and cleanup-failure suites | preserved directly | No |
| Partial bounded consumption | manual `break` | `first()` / `collect(limit=...)` | `test_partial_helper_closes_without_claiming_completion` | preserved under a different primitive | No |
| Final operation report | intermediate mutable report | immutable post-cleanup `OperationReport` | report freeze, exactly-once CLI and cleanup-error identity tests | preserved under a different primitive | No |
| Endpoint profiles | runtime profile selection | explicit typed mechanics beside each operation | root/package audit and absence gate | intentionally removed as unsafe | No |
| Shell direct/list use | no installed final CLI | `b24api call` / `b24api list` | `tests/cli_test.py`; built-wheel entry-point test | preserved under a different primitive | No |
| Business-filter proof | caller assumption | explicitly application-owned | C34/C35 and README limitations | intentionally removed as unsafe | No |
| Low-level plan composition | public/internal-plan knowledge | typed list/reference methods | facade and root-export tests; README mapping | preserved under a different primitive | No |

## Sampled issue traceability

Production code contains no issue-specific endpoint rule. The endpoint-shaped fixtures exist only
in tests.

| Issue | Executable evidence | Result | Final disposition | Application remainder |
|---|---|---|---|---|
| B1/B1a/B1b | `test_issue_b1_tolerant_batch_preserves_all_correlated_states`; tolerant reference suite | PASS | fixed mechanically | Consumer handles every tolerant variant |
| B2 | `test_issue_b2_unknown_write_is_not_replayed_after_dispatch`; replay companion tests | PASS | fixed mechanically | Reconcile an ambiguous write |
| B3 | `test_issue_b3_limit_conflict_rejects_before_network`; equal-control tests | PASS | fixed mechanically | Supply the real endpoint control path |
| B4 | `test_issue_b4_async_binding_correlation_and_selector` | PASS | preserved capability | Business-date stopping stays outside the client |
| B5 | `test_issue_b5_counted_stride_uses_observed_head_width`; missing-next regression | PASS | fixed mechanically | Assert stable offset and filtered-total applicability |
| C1b | `test_issue_c1b_distinct_item_filter_order_paths_are_exact` | PASS | fixed mechanically | Supply concrete item/filter/order casing |
| C5 | `test_issue_c5_repeated_cursor_boundary_is_not_reported_complete` | PASS | fail-closed, application contract required | Use a unique tie-breaker or direct reconciliation |
| C34 | `test_issue_c34_ignored_filter_requires_application_reconciliation` | PASS | application-only, no client fix claimed | Validate expected owners/business set |
| C35 | `test_issue_c35_overmatched_multifield_is_not_claimed_verified` | PASS | application-only, no client fix claimed | Sweep/classify or reconcile independently |

## Business failure ownership

| Failure class | Generic client responsibility now enforced | Application responsibility |
|---|---|---|
| Pagination non-progress / contradictory continuation | typed INCOMPLETE, never normal exhaustion | choose an applicable traversal |
| Counted overlap, missing page, duplicate or total mismatch | observed stride plus exact range/identity/total validation | assert stable offset and filtered total semantics |
| Ambiguous post-dispatch write | unknown outcome and no automatic replay unless SAFE | reconcile business state |
| Partial batch | exact four-state command outcomes | choose fail-fast or handle every tolerant state |
| Caller correlation | retain by reference and keep off-wire | choose correlation meaning/storage |
| Item/filter/order casing | independent typed paths/keys | provide endpoint-specific casing |
| Repeated child IDs across parents | identity scope per binding plus parent correlation | choose composite storage identity |
| Ignored/overmatching filter | do not overclaim semantic proof | independently validate expected set |
| Equal/non-unique cursor | reject when strict progress cannot be proved | provide tie-breaker or direct workflow |
| Early close/cancellation | stop admission, close ownership and report non-completion | never present partial consumption as complete |
| Oversized response | abort streaming at byte ceiling | raise ceiling explicitly only when justified |

## Repeat-review closure

The post-implementation adversarial review found several real boundary defects. They are closed on
the runtime SHA above rather than waived:

- every reference traversal now propagates its public `page_size` as the scheduler's local page
  cap even when no endpoint-specific wire `limit_path` exists;
- an empty page retaining `next` is a pagination contradiction and finalizes `INCOMPLETE`, never
  `COMPLETED`; a repeated pull re-raises the same typed terminal error and report;
- local binding rejection emits correlated `ReferenceNotExecuted(LOCAL_VALIDATION_FAILED)`, while
  per-reference pagination contradictions carry `IncompleteTraversalError`;
- a post-I/O counted-reference capability contradiction, including a missing exact total, also
  carries `IncompleteTraversalError`; pre-I/O capability rejection remains a capability error;
- `CountedTraversal` with `DirectDispatch` rejects before the binding source is pulled or any I/O;
- user-generator `TypeError`/`ValueError` is `SOURCE_FAILED`; only the client's own malformed-item
  check is `LOCAL_VALIDATION_FAILED`;
- early batch close reports the whole already-admitted physical window separately from the number
  of outcomes actually emitted;
- source failures in fan-out account every known command, then raise `InputSourceError`; they do
  not fabricate `CommandNotExecuted` for an exception or malformed value that has no correlation;
- source-specific report counters are explicitly wired and validated instead of discovered with
  `getattr(..., 0)` defaults;
- the installed-wheel test no longer depends on a warm package-manager cache and proves that the
  imported `b24api` module comes from the isolated wheel environment;
- the root surface now includes the public construction/inspection types required by its own
  cursor, report and injected-transport contracts. The owner amendment records this narrow
  manifest correction.

The exact full suite has 504 passing tests. The concurrent-pull and in-flight-close regressions,
typed terminal-error matrix, page-cap reverse direction and wheel entry-point checks are included.

## Architecture and legacy audit

- `Bitrix24` is a composition facade; algorithms remain in batch/traversal/reference layers.
- Contracts import no I/O layer; transport imports no batch/traversal/reference layer; runtime
  imports no evidence tooling.
- CLI validates non-finite JSON, method syntax and the complete closed list contract before client
  construction; it imports the root contracts and its closed contract router, never execution drivers.
- Production contains no Tasks/CRM/IM catalog and no mutable global registry.
- Root exports contain only the frozen v2 facade, contracts, outcomes, helpers and errors.
- Old wrappers, shape-changing flags, public plans/profiles, tuple payload input, top-level
  `api.py`/`helper.py`/`models.py`/`plans.py`/`profiles.py`/`protocol.py`/`query.py`/`type.py`,
  SQLite identity tracking and cardinality limits are absent.
- Ordinary modules remain at or below 400 lines. Explicit orchestration state machines remain at
  or below 700 lines. `tests/architecture_test.py` enforces both limits and layer boundaries.
- The wheel contains `b24api`, its installed console entry point and no tests/evidence/live tools.

## Performance and memory result

The exact measurements, commands and allocation-site interpretation are in
[`runtime-profile.md`](runtime-profile.md).

- Counted request and identity parity with frozen 1.0.1 is `1/2/5` requests for
  19/500/10,000 rows across dense and sparse cases.
- The exact refactor comparator used two warm-ups plus seven measured samples in two independent
  reproductions. No stable cell reproduced a regression above 10%.
- A 100,000-command generator stayed at seven buffered commands; its measured peak was below the
  10,000-command peak and correlation never entered wire JSON.
- Exact counted identity tracking above 100,000 rows continues in memory and warns once. There is
  no database, spill file or artificial identity-count failure.
- Memray 1.20.0 measured a 5,121,452-byte dense-10k peak. Immutable JSON thawing is the largest
  cumulative client allocation site; no retained stream/task resource or leak was observed.
- Response buffering, decoded-row ceilings, active-reference bounds and 100-iteration retention
  gates passed.
- The fresh deterministic benchmark on `a07b104` published 200 model oracles and 182 immutable evidence
  references in 208 evidence files (plus the persistent transaction lock), left zero pending
  markers and passed a separate `_scan_bundle` invocation on the exact runtime SHA.

## Known boundaries and intentional non-preservation

- Wall-clock behavior on a real portal is not admitted by deterministic local fixtures.
- Generic no-count remains exact sequential keyset traversal and may use more requests than unsafe
  historical shortcuts. `PartitionedKeysetPlan` is outside this release.
- Non-unique repeated cursor boundaries require application reconciliation.
- Portal acceptance of a filter's business meaning cannot be proven generically.
- Replay safety is a caller assertion; ambiguous writes still require verification.
- Correlation is opaque caller state and is deliberately excluded from wire data and diagnostics.
- No recipe or skill migration is part of this change. The sampled recipe/issue corpus was used
  read-only to establish capabilities and bounded regressions.

## Final business outcome

The implementation preserves the useful direct, response, logical batch, generator, list,
keyset, strict cursor, fan-out, bound-reference and off-wire-correlation capabilities without
retaining the old names as wrappers. It strengthens incomplete/ambiguous behavior, keeps the
frozen counted network shape, exposes bounded ownership and terminal reports, installs a compact
CLI and removes endpoint/profile/compatibility scaffolding. Within the declared correctness-first
scope, every required capability is evidenced and no accepted capability is missing or regressed.
