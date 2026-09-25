# Outcome registry: astra-fable review (#14, #15)

This registry gives the final outcome of every ID in §2 of the review specification, with the
evidence §6.2 asks for. Commits are on `feature/review-astra-fable` and nothing has been pushed.
Test paths are `file::test`. The last section lists what remains for the owner.

- **Done.** The change is committed, and the named test passes on the branch head.
- **Decided.** A rejected part of a C item, or a D item, with its reason.
- **Measured.** A recorded measurement, with its result.
- **Open.** Not achievable by code in this program. Each open item is listed in the last section.

## A: defects

| ID | Outcome | Commit | Evidence |
|---|---|---|---|
| A1 | Done | `4cd3998` | `tools/b24api_evidence/harness/live_test.py::test_live_portal_httpx_info_record_carries_no_webhook_token`, `::test_live_portal_sync_attribution_leaves_foreign_records_on_the_same_logger_unchanged` |
| A2 | Done; L2 confirmed | `15b5155` | `tests/internal/bounded_decoding_test.py::test_single_gzip_bomb_is_refused_inside_the_decompressor`, `::test_stacked_gzip_cascade_is_refused_before_decompression`; [L2](live-probes.md#l2-a2-content-encoding-and-the-explicit-accept-encoding) |
| A3 | Done | `228d0f8`, `866a47f`, `c6c7164`, `3e0d557`, `05bca75`, `b60a9b8`, `a9fbde5` | `tests/internal/execution_boundary_test.py::test_direct_replay_matrix`, `::test_physical_batch_replay_matrix`, `::test_permanent_transport_refusal_is_raised_once_instead_of_exhausting_the_budget`, `::test_physical_batch_after_an_unstructured_transient_status`, `::test_structured_refusal_of_a_safe_batch_and_a_transient_status_on_a_direct_call_keep_their_retry`, `::test_a_mixed_batch_that_may_have_run_sends_only_its_safe_commands_again`, `::test_commands_refused_by_their_own_transient_error_are_sent_again_alone`, `::test_a_configured_retry_code_or_status_does_not_prove_an_unsafe_request_never_ran`, `::test_a_replay_round_that_was_sent_keeps_its_own_outcome_when_the_budget_stops_the_next_attempt`, `::test_replay_rounds_and_the_retries_inside_them_share_one_attempt_budget` |
| A4 | Done | `06dfa00`, `de451d5` | `pydantic>=2.12.0` in `pyproject.toml`; the `min-deps` job (`tests/release_workflow_test.py::test_blocking_jobs_run_the_specified_checks`) |
| A5 | Done | `43fd34d` | `tests/internal/rate_coordinator_test.py::test_client_closes_its_own_coordinator_and_awaits_the_wake_task` |
| A6 | Done | `06dfa00`, `de451d5`, `05793ab` | `tests/release_workflow_test.py::test_wheel_check_refuses_an_untyped_wheel_or_files_beside_the_package`, `::test_wheel_typing_passes_every_canonical_import_and_flags_an_old_root_import` |
| A7 | Done | `93f5494`, `3bccfe4`, `c6c7164` | `tests/diagnostic_rendering_test.py::test_known_v3_code_is_diagnosable_in_str_repr_and_safe_dict`; the §3.9 matrix in `tests/redaction_property_test.py`; `tests/diagnostic_rendering_test.py::test_long_v3_field_is_aliased_before_its_text_is_bounded` |
| A8 | Done; L3 confirmed | `306cde0` | `tests/counted_rules_test.py::test_table_r_batched_counted_path`, `::test_table_r_sequential_counted_path`; [L3](live-probes.md#l3-a8-the-counted-validator-on-real-responses) |
| A9 | Done; L5 confirmed | `6cb745f` | `tests/offset_preflight_test.py::test_exact_qualified_total_refuses_a_nonzero_start_before_io`, `::test_disabled_total_termination_keeps_the_suffix_traversal`; [L5](live-probes.md#l5-a9-no-start--0-with-total-only-closure-in-examples-and-documentation) |
| A10 | Done | `d54dda7` | `tests/keyset_verifier_boundary_test.py::test_failed_boundary_read_is_an_unsupported_report` |
| A11 | Done | `37c3de2`, `a693344`, `cdf21d3`, `c6c7164`, `e2ec5c2` | `tests/stream_lifecycle_test.py::test_early_close_with_failing_source_close_publishes_one_report`, `::test_close_before_the_first_pull_closes_the_opened_source_once`, `::test_failing_source_close_before_the_first_pull_is_a_cleanup_failure`; `tests/batch_test.py::test_internal_outcome_stream_closed_before_the_first_pull_closes_its_source_once`; `tests/internal/runner_transitions_test.py::test_failing_finalizer_keeps_a_simultaneous_cleanup_failure`; `tests/internal/runner_transitions_test.py::test_failing_kernel_finalizer_still_publishes_one_public_fallback_report`, `tests/internal/runner_transitions_test.py::test_failing_fallback_hook_still_ends_publication` |
| A12 | Done; breaking, in the migration guide | `37c3de2` | `tests/stream_lifecycle_test.py::test_early_close_is_reported_as_closed_before_exhaustion`; golden delta A12 |
| A13 | Done; breaking, in the migration guide | `228d0f8`, `c6c7164` | `tests/internal/execution_boundary_test.py::test_unknown_transport_exception_is_a_non_retryable_dispatch_started_failure`, `::test_unknown_transport_exception_is_ambiguous_for_unproven_direct_requests`, `::test_batch_outcomes_are_total_after_an_unknown_transport_exception`; `tests/internal/execution_boundary_test.py::test_closed_default_transport_refuses_before_dispatch_without_implying_acceptance` |
| A14 | Done | `37c3de2` | `tests/stream_lifecycle_test.py::test_fail_fast_batch_reports_the_command_failure_not_its_carrier`; golden delta A14 |
| A16 | Done | `d702e06`, `5bf1801` | `tests/internal/owned_source_test.py::test_sync_source_retains_bounded_violations`, `::test_async_source_retains_bounded_violations` |
| A17 | Done | `a4fb7c0` | `tests/execution_test.py::test_transport_cancellation_drops_httpx_traceback_and_request_locals` filters frames under the installed `b24api` package path |
| A18 | Done | `4cdd743` | `tests/keyset_cursor_streams_2_test.py::test_slow_consumer_coalescing_is_bounded_per_wave_on_a_virtual_clock` |
| A19 | Done in code; **L1 open** | `29ff819` | `tests/throttle_headers_test.py::test_rate_limit_reset_accepts_delta_epoch_and_http_date`, `::test_epoch_rate_limit_reset_does_not_freeze_the_host_for_the_cap`; the live header form was not observed ([L1](live-probes.md#l1-a19-x-bitrix-ratelimit-reset-and-retry-after)) |
| A20 | Done | `6c585e5`, `c6c7164` | `tests/internal/httpx_log_shield_matrix_test.py::test_owned_http2_encode_and_decode_records_are_suppressed_until_close` and the rest of the hpack matrix; `tests/internal/httpx_log_shield_matrix_test.py::test_registered_secret_stays_scrubbed_while_the_injected_client_outlives_its_transport` |

A15 became B0.

Red on `c4cafdd`: every cited A test is absent at the base and first appears in one of the commits
its row lists; a row lists every commit that changed the item's behavior or added one of its tests. The exception is A17's test, which existed at the base and was changed in `a4fb7c0` to
inspect real library frames. The golden deltas in `tests/golden/DELTAS.md` show the base values that
each A12, A14 and B10 test rejects.

## B: improvements

| ID | Outcome | Commit | Evidence |
|---|---|---|---|
| B0 | Done | `43fd34d` | the permit tests pass unchanged; `tests/internal/rate_coordinator_test.py::test_synchronous_release_hands_capacity_over_before_marking_the_permit` |
| B1 | Done in code; **branch protection open** | `de451d5`, `0d333bb` | `tests/release_workflow_test.py::test_gate_is_the_only_ci_check_and_waits_for_every_blocking_job`, `::test_httpx_latest_is_non_blocking_evidence_on_the_latest_httpx`, `::test_publish_workflow_runs_the_full_ci_before_building` |
| B2, B3 | Done | `d0de275` | the `slow` marker, `tests/client_v2_test.py::test_logical_batch_pulls_its_generator_with_bounded_lookahead` (N=2,000) |
| B4 | Done | `7db6793` | `pyproject.toml` has no `fix`/`unsafe-fixes`; the `lint` job runs `ruff check --no-fix` |
| B6 | Done | `2991260`, `97dd73b` | `tests/import_graph_test.py::test_package_import_graph_respects_layers` |
| B7 | Done | `ff7bf49` | the single `PORTAL_BATCH_CAP` in `b24api/contracts/dispatch.py`, read by every batch capacity computation: `tests/architecture_test.py::test_batch_and_traversal_read_the_portal_batch_cap_from_its_one_owner` |
| B8 | Done; the strict-only error-body case is breaking, in the migration guide | `db3c529` | `tests/internal/execution_boundary_test.py::test_json_success_body_is_parsed_exactly_once`, `::test_structured_error_in_success_status_keeps_the_codec_classification`, `::test_strict_only_defect_in_a_success_error_body_is_an_envelope_contract_error`; [16 MiB benchmark](b8-b12-benchmark.md) |
| B9 | Done | `8cade30` | `tests/page_stop_test.py::test_default_auto_keyset_uses_sequential_path_for_page_stop` (auto with `page_stop` reports AUTO → SEQUENTIAL with `PAGE_STOP`); `::test_explicit_sequential_keyset_reports_its_selection` |
| B10 | Done | `79f4e6c` | `tests/client_findings_3_test.py::test_fixed_step_rejects_any_closure_after_a_short_unqualified_window`; golden deltas B10 |
| B11 | Done; breaking, in the migration guide | `357dc07`, `1640fb8` | [vulture outcomes](b11-dead-code.md); `tests/documentation_test.py::test_maintained_documentation_does_not_describe_removed_report_vocabulary_as_current` |
| B12 | Done; breaking, in the migration guide | `db3c529` | `tests/completion_gate_test.py::test_page_validated_carries_no_identity_digest` |
| B13 | Done | `aa2690c`, `3b0c9c9` | `tests/real_signature.py` binds the README and migration stubs to the real `Bitrix24` signatures. The `wheel-typing` job runs `tests/readme_test.py`, `tests/documentation_test.py` and `tests/root_surface_test.py` against the installed wheel (§6.1 item 4): `tests/release_workflow_test.py::test_wheel_typing_runs_the_migration_doc_tests_against_the_installed_wheel`. The same steps run locally: 147 passed, with `b24api` imported from site-packages. |
| B14 | Done | `467ef2f` | `tests/documentation_test.py::test_every_local_documentation_link_resolves` |
| B15 | Done | `aa2690c` | `tests/conftest.py` and `tests/scripting.py`; `tests/ratchet_test.py::test_homemade_test_transports_only_go_down` (49 → 35); `pytest-httpx` removed |
| B16 | Done | `93f5494`, `c3d33c2` | `tests/structural_property_test.py`, `tests/redaction_property_test.py` |
| B17 | Done | `de451d5` | mypy ratchet over `tests/` at 380, below 385: `tests/release_workflow_test.py::test_committed_mypy_baseline_never_exceeds_the_specified_starting_count` |
| B18 | Done | `2a27505` | `tests/settings_test.py::test_policy_from_settings_is_the_library_default_with_the_settings_timeout` |
| B19 | Done | `06dfa00`, `23bec8d` | `twine check --strict` on the built wheel and sdist; CHANGELOG split by release; `tests/documentation_test.py::test_readme_links_are_absolute_so_they_work_on_pypi` |
| B20 | Done; breaking, in the migration guide | `2a27505` | `tests/client_findings_3_test.py::test_envelope_contract_error_is_a_gateway_origin_protocol_error_and_is_not_retried`; the fast-keyset `page_trace` code: `tests/keyset_fast_test.py::test_fast_wave_success_status_without_an_envelope_rejects_every_observation_as_batch_envelope` |
| B21 | Done | `467ef2f`, `eb44210` | `tests/architecture_test.py::test_module_sizes_are_recorded` records sizes without gating on them; each source-substring check keeps a written justification |
| B22 | Done | `31a6ac7` | the new module layout; golden traces match |
| B23 | Done | `97dd73b`, `e5d1429` | `tests/import_graph_test.py::test_no_import_cycle_even_through_type_checking_imports` |
| B24 | Done | `68e67e4`, `aa2690c`, `10118a8` | `tests/ratchet_test.py::test_private_member_access_only_goes_down` (ratchet `slf001_tests`, counted by `ruff check --select SLF001 --ignore-noqa` over `tests/` outside `tests/internal`: 234 when introduced in `68e67e4` → 119; `noqa: SLF001` comments in `tests/`, counted by `git grep -o 'noqa: SLF001' <sha> -- tests \| wc -l`: 120 at `c4cafdd` → 98, 62 of them outside `tests/internal`) |
| B25 | Done | `68e67e4`, `d7c4a48`, `2f35f25` | `tests/ratchet_test.py::test_long_functions_only_shrink`; the two remaining long functions carry recorded justifications in `tests/ratchets.json` |
| B26 | Done | `c601960` | `tests/readme_test.py::test_quickstart_runs_exactly_against_a_scripted_portal` |
| B27 | Done | `de451d5` | `tests/release_workflow_test.py::test_coverage_floor_has_one_source_in_pyproject` |
| B29 | Done; breaking, in the migration guide | `228d0f8` | `tests/internal/execution_boundary_test.py::test_oversized_injected_response_is_rejected_before_decoding_for_safe_direct`, `::test_oversized_injected_batch_response_makes_every_command_unknown`, `::test_oversized_batch_response_from_the_default_transport_makes_every_command_unknown` |

B5 and B28 were never assigned (see §2).

## C: disputed recommendations

| ID | Outcome | Commit | Evidence |
|---|---|---|---|
| C1 | Factories done; implicit BARE decided (rejected) | `05793ab` | `tests/internal/client_factories_test.py::test_from_webhook_owns_and_closes_its_transport_and_coordinator`, the `Request.bare`/`Request.v3` tests. An implicit route would bring back the silent V3 → classic misdispatch that P-D02 rejected. |
| C2 | Replacement decided (rejected); pin measured | — | See [C2](#c2-httpx-pin) |
| C3 | Decided (rejected); L4 confirmed | — | Auto stays the default (`54542e2`, `test_omitted_execution_defaults_to_auto`); [L4](live-probes.md#l4-c3-auto-keyset-on-the-recipes-methods) shows the documented selection, request counts and report on the live portal |
| C4 | Measured | — | [gate cost before and after the lifecycle move](c4-gate-cost.md): the same 70,002 events, and wall time unchanged |
| C5 | Done | `ad9bcfa`, `e3e8157` | the planner, frozen plan and runtime split; SLF001 in `b24api/traversal` is 0 (`tests/ratchet_test.py::test_private_member_access_only_goes_down`) |
| C6 | Decided (rejected) | — | A global noqa ignore would hide new violations. Inline noqa keeps its reason, and the `noqa_package` ratchet (162) only goes down. |
| C7 | Done; breaking, no alias (owner decision below), in the migration guide | `05793ab` | `tests/root_surface_test.py`, the generated table in `docs/migration.md`, the positive and negative wheel smoke in `wheel-typing` |
| C8 | Done | `3a913e1` | [lock outcomes](c8-locks.md); `tests/internal/lock_free_state_test.py::test_every_ledger_mutator_completes_without_suspending` |
| C9 | Done | `c333754` | See [C9](#c9-public-archive) |
| C10 | Done | `6b7ef7d`, `434e721`, `fe66b63`, `df06ee6`, `785efee`, `ba5187c` | `OperationRunner` owns all five families; the §3.1 transition table is tested on each |
| C11 | Done (heuristic kept, with a counterexample) | `3bccfe4` | See [C11](#c11-bare-credential-heuristic) |
| C12 | Done | `306cde0`, `43944fc` | `tests/architecture_test.py::test_pagination_driver_composes_strategies_instead_of_inheriting_them`; `_counted` delegates to `judge_counted_page` |
| C13 | Positional route decided (rejected); solved by factories | `05793ab` | A positional `route` would silently shift existing `Request("m", params, ReplaySafety.X)` calls, and type checkers would not notice. |

### C2: httpx pin

- **Replacement rejected.** The shield stays. It attributes records through
  `_send_handling_auth`, guards against a swapped auth and redacts hop segments. A stateless filter
  would either break the positive controls for foreign `httpx` records or miss a redirect's secret.
- **Pin decision.** On 2026-09-24 the latest releases on PyPI were httpx 0.28.1, httpcore 1.0.9,
  h2 4.4.1 and hpack 4.2.0. The locked versions are the same, except h2 (4.3.0) and hpack (4.1.0).
- **Local run of the `httpx-latest` job.** The job runs the §3.8 shield
  (`tests/httpx_logging_shield_test.py`), the hpack matrix (`tests/internal/httpx_log_shield_matrix_test.py`,
  A20) and the LivePortal shield (`tools/b24api_evidence/harness/live_test.py`, A1).
  `tests/release_workflow_test.py::test_httpx_latest_is_non_blocking_evidence_on_the_latest_httpx`
  pins that. Run locally with `uv run --with 'httpx[http2]==0.28.1' --with 'httpcore==1.0.9' --with
  'h2==4.4.1' --with 'hpack==4.2.0' pytest` over those three files: 159 passed, including the
  hpack matrix on hpack 4.2.0, newer than the locked 4.1.0 (§9.7).
- **Result.** Only 0.28.x exists, so by the rule in §2.3 the bound stays `httpx[http2]>=0.28.1,<0.29`.
  The CI job itself runs on the first push.
- **hpack and h2 bounds.** The shield filters a fixed set of hpack logger names, so the package also
  declares `h2>=4.3.0,<4.5` and `hpack>=4.1.0,<4.3`: the locked line up to the newest release the
  matrix ran on. Widening either bound means running the `httpx-latest` matrix on the new line first.
  `tests/httpx_logging_shield_test.py::test_hpack_stack_is_bounded_to_the_verified_line` pins the
  bounds against the verified versions.

### C9: public archive

- `docs/specifications/README.md` marks the archive as non-normative design history.
- **Secret audit (2026-09-24).** It covered all 302 tracked files, including the 12 archive files.
  None contains the probe portal's host or webhook token, and none names a `*.bitrix24.*` portal
  host. The only webhook-shaped paths are redaction placeholders in `tests/settings_test.py` and
  `tools/b24api_evidence/harness/contracts.py`, plus the scanner's own allowlisted fixture in
  `tools/b24api_evidence/harness/contracts_test.py`.
- **Wheel exclusion.** `verify_release.py wheel` refuses a wheel that carries anything beside the
  package. The wheel and sdist built on this branch contain no `docs/specifications`.
- **Why the archive stays.** The owner keeps the design reasoning reviewable next to the code.

### C11: bare-credential heuristic

The coverage matrix in the docstring of `tests/diagnostic_rendering_test.py` asks, for each
credential source, whether it is registered as an exact secret before any output:

| Source | Registered as an exact secret | Other free-text coverage |
|---|---|---|
| `Settings` webhook | yes | full-URL pattern, heuristic |
| `from_webhook` | yes, through `Settings` | full-URL pattern, heuristic |
| Auth-flow values | yes, per request through its `DiagnosticContext` | sensitive keys, JSON/query/env patterns, heuristic |
| A different token in an injected transport's URL | no: the `Transport` protocol never exposes it | full-URL pattern, heuristic |
| Redirect hop tokens | no: they reach only HTTPX records | the log shield's hop pattern, full-URL pattern, heuristic |
| Foreign tokens in response data | no: unknowable before the response arrives | heuristic only |

The redirect and foreign-token rows are the counterexample: in free text, their only coverage is
the heuristic. `test_c11_foreign_response_token_is_covered_only_by_the_heuristic` pins it. So the
heuristic stays for free text, and diagnostic identifiers take the narrow structured path (§3.9).

## D: already done or no change needed

| ID | Outcome | Evidence |
|---|---|---|
| D-a | Decided | `c714084` (tag and distribution gate, `twine --strict`, Node 24); the missing CI dependency became B1 |
| D-b | Decided | runtime canaries were removed before the base; the leftovers were removed in B11 (`1640fb8`) |
| D-c | Decided | `await_cleanup_resistant`, `rearm_cancellation` and `AsyncIteratorController` live in `b24api/execution/context.py`; the duplicated orchestration moved into `OperationRunner` (C10) |
| D-d | Decided | `b24api/references/fanout.py` builds on `iter_references`; `batch_outcomes` differs from `batch` only by `fail_fast` (`b24api/batch/facade.py`) |
| D-e | Decided | no test runs `uv build` into `build/`; `tests/release_workflow_test.py` writes to `tmp_path`, and the build runs only in CI's `wheel-typing` job |

## L: live probes

| ID | Outcome | Evidence |
|---|---|---|
| L1 | **Open**: header form not observed | [L1](live-probes.md#l1-a19-x-bitrix-ratelimit-reset-and-retry-after) |
| L2 | Confirmed | [L2](live-probes.md#l2-a2-content-encoding-and-the-explicit-accept-encoding) |
| L3 | Confirmed; scenario 14 fixed (`8cfbbf9`) | [L3](live-probes.md#l3-a8-the-counted-validator-on-real-responses) |
| L4 | Confirmed | [L4](live-probes.md#l4-c3-auto-keyset-on-the-recipes-methods) |
| L5 | Confirmed | [L5](live-probes.md#l5-a9-no-start--0-with-total-only-closure-in-examples-and-documentation) |

## Open for the owner

These steps are outward-facing or need the owner's decision, so they were not performed.

1. **Phase order (§4, §6.1 item 1).** The program asked for a bootstrap PR in Ф0 and one PR per
   phase into a protected `develop`. All work instead sits on one unpushed branch. Two choices: split
   it into the phase PRs, or merge it as one PR and accept that the Ф0-time evidence below cannot be
   produced retroactively.
2. **Push and CI.** Push, then record the evidence CI produces on the exact SHA:
   - the `ci` aggregate check;
   - `httpx-latest` (C2);
   - the wheel doc-tests (§6.1 item 4);
   - the default-run time of the `tests` job (B2);
   - `gh pr checks --required` on the first PR (B1);
   - the manual full blocking run for the bootstrap PR (§6.1 item 1, §3.10).
3. **Branch protection (B1, §6.1 item 5).** Protect `master` and `develop` with the required `ci`
   check and `strict: true`. The spec wants the `gh api` output twice, in Ф0 and in Ф5; the Ф0
   snapshot no longer exists (item 1).
4. **L1.** Capture one real throttled response and record the form of `X-Bitrix-RateLimit-Reset` and
   `Retry-After`. That means a burst above the portal's limit, which is more load than was sent here.
5. **Release (§6.1 item 4).** Release `3.0.0` from the verified `master` SHA, with the tag on that SHA
   and the called `ci` green in the publish workflow.
6. **Issue comments (§6.1 item 6).** Post the table "ID → PR → test or evidence → outcome" on #14
   and #15, then close both issues after the §6.1 gate.

Three further observations; the first two are decided, the third stays with the owner:

- **Physical batch replay.** Owner decision, 2026-09-25, which replaces the whole-batch reading of
  §3.4: `SAFE` work is always replayed within the budget; `UNSAFE` and `UNKNOWN` work only when the
  failure proves it did not run, so a replay cannot duplicate an effect. A physical batch applies this
  per command. The executor retries the whole batch only when every command would be retried (all
  `SAFE`, or a proven refusal: a transport failure before dispatch, or a refusal listed in
  `AmbiguityPolicy.refusal_http_statuses` (423, 425, 429) or `AmbiguityPolicy.refusal_api_codes`
  (`QUERY_LIMIT_EXCEEDED`, `OPERATION_TIME_LIMIT`)). A code or status only in `RetryPolicy` proves
  nothing and retries `SAFE` work alone. Otherwise the batch engine sends again, in a smaller physical
  batch, the `SAFE` commands of a batch that failed with anything a `SAFE` request retries (whether or
  not it may have run), the `SAFE` commands its own `result_error`
  refused with a transient code, and the others it refused with a listed refusal; the `UNSAFE` and
  `UNKNOWN` commands of a batch that may have run become unknown. Every send of a command, across
  rounds and the executor's retries inside them, shares one `max_attempts_per_request` count and one
  retry clock from the start of its first attempt. A command keeps its last outcome when the budget
  stops a replay before it is sent. A round in which any send may have run makes its commands
  unknown whatever failure ends it, including a budget that runs out after a send or a batch answered
  after the time budget; only a listed refusal as the round's last answer keeps the refused outcome.
  The unknown outcome takes its reason and cause from the latest failure of a send that may have run,
  or, when the answer came after the budget, from the failure that ended the round. A fail-fast
  batch is not split. The same rule now also retries a direct `UNSAFE` request after a proven refusal.
  Tests:
  `tests/internal/execution_boundary_test.py::test_physical_batch_replay_matrix`,
  `tests/internal/execution_boundary_test.py::test_physical_batch_after_an_unstructured_transient_status`,
  `tests/internal/execution_boundary_test.py::test_structured_refusal_of_a_safe_batch_and_a_transient_status_on_a_direct_call_keep_their_retry`,
  `tests/internal/execution_boundary_test.py::test_a_mixed_batch_that_may_have_run_sends_only_its_safe_commands_again`,
  `tests/internal/execution_boundary_test.py::test_a_mixed_batch_that_was_not_accepted_is_sent_again_whole`,
  `tests/internal/execution_boundary_test.py::test_commands_refused_by_their_own_transient_error_are_sent_again_alone`,
  `tests/internal/execution_boundary_test.py::test_a_command_refused_on_every_round_keeps_its_error_once_the_attempt_budget_is_spent`,
  `tests/internal/execution_boundary_test.py::test_a_fail_fast_batch_is_never_split_for_a_replay`,
  `tests/internal/execution_boundary_test.py::test_a_configured_retry_code_or_status_does_not_prove_an_unsafe_request_never_ran`,
  `tests/internal/execution_boundary_test.py::test_a_replay_round_that_was_sent_keeps_its_own_outcome_when_the_budget_stops_the_next_attempt`,
  `tests/internal/execution_boundary_test.py::test_a_batch_answered_after_the_time_budget_reports_its_unsafe_command_unknown`,
  `tests/internal/execution_boundary_test.py::test_a_listed_refusal_answered_after_the_time_budget_still_proves_nothing_ran`,
  `tests/internal/execution_boundary_test.py::test_an_unknown_outcome_is_explained_by_the_send_that_may_have_run`,
  `tests/internal/execution_boundary_test.py::test_a_late_listed_refusal_after_a_send_that_may_have_run_stays_unknown`,
  `tests/internal/execution_boundary_test.py::test_the_safe_commands_of_a_mixed_batch_are_sent_again_after_a_failure_a_safe_request_retries`,
  `tests/internal/execution_boundary_test.py::test_a_batch_retried_after_a_send_that_may_have_run_stays_unknown_whatever_ends_the_retries`,
  `tests/internal/execution_boundary_test.py::test_replay_rounds_and_the_retries_inside_them_share_one_attempt_budget`,
  `tests/internal/execution_boundary_test.py::test_replay_rounds_share_the_retry_time_budget_measured_from_the_first_send`, and
  `tests/execution_test.py::test_unsafe_operation_time_limit_waits_for_the_method_and_replays`.
  Migration item 9 states it.
- **Root aliases (C7).** Owner decision, 2026-09-25, which replaces the 3.x deprecation period of
  §3.12: the 115 names that left the root have no alias; clients are adapted instead. An old root
  import raises `ImportError`, and attribute access raises an `AttributeError` that names the new path.
  No `DeprecationWarning` is emitted, and nothing else in the package was deprecated. Tests:
  `tests/root_surface_test.py::test_an_old_root_import_fails_in_3_0`,
  `tests/root_surface_test.py::test_attribute_access_to_a_moved_name_names_its_new_path_for_every_package_kind`
  and the negative wheel smoke in `wheel-typing`. Migration item 1 states it.
- **HTTPX logging on injected clients.** Two W2 gaps remain: the httpcore DEBUG record of a
  redirect token, and an unbounded `aread` on injected clients.
