# W5/W6 review packet v3 — bounded traversal foundation

## Decision requested

Review exact code SHA
`8ba3c40ca85f5e7c7123a2aec332c2dfe11f27d5` and either accept it as the
W5/W6 traversal foundation for W7 integration or return findings against that
same SHA.

The previous code subject `2d4ea6c...` and packet v2 are superseded. Their
findings and resolutions are recorded in `review-findings-v2-resolution.md`;
the earlier history remains in `review-findings-v1-resolution.md`.

## Immutable inputs, base, and lineage

- final code SHA: `8ba3c40ca85f5e7c7123a2aec332c2dfe11f27d5`;
- exact W5/W6 branch point: `99c13fa3be0bbdbd8248829da7df5ed55c7d2dc9`;
- accepted W3/W4 dependency: `521f0eb7cb107ec948c693496154f94e57dbf7c9`;
- W5 initial code: `bc75c3357ed17715f8461e83b3449efed3e69ece`;
- W6 initial code: `1f2ebe95b4539e736382ce359ed4b88f3878d77b`;
- remediation **code commits only**: `5e2c8a1`, `91b918b`, `6a26c73`,
  `bf6adca`, `e8c9b7b`, `0b9dfe9`, `10b1509`, `67f9d2e`, `2d4ea6c`,
  `8ba3c40`;
- packet commits: v1 `9fab54f9c86ffa2eb7d39407d3c5c01aa7371668`,
  v2 `a7752ed8360561ecb8b13749dd9e74bfbd91cdd5`;
- branch: `codex/bitrix24-client-benchmarks`.

The exact code comparison is:

```bash
git diff 99c13fa3be0bbdbd8248829da7df5ed55c7d2dc9..8ba3c40ca85f5e7c7123a2aec332c2dfe11f27d5 -- b24api
```

The normative input referenced as sections 7.7 and 27.6 is the user-owned file
`spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`. It is untracked by
design and governed by SHA-256
`2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`,
which exactly matches “Specification SHA-256” in W0 inventory. Verify with:

```bash
shasum -a 256 spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
```

This packet is a later documentation commit. Clean gate claims refer only to a
detached worktree at the code SHA; overlay claims are separately hash-bound.

## Authorized and refused boundary

The subject provides explicit sequential single, offset, counted-offset,
keyset, and item-cursor traversal; bounded fail-fast/tolerant batch streams;
bounded direct/batch reference scheduling; typed plan, identity, continuation,
consistency, budget, lifecycle, and report contracts; transactional logical
page accounting; exact duplicate/order/total and delivered-prefix evidence;
PHP-aware batch decoding; deterministic cleanup; and total tolerant
correlation.

Unset replay safety remains unset. The core never infers that an arbitrary
request is safe. Batch buffering uses an explicit top-level decoded-row model:
an array result weighs its element count, scalar/object weighs one, and empty
array weighs zero. `ItemCursorPlan.direction` orders cursor values from
`cursor_item_path`; row identity ordering is a separate optional
`order_semantics` contract.

Construction performs no I/O. Normative section 7.7 says first `__anext__` or
`__aenter__` starts execution. Context entry performs one lifecycle-priming
pull; it does not authorize the pipelined continuation/page lookahead candidate
in section 27.6.

The following values are constructible/exported but explicitly refuse before
source pull or HTTP I/O because their proof/profile is not admitted:

- `PartitionedKeysetPlan`;
- `CountedOffsetPlan(mode=PARALLEL_FIXED_STRIDE)`;
- `KeysetPlan(terminal=BOUNDARY_ID_SEEN)`;
- `IdentityTracker.MONOTONIC` with any plan other than keyset.

No compatibility facade, default strategy, automatic dispatch, pipelined
prefetch, partition profile, proactive rate profile, or benchmark admission is
authorized. W7 owns compatibility/defaults; W9 owns live evidence.

## Review history and closure

All code findings from `bc75c33...` through `2d4ea6c...` were replayed on the
final subject. The detailed v1–v10 history is in the two resolution documents.

| Candidate | Verdict | Final disposition |
|---|---|---|
| `0b9dfe9` | Human rejected: 3 P1, 1 P2 | Counted repetition, order, policy, and fan-out total closed before v2. |
| `10b1509` | Clean-room rejected: 2 P1, 1 P2 | Weighted fan-out, global preflight, and batch buffering closed. |
| `67f9d2e` | Clean-room rejected: 1 P2 | Canonical plan validation closed. |
| `2d4ea6c` | Human rejected: 2 P1, 1 P2 | Replay safety, nested batch weight, and cursor/row order separation closed. |
| `8ba3c40` | **Clean-room admitted** | Fresh audit P1/P2/P3 = 0/0/0. |

Rejected subjects receive no authorization.

## Exact clean-SHA gates

Executed with CPython 3.12.10 in a detached clean worktree at exactly
`8ba3c40ca85f5e7c7123a2aec332c2dfe11f27d5`:

| Check | Exact result |
|---|---|
| Full clean pytest | 231/231 passed |
| Stream-focused W4–W6: batch/pagination/references | 121/121 passed |
| Affected common + stream: execution/models/plans/batch/pagination/references | 163/163 passed |
| Consolidated committed remediation gate under asyncio debug and warnings-as-errors | 24/24 passed |
| Three new human-finding regressions under the same strict mode | 3/3 passed |
| Focused W4–W6 with asyncio debug and `-W error::RuntimeWarning` | 121/121 passed |
| Scoped Ruff `--no-fix`, six source files | Passed |
| Scoped strict mypy, six source files | Passed |
| `git diff --check`, `git diff --exit-code`, and status | Clean |

The exact new-finding gate is:

```bash
PYTHONASYNCIODEBUG=1 PYTHONWARNINGS=error \
uv run --python 3.12.10 pytest -q \
  -o asyncio_default_fixture_loop_scope=function \
  b24api/references_test.py::test_fan_out_does_not_infer_safe_replay_for_unset_requests \
  b24api/batch_test.py::test_batch_list_result_uses_nested_decoded_row_weight \
  b24api/pagination_test.py::test_item_cursor_orders_cursor_values_independently_from_row_identity
```

Result: 3/3 passed. The complete consolidated gate is:

```bash
PYTHONASYNCIODEBUG=1 PYTHONWARNINGS=error \
uv run --python 3.12.10 pytest -q \
  -o asyncio_default_fixture_loop_scope=function \
  b24api/pagination_test.py::test_counted_offset_detects_repeated_items_when_continuation_metadata_changes \
  b24api/pagination_test.py::test_declared_order_is_enforced_for_single_and_offset_plans \
  b24api/pagination_test.py::test_consistency_policy_requires_identity_before_io \
  b24api/pagination_test.py::test_consistency_policy_enforces_duplicates_order_total_and_confirmation \
  b24api/pagination_test.py::test_advisory_total_mismatch_is_reported_without_blocking_completion \
  b24api/references_test.py::test_invalid_reference_contract_refuses_even_empty_input \
  b24api/references_test.py::test_invalid_reference_contract_refuses_before_blocking_source_pull \
  b24api/references_test.py::test_invalid_reference_plan_type_refuses_at_construction \
  b24api/references_test.py::test_fan_out_accepts_list_result_whose_total_matches_list_length \
  b24api/references_test.py::test_batch_fan_out_accepts_list_result_whose_total_matches_list_length \
  b24api/references_test.py::test_fan_out_list_result_obeys_decoded_row_buffer \
  b24api/references_test.py::test_input_order_rejects_oversized_whole_result_behind_blocked_head \
  b24api/batch_test.py::test_batch_outcomes_obey_decoded_row_buffer_ceiling \
  b24api/batch_test.py::test_primary_batch_failure_survives_secondary_source_cleanup_failure \
  b24api/references_test.py::test_primary_reference_failure_survives_secondary_cleanup_budget_failure \
  b24api/references_test.py::test_fan_out_does_not_infer_safe_replay_for_unset_requests \
  b24api/batch_test.py::test_batch_list_result_uses_nested_decoded_row_weight \
  b24api/pagination_test.py::test_item_cursor_orders_cursor_values_independently_from_row_identity
```

Result: 24/24 passed; parameterized functions expand to the exact case count.

The clean full-tree static baseline remains the inherited W2 debt in untouched
`b24api/api_test.py`: exactly three Ruff findings and 24 strict-mypy
`union-attr` findings. The six scoped production files (`batch.py`,
`execution.py`, `models.py`, `pagination.py`, `plans.py`, `references.py`) add
none. Ruff always runs with `--no-fix`.

## Protected overlay and `error.py` provenance

W0 originally locked eight user-candidate files. W1 acceptance deliberately
superseded `b24api/error.py` at accepted code SHA `7fc7b22...`; see
`docs/bitrix24-client-2.0/w1/acceptance.md`, which names both the rejected W0
hash and W1 replacement hash. Accepted W3/W4 then added transport evidence
fields. Therefore `error.py` is no longer in the protected overlay; the seven
remaining files still are.

The main-tree overlay is supplemental and not attributed to the code SHA. It
passes 250/250 full tests, 121/121 stream-focused tests, 163/163 affected tests,
repository-wide Ruff `--no-fix`, and strict mypy for 27 source files.

Reproduce the seven protected hashes with:

```bash
shasum -a 256 \
  README.md \
  b24api/__init__.py \
  b24api/api.py \
  b24api/api_test.py \
  b24api/entity.py \
  b24api/helper.py \
  b24api/entity_types.py
```

Each output must equal the corresponding row in
`docs/bitrix24-client-2.0/w0/inventory.md`.

## Non-gating observations and open obligations

Untracked scratch probes also passed direct/batch write-safety, 10,000-row
tolerant/fail-fast weights, scalar/empty weights, early-close ledger cleanup,
opposite row/cursor directions, 500/500 admission races, and the prior
cancellation/lifecycle matrix. They are reviewer observations, not immutable
gates; committed tests and commands above govern admission.

`Transport` documents deadline/cancellation cooperation in code. `total=-1`
remains preserved data without endpoint semantics. Early close owns the input
iterator. Full-suite debug teardown may expose plugin-allocated
`pytest_asyncio` ResourceWarnings; W9 must settle harness configuration before
leak evidence. No live webhook, portal data, or write operation was used for
this review.

No W7 facade/default, optimization profile, benchmark threshold, or W9
evidence/seed/manifest/cleanup mechanism is admitted. Binding follow-ups remain
in `docs/bitrix24-client-2.0/w5-w7/review-obligations.md` and
`docs/bitrix24-client-2.0/w9/review-obligations.md`.
