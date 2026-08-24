# W5/W6 review packet v5 — bounded traversal foundation

> **Accepted semantic subject with a post-review evidence addendum.** Human
> review accepted `a29b58f...` as the W5/W6 foundation and returned nonblocking
> evidence/documentation findings. `review-packet-v5-addendum.md` binds their
> test/doc-only resolution, complete eight-control refusal surface, and updated
> gates to descendant `61345c1...`. The counts below remain the historical v5
> packet results rather than being silently rewritten.

## Decision requested

Review exact code-and-test SHA
`a29b58f3faba9a71202bbe1e9a4aab0f770b369b` and either accept it as the
W5/W6 traversal foundation for W7 integration or return findings against that
same SHA.

The previous code subject `fd1fb727...` remains production-equivalent, but
packet v4 is superseded because its immutable regression set did not prove one
of its evidence claims. The exact handoff finding and replacement test are in
`review-findings-v4-resolution.md`; earlier history remains in the v1–v3
resolution documents.

## Immutable inputs, base, and lineage

- final code-and-test SHA: `a29b58f3faba9a71202bbe1e9a4aab0f770b369b`;
- production tree is identical to clean-room-admitted `fd1fb727...`;
- exact W5/W6 branch point: `99c13fa3be0bbdbd8248829da7df5ed55c7d2dc9`;
- accepted W3/W4 dependency: `521f0eb7cb107ec948c693496154f94e57dbf7c9`;
- W5 initial code: `bc75c3357ed17715f8461e83b3449efed3e69ece`;
- W6 initial code: `1f2ebe95b4539e736382ce359ed4b88f3878d77b`;
- remediation **code/test commits only**: `5e2c8a1`, `91b918b`, `6a26c73`,
  `bf6adca`, `e8c9b7b`, `0b9dfe9`, `10b1509`, `67f9d2e`, `2d4ea6c`,
  `8ba3c40`, `fd1fb727`, `a29b58f`;
- packet commits: v1 `9fab54f9c86ffa2eb7d39407d3c5c01aa7371668`,
  v2 `a7752ed8360561ecb8b13749dd9e74bfbd91cdd5`,
  v3 `024af374a051bc877c5e514dc5e09672dafe4357`,
  v4 `d09158760e0054716a87942015d2afe79e465f86`;
- branch: `codex/bitrix24-client-benchmarks`.

The exact comparison is:

```bash
git diff 99c13fa3be0bbdbd8248829da7df5ed55c7d2dc9..a29b58f3faba9a71202bbe1e9a4aab0f770b369b -- b24api
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
detached worktree at the code-and-test SHA; overlay claims are separately
hash-bound.

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
an array result weighs its element count, scalar/object weighs one, and an empty
array weighs zero. Empty arrays therefore consume no decoded-row budget, while
the number of commands and outcomes remains bounded by the portal batch limit
of 50 and the admitted chunk ceiling.

`ItemCursorPlan.cursor_coercion` types the value at `cursor_item_path`
independently of row `IdentitySpec.coercion`. Cursor direction and strict
within-page monotonicity apply only to cursor values; row identity ordering is
a separate optional `order_semantics` contract. For
`PROFILE_CURSOR_EXHAUSTED`, a nonempty page whose cursor values are all missing
or null is delivered and then completes; mixed present and exhausted cursor
values fail before that page emits. A continued page requires cursor values to
be unique and strictly monotonic. Consequently a nonunique field such as a date
is not an admitted cursor unless W7 supplies a separately reviewed composite or
tie-breaker contract. Under this strict contract, `cursor_take=min/max` are
aliases of `first/last`, not distinct capabilities.

Construction performs no I/O. Normative section 7.7 says first `__anext__` or
`__aenter__` starts execution. Context entry performs one lifecycle-priming
pull; it does not authorize the pipelined continuation/page lookahead candidate
in section 27.6.

The following five controls are constructible/exported but explicitly refuse
before source pull or HTTP I/O because their proof/profile is not admitted:

- `PartitionedKeysetPlan`;
- `CountedOffsetPlan(mode=PARALLEL_FIXED_STRIDE)`;
- `KeysetPlan(terminal=BOUNDARY_ID_SEEN)`;
- `IdentityTracker.MONOTONIC` with any plan other than keyset;
- `IdentityRequirement.COMPOSITE` at either plan or consistency-policy level.

The fifth refusal now has committed plan/policy tests for both `iter_list` and
`iter_references`; the reference form also proves that the input source is not
pulled.

No compatibility facade, default strategy, automatic dispatch, pipelined
prefetch, partition profile, composite cursor profile, proactive rate profile,
or benchmark admission is authorized. W7 owns compatibility/defaults; W9 owns
live evidence.

## Review history and closure

All production-code findings from `bc75c33...` through `8ba3c40...` were
replayed on `fd1fb727...`. The final subject changes no production file and adds
only the handoff-requested committed regression. Detailed history is in the
four resolution documents.

| Candidate | Verdict | Final disposition |
|---|---|---|
| `0b9dfe9` | Human rejected: 3 P1, 1 P2 | Counted repetition, order, policy, and fan-out total closed before v2. |
| `10b1509` | Clean-room rejected: 2 P1, 1 P2 | Weighted fan-out, global preflight, and batch buffering closed. |
| `67f9d2e` | Clean-room rejected: 1 P2 | Canonical plan validation closed. |
| `2d4ea6c` | Human rejected: 2 P1, 1 P2 | Replay safety, nested batch weight, and cursor/row order separation closed. |
| `8ba3c40` | Human rejected: 2 P2 | Independent cursor typing and cursor-exhaustion terminal semantics closed. |
| `fd1fb727` + packet v4 | Code admitted; handoff rejected: 1 P2 evidence | Missing committed reference-source COMPOSITE regression added. |
| `a29b58f` | **Current candidate** | Production unchanged; immutable evidence completed. |

Rejected subjects and packets receive no authorization.

## Exact clean-SHA gates

Executed with CPython 3.12.10 in a detached clean worktree at exactly
`a29b58f3faba9a71202bbe1e9a4aab0f770b369b`:

| Check | Exact result |
|---|---|
| Full clean pytest | 239/239 passed |
| Stream-focused W4–W6: batch/pagination/references | 129/129 passed |
| Affected common + stream: execution/models/plans/batch/pagination/references | 171/171 passed |
| Consolidated committed remediation gate under asyncio debug and warnings-as-errors | 32/32 passed |
| Eight newest human/packet-finding regressions under the same strict mode | 8/8 passed |
| Focused W4–W6 with asyncio debug and `-W error::RuntimeWarning` | 129/129 passed |
| Scoped Ruff `--no-fix`, six source files | Passed |
| Scoped strict mypy, six source files | Passed |
| New reference regression Ruff `--no-fix` and strict mypy | Passed |
| `git diff --check`, `git diff --exit-code`, and status | Clean |

The exact newest-finding gate is:

```bash
PYTHONASYNCIODEBUG=1 PYTHONWARNINGS=error \
uv run --python 3.12.10 pytest -q \
  -o asyncio_default_fixture_loop_scope=function \
  b24api/pagination_test.py::test_item_cursor_uses_independent_cursor_coercion \
  b24api/pagination_test.py::test_profile_cursor_exhaustion_delivers_last_page_without_cursor \
  b24api/pagination_test.py::test_profile_cursor_exhaustion_rejects_mixed_cursor_presence \
  b24api/pagination_test.py::test_composite_identity_refuses_before_io \
  b24api/references_test.py::test_composite_reference_identity_refuses_before_source_pull_or_io
```

Result: 8/8 passed after parameterization. The complete consolidated gate is:

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
  b24api/pagination_test.py::test_item_cursor_orders_cursor_values_independently_from_row_identity \
  b24api/pagination_test.py::test_item_cursor_uses_independent_cursor_coercion \
  b24api/pagination_test.py::test_profile_cursor_exhaustion_delivers_last_page_without_cursor \
  b24api/pagination_test.py::test_profile_cursor_exhaustion_rejects_mixed_cursor_presence \
  b24api/pagination_test.py::test_composite_identity_refuses_before_io \
  b24api/references_test.py::test_composite_reference_identity_refuses_before_source_pull_or_io
```

Result: 32/32 passed; parameterized functions expand to the exact case count.

The clean full-tree static baseline remains the inherited W2 debt in untouched
`b24api/api_test.py`: exactly three Ruff findings and 24 strict-mypy
`union-attr` findings. The six scoped production files (`batch.py`,
`execution.py`, `models.py`, `pagination.py`, `plans.py`, `references.py`) and
the new `references_test.py` regression add none. Ruff always runs with
`--no-fix`.

## Protected overlay and `error.py` provenance

W0 originally locked eight user-candidate files. W1 acceptance deliberately
superseded `b24api/error.py` at accepted code SHA `7fc7b22...`; see
`docs/bitrix24-client-2.0/w1/acceptance.md`, which names both the rejected W0
hash and W1 replacement hash. Accepted W3/W4 later added transport evidence
fields. Therefore `error.py` is no longer in the protected overlay; the seven
remaining files still are.

The main-tree overlay is supplemental and not attributed to the code SHA. It
passes 258/258 full tests, 129/129 stream-focused tests, 171/171 affected tests,
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

Untracked scratch probes also passed all independent cursor-coercion forms,
cursor-zero and missing/null/mixed exhaustion, direct/batch reference cursor
paths, write-safety, nested decoded-row weights, early-close ledger cleanup,
admission races, and the prior cancellation/lifecycle matrix. They are reviewer
observations, not immutable gates; committed tests and commands above govern
admission.

`Transport` documents deadline/cancellation cooperation in code. `total=-1`
remains preserved data without endpoint semantics. Early close owns the input
iterator. Full-suite debug teardown may expose plugin-allocated
`pytest_asyncio` ResourceWarnings; W9 must settle harness configuration before
leak evidence. No live webhook, portal data, or write operation was used for
this review.

No W7 facade/default, optimization profile, composite cursor profile, benchmark
threshold, or W9 evidence/seed/manifest/cleanup mechanism is admitted. Binding
follow-ups remain in `docs/bitrix24-client-2.0/w5-w7/review-obligations.md` and
`docs/bitrix24-client-2.0/w9/review-obligations.md`.
