> **Superseded.** Human review rejected code subject `2d4ea6c...` and returned
> five packet findings. Use `review-packet-v3.md`.

# W5/W6 review packet v2 — bounded traversal foundation

## Decision requested

Review exact code SHA
`2d4ea6cc6141fee2343c7abd4151d81b9c167d99` and either accept it as the
W5/W6 traversal foundation for W7 integration or return findings against that
same SHA.

The prior human-review subject `0b9dfe9...` is rejected. The original packet is
superseded. The complete human and subsequent clean-room resolution is in
`review-findings-v1-resolution.md`.

## Immutable subject, base, and lineage

- final code SHA: `2d4ea6cc6141fee2343c7abd4151d81b9c167d99`;
- exact W5/W6 branch point: `99c13fa3be0bbdbd8248829da7df5ed55c7d2dc9`;
- accepted W3/W4 dependency: `521f0eb7cb107ec948c693496154f94e57dbf7c9`;
- W5 initial implementation: `bc75c3357ed17715f8461e83b3449efed3e69ece`;
- W6 initial implementation: `1f2ebe95b4539e736382ce359ed4b88f3878d77b`;
- remediation lineage: `5e2c8a1`, `91b918b`, `6a26c73`, `bf6adca`,
  `e8c9b7b`, `0b9dfe9`, `10b1509`, `67f9d2e`, `2d4ea6c`;
- branch: `codex/bitrix24-client-benchmarks`.

The exact code-only comparison is:

```bash
git diff 99c13fa3be0bbdbd8248829da7df5ed55c7d2dc9..2d4ea6cc6141fee2343c7abd4151d81b9c167d99 -- b24api
```

This packet is a later documentation commit. Gate claims refer to a detached,
clean worktree at the code SHA, not to the packet commit or protected overlay.

## Authorized boundary

The subject provides explicit sequential single, offset, counted-offset,
keyset, and item-cursor traversal; bounded fail-fast and tolerant batch
streams; bounded direct and batch reference scheduling; typed plan, identity,
continuation, consistency, budget, lifecycle, and terminal-report contracts;
transactional logical-page accounting; exact duplicate/order/total and
delivered-prefix evidence; PHP-aware batch decoding; deterministic cleanup;
and total tolerant command/reference correlation.

Construction performs no I/O. Per normative section 7.7, the first
`__anext__` or `__aenter__` starts execution. Context entry performs exactly
one lifecycle-priming pull so failure and ownership begin inside the context;
the item is not counted as emitted until the caller receives it. This is not
the pipelined continuation/page lookahead candidate called “prefetch” in
section 27.6. No pipelined prefetch is authorized here.

No compatibility facade, public default strategy, automatic dispatch,
partitioned traversal, proactive rate profile, or benchmark admission is added.
W7 owns compatibility and defaults.

## Review history and closure

`bc75c33...` was the W5-only implementation. Its findings were folded into the
first combined W5/W6 detached review at `1f2ebe9...`; it was never represented
as independently admitted.

| Candidate | Verdict | Finding status |
|---|---|---|
| `bc75c33` | Not admitted alone | W5-only findings folded into the first combined review. |
| `1f2ebe9` | Rejected | Source admission, dispatcher cancellation, budgets, snapshot, cursor identity, control collision, and W6 accounting findings. |
| `5e2c8a1` | Rejected | Provisional-page rollback, bounded cleanup, and delivered-prefix findings. |
| `91b918b` | Rejected | W5 prefix accounting, commit race, worker ownership, rollback cancellation, blocking close, and affected-W4 lifecycle findings. |
| `6a26c73` | Rejected | Input ownership, synchronous pull, cleanup observability, partial correlation, admitted-outcome loss, and report identity findings. |
| `bf6adca` | Rejected | Late cleanup observability, failed-finalization cancellation, admission race, post-terminal commit, and controller exhaustion findings. |
| `e8c9b7b` | Rejected (1 blocking) | Secondary cleanup could replace the primary typed failure. |
| `0b9dfe9` | Human rejected (3 P1, 1 P2 code) | Counted repetition, ordering, effective policy, and fan-out total; packet also returned ten documentation findings. |
| `10b1509` | Clean-room rejected (2 P1, 1 P2) | Whole-result row weight, global W6 preflight, and BatchStream buffer accounting. |
| `67f9d2e` | Clean-room rejected (1 P2) | Noncanonical W6 plan validation. |
| `2d4ea6c` | **Clean-room admitted** | Fresh audit P1/P2/P3 = 0/0/0. |

Every code finding from every earlier row was replayed and is closed on the
final SHA. Rejected subjects are lineage only and receive no authorization.

## Exact clean-SHA gates

Executed with CPython 3.12.10 in a detached clean worktree at exactly
`2d4ea6cc6141fee2343c7abd4151d81b9c167d99`:

| Check | Exact result |
|---|---|
| Full clean pytest | 228/228 passed |
| Stream-focused W4–W6: `batch_test.py pagination_test.py references_test.py` | 118/118 passed |
| Affected common + stream: `execution_test.py models_test.py plans_test.py batch_test.py pagination_test.py references_test.py` | 160/160 passed |
| Targeted human/clean-room remediation gate under asyncio debug and warnings-as-errors | 21/21 passed |
| Scoped Ruff `--no-fix`, six source files | Passed |
| Scoped strict mypy, six source files | Passed |
| `git diff --check` and `git status --porcelain` | Clean |

The six scoped source files are `batch.py`, `execution.py`, `models.py`,
`pagination.py`, `plans.py`, and `references.py`.

The exact primary/secondary cleanup regression gate is:

```bash
PYTHONASYNCIODEBUG=1 PYTHONWARNINGS=error \
uv run --python 3.12.10 pytest -q \
  -o asyncio_default_fixture_loop_scope=function \
  b24api/batch_test.py::test_primary_batch_failure_survives_secondary_source_cleanup_failure \
  b24api/references_test.py::test_primary_reference_failure_survives_secondary_cleanup_budget_failure
```

Result: 2/2 passed. The exact 21-case remediation gate is:

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
  b24api/references_test.py::test_primary_reference_failure_survives_secondary_cleanup_budget_failure
```

Result: 21/21 passed. Parameterized node IDs expand the listed functions to
the exact case count.

The clean full-tree static baseline remains exactly the inherited W2 debt in
untouched `b24api/api_test.py`: three Ruff findings and 24 strict-mypy
`union-attr` findings. Scoped W5/W6 files add none. Ruff commands use
`ruff check --no-fix`, so a failed check cannot rewrite the reviewed tree.

## Supplemental protected overlay

The main-tree overlay is not attributed to the immutable code SHA. With the
seven remaining W0-protected user files byte-identical to their recorded
SHA-256 values, the combined tree passes:

- 247/247 full tests;
- 118/118 stream-focused tests;
- 160/160 affected common-plus-stream tests;
- repository-wide Ruff with `--no-fix`;
- strict mypy for 27 source files.

The seven hashes are the values recorded in W0 inventory for `README.md`,
`b24api/__init__.py`, `api.py`, `api_test.py`, `entity.py`, `helper.py`, and
`entity_types.py`; they were rechecked before publishing this packet.

## Adversarial observations, not immutable gates

The independent reviewer also replayed untracked scratch probes: 500/500
simultaneous admission/source-error races for READY and INPUT order, a 400/400
early-close matrix, late-response ledger immutability, repeated cancellation,
late cleanup retention, and W4/W6 primary-error precedence. These observations
all passed on the final SHA, but the scripts live outside the repository. They
are supporting reviewer observations, not reproducible admission gates.

Committed tests and the exact commands above are the immutable evidence.

## Known boundaries and open obligations

- `Transport` now states in code that injected implementations must cooperate
  with `attempt_timeout` and cancellation. A custom transport that ignores the
  deadline and suppresses cancellation is nonconforming.
- `total=-1` is preserved protocol data, not endpoint semantics. W5/W7 still
  requires provenance before relying on it.
- Early close owns and closes the supplied iterator. W7 must document that it
  cannot be resumed after ownership transfer.
- Full-suite `PYTHONASYNCIODEBUG=1` plus all warnings as errors can surface
  three `pytest_asyncio` teardown ResourceWarnings for plugin-allocated sockets
  and an event loop. Targeted strict gates and focused RuntimeWarning gates are
  green; no library ownership was established. W9 must settle the harness
  filter/configuration before leak evidence.
- No live webhook, portal data, or write operation was used in W5/W6 review.
- No optimization, default, profile, benchmark threshold, W7 facade, or W9
  evidence/seed/manifest/cleanup mechanism is admitted by this checkpoint.

Binding follow-ups remain in
`docs/bitrix24-client-2.0/w5-w7/review-obligations.md` and
`docs/bitrix24-client-2.0/w9/review-obligations.md`.
