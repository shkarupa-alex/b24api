# W5/W6 review packet v5 — post-acceptance evidence addendum

## Status and exact subjects

Human review accepted semantic W5/W6 subject
`a29b58f3faba9a71202bbe1e9a4aab0f770b369b` with P1/P2/P3 = 0/0/0 in the
accepting review. A second review recommended closing two P2 evidence/document
gaps and one P3 public-documentation gap before recording acceptance.

This addendum asks review of exact test/doc-only descendant
`61345c16f39beef090b672d583abfe542ebdd92c`. It descends from packet commit
`e9439b4e30f8f74cbd1c70ab793f94ec13734522`; compare the accepted semantic
subject directly:

```bash
git diff a29b58f3faba9a71202bbe1e9a4aab0f770b369b..61345c16f39beef090b672d583abfe542ebdd92c -- b24api
```

The diff contains only:

- parameterized evidence in `b24api/pagination_test.py`;
- an explanatory `ItemCursorPlan` docstring in `b24api/plans.py`.

No executable production statement or accepted traversal behavior changes.
The original v5 packet remains the immutable human-review artifact; this
addendum corrects its evidence inventory without rewriting its historical
claims or counts.

The normative specification remains the user-owned file
`spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`, SHA-256
`2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

## Exhaustive categorically refused surface

An exported/canonical-surface audit matched every request-independent
`CapabilityError` against the closed enum members and plan union. Exactly eight
controls are constructible but categorically unadmitted:

1. `PartitionedKeysetPlan`;
2. `CountedOffsetPlan(mode=PARALLEL_FIXED_STRIDE)`;
3. `KeysetPlan(terminal=BOUNDARY_ID_SEEN)`;
4. `IdentityTracker.MONOTONIC` with any plan other than keyset;
5. `IdentityRequirement.COMPOSITE` at plan or consistency-policy level;
6. `ConfirmationPolicy.INDEPENDENT_ORACLE`;
7. `ConfirmationPolicy.BOUNDARY_ID_SEEN` because its only providing keyset
   terminal is itself unadmitted;
8. `OrderSemantics.INPUT` at plan or consistency-policy level.

All eight refuse with typed `CapabilityError` before HTTP I/O. COMPOSITE also
has committed plan/policy `iter_references` proof that refusal occurs before
the input source is pulled. Conditional invalid combinations—such as qualified
total confirmation without filtered-exact total, empty-boundary confirmation
without a matching terminal, or contradictory plan/policy order—are validation
errors inside otherwise admitted controls, not additional always-refused enum
modes.

The four newly enumerated plan/policy configurations are protected by
`test_unadmitted_consistency_controls_refuse_before_io`; all perform zero HTTP
requests. Existing committed nodes protect the first five controls and the
COMPOSITE reference-source boundary.

## Explicitly admitted profile-named terminals

`OffsetTerminalRule.PROFILE_SHORT_PAGE`,
`KeysetTerminalRule.PROFILE_SHORT_PAGE`, and
`CursorTerminalRule.PROFILE_SHORT_PAGE` are admitted only when the caller
explicitly selects the rule and supplies `requested_page_size`. A short page is
then delivered and terminates traversal with `CALLER_ASSERTED` assurance. The
rule does not infer endpoint behavior, load a packaged profile, or promote
assurance to `PROFILE_VERIFIED`.

Offset runtime coverage predates this addendum. The new parameterized
`test_profile_short_page_terminates_keyset_and_cursor_traversal` proves both
remaining families deliver the final row, make exactly one request, finish
`COMPLETED`, emit no violations, retain `CALLER_ASSERTED`, and record their
typed terminal reason.

No automatic optimization profile, profile chooser, default strategy, or
proactive profile is admitted. Those remain W8/W9 decisions. The word
`PROFILE_` on these explicit terminal rules denotes caller-authorized endpoint
knowledge, not an automatically applicable packaged profile.

## Item-cursor alias documentation

Continued item-cursor pages require unique, strictly monotonic cursor values.
Within that admitted contract, `cursor_take=min/max` are compatibility aliases
of `first/last`. `ItemCursorPlan` now states this in its public docstring. W7
must expose only the two distinct choices or document the aliases explicitly;
it may not claim four independent traversal semantics.

## Exact addendum gates

Executed with CPython 3.12.10 in a detached clean worktree at exactly
`61345c16f39beef090b672d583abfe542ebdd92c`:

| Check | Exact result |
|---|---|
| Full clean pytest | 245/245 passed |
| Stream-focused batch/pagination/references | 135/135 passed |
| Affected execution/models/plans/batch/pagination/references | 177/177 passed |
| Newest strict remediation gate | 14/14 passed |
| Consolidated strict remediation gate | 38/38 passed |
| Focused asyncio debug with runtime warnings as errors | 135/135 passed |
| Scoped Ruff `--no-fix` and strict mypy | Passed |
| Clean status, `git diff --check`, `git diff --exit-code` | Passed |

The six new cases are exactly:

```bash
PYTHONASYNCIODEBUG=1 PYTHONWARNINGS=error \
uv run --python 3.12.10 pytest -q \
  -o asyncio_default_fixture_loop_scope=function \
  b24api/pagination_test.py::test_unadmitted_consistency_controls_refuse_before_io \
  b24api/pagination_test.py::test_profile_short_page_terminates_keyset_and_cursor_traversal
```

Result: 6/6 after parameterization. The newest 14/14 gate is the exact 8/8
command in `review-packet-v5.md` with these two node IDs appended. The
consolidated 38/38 gate is the exact v5 consolidated command with the same two
nodes appended. This construction is executable and preserves every prior node
rather than replacing the earlier regression set.

The clean full-tree baseline remains exactly three Ruff findings and 24 strict
mypy `union-attr` findings, all inherited in untouched `b24api/api_test.py`.
The main-tree protected overlay remains supplemental and separately passes:

- full pytest 264/264;
- stream-focused 135/135;
- affected 177/177;
- repository-wide Ruff `--no-fix`;
- strict mypy for 27 source files.

The seven protected overlay hashes and the specification hash are unchanged.
No webhook, portal data, live read, or write was used.

## Non-impact and dependent decisions

This addendum strengthens immutable evidence and public explanation only. It
does not invalidate the human acceptance of `a29b58f...`, authorize a new
strategy/default/profile, or change any benchmark evidence. W7 remains bound by
`docs/bitrix24-client-2.0/w5-w7/review-obligations.md`; W9 remains bound by
`docs/bitrix24-client-2.0/w9/review-obligations.md`.
