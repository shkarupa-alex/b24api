# W5/W6 acceptance — bounded traversal foundation

## Accepted subjects

- Human-accepted semantic SHA:
  `a29b58f3faba9a71202bbe1e9a4aab0f770b369b`
- Human-reviewed packet commit:
  `e9439b4e30f8f74cbd1c70ab793f94ec13734522`
- Post-review test/docstring evidence SHA:
  `61345c16f39beef090b672d583abfe542ebdd92c`
- Independently admitted evidence-addendum commit:
  `eafa8c1559f938e35d6ac8ea6e475588a5af6388`
- Branch: `codex/bitrix24-client-benchmarks`

Human review accepted `a29b58f...` as the W5/W6 foundation for W7 with
P1/P2/P3 = 0/0/0. A second review found two nonblocking evidence/documentation
gaps and one public-docstring gap. Their resolution changes no executable
production statement: it adds committed tests and documents the already
accepted item-cursor alias contract. Independent clean-room review admitted the
exact evidence/addendum pair with P1/P2/P3 = 0/0/0.

The complete post-review disposition is in
`review-findings-v5-resolution.md`; immutable supporting evidence is in
`review-packet-v5-addendum.md`.

## Reproduced final gates

At exact evidence SHA `61345c1...` under CPython 3.12.10:

| Check | Result |
|---|---|
| Full clean pytest | 245/245 passed |
| Stream-focused W4–W6 | 135/135 passed |
| Affected common + stream | 177/177 passed |
| Newest strict remediation gate | 14/14 passed |
| Consolidated strict remediation gate | 38/38 passed |
| Focused asyncio-debug gate | 135/135 passed |
| Scoped Ruff and strict mypy | Passed |
| Protected overlay full/focused/affected | 264/135/177 passed |
| Protected overlay repository Ruff/mypy | Passed |

The inherited clean-tree static baseline remains exactly three Ruff findings
and 24 strict-mypy `union-attr` findings, all in untouched
`b24api/api_test.py`. The specification SHA-256 and all seven protected overlay
hashes match W0 inventory.

## Authorization boundary

This acceptance authorizes W7 to integrate the reviewed explicit sequential
single, offset, counted-offset, keyset, and item-cursor traversals; bounded
fail-fast/tolerant batch streams; and bounded direct/batch reference scheduling.
It accepts their typed lifecycle, safety, budget, correlation, cleanup,
consistency, and report contracts.

The categorically unadmitted surface remains exactly:

1. `PartitionedKeysetPlan`;
2. counted `PARALLEL_FIXED_STRIDE`;
3. keyset terminal `BOUNDARY_ID_SEEN`;
4. `IdentityTracker.MONOTONIC` outside keyset;
5. plan/policy `IdentityRequirement.COMPOSITE`;
6. `ConfirmationPolicy.INDEPENDENT_ORACLE`;
7. `ConfirmationPolicy.BOUNDARY_ID_SEEN`;
8. plan/policy `OrderSemantics.INPUT`.

Explicit caller-selected `PROFILE_SHORT_PAGE` is admitted for offset, keyset,
and item-cursor plans only with `requested_page_size`; it retains
`CALLER_ASSERTED` assurance. It does not authorize a packaged or automatic
optimization profile. Item-cursor `min/max` remain documented compatibility
aliases of `first/last` under the strict unique/monotonic cursor contract.

No compatibility facade behavior, default strategy, automatic chooser,
pipelined prefetch, composite cursor, partition profile, proactive rate
profile, benchmark threshold, live write, or W9 evidence admission is accepted
here. W7 must satisfy `docs/bitrix24-client-2.0/w5-w7/review-obligations.md`;
W9 remains bound by `docs/bitrix24-client-2.0/w9/review-obligations.md`.

No webhook, portal data, live read, or write was used for this checkpoint.
