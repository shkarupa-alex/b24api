# W5/W6 human-review and pre-handoff findings resolution

Rejected human-review subject:
`0b9dfe9c1e80753c09184f259a021b198ca8d9b1`.

Replacement code subject:
`2d4ea6cc6141fee2343c7abd4151d81b9c167d99`.

The replacement was not handed back immediately after the human findings. Two
additional detached clean-room cycles rejected `10b1509...` and `67f9d2e...`.
Only `2d4ea6c...` received a zero-finding code admission verdict.

## Human code findings

| Finding against `0b9dfe9...` | Resolution in the replacement | Regression evidence |
|---|---|---|
| Counted offset could report `COMPLETED` when a server ignored offset but changed `next`/`total`. | Page repetition fingerprints only decoded qualified items; continuation metadata can no longer disguise a repeated page. | A two-page `[1,2]` replay with changed continuation metadata raises `PaginationError`. |
| Declared ordering was ignored by single, offset, and counted drivers. | Effective plan/policy order is merged once and every page validates the declared ascending or descending identity order. | Single and offset descending/ascending counterexamples fail with typed `PaginationError`. |
| `ConsistencyPolicy` controls other than snapshot were not enforced. | Plan and policy identity, duplicate, total, order, and confirmation controls are merged or rejected before I/O; advisory total drift/mismatch remains a warning. | Required identity and incompatible contracts refuse with zero calls; duplicate/order/total/confirmation cases have regressions. |
| `fan_out` compared a list result's total with one wrapped outcome. | Whole-result delivery remains one correlated `ReferenceItem`, while qualified-total validation uses the nested list length. | Direct and batch list results with matching totals complete exactly once. |

## Additional clean-room code findings closed before handoff

| Rejected subject | Finding | Resolution in `2d4ea6c...` |
|---|---|---|
| `10b1509...` | A nested whole result could retain arbitrarily many decoded rows behind one buffer unit. | Pages carry exact per-item decoded-row weights. Direct and batch fan-out reserve a concurrency-safe share before I/O; oversized results fail without entering the retained-row ledger. |
| `10b1509...` | W6 global contract validation depended on receiving a reference. | Source-independent plan/policy validation runs before context start or source pull, including empty and blocked input. |
| `10b1509...` | `BatchStream` retained decoded outcomes without enforcing or reporting `max_buffered_rows`. | Effective chunk size is bounded by the row ceiling; live buffering is charged through delivery and terminal high-water is reported. |
| `67f9d2e...` | W6 accepted a noncanonical plan at construction and later leaked `AttributeError`. | `iter_references()` performs the same closed-union validation as `iter_list()` and raises construction-time `TypeError` before touching input. |

## Review-packet findings

| Packet finding | v2 disposition |
|---|---|
| The old packet denied prefetch while `__aenter__` performs an initial pull. | v2 names this exact behavior lifecycle priming required by normative section 7.7. It separately denies pipelined continuation/page lookahead, which is the performance-prefetch candidate from section 27.6. |
| The history omitted initial W5 SHA `bc75c33...`. | v2 includes it and explains that W5-only findings were folded into the first combined W5/W6 clean-room row. |
| Untracked 500/500 loops and `/tmp` scripts were presented as reproducible gates. | They are explicitly non-gating reviewer observations. Immutable-SHA admission relies on committed tests and exact commands. |
| The 2/2 primary/cleanup gate omitted node IDs. | v2 gives both exact pytest node IDs and the full invocation. |
| “Focused W4–W6” omitted changed common-layer files. | v2 reports both the 118-test stream-focused set and the 160-test affected common-plus-stream set. |
| Rejected rows did not distinguish severity or explicitly close all findings. | v2 records finding counts for the human and final clean-room iterations and states that every earlier code finding is closed on the admitted SHA. |
| The diff base included accepted W3/W4 documentation commits. | The code review command uses exact branch point `99c13fa3be0bbdbd8248829da7df5ed55c7d2dc9` and `-- b24api`. |
| Bare Ruff could mutate because project configuration enables fixes. | Every normative Ruff invocation uses `ruff check --no-fix`. |
| The injected-transport deadline/cancellation contract existed only in packet prose. | `Transport` now documents cooperation with `attempt_timeout` and cancellation in `b24api/execution.py`. |
| Full debug teardown exposes plugin-owned unclosed sockets/event loop. | v2 discloses the observation as a W9 harness obligation; targeted runtime gates remain strict and green. No library ownership was established. |

## Closure statement

Every code finding recorded from `bc75c33...` through `67f9d2e...`, including
the four human findings, was replayed against the final subject. Independent
clean-room review of `2d4ea6c...` reported P1/P2/P3 = 0/0/0. This closes only
the W5/W6 instrumentation boundary; it does not authorize W7 facade defaults,
automatic strategy selection, pipelined prefetch, profiles, or W9 evidence.
