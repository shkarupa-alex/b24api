# W5/W6 v3 human findings resolution

Rejected code subject:
`8ba3c40ca85f5e7c7123a2aec332c2dfe11f27d5`.

Replacement code subject:
`fd1fb727c4a13d748539948188a8375e8bbf80aa`.

Superseded packet commit:
`024af374a051bc877c5e514dc5e09672dafe4357`.

## Code findings

| Finding against `8ba3c40...` | Resolution in `fd1fb727...` | Regression evidence |
|---|---|---|
| `ItemCursorPlan` coerced cursor values with the row `IdentitySpec`, so a string row identity plus integer cursor failed after I/O. | `ItemCursorPlan.cursor_coercion` is an independent `IdentityCoercion`, defaulting to exact integer. Row identity coercion is no longer consulted for cursor extraction or continuation. | String row IDs with integer cursors complete. Clean-room probes also covered integer rows with string cursors, decimal-string cursor normalization, and incompatible cursor values failing before page emission. |
| `PROFILE_CURSOR_EXHAUSTED` attempted strict cursor extraction before recognizing a nonempty final page with no cursor. | Cursor extraction now distinguishes present, wholly exhausted (missing or null), and mixed pages. A wholly exhausted page is delivered and then completes; a mixed page fails before emitting any row from that page. Other terminal rules retain strict missing-cursor failure. | Parameterized missing/null final-page tests complete after delivering the row. A mixed present/missing page raises typed `PaginationError` with zero rows emitted from that page. Empty, cursor-zero, continued, direct-reference, and batch-reference forms were independently replayed. |

Independent clean-room review replayed both findings on the rejected subject,
then reported P1/P2/P3 = 0/0/0 on the replacement subject. It also replayed the
full prior cancellation, cleanup, correlation, buffering, safety, and plan
contract matrix.

## Packet and obligation findings

| v3 packet finding | v4 disposition |
|---|---|
| The pre-I/O refusal list omitted `IdentityRequirement.COMPOSITE`, and no committed test protected it. | v4 lists it as the fifth constructible but unadmitted control. A parameterized regression proves plan-level and policy-level refusal before source pull or HTTP I/O. |
| The admitted ItemCursor surface did not explain that cursor values must be unique and strictly monotonic within each continued page. | v4 states this restriction and records a binding W7 obligation not to expose a nonunique cursor field without a separately reviewed tie-breaker/composite contract. |
| `cursor_take=min/max` are observationally equivalent to `first/last` under the admitted strict ordering contract. | v4 and the W7 obligations identify these as aliases, not distinct public capabilities. |
| Empty batch arrays weigh zero but the consequence was unstated. | v4 states that they consume no decoded-row budget while command/outcome count remains bounded by the hard portal batch limit of 50. |
| The `error.py` provenance note was partly overstated. | No code change was made. v4 retains the accurate chain: W1 deliberately replaced the W0 candidate and accepted W3/W4 later extended the tracked file. |
