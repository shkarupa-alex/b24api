# W5/W6 accepted-subject review findings resolution

Human-accepted semantic subject:
`a29b58f3faba9a71202bbe1e9a4aab0f770b369b`.

Test/doc-only evidence addendum:
`61345c16f39beef090b672d583abfe542ebdd92c`.

Accepted review packet:
`e9439b4e30f8f74cbd1c70ab793f94ec13734522`.

## Findings and disposition

| Finding after acceptance | Resolution in `61345c1...` | Evidence |
|---|---|---|
| The packet's closed count of five categorically refused controls omitted `ConfirmationPolicy.INDEPENDENT_ORACLE`, `ConfirmationPolicy.BOUNDARY_ID_SEEN`, and plan/policy `OrderSemantics.INPUT`. | The addendum records the exhaustive eight-control surface. A four-case parameterized test covers both confirmation modes and plan/policy INPUT; the existing strategy-plan node now also executes the boundary-ID keyset refusal. | Every case raises typed `CapabilityError` before HTTP and leaves transport requests empty. An independent audit traversed exported enum members, the canonical plan union, and every production `CapabilityError`; it found no ninth categorically unadmitted control. |
| Explicit keyset/cursor `PROFILE_SHORT_PAGE` termination was implemented but absent from the authorized boundary and lacked runtime traversal tests. | The addendum explicitly admits caller-selected short-page termination for offset, keyset, and item-cursor plans when `requested_page_size` is set. It distinguishes this from an automatic or packaged optimization profile. | New keyset/cursor cases deliver the short final row, make one request, finish `COMPLETED` with `CALLER_ASSERTED`, emit no violations, and preserve exact terminal reasons. Offset runtime coverage already existed. |
| `cursor_take=min/max` are aliases of `first/last` under strict monotonic cursor validation, but that limitation appeared only in packet/W7 prose. | `ItemCursorPlan` now documents the aliases and strict unique/monotonic precondition in its own public docstring. Runtime semantics and accepted fields are unchanged; W7 remains obligated not to present aliases as distinct capabilities. | The diff from the accepted subject changes no executable production statement: only the plan docstring and committed tests differ. |

The addendum does not reopen or change the human-accepted traversal semantics.
Per section 21, documentation-only and unrelated test-evidence changes record a
non-impact determination rather than invalidating the accepted subject.
