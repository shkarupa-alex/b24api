# Resolution of clean-room findings against rejected W7 subject `2f7c3ee...`

| Rejected finding | Remediation in the combined candidate |
|---|---|
| Outer compatibility generators stranded owned streams/tasks on early close. | Every wrapper retains the inner iterator and closes it from `finally`; sync and async source-finally regressions cover batch/list/reference surfaces. |
| Positional explicit settings were swallowed by dependency injection. | `Bitrix24(settings: Settings | None = None)` preserves positional/keyword identity and resolves the environment only when `settings is None`. |
| Cursor `min` kept ascending direction. | The compatibility plan declares descending direction for `min`; multi-row descending characterization passes. |
| `batch_outcomes` rejected `(request, payload)`. | The adapter accepts either form and retains payload correlation in typed outcomes. |
| No-count wrappers dropped the identity from explicit `select`. | The exact legacy identity is appended without mutating caller input. |
| Sequential default required a total before following `next`. | The default uses advisory total semantics and authorized server-next/observed continuation with empty/qualified terminal. |
| Settings bounds were ignored. | Facade defaults map batch/list sizes, retry timeout, retry controls, and logger name. |
| Reference order changed from page rounds to per-reference grouping. | Legacy wrappers use READY scheduling with finite-source admission and page-cap coalescing; A1/B1/A2/B2 and exact batch counts are characterized. |
| `batch(list_method=True)` rejected `{}`. | Empty mapping preserves the committed empty-list compatibility result. |
| Type/signature surface used generic names. | `BatchOutcomeStream` is exported and facade bridges annotate `EndpointProfile`; signature snapshots include kinds, defaults, annotations, and returns. |
| Ownership/cursor/empty-array documentation was absent. | README records iterator ownership, cursor aliases/exhaustion behavior, and zero row weight for an empty batch array. |

This resolution is not an acceptance record. Independent same-SHA clean-room
review remains mandatory before human handoff.
