# Design record archive

This directory is a public archive of the design records (specifications, syntheses and decision
ledgers) that shaped b24api. It is kept in the repository deliberately, so that the reasoning
behind a contract stays reviewable next to the code.

These records are **not normative documentation**. They describe the design at the time each one
was written and are not updated when the code changes; later records and the code supersede them.
The maintained documentation is the [README](../../README.md) and the pages under
[`docs/`](../): [architecture](../architecture.md), [migration](../migration.md),
[performance](../performance.md) and [endpoint recipes](../recipes.md). When a record and the
maintained documentation disagree, the maintained documentation and the tests win.

The archive is not shipped in the wheel: the published distribution contains only the `b24api`
package, and the release gate (`.github/scripts/verify_release.py wheel`) refuses a wheel that
carries `docs/` or any other file beside the package and its metadata.

Records must never contain credentials. Portal webhooks, tokens and other secrets used during live
probes are replaced with placeholders such as `/rest/<user>/<token>/` before a record is added here.

| Record | Topic |
|---|---|
| [`b24api-issues-architecture/`](b24api-issues-architecture/specification.md) | Architecture program for issues #1–#8: routes, traversal, transport and error contracts, examples |
| [`b24api-open-issues-release-ci/`](b24api-open-issues-release-ci/specification.md) | Release workflow gate for issues #10 and #11 |
| [`review-astra-fable/`](review-astra-fable/b11-dead-code.md) | Astra-fable review follow-up: dead-code outcomes (B11), [16 MiB parse and digest benchmark](review-astra-fable/b8-b12-benchmark.md) (B8, B12), [lock outcomes](review-astra-fable/c8-locks.md) (C8) |
