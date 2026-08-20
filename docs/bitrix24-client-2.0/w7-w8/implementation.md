# W7 compatibility and W8 profile implementation

This package is part of the combined W7-W9 pre-live candidate. It does not
authorize a live probe, write, profile default, or performance claim.

## W7 compatibility facade

The first W7 subject `2f7c3ee...` was rejected by independent clean-room review.
The remediation preserves explicit positional and keyword `Settings`, applies
the configured list/batch/timeout/logger defaults, and keeps the environment
constructor only for the zero-argument case.

Every public compatibility async generator owns and closes its inner canonical
stream in `finally`. This applies to batch, list, keyset/cursor, and reference
wrappers for sync and async sources. Early close therefore runs source cleanup
and freezes the canonical report instead of leaving dispatcher tasks behind.

The committed mappings now preserve:

- payload tuples in tolerant `batch_outcomes`;
- `BatchOutcomeStream` and `EndpointProfile` annotations;
- descending direction for legacy cursor `min`;
- identity fields in explicit no-count `select` values;
- no-total server-next/empty-page sequential traversal;
- page-round reference yield order and batch coalescing;
- the empty-mapping `list_method=True` result;
- settings-backed batch size, list page cap, retry timeout, and logger.

Compatibility `list_size` is a caller-declared decoded page cap. Only the
cursor wrapper sends the committed `LIMIT` control; other wrappers do not
invent endpoint-specific parameter names.

## W8 profiles

`b24api.profiles` provides immutable versioned profiles, strict JSON loading,
exact `QueryShape`, pure `choose_plan`, and value-free `explain_plan`. A profile
is applicable only when query shape, portal build, scopes, evidence window, and
execution policy all match. Unknown or expired evidence refuses. Runtime probes
can only contradict or downgrade; they never promote assurance.

The library packages the profile schema and an intentionally empty profile set.
No endpoint profile or automatic strategy default is admitted before live
evidence and a separate human decision. The facade refuses a raw profile when
it lacks exact runtime build/scope context rather than guessing applicability.

`OperationReport` records complete hash-only profile provenance. A
`PROFILE_VERIFIED` report requires profile ID/version, applicability, source
SHA-256, and evidence SHA-256 together.

## Gates before combined review

The combined candidate must reproduce:

- all repository tests;
- W7 facade and cleanup characterization;
- W8 profile/schema/chooser tests;
- repository Ruff with `--no-fix`;
- strict mypy;
- `git diff --check`;
- a wheel inventory containing `b24api` profile JSON but no evidence harness.
