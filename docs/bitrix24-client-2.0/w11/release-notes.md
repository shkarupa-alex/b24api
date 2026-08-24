# Bitrix24 client 2.0 release-candidate notes

## Summary

This candidate replaces separate retry, batch, pagination, and reference
helpers with one correctness-first asynchronous execution architecture while
retaining the committed facade wrappers. It is a source-level 2.0 candidate;
the installable version remains derived from the eventual release tag by
`setuptools-git-versioning`.

## Highlights

- Immutable `Request`, `Response`, plans, policies, identities, reports, and
  correlated outcome models.
- Replay-aware direct and batch execution with bounded attempts, decoded-row
  budgets, and failure-phase classification.
- Explicit offset, counted-offset, keyset, item-cursor, and partitioned-keyset
  plans sharing validated traversal drivers.
- Bounded reference fan-out with ready/input ordering, direct/batch dispatch,
  backpressure, cancellation, and deterministic cleanup.
- Typed terminal reports distinguish state-machine completion, assurance, and
  snapshot status. Incomplete work cannot appear completed.
- Strict immutable endpoint-profile loading and applicability checks. Runtime
  probes may contradict but never authorize a profile.
- Shared credential redaction across settings, errors, transport, traceback
  boundaries, reports, logs, and repository-only evidence artifacts.
- Repository-only deterministic evidence harness with schema validation,
  exact candidate/lineage binding, atomic multi-file publication, resumable
  manifest ownership, and exact-identity cleanup.

## Compatibility

The original construction, call, batch, list, and reference entry points remain
callable. Existing positional parameters and valid yield shapes are preserved;
reviewed controls are keyword-only. Legacy entity, error, query, type, and
settings import paths remain available for the documented compatibility period.

See `compatibility-ledger.md` for the exact root exports, facade signatures,
legacy imports, and deliberate corrections.

## Behavior corrections

- Unsafe or ambiguous requests are not blindly retried.
- Pagination cannot silently complete after a contradictory total, invalid
  range, duplicate, non-progressing continuation, cursor inconsistency, budget
  exhaustion, or cleanup failure.
- Counted traversal retains the committed head-plus-batched-tail shape while
  validating physical and logical accounting.
- Compatibility page-size overrides narrow the selected plan instead of making
  the client reject its own valid response.
- Structured Bitrix errors take precedence over generic HTTP status handling;
  impossible status values and malformed response encoding remain correctness
  failures.
- The observed CRM post-delete absence tuple is admitted only for
  `crm.deal.get` with HTTP 400, an empty error code, and exact `Not found`
  description; neighboring methods, statuses, and descriptions remain
  correctness failures.
- Stream cleanup preserves the first detected failure and replays genuine
  external cancellation before unrelated subsequent I/O.

## Evidence and release boundaries

- The packaged default profile set is empty.
- No W10 optimization is admitted.
- No live performance claim or live endpoint profile is included.
- The supplied portal currently exposes no documented exact portal build.
  Live plans record the unknown build as null and bind portal identity to the
  normalized host plus HMAC role/principal fingerprint. Unknown build cannot
  authorize a build-scoped endpoint profile.
- Two five-entity live plans passed their separate human approval checkpoint.
  Tasks stopped inconclusive and cleanup proved absence. CRM seed and
  independent verify passed; its first cleanup exposed a previously unknown
  absence envelope, after which exact-manifest recovery removed all five deals
  with zero marker mismatches. The failed cleanup is not reported as an
  evidence PASS; the complete sanitized record is in
  `../w9/live-dataset-execution-8d76a07.md`.
- Large live stress data is not required for the correctness core, and all
  claims depending on unavailable live evidence remain unclaimed.
- Evidence/seed/cleanup tooling is excluded from the wheel.

## Verification

The release candidate is gated by full pytest, Ruff check and format, strict
mypy, candidate diff checks, export/signature/import snapshots, deterministic
model matrices, wheel inventory, and the accumulated compatibility and failure
regressions from W0 through W9. Final acceptance requires the W11 same-SHA human
review packet.
