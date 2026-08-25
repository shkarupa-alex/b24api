# Owner amendment: identity tracking without storage backends or cardinality limits

Date: 2026-08-24

Status: approved owner decision

Supersedes the identity-tracking decisions in the finalized council specification at
`spec/2026-08-24-b24api-capability-architecture/spec-review.md`. The finalized council bundle
remains immutable; where the two documents disagree, this amendment is authoritative.

## Decision

The client must not expose an identity-storage backend or impose an identity-cardinality limit.
Identity tracking is an internal traversal mechanic, not a user-selectable capability.

The target architecture therefore has none of the following:

- `IdentityTracker` in the public or internal configuration surface;
- `ExecutionPolicy.max_tracked_identities`;
- SQLite, temporary databases, spill files or any other database-backed identity store;
- `BudgetExceededError` caused solely by the number of identities already observed;
- an automatic change from exact tracking to probabilistic or disabled tracking.

## Required behavior

For sequential and counted offset traversals that promise exact identity validation, the client
keeps the observed identities in an in-memory set for the lifetime of the operation. Tracking
continues for the complete traversal; its memory consumption is intentionally proportional to the
number and representation size of distinct identities.

When an operation crosses 100,000 distinct in-memory identities, it emits exactly one ordinary
Python `RuntimeWarning` for that operation. The warning states that exact duplicate/loss detection
continues and that a very large result may consume additional memory or run more slowly. The
warning does not change the operation result, terminal state, assurance level or tracking mode.
Users may handle or suppress it through Python's standard warnings filters.

Keyset and cursor traversals use their contract's strict progression checks and retain only the
state needed to validate that progression; they do not keep the complete identity set when exact
monotonic progression makes it unnecessary.

The existing row-buffer, request, page, elapsed-time and concurrency budgets remain in force.
They are independent from identity cardinality and are not changed by this amendment.

## Required implementation changes

The implementation work derived from the council specification must:

1. remove `IdentityTracker`, `_SqliteIdentityStore` and their SQLite dependency;
2. remove `max_tracked_identities` from `ExecutionPolicy` and all validation/branching based on it;
3. retain exact in-memory duplicate detection for sequential and counted traversal without a
   cardinality cutoff;
4. keep monotonic progression tracking as an internal mechanic for eligible keyset/cursor plans;
5. emit the one-shot warning at the 100,000-distinct-identity crossing without interrupting the
   traversal;
6. remove `IdentityTracker` from the proposed root export manifest and documentation examples.

## Acceptance tests

- A counted traversal with more than 100,000 distinct identities completes exactly and does not
  create a database, temporary spill file or `BudgetExceededError`.
- The same operation emits one warning, not one warning per row or page.
- A duplicate above and below the warning boundary is still detected and reflected in the exact
  terminal report according to the traversal contract.
- A large keyset/cursor traversal validates strict progression without retaining every identity.
- No public constructor, policy, CLI flag or README example mentions an identity backend or maximum
  tracked-identity count.
- Packaging and dependency inspection finds no SQLite-related client code introduced for identity
  tracking.

## Explicit replacements in the council specification

- The `ExecutionPolicy` sketch around line 365 loses `max_tracked_identities` and
  `identity_tracker`.
- Section 32.4's MEMORY/SQLITE ceiling and fail-before-tail rule are replaced in full by this
  document's required behavior.
- The proposed root export list around lines 1723-1725 loses `IdentityTracker`.
- The acceptance item around line 1733 becomes: “counted total above 100,000 completes with exact
  in-memory tracking, emits one warning and creates no spill storage.”

## Rationale

A Python set of identities has real but gradual, workload-dependent cost; 100,000 records is not a
meaningful universal failure boundary. Refusing an otherwise correct export at that arbitrary point
would reduce the client's usefulness. Introducing SQLite to avoid that refusal would couple a thin
API client to a storage mechanism the product does not need. The chosen design keeps correctness,
removes configuration and storage complexity, and makes the resource trade-off visible without
turning it into an artificial operational failure.
