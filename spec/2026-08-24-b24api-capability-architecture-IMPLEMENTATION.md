# b24api capability and architecture — canonical implementation handoff

Status: **approved for implementation**

Date: 2026-08-24

This is the canonical entry point for implementing the b24api capability and architecture
refactor. Give an implementation agent this file. It defines the complete normative document set
and its precedence; the agent must read both linked documents completely before decomposing or
editing code.

## Normative documents

1. Base specification:
   [`2026-08-24-b24api-capability-architecture/spec-review.md`](2026-08-24-b24api-capability-architecture/spec-review.md)
2. Owner amendment, which is normative wherever it differs from the base:
   [`2026-08-24-b24api-capability-architecture-owner-amendment.md`](2026-08-24-b24api-capability-architecture-owner-amendment.md)

Precedence is strict:

1. the owner amendment;
2. the base specification;
3. council proposals, reviews and other diagnostic files are non-normative context only.

The base council bundle is finalized and immutable. Its obsolete SQLite/cardinality-limit text is
preserved solely to keep the reviewed bundle verifiable; it is not an implementation requirement.

## Final owner decisions that must not be reopened during implementation

- Backward compatibility and preservation of legacy wrappers are not goals. Remove the legacy API
  surface instead of maintaining a second compatibility surface.
- Preserve useful capabilities, correctness and structural performance rather than old method
  names.
- The client remains thin and knows no concrete Bitrix24 entity, method, recipe or application
  storage convention.
- A logical `batch` accepts an arbitrary-length command stream and internally uses bounded physical
  Bitrix batches.
- Canonical list traversal is the conservative sequential mechanism. Counted, keyset and cursor
  strategies are explicit alternatives.
- Generic safe no-count behavior is the already-established sequential keyset fallback;
  `PartitionedKeysetPlan` is not admitted in this scope.
- Recipe migration and changes to the Bitrix24 skill are outside this implementation. Recipes are
  sampled only as a read-only requirements corpus; do not inspect the entire corpus.
- Add a compact installed `b24api` CLI for decoded/raw calls and JSONL list output as specified.
- The supplied live webhook is out-of-band test configuration only. Never write its literal value
  to source, tests, documentation, evidence or Git. Live writes require separate explicit approval.
- No database is part of the client. Remove `IdentityTracker`, SQLite identity storage and
  `max_tracked_identities`. Exact sequential/counted tracking grows in memory without an artificial
  cardinality failure and emits one warning after crossing 100,000 distinct identities. Eligible
  keyset/cursor traversals use internal monotonic progression validation.

## Review boundary

The design council and its maximum three review iterations are complete. Implementation review
must evaluate conformance to the normative documents above; it must not restart architecture
deliberation unless an actual contradiction makes implementation impossible.

## Completion rule

Implementation is complete only when the requirements, acceptance tests, capability checks,
profiling checks, CLI behavior, legacy deletion, README update and packaging constraints from the
base specification—modified by the owner amendment—are all satisfied. Passing the existing test
suite alone is insufficient.
