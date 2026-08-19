# W0 cross-review packet

## Immutable subject

- Evidence SHA: `d760c438826271b941abee291696ed7eea5c352d`
- Original baseline SHA: `08277c4d921b83b9252177b3e72a21a4c0c86109`
- Branch: `codex/bitrix24-client-benchmarks`
- Diff: 7 new files, 683 insertions; no production or test file changed.
- This packet is documentation-only and does not alter the evidence SHA.

Review exactly:

```text
git diff 08277c4d921b83b9252177b3e72a21a4c0c86109..d760c438826271b941abee291696ed7eea5c352d
```

## Contents

- `inventory.md`: committed/dirty/corpus symbol classifications, callsite
  frequencies, locked input hashes, baseline results, assumptions, and gaps.
- `dataset-plan.schema.json`: preview versus explicitly approved seed plans.
- `dataset-manifest-record.schema.json`: append-only creation, reconciliation,
  verification, cleanup, and orphan records.
- `oracle-record.schema.json`: qualification, normalization, set/multiset/order
  hashes, and pre/post snapshot state.
- `benchmark-plan.schema.json`: Track-A0 controls, same-SHA inputs, cases, and
  correctness-before-benefit gates.
- `evidence-artifact.schema.json`: redacted outcomes, reports, metrics, controls,
  and evidence links.
- `batch-chaining-probe.json`: sanitized result and exact non-secret request
  shape for the one required W0 read-only probe.

## Checks run

| Check | Result |
|---|---|
| Original HEAD pytest in detached clean worktree | 54/54 passed. |
| Original HEAD Ruff | 3 pre-existing test findings recorded. |
| Original HEAD strict mypy | 24 pre-existing test errors recorded. |
| Current dirty candidate pytest | 73/73 passed. |
| Current dirty candidate Ruff | Passed. |
| Current dirty candidate strict mypy | Passed. |
| Draft 2020-12 metaschema validation | All 5 schemas valid. |
| W0 webhook/token pattern scan | Passed. |
| `git diff --cached --check` before evidence commit | Passed. |
| Pre-existing dirty-file SHA-256 verification | All 8 hashes unchanged. |

## Assumptions to challenge

1. Original committed HEAD, not the dirty candidate, is the compatibility
   baseline.
2. Unprefixed helper classes are importable implementation details but not root
   API promises; they remain importable during migration.
3. Current dirty-corpus lexical call counts are compatibility pressure, not a
   claim about production frequency.
4. Schema fields are sufficient to reject secret-bearing, lineage-incompatible,
   unreviewed, or non-resumable live artifacts before W9 implementation.
5. Successful `$result[...]` substitution is query-shape-specific evidence and
   authorizes no plan by itself.

## Counterexamples captured

- `tasks.task.list` can expose different item, filter, and order casing.
- Requested list size can differ from the returned stride.
- Offset, total, continuation, ordering, and filters can be ignored or false.
- Batch envelopes can omit or mis-correlate individual commands.
- Writes can become ambiguous after dispatch and must not be replayed merely
  because an error is transient.
- Duplicate IDs can be valid only under a declared multiset/composite contract.
- A stable traversal result does not establish a stable snapshot.
- Chained batch commands are possible on the observed portal, contradicting a
  universal separate-round-trip assumption.

## Known gaps

- W7 still needs executable characterization against the 1,255-file corpus.
- No concrete live dataset plan, generator, manifest, oracle, endpoint profile,
  or performance threshold instance has been approved.
- No W1/W2 production code exists at this SHA.
- Original static failures remain to be corrected without weaker configuration.
- The specification and corpus working trees remain dirty user-owned inputs,
  locked by hashes rather than copied into this commit.

## Decisions requested

Approve or reject the following as one W0 packet:

1. the committed symbol census and dirty-tree dispositions;
2. the corpus scope and compatibility priorities;
3. the five preregistered schema boundaries;
4. the conclusion that batch command chaining deserves only a separately
   specified candidate, not immediate implementation;
5. proceeding to W1 (redaction/errors) and W2 (immutable models/policies).

Approval does not authorize live writes, evidence-based retry/batch conclusions,
traversal defaults, optimizations, performance regressions, or release. Those
remain behind later blocking checkpoints.
