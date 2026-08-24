# W9 live dataset execution — accepted 8d76a07 plans

## Authority and boundary

- execution candidate: `8d76a07e85e359df2e96109f0555b7ec2503e7a5`;
- review packet: `98b9b3adefff1b9c82127f70f40c4809f9902932`;
- Tasks approval subject:
  `badff1d0a65bb8b4239c268ba1cde9c6807fc7417bfb3d2f29f8d108378b59b6`;
- CRM approval subject:
  `dac387bed41c325129809778c8bcd76fd1a93b18e6df76ad5b1f64ad9a7d1f70`;
- human decision: ACCEPT;
- recorded approval time: `2026-08-24T07:00:28+00:00`;
- review commit time: `2026-08-24T09:24:37+03:00`
  (`2026-08-24T06:24:37+00:00`);
- Tasks final plan SHA-256:
  `935ea278a2574d8a867a2726751d995da99c04d2020bc1f0e48622b1cbbfba52`;
- CRM final plan SHA-256:
  `b77013b3e3b4e1828acf7001c4fe51c9528c736d447ded1c79e56339d6f485bf`.

The approval time is after the review commit. Both final plans validated,
reproduced their reviewed approval-subject hashes, used the lower ceiling five,
and passed the exact review-commit trailer gate before any write command.

## Tasks result

Seed made four HTTP attempts and stopped `INCONCLUSIVE` on the first
correlation after an ambiguous create. It did not attempt the remaining four
correlations. The command then immediately entered the approved
manifest-owned cleanup path. Exact-marker reconciliation found no entity and
recorded `absence_verified`.

```text
seed outcome: INCONCLUSIVE
terminal correlations: 1
cleanup outcome: PASS
orphan_count: 0
absence_verified: true
final bundle scan: PASS
```

No Tasks entity from the plan remained on the portal.

## CRM result

Seed created and read-back verified all five exact-marker deals. Independent
verify performed two complete point-read snapshots and passed with equal
expected/actual hashes.

```text
seed outcome: PASS
seed terminal entities: 5 verified
seed HTTP attempts: 12
verify outcome: PASS
snapshot_state: verified
assurance: oracle_verified
raw_count / unique_count: 5 / 5
verify HTTP attempts: 12
```

Cleanup successfully deleted the first exact manifest-owned deal, journaled
`deleted`, and then stopped because the subsequent `crm.deal.get` returned the
previously unreviewed absence envelope `error: ""` plus
`error_description: "Not found"`. Consequently the original bundle has no
`cleanup-evidence.json` PASS and MUST NOT be represented as a successful
harness cleanup run.

To avoid leaving test data behind, an operator recovery pass used only the five
exact IDs and marker values already present in the validated manifest. For each
identity it point-read the ID, required exact `TITLE == marker_value` before
delete, and required the exact observed absence envelope after delete.

```text
manifest correlations: 5
already absent: 1
deleted during recovery: 4
marker mismatches: 0
all absent: true
```

No prefix or list-derived deletion was used. No CRM entity from the plan
remained on the portal.

## Closure of the observed contract gap

Fix candidate `10e91a217fd85e597809d3d91146d4a5db5a548c` recognizes only the complete
observed tuple — `crm.deal.get`, HTTP 400, empty code, and `Not found` — as
typed absence. Other descriptions, CRM HTTP 200/500, and the same envelope from
the Tasks method remain correctness failures. Its full Python 3.14 suite
reports 498 passed;
Ruff check/format, strict mypy, cumulative diff-check, detached status, and the
normative spec hash pass.

A read-only live replay through the fixed `DisposableAdapter.read` checked all
five deleted manifest IDs:

```text
manifest correlations: 5
classified absent: 5
HTTP attempts: 5
all absent: true
```

This replay validates the newly observed absence classification. It does not
retroactively manufacture a cleanup PASS artifact for the older execution
candidate.
