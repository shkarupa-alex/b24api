# W9 live dataset plan review — candidate 8d76a07

This packet supersedes the rejected
`live-dataset-plan-review-9946f4b.md`. No approval or live mutation from that
older packet is carried forward.

## Decision requested

Approve or reject two small disposable live dataset plans generated read-only
by exact evidence candidate
`8d76a07e85e359df2e96109f0555b7ec2503e7a5`:

- five Tasks entities using `tasks-task-v1`;
- five CRM deals using `crm-deal-v1`.

Approval authorizes only `seed`, independent `verify`, and exact
manifest-owned `cleanup` for these ten identities on the supplied test portal.
It does not authorize a larger dataset, arbitrary deletion, W10, performance
admission, or release.

## Evidence-producing candidate

- exact SHA: `8d76a07e85e359df2e96109f0555b7ec2503e7a5`;
- independent clean-room verdict: ACCEPT, P1/P2/P3 = 0/0/0;
- Python 3.14 full suite: 490 passed;
- Ruff check/format, strict mypy, cumulative diff-check, detached status:
  PASS/clean;
- normative specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

The portal build is unknown and stored as null. Portal identity binds the
normalized host to an HMAC fingerprint over host, credential role, and
principal. `b24api/profiles.py` marks an unknown build with
`ProfileReasonCode.UNKNOWN_BUILD`, so it cannot authorize a build-scoped
`PROFILE_VERIFIED` profile. This restriction is independent of the live
dataset lifecycle authorized here.

## Shared portal boundary

```text
host: bxtest851.bitrix24.ru
credential role: admin_full
portal fingerprint: bbfa46964cb3cb6d23afd84c3d3de3b33c19a34996ce53277382456e8d8fac45
fingerprint algorithm: hmac-sha256-host-role-principal-v1
fingerprint key format: base64url-no-padding-32-bytes
portal build: null
scope hash: 701e3deb8a644cfa4b3be864e4744fbd65ba1cc4a8246e849398b0b4ab0a793f
```

The webhook, fingerprint key, principal ID, and response bodies are absent
from the plan and repository. Read-only preview used only `scope` and
`app.info`.

## Tasks plan

```text
run_id: f29fa563-d08d-41fd-8899-4ea060d54ff9
lineage_id: 9f079ba0-f60f-4434-8259-271e69f527d7
namespace: b24api-evidence-f29fa563-d08d-41fd-8899-4ea060d54ff9
cell: LIVE-SMALL
profile: tasks-task-v1
entity family: task
target entities: 5
relationships: 0
create/delete strategy: direct/direct
estimated lifecycle requests: 41
estimated quota impact: 41.0
estimated duration: 4.1000000000000005 seconds
batch commands: 0
preview content SHA-256: 83b7e7a7b89a11118d76b4bce97762d20df1f8167fdbc5cd712753d011391cac
reviewed approval-subject SHA-256: badff1d0a65bb8b4239c268ba1cde9c6807fc7417bfb3d2f29f8d108378b59b6
preview bundle scan: PASS
```

## CRM plan

```text
run_id: db21d131-0799-45a6-994f-c9ec740fc1d1
lineage_id: 77f0dc0d-7e02-4396-a384-58e51a15d579
namespace: b24api-evidence-db21d131-0799-45a6-994f-c9ec740fc1d1
cell: LIVE-SMALL
profile: crm-deal-v1
entity family: crm_deal
target entities: 5
relationships: 0
create/delete strategy: direct/direct
estimated lifecycle requests: 41
estimated quota impact: 41.0
estimated duration: 4.1000000000000005 seconds
batch commands: 0
preview content SHA-256: d1c19ef6e600cff6e375d3a8cf068f0d7f189f9df3c316a628178821da06a7d5
reviewed approval-subject SHA-256: dac387bed41c325129809778c8bcd76fd1a93b18e6df76ad5b1f64ad9a7d1f70
preview bundle scan: PASS
```

The 41-request estimate covers the nominal future lifecycle: two preflight
calls for each of seed, verify, and cleanup; create/read-back; two independent
verification point reads; and ownership-read/delete/absence-read for each of
five identities. Retry or ambiguity reconciliation can only increase actuals.

## Approval-subject projection

The two reviewed hashes above cover the complete proposed plan with this
authorization block:

```json
{
  "allow_writes": true,
  "approved_at": null,
  "approved_by_user": true,
  "live": true,
  "max_entities_per_cell": 5,
  "plan_review_sha": null,
  "state": "approved_for_seed"
}
```

The operational ceiling is therefore exactly five per cell, not the machine
maximum of 500 shown in the read-only preview. Both the cell count and this
lower ceiling are covered by the reviewed hash.

`approved_at` is null because the human decision has not happened yet.
`plan_review_sha` is null because the commit containing this packet does not
exist while its own contents are being prepared. The review hash projects only
these two necessarily post-decision fields to null.

After an explicit ACCEPT, the operator records the actual response time and
the exact packet commit SHA in schema-valid final plans. Their final
full-content SHA-256 values are then computed and supplied to
`--confirm-plan-content-sha256`. Those execution-confirmation hashes cannot be
reported before the decision and are not values the reviewer is asked to
approve. The packet commit carries one `Dataset-Plan-SHA256:` trailer for each
reviewed approval-subject hash.

## Mutation and recovery boundary

For each planned correlation, seed writes a journal dispatch record before the
create call, reads the exact created entity back, and verifies the unique
marker. Resume reconciles ambiguous creates by exact marker and never blindly
repeats a possibly delivered create.

Cleanup only point-reads the exact manifest-owned ID, verifies the marker,
deletes that ID, and verifies absence. A reused or mismatched ID becomes an
orphan and is not deleted. Cleanup is resumable from dispatched/cancelled
states. The plans use separate runs, lineages, namespaces, manifests, and
artifact directories.

## Stop conditions

Do not seed if any of these differ at execution time:

- candidate SHA or clean tracked state;
- portal host, HMAC fingerprint, credential role, scope hash, or nullable
  portal build;
- reviewed approval-subject hash, packet commit SHA, run, lineage, namespace,
  profile, count, or lower authorization ceiling;
- explicit `--live`, `--allow-writes`, review-SHA confirmation, or final
  plan-content-hash confirmation.

After seed, run verify and then cleanup even if later evidence is inconclusive.
Any orphan or unverified absence blocks PASS and requires operator attention.

## Human response requested

The exact review commit SHA is reported alongside this packet after the commit
exists. Respond by binding that SHA and both reviewed approval-subject hashes,
or list findings and reject. No final approval timestamp is created and no live
mutation runs before that explicit response.
