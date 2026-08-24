# W9 live dataset plan review — candidate 9946f4b

> **SUPERSEDED — DO NOT EXECUTE.** This packet was rejected and grants no
> mutation authority. Its two local `approved_for_seed` files were removed.
> The accepted replacement is `live-dataset-plan-review-8d76a07.md`, bound to
> review commit `98b9b3adefff1b9c82127f70f40c4809f9902932`.

## Decision requested

Approve or reject two small disposable live dataset plans generated read-only
by exact evidence candidate
`9946f4bd3a07ea3e24c95c14a6de2015eed09c0d`:

- five Tasks entities using `tasks-task-v1`;
- five CRM deals using `crm-deal-v1`.

Approval authorizes only `seed`, `verify`, and exact manifest-owned `cleanup`
for these ten identities on the supplied test portal. It does not authorize a
larger dataset, arbitrary deletion, W10, performance admission, or release.

## Evidence-producing candidate

- exact SHA: `9946f4bd3a07ea3e24c95c14a6de2015eed09c0d`;
- independent clean-room verdict: ACCEPT, P1/P2/P3 = 0/0/0;
- Python 3.14 full suite: 489 passed;
- Python 3.12.10 full suite: 489 passed;
- Ruff check/format, strict mypy, diff-check, detached status: PASS/clean;
- normative specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

The portal build is unknown and stored as null. Portal identity instead binds
the normalized host to an HMAC fingerprint over host, credential role, and
principal. Unknown build cannot authorize a build-scoped endpoint profile.

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
run_id: 4fe045a1-7604-4d7c-a095-320d2746e3d4
lineage_id: da408268-6322-4825-aa89-f3e0afdfa0ab
namespace: b24api-evidence-4fe045a1-7604-4d7c-a095-320d2746e3d4
cell: LIVE-SMALL
profile: tasks-task-v1
entity family: task
target entities: 5
relationships: 0
create/delete strategy: direct/direct
estimated requests: 29
estimated quota impact: 29.0
estimated duration: 2.9000000000000004 seconds
batch commands: 0
preview content SHA-256: 1f314dff0014f2a2e13d7521c2e4c61c332f1201b09aebe01d2321ff8c52ffba
reviewed approved-plan SHA-256: 1d8a493ce0e5303fafc1fe5d91642555f802b382f6d48f56506f16094f36aad6
preview bundle scan: PASS
```

## CRM plan

```text
run_id: 71a23226-a975-4e1f-831f-a4876917f9b7
lineage_id: 2637ce5e-e05a-45c6-b034-82aa65336293
namespace: b24api-evidence-71a23226-a975-4e1f-831f-a4876917f9b7
cell: LIVE-SMALL
profile: crm-deal-v1
entity family: crm_deal
target entities: 5
relationships: 0
create/delete strategy: direct/direct
estimated requests: 29
estimated quota impact: 29.0
estimated duration: 2.9000000000000004 seconds
batch commands: 0
preview content SHA-256: cc864ea773dd1251ce0d7d9a5023aff2ee052e7f3df1687f0722786ac114769e
reviewed approved-plan SHA-256: 68bb6b468e1fd0b773af0bf7e53580be430d2f0e74cfa2bd4a5aeb1ee42f57fd
preview bundle scan: PASS
```

## Approval projection

Both reviewed hashes cover the complete plan with this authorization block,
except that `plan_review_sha` is projected to null by the normative hash
function to avoid circularity:

```json
{
  "allow_writes": true,
  "approved_at": "2026-08-21T17:36:24+00:00",
  "approved_by_user": true,
  "live": true,
  "max_entities_per_cell": 500,
  "plan_review_sha": null,
  "state": "approved_for_seed"
}
```

The Git commit containing this packet carries one
`Dataset-Plan-SHA256:` trailer for each reviewed hash. After that commit exists,
the two local approved plan files replace null `plan_review_sha` with the exact
packet commit SHA. Their final full-content hashes are reported separately for
the explicit seed confirmation flags.

## Mutation and recovery boundary

For each planned correlation, seed writes a journal dispatch record before the
create call, reads the exact created entity back, and verifies the unique
marker. Resume reconciles ambiguous creates by exact marker and never blindly
repeats a possibly delivered create.

Cleanup only point-reads the exact manifest-owned ID, verifies the marker,
deletes that ID, and verifies absence. A reused or mismatched ID becomes an
orphan and is not deleted. Cleanup is resumable from dispatched/cancelled
states. The two plans use separate runs, lineages, namespaces, manifests, and
artifact directories.

## Stop conditions

Do not seed if any of these differ at execution time:

- candidate SHA or clean tracked state;
- portal host, HMAC fingerprint, credential role, or scope hash;
- reviewed plan hash, packet commit SHA, run, lineage, namespace, profile, or
  count;
- explicit `--live`, `--allow-writes`, review-SHA confirmation, or final
  plan-content-hash confirmation.

After seed, run verify and then cleanup even if later evidence is inconclusive.
Any orphan or unverified absence blocks PASS and requires operator attention.

## Human response requested

Respond with either:

```text
ACCEPT both live dataset plans bound by the packet commit and the exact final
plan content hashes reported with this checkpoint.
```

or list findings and reject. No mutation runs before that explicit response.
