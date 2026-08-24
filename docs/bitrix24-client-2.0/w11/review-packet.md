# Bitrix24 client 2.0 — final W11 review packet

This is the final same-SHA human checkpoint for the implementation of
`spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`. The immutable
packet commit is reported outside this file after the commit exists; the code
and completion-audit parent is
`5b6d743eb90ec73fc1de0bddd29f31a9c0067542`.

## Frozen review boundary

- original baseline: `08277c4d921b83b9252177b3e72a21a4c0c86109`;
- completion candidate: `5b6d743eb90ec73fc1de0bddd29f31a9c0067542`;
- normative specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`;
- packet commit: supplied with the human-review request;
- review range: baseline through the packet commit;
- required verdict: `ACCEPT` only when `P1/P2/P3 = 0/0/0`.

The packet commit is documentation-only over the completion candidate. Review
the cumulative range, not only the final documentation commit.

## What is being accepted

The candidate supplies the correctness-first 2.0 execution core, bounded
direct/batch/pagination/reference traversal, compatibility facade, immutable
plans/reports/policies, strict endpoint-profile machinery, and the
repository-only evidence harness. The exact public surface and deliberate
corrections are frozen in `compatibility-ledger.md`; the R1-R15 and W0-W11
mapping is in `completion-audit.md`.

The packaged profile set is empty. No W10 optimization, endpoint-profile
default, real-build assertion, live performance result, or large-live-dataset
claim is included.

## Material changes since the last accepted functional checkpoint

The cumulative range after `8e03127621fa1892e629721d4b166b30846e755b`
adds the nullable portal-build boundary, post-decision approval hashing,
human-reviewed five-entity Tasks and CRM plans, the sanitized live execution
record, exact CRM post-delete absence classification, and W11 release closure.

The observed CRM absence correction is intentionally narrow. Only the complete
tuple below qualifies absence:

```text
method = crm.deal.get
HTTP status = 400
error = ""
error_description = "Not found"
```

CRM status 200/500 with the same body, Tasks status 400 with the same body,
other descriptions, impossible HTTP statuses, and malformed responses remain
fail-closed. Typed `ERROR_NOT_FOUND` behavior is unchanged.

## Live checkpoint — sanitized disposition

The user separately approved two exact five-entity plans bound to review commit
`98b9b3adefff1b9c82127f70f40c4809f9902932`. That authorization was used only
for the approved seed, verify, and exact manifest-owned cleanup lifecycle.

- Tasks seed stopped `INCONCLUSIVE` after an ambiguous create. Harness cleanup
  proved absence and zero orphans.
- CRM seed and independent verify passed for five exact-marker deals.
- CRM harness cleanup deleted one deal, then stopped on the previously unknown
  absence envelope. There is **no CRM cleanup PASS artifact**.
- Under the existing exact-manifest cleanup authorization, operator recovery
  verified every marker, found one ID already absent, deleted the remaining
  four, and confirmed all five absent with zero marker mismatches.
- After the narrow adapter correction, read-only replay classified those five
  IDs absent.

This record demonstrates the limited lifecycle above. It does not retroactively
create a CRM cleanup PASS, admit a profile, or support a latency/default claim.
The detailed sanitized accounting is in
`../w9/live-dataset-execution-8d76a07.md`.

No further live writes, W10 work, release action, credential publication, or
unreviewed cleanup is requested by this checkpoint.

## Exact verification results

The completion candidate was checked in a new detached worktree.

```text
Python 3.14 full pytest: 498 passed in 179.53s
Python 3.12.10 full pytest: 498 passed in 175.27s
Ruff check: PASS
Ruff format --check: 41 files already formatted
mypy --strict b24api tools/b24api_evidence: PASS, 25 source files
git diff --check baseline..candidate: PASS
detached tracked status: clean
W0 validator: 12 positive accepted, 56 negative rejected
specification SHA-256: exact match
```

The deterministic offline model benchmark completed with outcome `PASS`:

```text
terminal state: completed
oracle files: 100
bundle files: 107
pending transaction markers: 0
independent bundle scan: PASS
```

The built wheel contains 25 entries, includes
`b24api/data/endpoint-profile.schema.json` and
`b24api/data/endpoint-profiles.json`, and contains zero `tools/`,
`b24api_evidence`, or evidence-bundle entries.

The exact commands for the same-SHA packet rerun are:

```bash
uv sync --frozen --group dev --python 3.14
.venv/bin/pytest -q -p no:cacheprovider
.venv/bin/ruff check . --no-fix
.venv/bin/ruff format --check .
.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check 08277c4d921b83b9252177b3e72a21a4c0c86109..HEAD
git status --short --branch
shasum -a 256 spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
```

Use a separately prepared Python 3.12.10 environment for the minimum-version
full-suite replay; do not interpret `mypy --strict .` as the declared mypy
gate.

## Human review checklist

1. Confirm the packet commit and cumulative baseline-to-packet diff.
2. Confirm the normative specification hash and clean detached status.
3. Reproduce the exact static and full-suite gates above.
4. Verify the eleven root exports, facade signatures, compatibility ledger,
   empty packaged profile set, and absence of harness tooling from the wheel.
5. Inspect the completion audit's R1-R15 and W0-W11 mappings for unsupported
   claims.
6. Confirm the model bundle outcome, 100 oracle files, independent scan, and
   absence of pending publication markers.
7. Confirm the CRM absence classifier requires the complete observed tuple and
   that neighboring method/status/description cases fail.
8. Confirm the live report does not call the original CRM cleanup a PASS and
   that all live/profile/performance non-claims remain explicit.
9. Report every actionable functional finding by severity. Do not approve if
   any P1, P2, or P3 remains.

## Requested response

If and only if no actionable functional finding remains, respond with:

```text
P1/P2/P3 = 0/0/0
ACCEPT <exact packet commit SHA>
```

Otherwise respond with `REJECT`, the exact packet SHA, and reproducible
functional findings. This checkpoint authorizes acceptance of the candidate;
it does not itself authorize publication or a release action.
