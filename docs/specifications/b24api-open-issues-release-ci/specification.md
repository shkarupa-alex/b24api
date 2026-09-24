# b24api: open traversal issues and release workflow repair

Status: design. The #10 and #11 decisions were approved by the owner on 2026-09-23. The release-workflow additions below capture the subsequently reported CI failure and Node.js warning. This document specifies work; it does not assert that code, a release tag, or a PyPI upload has been changed.

The local sealed council bundle at `spec/2026-09-23-open-issues-11-10/` records the two-model discussion and its remaining disagreement. That bundle is immutable and ignored by Git; this tracked document is the self-contained implementation handoff. The run reached its three-round limit without formal convergence, so the final safeguards below include the review findings that blocked both candidate proposals.

## Evidence and boundaries

- [Issue #11](https://github.com/shkarupa-alex/b24api/issues/11): a live `user.get` empty filtered result has `result: []` and no `total`. Current `iter_list_counted` rejects it before committing the first page.
- [Issue #10](https://github.com/shkarupa-alex/b24api/issues/10): the legacy `task.elapseditem.getlist` binds arguments positionally. PR #12 later added `PositionalArguments` and an ordinary `iter_list` example, so the remaining task is documentation, regression coverage, and qualification of any optional keyset use.
- [Failed release-build job](https://github.com/shkarupa-alex/b24api/actions/runs/35912435963/job/107355379282): the `fix-2.2.1` tag at `5e46cda` fails during `python -m build`. `setuptools-git-versioning` produces `fix.2.2.1`; `packaging.version.Version` rejects it. Artifact upload and PyPI publication were skipped. The same job warns that `actions/checkout@v4` and `actions/setup-python@v5` target Node 20. The artifact actions were skipped before the runner could warn about them.
- Current workflow: [`.github/workflows/publish-to-pypi.yml`](../../../.github/workflows/publish-to-pypi.yml). The package derives its version from Git tags through `setuptools-git-versioning` in [`pyproject.toml`](../../../pyproject.toml). The workflow runs on every pushed tag and uses checkout v4, setup-python v5, upload-artifact v4, and download-artifact v4.

## 1. Empty counted result without `total` (#11)

### Contract

`iter_list_counted` accepts one direct head response as an empty-source witness if all of these are true:

1. The request is for the first page at offset zero; the source collection and adapted collection are both empty.
2. `Response.total` is absent/null or `-1`, and `Response.next` is absent/null. `-1` is the driver's existing unknown-total sentinel, never a count of zero.
3. The effective continuation is not `FIXED_STEP`; the request has no sparse raw range contract and no earlier exact total.
4. The effective `ConfirmationPolicy` does not require `QUALIFIED_TOTAL`, and all normal shape, adaptation, page-cap, snapshot, and completion checks pass.

The stream yields zero rows after one direct request and schedules no batch tail. It closes the binding with `BindingClosure.SOURCE_EMPTY`, reports `COMPLETED` and `exhausted=true`, and records a value-free empty-source terminal reason. Its assurance is `MECHANICS_ONLY`, or `IDENTITY_EXACT` only if the existing identity contract and assurance rules justify that stronger result. It never claims `COUNT_MATCHED` or `IDENTITY_AND_COUNT_MATCHED` without an observed nonnegative total. A head with `total=0` retains today's qualified-total closure and count-matched assurance.

The allowance is internal and available only at the `iter_list_counted` head validation call. The preliminary eligibility check is pure. The validation transaction owns any acceptance state and terminal reason, and a rejected page leaves no empty-source evidence. The recorder closure and report witness derive from one typed value. The driver must bypass both exact-total presence checks only for this validated case; it must not synthesize `total=0` or restart a nonempty traversal as sequential.

Nonempty heads without a usable total, empty heads with continuation, positive totals with no rows, later-page missing totals, fixed-step windows, invalid page shapes, and explicit `ConfirmationPolicy.QUALIFIED_TOTAL` remain incomplete. `iter_list` with `EXACT_QUALIFIED` and reference `CountedTraversal` retain their current strict behavior.

### Verification

Use scripted responses for missing/null/`-1`/zero/positive totals, empty and nonempty heads, continuation present/absent, identity present/absent, explicit qualified-total confirmation, fixed-step, snapshot requirement, and rejected-page rollback. Assert rows, request count, absence of tail dispatch, page trace, typed closure, final assurance, and error/report state. Recheck `user.get` on the test portal before claiming the live defect fixed.

## 2. Positional elapsed-item requests (#10)

### Contract and disposition

Keep `PositionalArguments`/`PositionalLayout` as the explicit positional ABI and the five-slot, task-scoped `iter_list` + `NAV_PARAMS.iNumPage` example as the supported route. Do not reorder named mappings based on a method name or parse a server error into a method-specific hint. The client has no endpoint catalog, and a four-slot unscoped request can return HTTP 200 while selecting a different scope from the five-slot form.

Typed positional requests currently cannot use physical batching, so `iter_list_counted` rejects them. Auto and fast keyset execution reject positional requests before I/O. A separately qualified sequential keyset request may use present order/filter object slots, declared writable control leaves `(1, "ID")` and `(2, ">ID")`, `KeysetSpec(order_path=(1,), filter_path=(2,), start_suppression_path=None)`, and explicit `SequentialKeysetExecution`. The old live 131-row run used named mapping parameters; it does not qualify this typed five-slot profile.

Add focused documentation and an offline sequential-positional-keyset regression if that optional path is documented as usable. Test exact wire slot order, strict ID progression, and rejection before I/O for missing writable parents, undeclared paths, default auto execution, and batch dispatch. Improve the generic positional-control `CapabilityError` with a value-free reason while retaining its class and cause. Keep `iter_list` primary; do not claim portal-qualified positional keyset until a live task-scope and independent-ID oracle verifies it. Close #10 as superseded by PR #12 only after these docs and tests land.

## 3. Release build failure on `fix-2.2.1`

### Cause and release contract

The tag name is the cause of the build failure, not an action runtime failure. The workflow must use canonical stable release tags `MAJOR.MINOR.PATCH` (for example `2.2.1`), matching `^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`, with no `fix-` prefix. Validate `GITHUB_REF_NAME` against this rule before invoking the build backend, using an environment variable rather than shell-interpolating the GitHub expression. Report an actionable validation error that names the accepted form. Keep the `setuptools-git-versioning` source of truth; do not hard-code a second package version or rewrite arbitrary tags into versions.

Build both wheel and sdist, then check that their version metadata and filenames equal the validated tag before upload. A mismatched version, missing artifact, or invalid metadata fails `release-build`; `pypi-publish` must remain blocked by `needs: release-build`. Preserve the current trusted-publishing environment and `id-token: write` boundary.

The already failed run cannot be repaired retroactively. If `2.2.1` is still intended for publication, make a maintenance commit from the `fix-2.2.1` target (`5e46cda`) that contains the workflow repair, create a new annotated `2.2.1` tag on **that** commit, and let its tag-push run build from the corrected workflow. Do not retarget the existing `fix-2.2.1` tag. Before publishing, confirm the intended source and that the version is not already present on PyPI; at spec time, PyPI lists `2.3.0` and no `2.2.1` files. Publishing an older maintenance version after `2.3.0` is a separate release decision.

### Verification

- On a disposable checkout of the intended maintenance commit, a `2.2.1` tag builds an sdist and wheel whose metadata is exactly `2.2.1`.
- `fix-2.2.1` and malformed or prerelease tag names fail the pre-build validation with a clear message and cannot reach artifact upload or PyPI publication.
- A successful tag run uploads exactly the checked distributions; the downstream job retrieves them by the same artifact name and publishes only after the build job succeeds.
- A workflow run against the new tag is required to claim the CI failure fixed. No automatic tag push or PyPI publication is part of this specification change.

## 4. Node.js 24 action runtime upgrade

The [GitHub deprecation notice](https://github.blog/changelog/2025-09-19-deprecation-of-node-20-on-github-actions-runners/) directs workflow users to versions that run on Node 24. Upgrade the release workflow's first-party JavaScript actions together:

| Step | Current | Target | Reason |
|---|---|---|---|
| Checkout | `actions/checkout@v4` | `actions/checkout@v7` | v7 declares `runs.using: node24`. |
| Python setup | `actions/setup-python@v5` | `actions/setup-python@v7` | v7 declares `runs.using: node24`. |
| Artifact upload | `actions/upload-artifact@v4` | `actions/upload-artifact@v7` | v4 declares Node 20; v7 declares Node 24. |
| Artifact download | `actions/download-artifact@v4` | `actions/download-artifact@v7` | v4 declares Node 20; v7 declares Node 24. |

Both artifact steps retain `name: release-dists` and `path: dist/`. The publishing action is a composite/Docker action, not one of the warned JavaScript actions. The failed run's hosted runner was `2.337.0`, above the Node 24 actions' documented minimum `2.327.1`; no self-hosted runner migration is needed for this workflow. Keep the explicit Python `3.12` input. Verify a successful tag run reaches artifact upload/download and has no Node 20 deprecation warning for these actions. Merely forcing Node 24 on old action majors does not satisfy the upgrade.

## Implementation units and order

1. Implement and test #11's empty-head evidence path without changing other traversal surfaces.
2. Document and test #10's positional request behavior; qualify typed positional keyset live only if that capability is required.
3. Update the release workflow's tag preflight and artifact-version gate, and migrate the four first-party actions together.
4. Verify the workflow on a valid release tag from a commit containing the workflow changes. Decide separately whether to publish `2.2.1` from the maintenance line and when to close the two GitHub issues.

## Decision ledger

| Decision | Status | Reason |
|---|---|---|
| Empty missing-total counted head closes as `SOURCE_EMPTY` under default confirmation | Approved | Fixes #11 without inventing a count. |
| Explicit `QUALIFIED_TOTAL` and other strict traversals stay strict | Approved | Preserves caller-declared proof requirements and scope. |
| Typed positional `iter_list` remains the elapsed-item default | Approved | Existing generic ABI and example already handle legacy scope. |
| No endpoint-specific dict reordering | Approved | Would risk changing wire meaning and method scope. |
| Canonical numeric stable release tags and early validation | Proposed addition | Addresses the exact `InvalidVersion: 'fix.2.2.1'` failure. |
| Verify built artifact metadata against the tag | Proposed addition | Prevents a syntactically valid tag from publishing the wrong version. |
| Upgrade checkout, setup-python, upload-artifact, and download-artifact to Node 24 action majors | Proposed addition | Removes the observed warning and the warnings that skipped steps would produce. |
| Rewrite `fix-` tags or hard-code the package version | Rejected | Creates a second version authority and conceals a release naming error. |

## Open release decision

Whether to publish a `2.2.1` maintenance release now, after `2.3.0`, depends on release intent. The CI repair and action upgrades do not require publishing it.
