# W5/W6 v2 human findings resolution

Rejected code subject:
`2d4ea6cc6141fee2343c7abd4151d81b9c167d99`.

Replacement code subject:
`8ba3c40ca85f5e7c7123a2aec332c2dfe11f27d5`.

Superseded packet commit:
`a7752ed8360561ecb8b13749dd9e74bfbd91cdd5`.

## Code findings

| Finding against `2d4ea6c...` | Resolution in `8ba3c40...` | Regression evidence |
|---|---|---|
| `fan_out` promoted `Request.replay_safety=None` to `SAFE`, allowing an arbitrary write to replay after HTTP 503. | Canonical traversal preserves the exact caller safety value. Direct and batch dispatch treat unset as `UNKNOWN`; only an explicit wrapper/profile may mark a request safe. | An unset `tasks.task.add` receives one 503 request, zero retries, typed `HTTPGatewayError`, and a FAILED report in both direct and batch forms. |
| `BatchStream` counted outcomes rather than decoded rows inside list results. | `BatchSuccess` records a top-level decoded-row weight while freezing the payload: arrays weigh their length, scalar/object results weigh one, and an empty array weighs zero. Stream buffering charges and releases that exact weight. | Tolerant and fail-fast 10,000-row results reject at a ceiling of one and report exact live/final 10,000 at an admitted ceiling; early close restores the live ledger to zero. |
| `ItemCursorPlan.direction` incorrectly forced row identity monotonicity even when `cursor_item_path` was separate. | Cursor direction is validated only over cursor values. `PlanContract.order_semantics` remains an independent optional row-identity order contract; duplicate policy independently governs seams. | Nonmonotonic row IDs with ascending cursors complete; a seam duplicate is retained, reported, and counted once. Opposite row-ASC/cursor-DESC contracts also complete. |

Independent clean-room review replayed all three findings on the rejected SHA,
then reported P1/P2/P3 = 0/0/0 on the replacement.

## Packet findings

| v2 packet finding | v3 disposition |
|---|---|
| The eighth W0-protected file, `b24api/error.py`, was omitted without provenance. | v3 points to `w1/acceptance.md`, which deliberately superseded the W0 candidate at accepted W1 SHA `7fc7b22...`; accepted W3/W4 later added the transport evidence fields. It explains why seven files remain protected. |
| Refused submodes inside the public plan union were not enumerated. | v3 explicitly lists `PartitionedKeysetPlan`, parallel fixed-stride counted offset, boundary-ID keyset termination, and monotonic identity tracking outside keyset as construction-valid but pre-I/O refused. |
| Normative sections 7.7 and 27.6 did not resolve to a versioned artifact. | v3 names `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md` and locks it to SHA-256 `2fb7acb7...`, exactly the Specification SHA-256 recorded by W0 inventory. The spec remains a user-owned untracked input governed by that hash. |
| “Remediation lineage” omitted packet v1 commit `9fab54f...`. | v3 labels the implementation sequence “code commits only” and lists packet commits separately. |
| Overlay hash recheck lacked an executable command. | v3 gives the exact seven-path `shasum -a 256` command and points to the expected W0 table. |

The W0 inventory's historical dirty-diff command and its eight-row protected
table serve different purposes: the command covered the then-tracked paths;
the table also locked the untracked `entity_types.py`. v3 uses the table as the
protected-file authority and does not rewrite accepted W0 evidence.
