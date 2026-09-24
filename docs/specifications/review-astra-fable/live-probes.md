# L1–L5: live portal protocols

Read-only probes for the astra-fable review (#14, #15), run with
`uv run python tools/review_live_probes.py --env-file ../.env --throttle-probe 150` on
2026-09-24 at 17:18 UTC, revision `8cfbbf9` (clean checkout), httpx 0.28.1.

- **Portal.** The owner's Bitrix24 cloud development portal, over HTTP/2. The tool never prints the
  portal host, the webhook or row values. It reduces headers to names and numeric shapes, bodies
  to digests and counts, and failures to exception classes. The REST API does not expose the portal
  build, so the protocol records the date instead.
- **Methods.** Only the allowlisted read methods in the tool are sent: `scope`, `server.time`,
  `crm.deal.list`, `crm.item.list`, `crm.requisitelink.list`, `crm.timeline.comment.list`,
  `tasks.task.list`, `user.get` and `batch` over them. Every library request is `ReplaySafety.SAFE`.
- **Scope.** Each probe below lists its expectation, the observed fact and the outcome. An
  unconfirmed guarantee is reported as unconfirmed, not as passed.

## L1 (A19): `X-Bitrix-RateLimit-Reset` and `Retry-After`

| Field | Value |
|---|---|
| Parameters | 10 raw HTTPX calls (`scope`, `server.time`, five `Accept-Encoding` variants each), one library call, then a burst of at most 150 `server.time` calls from 20 concurrent workers that stops at the first non-200 answer |
| Expectation | See which form the reset header takes (delta seconds, epoch seconds or HTTP-date) and whether `Retry-After` arrives with it |
| Fact | None of the 161 responses carried `X-Bitrix-RateLimit-Reset`, `Retry-After` or any `*ratelimit*` header. All 150 burst calls answered 200, so the portal's limit was not reached. The only Bitrix headers were `x-bitrix-lb`, `x-bitrix-rest-time`, `x-bitrix-rest-system-time`, `x-bitrix-rest-user-time`, `x-bitrix-ri`, `x-bitrix24-date` and `x-bitrix24-user`. The body's `time.operating_reset_at` is epoch seconds, about 600 s ahead: the portal's own reset timestamps use the epoch form. |
| Outcome | **Header form not observed.** A19 stays implemented and tested offline for all three forms, and an invalid header never freezes the host (`tests/throttle_headers_test.py::test_rate_limit_reset_accepts_delta_epoch_and_http_date`, `::test_epoch_rate_limit_reset_does_not_freeze_the_host_for_the_cap`). The spec's "format confirmed by L1" is **not met**, so under §6.1 item 3 this guarantee is not declared. Confirming it needs a real throttled response, and the load was not raised further without the owner's consent. |

## L2 (A2): `Content-Encoding` and the explicit `Accept-Encoding`

| Field | Value |
|---|---|
| Parameters | `scope` and `server.time` with the HTTPX default `Accept-Encoding` (here `gzip, deflate`, because brotli and zstandard are not installed), explicit `gzip, deflate`, `identity`, `br` and `zstd`; then `scope` through the library's `HttpxTransport`, which sends `BOUNDED_ACCEPT_ENCODING = "gzip, deflate"` |
| Expectation | The explicit header does not change responses, and every encoding the portal uses is one the bounded decoder accepts |
| Fact | `gzip, deflate` gets `Content-Encoding: gzip`. `identity`, `br` and `zstd` each get an uncompressed body without `Content-Encoding`: the portal offers neither brotli nor zstd. The decoded `scope` result has the same digest (`f929ce4ecbedeb66`) in all five variants. The library request gets gzip, and the bounded decoder decodes it. |
| Outcome | **Confirmed.** Gzip is the only encoding observed, and the explicit header changes nothing. |

## L3 (A8): the counted validator on real responses

Every traversal uses the parameters of the matching recipe. The tool records each
`judge_counted_page` verdict (rule R2) by path.

| Traversal | HTTP | Rows | Report | R2 verdicts |
|---|---|---|---|---|
| `iter_list_counted` `crm.deal.list` | 2 (head + batch of 36) | 1847 | completed, `identity_and_count_matched` | batched head: 1 non-terminal, no contradiction |
| `iter_list_counted` `tasks.task.list` (`tasks`) | 2 (head + batch of 18) | 937 | completed, `identity_and_count_matched` | batched head: 1 non-terminal, no contradiction |
| `iter_list_counted` `crm.requisitelink.list`, scenario 14 | 2 (head + batch of 8) | 441 | completed, `identity_and_count_matched` | batched head: 1 non-terminal, no contradiction |
| `iter_list_counted` `crm.timeline.comment.list`, scenario 10, three deals | 1 each | 6, 0, 0 | completed, `identity_and_count_matched` | batched head: 3 terminal, no contradiction |
| `iter_list` with `EXACT_QUALIFIED`, `crm.deal.list` | 37 | 1847 | completed, `identity_and_count_matched` | not judged: plain `iter_list` uses the offset strategy |
| `iter_list` with scenario 14's fixed step, `crm.requisitelink.list` | 9 | 441 | completed, `identity_and_count_matched` | not judged, as above |
| counted references, `crm.timeline.comment.list` by five deals | 1 (batch of 5) | 6 items, 5 complete | completed | sequential: 5 terminal, no contradiction |
| counted references, `crm.requisitelink.list` by entity types 2, 3 and 4 | 9 batches | 441 items, 3 complete | completed | sequential: 8 non-terminal and 3 terminal, no contradiction |

- **Outcome: confirmed.** No counted rule rejected a real response. R2 judged 22 real pages on both
  paths and never found a contradiction. The driver's H1, H2 and T2 raised no violation.
- **Recipe finding.** Scenario 14 used uppercase keys (`ENTITY_TYPE_ID`, `REQUISITE_LINKS`).
  The live `crm.requisitelink.list` silently ignores them: the filter had no effect (851 rows
  instead of 441), and it returns `{"requisiteLinks": [...]}` with camelCase integer fields. The
  library failed closed ("response result does not satisfy the declared selector"). The recipe
  moved to the live shape in `8cfbbf9`, with golden deltas declared in `tests/golden/DELTAS.md`
  (ID `L3`).
- **Transient failure in an earlier exploratory run.** Before the probes marked reads `SAFE`, the
  18th page of a request with `UNKNOWN` replay safety failed after dispatch. The library raised
  `AmbiguousExecutionError` and did not replay, as documented. The same traversal completed on
  every later run.

## L4 (C3): auto keyset on the recipes' methods

| Method | HTTP | Rows | Selection | Report |
|---|---|---|---|---|
| `crm.deal.list` | 8: bounds batch of 2, lane batches of 20, 21, 20, 11, 9 and 4, then one empty finishing call | 1847 | auto → partitioned, `wide_span_partitioning` | completed, `identity_exact`, 20 anchor-fence and 1 empty closure witnesses |
| `crm.item.list` (`entityTypeId: 2`), scenario 8 | 8, same shape | 1862 | auto → partitioned, `wide_span_partitioning` | completed, `identity_exact` |
| `tasks.task.list` | 20: bounds batch of 2 whose ascending page is admitted as the first page, then 18 calls ending with an empty confirmation | 937 | auto → sequential, `insufficient_predicted_gain` | completed, `identity_exact` |
| `user.get` (negative control) | 1 (bounds batch of 2) | 0 | auto, `insufficient_predicted_gain` | incomplete, `pagination_invariant` + `range_contradiction` |

- **Outcome: confirmed.** Auto plans first and then selects partitioned or sequential execution
  from the observed geometry. It pays the planning barrier before the first row. Every sequential
  or partitioned run ends with an empty confirmation, and every report carries `keyset_execution`
  and `keyset_selection`. This is what the README documents. For comparison, `iter_list_counted`
  read the same 1847 deals in 2 HTTP requests (L3).
- **Negative control.** `user.get` sorts by `sort`/`order` strings rather than an order map, so
  the default `KeysetSpec` cannot order it. The traversal failed closed before emitting any row.

## L5 (A9): no `start ≠ 0` with total-only closure in examples and documentation

| Field | Value |
|---|---|
| Parameters | Every `TotalTermination.EXACT_QUALIFIED` use in `examples/`, `README.md` and `docs/*.md`: `disk_mirror`, `requisite_links`, `timeline_comments`, `task_role_union` and the `docs/recipes.md` fixed-step block |
| Expectation | Every such traversal starts at the plan's initial control |
| Fact | Every entry request starts with no `start` or with `start: 0`. The other `start` values in these files are fixture expectations for later pages. The A9 preflight refuses the forbidden combination before I/O (`tests/offset_preflight_test.py::test_exact_qualified_total_refuses_a_nonzero_start_before_io`), so a hidden case would fail the offline recipe tests, which pass. |
| Outcome | **Confirmed.** |
