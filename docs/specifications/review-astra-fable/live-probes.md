# L1–L5: live portal protocols

Read-only probes for the astra-fable review (#14, #15), run with
`uv run python tools/review_live_probes.py --env-file ../.env --throttle-probe 150` on
2026-09-24 at 17:44 UTC, revision `3b0c9c9` (clean checkout), httpx 0.28.1.

- **Portal.** The owner's Bitrix24 cloud development portal, over HTTP/2. The REST API does not
  expose the portal build, so the protocol records the date instead.
- **What the tool prints.** Never the portal host, the webhook or row values. Headers are reduced to
  their names, plus the numeric shape of `X-Bitrix-RateLimit-Reset` and `Retry-After`. Bodies are
  reduced to digests and counts, failures to exception classes.
- **Methods.** Only the allowlisted read methods are sent, and every batch command is checked too:
  `scope`, `server.time`, `crm.deal.list`, `crm.item.list`, `crm.requisitelink.list`,
  `crm.timeline.comment.list`, `tasks.task.list`, `user.get` and `batch` over them. Every library
  request is `ReplaySafety.SAFE`.
- **Outcomes.** Each probe lists its parameters, expectation, observed fact and outcome. A guarantee
  that could not be confirmed is reported as unconfirmed.

## L1 (A19): `X-Bitrix-RateLimit-Reset` and `Retry-After`

| Field | Value |
|---|---|
| Methods and parameters | `scope` and `server.time` over plain HTTPX (five `Accept-Encoding` variants each, 10 calls); `scope` through the library; then a burst of at most 150 `server.time` calls from 20 concurrent workers that stops at the first non-200 answer |
| Expectation | See which form the reset header takes (delta seconds, epoch seconds or HTTP-date) and whether `Retry-After` arrives with it |
| Fact | None of the 161 responses carried `X-Bitrix-RateLimit-Reset`, `Retry-After` or any `*ratelimit*` header, and neither did any library response in L3 and L4. All 150 burst calls answered 200, so the portal's limit was not reached. The Bitrix headers present were `x-bitrix-lb`, `x-bitrix-rest-time`, `x-bitrix-rest-system-time`, `x-bitrix-rest-user-time`, `x-bitrix-ri`, `x-bitrix24-date` and `x-bitrix24-user`. The body's `time.operating_reset_at` is epoch seconds, about 600 s ahead: the portal's own reset timestamps use the epoch form. |
| Outcome | **Header form not observed.** A19 stays implemented and tested offline for all three forms, and an invalid header never freezes the host (`tests/throttle_headers_test.py::test_rate_limit_reset_accepts_delta_epoch_and_http_date`, `::test_epoch_rate_limit_reset_does_not_freeze_the_host_for_the_cap`). The spec's "format confirmed by L1" is **not met**, so under §6.1 item 3 this guarantee is not declared. Confirming it needs a real throttled response, and the load was not raised further without the owner's consent. |

## L2 (A2): `Content-Encoding` and the explicit `Accept-Encoding`

| Field | Value |
|---|---|
| Methods and parameters | `scope` and `server.time` with the HTTPX default `Accept-Encoding` (here `gzip, deflate`, because brotli and zstandard are not installed), explicit `gzip, deflate`, `identity`, `br` and `zstd`; then `scope` through the library's `HttpxTransport`, which sends `BOUNDED_ACCEPT_ENCODING = "gzip, deflate"` |
| Expectation | The explicit header does not change responses, and every encoding the portal uses is one the bounded decoder accepts |
| Fact | `gzip, deflate` gets `Content-Encoding: gzip`. `identity`, `br` and `zstd` each get an uncompressed body without `Content-Encoding`: the portal offers neither brotli nor zstd. The decoded `scope` result has the same digest (`f929ce4ecbedeb66`) in all five variants. The library request gets gzip, and the bounded decoder decodes it. |
| Outcome | **Confirmed.** Gzip is the only encoding observed, and the explicit header changes nothing. |

## L3 (A8): the counted validator on real responses

| Field | Value |
|---|---|
| Methods and parameters | Scenario 10 (`crm.timeline.comment.list` by deal) and scenario 14 (`crm.requisitelink.list` by entity type 2, 3 and 4), each with the recipe's fixed step and with the default continuation (`SERVER_NEXT_OR_OBSERVED_COUNT`). They run as counted references (`CountedTraversal`, `BatchDispatch`) and as `iter_list_counted`. Plus `crm.deal.list` and `tasks.task.list`, which have no recipe, as counted lists and as a qualified `iter_list`. The tool records every `judge_counted_page` verdict with its path and continuation. |
| Expectation | No counted rule rejects a real response. Rule R2 applies only to a page that closes its exact total under a continuation other than `FIXED_STEP`; it must accept every such real page, because a real portal omits `next` on its last page. |
| Fact | Every traversal completed with `identity_and_count_matched` (or `completed` for the reference streams) and no violation (table below). Pages where R2 applies: 11 terminal pages under the default continuation (3 batched heads, 5 timeline bindings, 3 requisite-link bindings), plus 10 non-terminal ones. R2 accepted all 21 of them. Under `FIXED_STEP`, 20 more pages were judged, where R2 is off by design. |
| Outcome | **Confirmed.** The validator does not reject real responses. A real contradiction (`total` reached while `next > 0`) did not occur, so it is pinned only by the offline table R (`tests/counted_rules_test.py`). |

| Traversal | HTTP | Rows | Report | Verdicts (path, continuation) |
|---|---|---|---|---|
| `iter_list_counted` `crm.deal.list` (no recipe) | 2 (head + batch of 36) | 1847 | completed, `identity_and_count_matched` | batched, default: 1 non-terminal |
| `iter_list_counted` `tasks.task.list` (no recipe) | 2 (head + batch of 18) | 937 | completed, `identity_and_count_matched` | batched, default: 1 non-terminal |
| `iter_list_counted` scenario 14 | 2 (head + batch of 8) | 441 | completed, `identity_and_count_matched` | batched, fixed step: 1 non-terminal |
| `iter_list_counted` scenario 10, three deals, fixed step | 1 each | 6, 0, 0 | completed, `identity_and_count_matched` | batched, fixed step: 3 terminal |
| `iter_list_counted` scenario 10, three deals, default | 1 each | 6, 0, 0 | completed, `identity_and_count_matched` | batched, default: 3 terminal |
| counted references, scenario 10, five deals, fixed step | 1 (batch of 5) | 6 items, 5 complete | completed | sequential, fixed step: 5 terminal |
| counted references, scenario 10, five deals, default | 1 (batch of 5) | 6 items, 5 complete | completed | sequential, default: 5 terminal |
| counted references, scenario 14, three types, fixed step | 9 batches | 441 items, 3 complete | completed | sequential, fixed step: 3 terminal, 8 non-terminal |
| counted references, scenario 14, three types, default | 9 batches | 441 items, 3 complete | completed | sequential, default: 3 terminal, 8 non-terminal |
| `iter_list` `EXACT_QUALIFIED` `crm.deal.list` (no recipe) | 37 | 1847 | completed, `identity_and_count_matched` | none: plain `iter_list` uses the offset strategy |
| `iter_list` scenario 14, fixed step | 9 | 441 | completed, `identity_and_count_matched` | none, as above |

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

| Field | Value |
|---|---|
| Methods and parameters | `iter_list_keyset` with the default `KeysetSpec()` and the default auto execution on `crm.deal.list` (`select: ["ID"]`), `crm.item.list` (`entityTypeId: 2`, selector `items`, identity `id`; scenario 8's method), `tasks.task.list` (selector `tasks`) and, as a negative control, `user.get` |
| Expectation | As the README documents: auto plans first, then selects boundary-only, sequential, range or partitioned execution from the observed geometry, completes with an empty confirmation, and reports `keyset_execution` and `keyset_selection`. An endpoint that cannot be ordered this way fails closed. |
| Fact | See the table below. |
| Outcome | **Confirmed.** For comparison, `iter_list_counted` read the same 1847 deals in 2 HTTP requests (L3). |

| Method | HTTP | Rows | Selection | Report |
|---|---|---|---|---|
| `crm.deal.list` | 8: bounds batch of 2, lane batches of 20, 21, 20, 11, 9 and 4, then one empty finishing call | 1847 | auto → partitioned, `wide_span_partitioning` | completed, `identity_exact`; closure witnesses: 20 anchor fences, 1 empty |
| `crm.item.list`, scenario 8 | 8, same shape | 1862 | auto → partitioned, `wide_span_partitioning` | completed, `identity_exact` |
| `tasks.task.list` | 20: bounds batch of 2 whose ascending page is admitted as the first page, then 18 calls ending with an empty confirmation | 937 | auto → sequential, `insufficient_predicted_gain` | completed, `identity_exact` |
| `user.get` (negative control) | 1 (bounds batch of 2) | 0 | auto, `insufficient_predicted_gain` | incomplete, `pagination_invariant` + `range_contradiction` |

`user.get` sorts by `sort`/`order` strings rather than an order map, so the default `KeysetSpec`
cannot order it. The traversal failed closed before emitting any row.

## L5 (A9): no `start ≠ 0` with total-only closure in examples and documentation

| Field | Value |
|---|---|
| Methods and parameters | Every `TotalTermination.EXACT_QUALIFIED` use in `examples/`, `README.md` and `docs/*.md`: `disk_mirror`, `requisite_links`, `timeline_comments`, `task_role_union` and the `docs/recipes.md` fixed-step block |
| Expectation | Every such traversal starts at the plan's initial control |
| Fact | Every entry request starts with no `start` or with `start: 0`. The other `start` values in these files are fixture expectations for later pages. The A9 preflight refuses the forbidden combination before I/O (`tests/offset_preflight_test.py::test_exact_qualified_total_refuses_a_nonzero_start_before_io`), so a hidden case would fail the offline recipe tests, which pass. |
| Outcome | **Confirmed.** This is a repository check: the combination is a property of the examples, not of the portal. |
