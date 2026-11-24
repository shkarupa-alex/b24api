# Performance and memory profile

Date: 2026-08-25
Environment: Python 3.12.10, macOS arm64, deterministic credential-free portal fixtures.

These measurements characterize local resource use and request shape. They are not a live-network
latency admission.

## Reproduce

```console
.venv/bin/python tools/b24api_evidence/profile_runtime.py --capability-suite
.venv/bin/python tools/b24api_evidence/profile_runtime.py --samples 7 --warmups 2
.venv/bin/python tools/b24api_evidence/profile_runtime.py \
  --case dense-10k --plan counted_batch --samples 7 --warmups 2 \
  --memray-output /tmp/b24api-dense.bin
.venv/bin/memray stats /tmp/b24api-dense.bin --json -o /tmp/b24api-dense-stats.json

# Batched-keyset fixture matrix and automatic threshold analysis
.venv/bin/python tools/b24api_keyset_admission.py fixture /tmp/keyset-fixture.json

# Read-only live range/partitioned/auto sandwiches (uses BITRIX24_API_WEBHOOK_URL)
.venv/bin/python tools/b24api_keyset_admission.py live /tmp/keyset-live.json --samples 5

# Release admission: combine same-SHA live and fixture artifacts with an explicit substitution map
.venv/bin/python tools/b24api_keyset_admission.py combine /tmp/keyset-admission.json \
  --live-artifact /tmp/keyset-live.json --fixture-artifact /tmp/keyset-fixture.json \
  --substitutions /tmp/keyset-substitutions.json
```

The live matrix records task filters, identity-role geometry, portal-dependent density/size
shortfalls, and deterministic-fallback reason codes in the artifact manifest. Same-case coverage
requires `B24API_KEYSET_SAME_CASE_CELL` to contain an explicit JSON contract for a read-only
`*.list` method, parameters, selector/item paths, identity field, and expected auto selection; task
lists are not used for that cell because their numeric filter and order roles differ.
Standalone deterministic fixtures verify correctness but cannot pass the release performance gate.
Fixture performance is admitted only through the combined artifact for a deficient live cell/mode
whose samples cover all three attempt windows and whose exact shortfall reasons are recorded.

## Results

| Case | Rows/events | Requests | High-water | Wall | CPU | Peak Python allocation |
|---|---:|---:|---:|---:|---:|---:|
| direct call | 1 | 1 | — | 2.3 ms | 1.9 ms | 57,723 B |
| logical batch 10k | 10,000 | 1,429 | 7 commands | 2.29 s | 1.72 s | 292,231 B |
| logical batch 100k | 100,000 | 14,286 | 7 commands | 22.86 s | 17.19 s | 284,119 B |
| counted 19 | 19 | 1 | 19 rows | 13.1 ms | 12.1 ms | 2,056,776 B |
| counted 500 | 500 | 2 | 450 rows | 54.8 ms | 38.1 ms | 2,056,776 B |
| counted dense 10k | 10,000 | 5 | 2,500 rows | 769 ms | 507 ms | 3,767,636 B |
| counted uniform-sparse 10k | 10,000 | 5 | 2,500 rows | 744 ms | 499 ms | 3,781,387 B |
| references, input order | 16 | 16 | 1 row / 2 active | 35.4 ms | 13.3 ms | 91,258 B |
| oversized response refusal | — | 1 | 64 KiB + chunk | 1.3 ms | 1.3 ms | 108,339 B |

Traced wall and CPU values include `tracemalloc` overhead. Counted traversal used exactly 1, 2 and
5 physical requests for 19, 500 and 10,000 rows respectively, with exact identity agreement in
dense and sparse fixtures.

The 100,000-command generator retained a seven-command window and used no more peak Python memory
than the 10,000-command run. One hundred repeated traversals stayed below the 1 MiB retention-growth
ceiling, and no stream or owned task remained after close.

### Batched references under load

A fake portal answering in 50 ms, 50 commands a batch, bindings of 0 or 7 rows alternately, under the
default row budget. "Before" is the row pool that kept a whole room for every binding ahead of the
`INPUT` frontier; "after" keeps only the part its pages in flight do not hold and asks the pool whether
a page may still come. The ideal is one request per 50 bindings.

| Load | Ideal | Requests before | Requests after | Wall before | Wall after |
|---|---:|---:|---:|---:|---:|
| 50,000 bindings, `READY` | 1,000 | 1,022 | 1,021 | 81.2 s | 79.8 s |
| 50,000 bindings, `INPUT` | 1,000 | 1,929 | 1,246 | 143.6 s | 94.8 s |
| 20,000 bindings, `concurrency=4`, `READY` | 400 | 698–798 | 408–409 | 31.6 s | 31.9 s |
| 20,000 bindings, `concurrency=4`, `INPUT` | 400 | 801 | 502 | 56.9 s | 37.6 s |

Before, `concurrency=4` under `READY` split the pool between half-empty waves by timing: 796 to 798
requests on an idle machine, 698 on a loaded one. `INPUT` still sends more than `READY`: rows read ahead of the frontier wait for it, so its waves
fill only as far as the pool the frontier leaves.

Memray 1.20.0 measured a 5,121,452-byte peak for seven dense-10k counted samples after two warmups.
Immutable JSON thawing was the largest cumulative client allocation site; no retention leak was
observed.

On 2026-09-10, the committed keyset admission harness ran five accepted read-only range sandwiches
after one warm-up on a 934-row list. Every ordered digest matched both sequential controls, with no
exclusions, omissions, duplicates, or output over-fetch. Range used 5 physical requests versus 20;
the paired median request ratio was 0.25 and the paired median wall-time ratio was 0.318. These are
harness-produced observations for that portal cell, not general latency promises.

## Boundaries

- Auto is the default for direct no-count keyset traversal and may select boundary-only,
  sequential, range, or partitioned execution under the caller-asserted stable integer-keyset
  contract. It pays a planning barrier even when the consumer stops early; pass
  `SequentialKeysetExecution()` explicitly to retain request-by-request traversal.
- Output is strictly ordered, so the rows of a lane behind the first open one wait in
  `max_buffered_rows` until the lanes before it close. Once lanes are deeper than the buffer can hold
  for a full wave, a wave advances only as many lanes as the buffer has room for, and partitioned
  execution approaches sequential traversal. Auto prices every plan by running that same wave rule
  over lanes of estimated rows, so it takes range windows, which open no more windows than a wave
  carries, for large dense selections. Pricing runs in a worker thread, off the event loop, and takes
  about 0.3 µs per page the plans it prices read: Auto prices a 353,000-request plan of 10.8
  million pages in about 2.5 s before its probes and again after them, a third time when it weighs a
  requested advisory total, and the first row waits for all of them. Reference bindings that plan at
  once share a batch only when their pricing ends within `BatchDispatch.coalesce_wait`; a binding
  still pricing then sends its next wave in a later batch. Physical requests
  at page 50, the default 2,500 buffered rows and default batch size, on a deterministic fixture:

  | Selection | Sequential | Auto | Range | Partitioned | Auto, 2.6.2 | Range, 2.6.2 |
  |---|---:|---:|---:|---:|---:|---:|
  | 5,000 dense | 101 | 5 | 5 | 15 | 15 | 5 |
  | 20,000 dense | 401 | 14 | 11 | 227 | 227 | 11 |
  | 50,000 dense | 1,001 | 27 | 24 | 790 | 790 | 24 |
  | 100,000 dense | 2,001 | 48 | 45 | 1,790 | 1,790 | 45 |
  | 20,000 at random over 140,000 identities | 401 | 43 | 40 | 252 | 252 | 122 |
  | 20,000 at random over 6,000,000 identities | 401 | 47 | 44 | 251 | 251 | fails after 333 |

  Explicit partitioning and sequential traversal take what they took in 2.6.2. Auto spends a few
  requests more than explicit range traversal on its anchor probes. Explicit partitioning keeps
  `target_lanes` lanes whatever the buffer holds; prefer range traversal for large dense selections.
  Range windows now follow the measured density, so on sparse data they no longer read a page of
  identity values each until `max_pages` runs out.

  Shapes Auto cannot measure from its probes stay near sequential traversal (20,000 rows, page 50):

  | Selection | Sequential | Auto | Auto, 2.6.2 |
  |---|---:|---:|---:|
  | 60 rows at each end, the rest in a block a million identities away | 401 | 402 | 402 |
  | 60 sparse rows at each end, the rest in a dense block | 401 | 404 | 402 |
  | 95% in a block right below the tail, 5% spread below it | 401 | 383 | 383 |
  | 95% in a block at the head, 5% spread above it | 401 | 402 | 384 |

  In the last shape the dense block lies inside the first lane the probes measure, so every fast plan
  is priced as reading that lane alone, and Auto traverses sequentially; 2.6.2 partitioned it, and
  the lanes above the block read alongside it.
- Auto partitioning probes `target_lanes` anchors, or up to twice the batch capacity when finer lanes
  are priced cheaper and their estimate fits what is left of `max_pages` and `max_requests`. Each
  probe reads a page: its first row is the anchor and the rest measure the density of the lane above
  it. After the probes Auto prices partitioning, range windows and sequential traversal over the
  measured lanes. Rows packed into a cluster that no probe lands in are not measured: such a
  selection is priced from the density the probes did see, and Auto mostly traverses it
  sequentially. Auto refuses a range or partitioned plan when its minimum cost (a page per window,
  probe or lane, never extrapolated rows) exceeds the budget left, and then takes the other fast plan
  if it fits and pays off, or traverses sequentially.
- Auto's choices rest on estimates extrapolated from the boundary and probe pages. Under a
  `max_pages` or `max_requests` set close to the need, it can end with `IncompleteTraversalError`
  on the exhausted budget where a different choice would have fitted. Across 1,512 runs on a
  fixture (2,000, 8,000 and 20,000 rows; pages of 10 and 50; 600, 2,500 and 10,000 buffered rows;
  six data shapes; `max_pages` or `max_requests` from 0.3 to 1.3 times the pages of sequential
  traversal), Auto failed in 33 runs that 2.6.2 completed and completed 64 that 2.6.2 failed;
  without a budget it took a third fewer requests (32,011 against 47,578). The default budgets are
  budgets like any other: a selection whose need comes close to them can fail the same way.
  497,000 rows at page 50 with 200 identities 1,000 apart at each end, the top ones a hundred
  million values up, take 96 anchor probes and then sequential traversal, which runs out of the
  default 10,000 pages after 495,150 rows; 2.6.2 completed them in 9,962 pages. For a predictable
  need under a tight budget, pass `SequentialKeysetExecution()` or `RangeKeysetExecution`
  explicitly.
- A returned `total` is advisory only; one requested with `TotalHintMode.REQUEST_ADVISORY` may raise
  an auto cost estimate. It never closes a lane or strengthens correctness evidence. Normal range/partition reports use caller-asserted bounds
  (`assurance_source=CALLER_ASSERTED_BOUNDS`); explicit verification is a separate operation,
  `verify_keyset_capability()`.
- Reference `BatchDispatch` gives an underfilled wave an absolute, capacity-aware coalescing
  deadline of 20 ms while a page that fits may still join it. A wave whose next pages wait only for
  the rows a wave in flight holds waits for that wave to land instead, without a deadline, so the
  pool is not split between half-empty waves. That trades wall time for requests: a send slot
  stays idle while the rows it could use are still in flight. With a fake portal answering every batch in
  100 ms and no rate limit, 2,000 one-row commands at `batch_size=20, concurrency=4` took 100
  requests and 5.5 s (40 commands in flight), against 165 requests and 4.7 s with
  `coalesce_wait=0`; 200 commands at `batch_size=10, concurrency=2` under 750 rows took 20
  requests and 2.1 s, against 31 and 1.7 s. Under the portal's request rate limit the requests
  saved are time saved. Full waves and waves with no eligible producers leave immediately;
  `coalesce_wait=0` sends every wave at once, never waiting for a deadline or a landing. With
  multiple workers, only aggregate work conservation—not an exact command split per physical
  batch—is guaranteed.
- Real portal latency, server work and network variance require a separately controlled live A/B.
- Exact traversal retains identities for the operation lifetime under the hard
  `ExecutionPolicy.max_identity_keys` budget (100,000 by default); exceeding it fails the operation
  with `BudgetExceededError` instead of warning. For larger sources pass `identity_store=` to
  `iter_list`/`iter_list_counted`: a caller-owned `IdentityStore` (for example a SQLite table with a
  primary key) records each `identity_store_key(...)` so in-process identity memory stays bounded by
  one page. Repeated-page detection still retains one 64-character fingerprint per page, which grows
  with the page count and is bounded by `ExecutionPolicy.max_pages` (10,000 by default).
