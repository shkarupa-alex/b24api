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
```

The live matrix records task filters, identity-role geometry, portal-dependent density/size
shortfalls, and deterministic-fallback reason codes in the artifact manifest. Same-case coverage
requires `B24API_KEYSET_SAME_CASE_CELL` to contain an explicit JSON contract for a read-only
`*.list` method, parameters, selector/item paths, identity field, and expected auto selection; task
lists are not used for that cell because their numeric filter and order roles differ.

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

Memray 1.20.0 measured a 5,121,452-byte peak for seven dense-10k counted samples after two warmups.
Immutable JSON thawing was the largest cumulative client allocation site; no retention leak was
observed.

On 2026-09-10, the committed keyset admission harness ran five accepted read-only range sandwiches
after one warm-up on a 934-row list. Every ordered digest matched both sequential controls, with no
exclusions, omissions, duplicates, or output over-fetch. Range used 5 physical requests versus 20;
the paired median request ratio was 0.25 and the paired median wall-time ratio was 0.318. These are
harness-produced observations for that portal cell, not general latency promises.

## Boundaries

- Sequential no-count traversal remains the conservative default. Explicit range, partitioned, and
  auto keyset modes recover physical batching only under a caller-asserted stable integer-keyset
  contract; they pay a planning barrier even when the consumer stops early.
- A returned `total` is advisory only and may raise an auto cost estimate. It never closes a lane or
  strengthens correctness evidence. The terminal report records both the selected plan and whether
  assurance came from the ordered prefix alone or from validated numeric-bound canaries.
- Real portal latency, server work and network variance require a separately controlled live A/B.
- Exact counted traversal retains identities for the operation lifetime; large exact traversals may
  therefore use substantial memory and emit a warning above 100,000 identities.
