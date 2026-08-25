# v2 capability architecture runtime profile

Date: 2026-08-25

Runtime candidate: `083490d92b5b9d1fa876d96e14ec04beff6faa0c`

Frozen pre-refactor comparison: `e5fd427eddb28b7079b75bfef6443c04aa6350d1`

Python: 3.12.10, macOS arm64. All portals and responses were deterministic, credential-free
fixtures. These results are local runtime evidence, not a live latency admission.

## Reproduction

```console
.venv/bin/python tools/b24api_evidence/profile_runtime.py --capability-suite
.venv/bin/python tools/b24api_evidence/profile_runtime.py --samples 7 --warmups 2
.venv/bin/python tools/b24api_evidence/profile_runtime.py \
  --case dense-10k --plan counted_batch --samples 7 --warmups 2 \
  --memray-output /tmp/b24api-dense.bin
.venv/bin/memray stats /tmp/b24api-dense.bin --json -o /tmp/b24api-dense-stats.json
```

The last two timing commands were run twice independently for both exact SHAs. The base worktree
was selected through `PYTHONPATH`; the same interpreter and dependencies were used.

## Resource invariants

Every blocking invariant passed:

- the logical batch closes its async source and never serializes caller correlation;
- 10,000 and 100,000 command runs retain a physical window of exactly 7;
- the 100,000-command peak is below the 10,000-command peak, comfortably satisfying the
  `10k peak + max(25%, 32 MiB)` plateau;
- counted physical request shape is exactly `1 / 2 / 5` for 19 / 500 / 10,000 rows;
- dense and sparse counted identities match their exact oracles;
- input-order references stay within row and active-reference bounds;
- no stream or owned task remains after close;
- 100 repeated traversals after warm-up grow by 24,071 bytes, below the 1 MiB ceiling;
- oversized response collection retains at most the 64 KiB accepted prefix plus the one 16 KiB
  incoming chunk and refuses the response.

Selected traced measurements:

| Case | Rows/events | Requests | High-water | Wall | CPU | Peak Python allocation |
|---|---:|---:|---:|---:|---:|---:|
| direct call | 1 | 1 | — | 2.9 ms | 2.2 ms | 57,515 B |
| logical batch 10k, buffer 7 | 10,000 | 1,429 | 7 commands | 2.49 s | 1.88 s | 293,611 B |
| logical batch 100k, buffer 7 | 100,000 | 14,286 | 7 commands | 24.47 s | 18.56 s | 288,234 B |
| counted 19 | 19 | 1 | 19 rows | 13.4 ms | 12.5 ms | 2,056,776 B |
| counted 500 | 500 | 2 | 450 rows | 52.6 ms | 38.3 ms | 2,056,776 B |
| counted dense 10k | 10,000 | 5 | 2,500 rows | 759 ms | 514 ms | 3,767,431 B |
| counted uniform-sparse 10k | 10,000 | 5 | 2,500 rows | 761 ms | 514 ms | 3,780,971 B |
| references, INPUT order | 16 events | 16 | 1 row / 2 active | 36.1 ms | 14.0 ms | 91,505 B |
| 110 repeated traversals | 0 | 110 | — | 76.7 ms | 69.8 ms | 94,079 B |
| oversized response refusal | — | 1 | 64 KiB + chunk | 1.2 ms | 1.2 ms | 108,245 B |

Traced wall/CPU values include `tracemalloc` overhead and are resource-characterization figures,
not user-facing throughput claims.

## Refactor performance comparison

Each reproduction used two discarded warm-ups and seven measured samples. The 19-row microcell was
also repeated twice with five warm-ups and 31 measured samples because seven-sample sub-millisecond
ratios were visibly scheduler-noisy. Ratios below compare the current counted traversal to the same
counted traversal at the frozen pre-refactor HEAD. Identity hashes and physical request counts were
identical in every cell.

| Case | Requests base → current | Wall ratio R1 | Wall ratio R2 | CPU ratio R1 | CPU ratio R2 |
|---|---:|---:|---:|---:|---:|
| 19 | 1 → 1 | 1.030 | 0.989 | 1.024 | 1.006 |
| 500 | 2 → 2 | 1.056 | 1.021 | 1.054 | 1.010 |
| dense 10k | 5 → 5 | 1.040 | 0.996 | 1.042 | 0.993 |
| uniform-sparse 10k | 5 → 5 | 1.025 | 1.019 | 1.028 | 1.018 |
| clustered-sparse 10k | 5 → 5 | 0.995 | 1.017 | 1.003 | 1.015 |

No cell reproduced a regression above 10%. The refactor therefore satisfies its local stable
performance gate. This does not replace live portal A/B evidence.

## Frozen 1.0.1 structural parity

The deterministic `fixed_1x_batch` replay bypasses the current executor and reproduces the 1.0.1
direct-head plus batched-tail algorithm. Current counted traversal has exact identity and request
parity with it:

| Case | 1.0.1 replay → v2 physical requests |
|---|---:|
| 19 | 1 → 1 |
| 500 | 2 → 2 |
| dense 10k | 5 → 5 |
| uniform-sparse 10k | 5 → 5 |
| clustered-sparse 10k | 5 → 5 |

The `10 → 2` (500 rows) and `200 → 5` (10k rows) figures remain useful comparisons against
sequential offset traversal, not claims of improvement over the already-batched 1.0.1 method.

## Memray

Memray 1.20.0 recorded seven dense-10k counted samples after two warm-ups:

- peak memory: 5,121,452 bytes;
- no retained stream/task resources;
- largest cumulative Python allocation site: immutable JSON thawing in
  `b24api/contracts/json.py`;
- the remaining largest sites are deterministic fixture construction, JSON encoding and evidence
  finite-number validation.

The allocation ranking is expected for an immutable decoded-JSON client. It does not reveal a
retention leak or a blocking regression; peak memory stays within the numeric resource gates.

## Interpretation

The new architecture preserves the old counted-list network efficiency while retaining the v2
correctness checks, immutable outcomes, bounded decoded buffers and explicit lifecycle. It is not
performance-admitted for generic no-count traversal or real network wall time. Keyset remains the
safe sequential fallback, and a faster partitioned no-count plan remains outside this release.
