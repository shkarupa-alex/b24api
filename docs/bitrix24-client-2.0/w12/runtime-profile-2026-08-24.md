# Runtime profile and old/new comparison — 2026-08-24

## Subject and method

- Profiled runtime SHA: `6542800570d5f252f83f1b73ee7526757897c174`.
- Frozen predecessor: `08277c4d921b83b9252177b3e72a21a4c0c86109` (`1.0.1`).
- Python: 3.12.10 on macOS arm64.
- Memory profiler: Memray 1.20.0 with Python allocator tracing.
- The local comparison alternated the frozen `fixed_1x_batch` algorithm and
  production `counted_batch` on the same zero-latency deterministic portal.
- The live comparison alternated the two algorithms against existing Tasks and
  CRM data through persistent HTTP/2 clients. It selected identities only and
  performed no writes.

The deterministic benchmark measures local Python overhead and request shape.
The live benchmark measures end-to-end wall time but remains an operator
observation on a mutable shared portal, not immutable performance admission.

## Local speed and CPU

Medians from 31 measured samples after five warmups:

| Case | Rows | 1.0.1 wall | 2.0 wall | Wall ratio | 1.0.1 CPU | 2.0 CPU | CPU ratio | Requests old/new |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Small | 19 | 0.044 ms | 0.173 ms | 3.96x | 0.043 ms | 0.174 ms | 4.05x | 1 / 1 |
| Medium | 500 | 0.686 ms | 4.108 ms | 5.99x | 0.684 ms | 3.185 ms | 4.66x | 2 / 2 |
| Dense | 10,000 | 16.778 ms | 81.494 ms | 4.86x | 16.767 ms | 65.785 ms | 3.92x | 5 / 5 |
| Uniform sparse | 10,000 | 17.373 ms | 82.748 ms | 4.76x | 17.359 ms | 67.173 ms | 3.87x | 5 / 5 |
| Clustered sparse | 10,000 | 16.765 ms | 82.400 ms | 4.92x | 16.663 ms | 66.557 ms | 3.99x | 5 / 5 |

Every pair produced the exact same identity hash. The extra CPU is real: 2.0
validates totals, ranges, continuations, identities, duplicate policy,
budgets, batch correlation, and immutable evidence. The absolute zero-latency
cost remains about 65 ms for 10,000 rows.

## Memray allocation profile

Peak memory tracks live allocations inside measured samples; imports and
warmups are outside the tracker.

| Workload | 1.0.1 peak | 2.0 peak | 2.0 allocated over sample |
|---|---:|---:|---:|
| Synthetic ID-only, 19 rows | 0.017 MiB | 0.041 MiB | 0.5 MiB |
| Synthetic ID-only, 500 rows | 0.140 MiB | 0.378 MiB | 9.9 MiB |
| Synthetic ID-only, dense 10,000 | 2.519 MiB | 4.518 MiB | 208.3 MiB |
| Synthetic ID-only, uniform-sparse 10,000 | — | 4.525 MiB | 208.8 MiB |
| Live full Tasks response, 932 rows | — | 7.975 MiB | 291.0 MiB |
| Live full CRM response, 1,060 rows | — | 5.932 MiB | 186.1 MiB |

Twenty consecutive synthetic dense-10,000 traversals peaked at 4.896 MiB,
compared with 4.518 MiB for one traversal. Total allocations scaled with the
number of traversals while the live peak stayed nearly flat; no accumulating
memory signature was observed.

The dominant allocation sites are `_freeze_json` and `_thaw_json`, followed by
JSON encoding/decoding and deterministic contract hashing. This is the cost of
deep immutable public models and detached caller-owned results. Removing those
copies would touch correctness guarantees and is not justified by the bounded
peaks or the live timing result.

## Fresh read-only portal timing

Each cell used alternating execution order. The interval is a seeded bootstrap
95% interval for the median of the per-pair new/old ratios.

| Cell | Pairs | Rows | Median paired ratio | Bootstrap 95% | Identity |
|---|---:|---:|---:|---:|---|
| Tasks small tail | 25 | 19 | 1.010 | 0.975–1.035 | exact |
| CRM small tail | 25 | 19 | 0.993 | 0.972–1.022 | exact |
| Tasks existing selection | 21 | 932 | 1.013 | 0.984–1.048 | exact |
| CRM existing selection | 21 | 1,060 | 0.999 | 0.976–1.025 | exact |

All intervals include 1.0. The defensible conclusion is wall-time parity, not a
speed win. Unpaired medians were visibly sensitive to transient portal phases,
which is why they are not used for the decision.

## Business-goal status

| Goal | Status | Evidence |
|---|---|---|
| Never return an incomplete traversal as success | Achieved | Typed fail-closed terminal reports and exact model/live identity checks |
| Cover direct, batch, counted, keyset, and reference scenarios | Achieved for the reviewed 2.0 scope | One replay-aware executor and immutable explicit plans |
| Preserve old `list_batched` network efficiency | Achieved | Exact 1/2/5 request parity and equal identities |
| Avoid a meaningful production wall-time regression | Supported, not normatively admitted | Four fresh live paired intervals include parity |
| Bound memory | Achieved for measured workloads | 4.52 MiB at synthetic 10k IDs; 7.98 MiB at live full 932 Tasks; no repeated-run growth |
| Make the new engine cheaper than 1.0.1 locally | Not achieved | Correctness validation costs roughly 4-6x local CPU, though absolute 10k overhead is about 65 ms |
| Fast and safe generic no-count traversal | Not achieved | Sequential keyset remains the safe fallback; partitioned keyset is unadmitted |
| Normative performance admission | Not claimed | Requires a separately reviewed immutable live cell |

The candidate is suitable as a correctness-first release with restored
structural performance compatibility. It must not be marketed as a universally
faster client.

## Reproduction

~~~text
uv run python tools/b24api_evidence/profile_runtime.py --samples 31 --warmups 5

uv run --with memray==1.20.0 python tools/b24api_evidence/profile_runtime.py \
  --case dense-10k --plan counted_batch --samples 1 --warmups 3 \
  --memray-output /tmp/b24api-dense-10k.bin

uv run --with memray==1.20.0 memray stats /tmp/b24api-dense-10k.bin
uv run --with memray==1.20.0 memray flamegraph /tmp/b24api-dense-10k.bin
~~~
