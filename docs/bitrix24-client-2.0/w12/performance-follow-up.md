# Performance follow-up after the W11 correctness candidate

## Scope and conclusion

This follow-up restores the performance properties of the committed counted
wrapper without weakening the correctness-first runtime. It does not admit the
optional W10 partitioned-keyset candidate and does not claim a generic fast
no-count traversal.

The frozen predecessor baseline is the 1.0.1 direct-head plus 50-command
batched-tail algorithm at `08277c4d921b83b9252177b3e72a21a4c0c86109`.
The model implements that algorithm independently rather than routing it
through candidate traversal code. Across 19, 500, dense 10,000, uniform-sparse
10,000, and clustered-sparse 10,000 rows, it produces the same identities and
the same physical-request counts as candidate counted batching: 1, 2, 5, 5,
and 5 respectively.

The often quoted 10 to 2 and 200 to 5 reductions compare counted batching with
sequential offset traversal inside the model. They are useful request-efficiency
context, but they are not an improvement over the old `list_batched`, which was
already batched. The model benchmark therefore remains `draft`, its thresholds
remain non-normative, and `--admission-ready` refuses before execution.

## Runtime changes

- `ExecutionPolicy.max_buffered_rows` defaults to 2,500, the finite maximum of
  50 Bitrix batch commands times the 50-row page cap. Smaller user limits remain
  authoritative.
- The default transport again enables HTTP/2 with HTTP/1 fallback.
- Recursive body previews are computed only for structured errors, HTTP errors,
  and malformed success bodies, never for a valid successful response.
- Trusted batch decoding shares an already immutable correlated `Response`
  instead of freezing and thawing the same command result repeatedly. Public
  `Response.result` and `BatchSuccess.result` still return detached mutable
  copies.

## Read-only operator observations

Nine interleaved old/new pairs were run against existing disposable portal data
without entity creation. Every pair produced equal identity hashes and equal
physical-request counts.

| Cell | Old/new requests | Paired new/old wall ratio |
|---|---:|---:|
| Tasks, 19 rows | 1 / 1 | 1.0067 |
| CRM, 19 rows | 1 / 1 | 1.0465 |
| Tasks, 932 rows | 2 / 2 | 0.9655 |
| CRM, 1,060 rows | 2 / 2 | 1.0457 |

These are operator observations on mutable live data, not bundled admission
evidence. In particular, the 5% small-cell ceiling applies only to the 19-row
cells; the roughly 1,000-row observations are disclosed without relabeling them
as a passing normative medium gate.

## Remaining boundary

The old generic no-count strategy is not restored. On Tasks it was observed to
return 2,050 rows for a 932-row selection, so treating its speed as acceptable
would reintroduce a silent completeness failure. The new generic wrapper uses
exact sequential keyset traversal until a separately reviewed endpoint profile
can authorize a correct partitioned strategy.
