# W2 acceptance — immutable values and policies

## Immutable subject

- W2 implementation SHA: `abe42e097b81052fdca5cd6873ef065a1e422d2f`
- W1 implementation SHA: `7fc7b221b91636e13231540d062f824398c80421`
- W0 approved packet HEAD: `90a449e9ba6f7cbc88ee577ef7d996db498cba9b`
- Original baseline SHA: `08277c4d921b83b9252177b3e72a21a4c0c86109`
- Branch: `codex/bitrix24-client-benchmarks`

## Outputs

- `b24api/models.py`: deeply immutable request/response JSON values, exact
  selectors and parameter paths, identity and execution policies, retry and
  consistency policies, pre-scheduling budget counters, immutable operation
  reports, and safe reference/outcome value types.
- `b24api/plans.py`: frozen, validated single-response, offset, counted-offset,
  keyset, item-cursor, partitioned-keyset, batch, and direct plan contracts.
- `b24api/models_test.py`: mutation, finite-number, selector, collision,
  policy, budget, report, and representation counterexamples.
- `b24api/plans_test.py`: valid construction plus invalid direction,
  cardinality, stride, identity, filter/order, and lane-bound cases.

The canonical request and response types are intentionally not exported through
the legacy package facade in W2. W7 owns compatibility wrappers and public
exports; adopting the facade now would overlap the protected user changes in
`b24api/__init__.py`, `b24api/api.py`, and `b24api/entity.py`.

## Contract matrix

The W2 tests establish that:

- constructor input cannot mutate stored request or response JSON afterward;
- returned parameter/result copies cannot mutate stored values;
- cycles, non-string object keys, sets, `NaN`, `Infinity`, and overflowed
  `1e400` values are rejected;
- parameter control injection is case-insensitive and fails before I/O for
  ambiguity, conflicts, missing containers, and non-mapping containers;
- selectors use exact paths and reject malformed segments;
- traversal plans reject underspecified identity/order contracts and invalid
  fixed-stride or partition bounds;
- retry, consistency, execution, and confirmation policies validate their
  invariants;
- physical attempts, pages, per-reference expansion, and buffered items are
  charged before scheduling and fail with typed budget errors;
- completed reports cannot contain blocking violations and expose immutable
  counters, violations, and evidence.

## Checks

| Check | Result |
|---|---|
| CPython 3.12.10 pytest | 103/103 passed in 2.28s. |
| Ruff 0.15.12 full tree, no fix | Passed. |
| mypy 1.20.2 `b24api` | Passed for 19 source files. |
| `git diff --check` | Passed. |

The seven remaining W0-protected user files are byte-identical to their
recorded hashes. W2 did not commit `b24api/entity_types.py`, the local
specification tree, a webhook, a principal identifier, or a credential.
