# W7-W9 pre-live review packet

## Decision requested

Review exact code-and-test SHA
`078b3a085afa86cb9a115a080ac474523d68785b` and either:

1. accept it as the W7 compatibility, W8 profile, and W9 pre-live evidence
   foundation; or
2. return prioritized findings against that exact SHA.

Acceptance does **not** authorize live access, writes, cleanup, an optimization
default, or a performance claim. A separate explicit human decision is required
before any supplied webhook may be used. Final acceptance follows reviewed live
evidence and verified cleanup.

This packet is a later documentation-only descendant. It does not change the
immutable review subject.

## Immutable inputs and base

- review subject: `078b3a085afa86cb9a115a080ac474523d68785b`;
- accepted W5/W6 dependency: `c63c827` (semantic foundation `a29b58f...`);
- branch: `codex/bitrix24-client-benchmarks`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`;
- reviewed CPython: `3.12.10`.

The exact implementation comparison is:

```bash
git diff c63c827..078b3a085afa86cb9a115a080ac474523d68785b -- \
  b24api tools pyproject.toml docs/bitrix24-client-2.0/w7-w8 \
  docs/bitrix24-client-2.0/w9
```

Review in a detached clean worktree or archive. Do not attribute results from
the protected main-tree overlay to the immutable SHA.

## Implemented boundary

### W7 compatibility facade

The public facade maps legacy batch, list, cursor, and reference entry points to
the accepted canonical execution/traversal core. It preserves explicit
positional and keyword `Settings`, settings-backed defaults, payload
correlation, identity selection, no-total continuation, committed page-round
reference ordering, typed reports, and deterministic ownership/cleanup of sync
and async sources.

PHP-shaped batch decoding deliberately distinguishes an empty JSON array from
an associative object. A non-empty JSON array is not accepted as an associative
command map.

### W8 profiles

Profiles, query shapes, capability sets, decisions, and explanations are closed
typed values with strict runtime validation. Applicability binds build, scope,
query shape, policy, source hash, and evidence hash. Runtime probes may only
downgrade or contradict. The wheel contains the schema and an intentionally
empty profile set: no automatic strategy or optimization default is admitted.

### W9 pre-live evidence harness

The harness is excluded from the distributable library. It provides strict
plans, content-addressed lineage, atomic artifacts/manifests, deterministic
offline model evidence, bounded secret scanning, guarded live lifecycle
commands, oracle-first verification, recovery preview binding, exact-marker
ownership, resumable cleanup, and explicit stable exit codes.

Live seed/cleanup require both flags, exact candidate/portal/profile/scope/build
binding, a human-approved plan, and refuse ordinary pytest. Live benchmark is
not admitted and cannot fall back to the offline model. No live command has
been run for this candidate.

The offline benchmark preregisters a deterministic 1 ms/request latency model.
Its timing fields are normalized model values, not host-clock measurements and
cannot authorize a live performance claim. Per-observation counters and timing
are exactly revalidated; aggregate-only redistribution and Python `bool`/`int`
aliases are rejected.

The reviewed live-write envelope is at most two unique disposable profile
cells and 500 entities in aggregate, with a 500 per-cell ceiling. Re-running
`plan` against a non-empty artifact directory refuses before changing it.
Manifest logical appends use a locked whole-file atomic replacement, so a
failed replace preserves the last complete chain.

## Known non-authorizations

This checkpoint does not admit:

- any live operation before the next human decision;
- live benchmark evidence or a live performance conclusion;
- non-empty packaged endpoint profiles or automatic plan selection;
- partition, prefetch, automatic dispatch, or proactive rate defaults beyond
  separately accepted behavior;
- arbitrary write methods, arbitrary entity scale, or caller-raised ceilings;
- cleanup outside exact manifest/marker ownership;
- secret-bearing artifacts, tracebacks, webhook URLs, or tokens in commits.

## Exact gates

Executed on a clean detached worktree at the exact subject:

| Check | Result |
|---|---:|
| Full pytest | 336 passed |
| Profiles + W9 focused | 98 passed |
| Strict-warning profile gate | 18 passed |
| Ruff `--no-fix` | Passed |
| strict mypy | 38 source files, passed |
| `git diff --check` and tracked status | Clean |
| Specification SHA-256 | Exact match |

Representative commands, using the project's pinned environment:

```bash
/Users/alex/Develop/b24api/client/.venv/bin/python -m pytest -q

/Users/alex/Develop/b24api/client/.venv/bin/python -m pytest -q \
  b24api/profiles_test.py \
  tools/b24api_evidence/harness/contracts_test.py \
  tools/b24api_evidence/harness/live_test.py

PYTHONASYNCIODEBUG=1 \
  /Users/alex/Develop/b24api/client/.venv/bin/python -W error -m pytest -q \
  b24api/profiles_test.py

/Users/alex/Develop/b24api/client/.venv/bin/ruff check . --no-fix
/Users/alex/Develop/b24api/client/.venv/bin/mypy --strict b24api tools/b24api_evidence
git diff --check
git status --short
shasum -a 256 spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
```

The main tree also contains the user-owned untracked paths `spec/` and
`b24api/entity_types.py`; they are not part of the immutable subject and were
not staged, modified, or removed.

## Clean-room admission

The final clean-room replay reviewed exact
`078b3a085afa86cb9a115a080ac474523d68785b` without edits or live access.
It specifically replayed:

- preservation of an existing bundle on refused `plan`;
- atomic manifest append failure;
- aggregate scale multiplication;
- observation timing redistribution and boolean counter aliases;
- a real mid-benchmark dirty-tree mutation.

All dedicated regressions passed (7/7), the real dirty-tree race exited `2`
without a benchmark PASS artifact, and the same-SHA limited verdict was
**ACCEPT**, P1/P2/P3 = 0/0/0 within that replay scope.

## Copy-paste prompt for independent reviewers

```text
Проведи независимое состязательное ревью exact SHA
078b3a085afa86cb9a115a080ac474523d68785b в отдельном detached clean
worktree/archive. Ничего не редактируй и не используй live webhook/network.

Нормативная спецификация:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256: 2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7
Принятая база W5/W6: c63c827; сравнение: c63c827..078b3a0.

Проверь не изложение пакета, а сам immutable subject:
1) происхождение SHA, clean status и воспроизводимость exact gates;
2) W7 legacy-совместимость, Settings/default mapping, ordering/correlation,
   PHP array-vs-object batch envelopes, cancellation/early-close ownership;
3) W8 closed runtime types, profile applicability/provenance, downgrade-only
   probes и отсутствие автоматически допущенных packaged profiles;
4) W9 schema+semantic invariants, lineage/content dependencies, oracle and
   metric algebra, secret redaction/scanning, bounded inputs, atomicity,
   recovery/resume/cleanup state machine, refusal-before-I/O/write;
5) отсутствие пути к live write без двух flags, exact approved plan,
   candidate/portal/profile/scope/build binding и human approval;
6) отсутствие model fallback для live/non-model benchmark evidence;
7) последние контрпримеры: existing-bundle overwrite, manifest short-write,
   aggregate cell multiplication, per-observation timing redistribution,
   bool/int aliases и dirty-tree TOCTOU.

Особенно атакуй внутренне противоречивые PASS/COMPLETED документы, подмену
content refs, mixed lineage, PHP-сериализацию пустых/непустых массивов,
нефинитные числа, cancellation races, неоднозначные create/delete состояния и
утечки секрета через ошибки/JSON encoding/path fixtures.

Верни сначала Findings с P1/P2/P3, точными файлами/строками и минимальными
репро. Если findings нет — явно напиши P1/P2/P3 = 0/0/0 и дай verdict:
ACCEPT или REJECT exact 078b3a085afa86cb9a115a080ac474523d68785b.
Acceptance означает только pre-live W7-W9 foundation; live/default/performance
authorization не входит в этот голос.
```

## Human checkpoint

Stop here. The requested response is an explicit acceptance of the exact SHA
or findings against it. Do not begin live evidence until that decision is
recorded.
