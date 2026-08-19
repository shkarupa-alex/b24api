# W0 inventory and isolation

This document is the preregistered W0 ledger for the Bitrix24 client 2.0
implementation. It applies the normative dispositions in section 27.2 of the
approved specification; it does not promote the pre-existing working-tree
implementation to a baseline.

## Locked inputs

| Input | Locked value |
|---|---|
| Original client HEAD | `08277c4d921b83b9252177b3e72a21a4c0c86109` |
| Implementation branch | `codex/bitrix24-client-benchmarks` |
| Specification SHA-256 | `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7` |
| Specification path | `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md` |
| Corpus HEAD | `a353a58fb1de7abf2c81dd86fc7d8ad1caea8f2f` |
| Current corpus recipe-tree SHA-256 | `7c6c83487df9913076f16f1cab8630b065df2250f026c4679871957e51343b66` |
| Corpus Git-status SHA-256 | `237cdb8c384441c71869dbb5e5b2f13246f050d792d9a2e9a69399ff9f9a6340` |
| Census source files | 1,255 `scripts/recipes/*.py` files + 2 top-level Python files = 1,257 total |

The specification and corpus are uncommitted/dirty inputs owned by the user.
Their hashes make the census reproducible without committing, stashing, or
normalizing them. Later evidence records both the committed corpus SHA and the
current recipe-tree hash.

Implementation is in the main working directory at the user's request, on the
dedicated branch above. The original committed HEAD remains the sole behavior
baseline. Pre-existing changes are candidate material reviewed hunk by hunk.

## Pre-existing client dirty-state fingerprint

The following files existed before W0 work began. Their contents must retain
these hashes until a later work package deliberately adopts or replaces an
individual hunk.

| Path | SHA-256 |
|---|---|
| `README.md` | `e0978c6ca3b0f91ef698d8b09cb0a5312663808045d4fc2f8f9811a337dd4e8f` |
| `b24api/__init__.py` | `cf888ecafe019990ede729d8efc107541be95de5c9715f95bd4846dd3990f88f` |
| `b24api/api.py` | `721779832ed359b79db5375e9a050807bb8c84998dae36a23ce3bcac2034aa0f` |
| `b24api/api_test.py` | `b001009a4895db56a5d27197ee4fb5d52de252cc0d7df6e214126e86bb14c01e` |
| `b24api/entity.py` | `f9b2086eb731a38cda7d867035921dbe77e18e8d6eab3c34ad8fd90c38aa479b` |
| `b24api/error.py` | `7e4cb5d836d6ba6e273f13d927cbd3f75e9556a331ad1f62610a856b473c5f8a` |
| `b24api/helper.py` | `87076e6f54d0027597096d0ea7bad190c4c32a6aa44bfdd8eb41c71a9f24759b` |
| `b24api/entity_types.py` | `d0d7a052c78d0a4a365f21da278e0ecb5c2549220954f3e451a765a16268a757` |

The tracked diff against original HEAD was 1,889 insertions and 213 deletions;
its SHA-256 was
`e707b50fb23d66bd81521291faf3a2223f6b01299829b40866accd3c0b563a27`.
It was produced with Git 2.55.0 using this exact locale/configuration and path
set (the working-tree diff is relative to the checked-out HEAD, whose copies of
these paths equal original HEAD):

```text
LC_ALL=C git -c core.autocrlf=false diff --binary --no-ext-diff --no-renames -- README.md b24api/__init__.py b24api/api.py b24api/api_test.py b24api/entity.py b24api/error.py b24api/helper.py | shasum -a 256
```

## Committed public-symbol ledger

`preserve` means the final compatibility suite must retain the import path,
valid behavior, positional arguments, and existing keyword arguments. New
keyword-only arguments may be added where the approved specification permits.

| Committed symbol | Classification | 2.0 disposition |
|---|---|---|
| `b24api.Bitrix24` | Root export | Preserve. |
| `b24api.api.Bitrix24.__init__` | Constructor using `ApiSettings` injection | Preserve zero-explicit-argument construction and explicit settings injection. |
| `Bitrix24.aclose`, async context management | Lifecycle | Preserve; deterministic stream cleanup is added beneath it. |
| `Bitrix24.host` | Property | Preserve host-only return value. |
| `Bitrix24.call(request, *, raw=False)` | Facade method | Preserve; `policy` and the temporary retry bridge are keyword-only. |
| `Bitrix24.batch(requests, *, batch_size=None, list_method=False, with_payload=False)` | Facade method | Preserve fail-fast, input order, payload shape, and unbounded sync/async inputs. |
| `Bitrix24.list_sequential(request, *, list_size=None)` | Compatibility traversal | Preserve as a thin `OffsetSequentialPlan` wrapper. |
| `Bitrix24.list_batched(request, *, list_size=None, batch_size=None)` | Compatibility traversal | Preserve as counted traversal with a correctness-first fallback. |
| `Bitrix24.list_batched_no_count(request, *, id_key="ID", list_size=None, batch_size=None)` | Compatibility traversal | Preserve; legacy `id_key` fills all identity roles only when no explicit `IdentitySpec` exists. |
| `Bitrix24.reference_batched_no_count(request, updates, *, id_key="ID", list_size=None, batch_size=None, with_payload=False)` | Compatibility traversal | Preserve through the reference scheduler. |
| `Bitrix24.reference_cursor_no_count(request, updates, *, cursor_param="LAST_ID", cursor_field="id", cursor_take="max", list_size=None, list_size_param="LIMIT", batch_size=None, result_key=None, with_payload=False)` | Compatibility traversal | Preserve through `ItemCursorPlan` and explicit batch dispatch. |
| `b24api.entity.Request`, `Request.query` | Importable value | Preserve; canonical storage becomes deeply immutable and wire conversion moves to an explicit method. |
| `ListRequestParameters`, `ListRequest` | Importable compatibility values | Preserve module paths and valid construction; wrappers translate them into canonical requests. |
| `Response`, `Response.list_result`, `ResponseTime` | Importable response values | Preserve; `time` becomes optional for valid Bitrix envelopes. |
| `ErrorResponse`, `BatchResult` | Importable protocol values | Retain module compatibility while codec ownership moves out of models. |
| `ApiResponseError`, `RetryApiResponseError`, `RetryHTTPStatusError` | Importable error types | Preserve exact paths and aliases within the new hierarchy. |
| `Settings`, `ApiSettings`, `api_settings` | Configuration API | Preserve all existing environment-backed fields and map them into default `ExecutionPolicy`. |
| `build_query` | Importable protocol helper | Preserve behavior while the codec becomes its owner. |
| `ApiTypes` | Importable legacy type alias | Preserve as a compatibility alias to the canonical JSON types. |
| `BatchedNoCountHelper`, `ReferenceNoCountHelper`, `CursorNoCountHelper` | Unprefixed implementation classes, not root exports | No new contract. Keep importable while W7 removes wrapper dependence; do not use them as a second traversal engine. |

The committed root `__all__` contains only `Bitrix24`. Root-export expansion is
therefore a new 2.0 decision, not a compatibility requirement.

## Dirty-tree additions and concepts

| Dirty-tree addition or change | Disposition | Required replacement or guard |
|---|---|---|
| Root exports for `Request`, `Response`, errors, and outcomes | Redesign | Export only the normative 2.0 census; do not treat the dirty `__all__` as committed. |
| Structured error `.code`, `.description`, and request context | Accept concept | Retain original code separately from normalized code; redact request presentation. |
| `call(raw=...)` overloads | Accept concept | Preserve runtime behavior and add normative immutable `Response`. |
| `retry: bool` on every method | Redesign | `ExecutionPolicy` plus `ReplaySafety`; keep only the specified temporary `call` bridge. |
| `batch(errors="yield")` / `BatchErrorMode` | Reject canonical API | Separate `batch_outcomes()` returning tagged outcomes. |
| Nullable `BatchOutcome` dataclass | Replace | Use `BatchSuccess | BatchFailure` with total correlation. |
| Per-command `Response` on an outcome | Reject | Use bounded `BatchCommandEvidence`; do not invent per-command HTTP facts. |
| `Response.items(result_key)` | Replace | Add unambiguous `list_items(ResultSelector)`; no dict-like public `.items()`. |
| `Request.dump_parameters` | Internalize | Expose copied parameter access and `to_wire_parameters()` under immutable storage. |
| `ErrorResponse.to_error` | Internalize | Move structured-body-first parsing to `ProtocolCodec`. |
| `RequestLike` / `entity_types.py` | Internalize | Use an internal cycle-breaking protocol only if still needed. |
| Custom/nested parameter names and `result_key` | Accept concept | Represent through `ParameterPath` and `ResultSelector`. |
| Split item/filter/order identity keys | Accept concept | Replace keyword proliferation with `IdentitySpec`. |
| `strict=False` | Reject canonical API | Use typed `ConsistencyPolicy`; add a temporary shim only if W7 proves a consumer. |
| `CursorStrategy` and range/sequential strings | Reject | Callers choose distinct immutable plans. |
| `list_keyset` | Accept | Thin public wrapper over `KeysetPlan`, excluded from root `__all__`. |
| `reference_batch` | Redesign | Replace with `fan_out`; no compatibility promise based only on dirty code. |
| Automatic top-level OR splitting | Reject | Never rewrite caller query semantics. |
| Unconditional `start=-1` and adaptive range scan | Reject | Use explicit, locally validated plan controls; adaptive `IdWindowScan` does not ship. |
| Duplicate/total/cursor validation | Accept concept | Implement in shared drivers with typed terminal reports and exact budget accounting. |
| Bounded direct fan-out | Accept concept | Implement in W6 reference scheduler with backpressure and cancellation. |
| Dirty README and 19 added tests | Evidence only | Rebuild docs/tests against normative interfaces; preserve useful counterexamples. |

No dirty-tree hunk is adopted merely because the current dirty suite passes.

## Corpus callsite-frequency ledger

The read-only corpus is
`/Users/alex/Develop/b24api/skills/bitrix24`. The census covers 1,255 current
Python recipe files under `scripts/recipes/` and 2 top-level Python scripts,
for an exact total of 1,257 source files.
Counts are lexical call occurrences and distinct files, so wrappers/spies are
intentionally included as compatibility pressure rather than claimed runtime
traffic.

| Method | Occurrences | Files |
|---|---:|---:|
| `call` | 6,610 | 1,199 |
| `batch` | 113 | 72 |
| `list_sequential` | 0 | 0 |
| `list_batched` | 1,586 | 716 |
| `list_batched_no_count` | 151 | 90 |
| `reference_batched_no_count` | 16 | 11 |
| `reference_cursor_no_count` | 32 | 28 |
| `aclose` direct calls | 0 | 0 |

Keyword pressure in the same corpus:

| Keyword | Occurrences | Consequence |
|---|---:|---|
| `raw=True` | 238 | Preserve overload/type behavior. |
| `with_payload=` | 91 | Preserve legacy payload correlation exactly. |
| `id_key=` | 126 | Preserve the legacy argument and map it conservatively. |
| `cursor_param=` / `cursor_field=` / `cursor_take=` | 34 / 35 / 38 | Preserve cursor wrapper positional and keyword contract. |
| `result_key=` | 30 | Preserve wrapper keyword; translate to a selector. |
| `list_size=` | 35 | Preserve wrapper override. |
| `batch_size=` | 4 | Preserve wrapper override. |
| `errors=` | 15 | Dirty-corpus dependency only; migrate to tolerant streams rather than freezing polymorphism. |
| `retry=` | 0 | No current recipe pressure to freeze the dirty retry keyword broadly. |

Import compatibility in the same 1,257-file scope:

| Import or symbol | Files | Consequence |
|---|---:|---|
| Root `Bitrix24` import | 1,246 | Root construction/import compatibility is the broadest corpus contract. |
| `from b24api.error import ...` | 647 | W1 must preserve error module paths and aliases explicitly. |
| `BatchedNoCountHelper`, `ReferenceNoCountHelper`, or `CursorNoCountHelper` mention | 0 | Treating these as non-root implementation details has direct corpus support. |

The dominant compatibility risk is `list_batched`: it appears in 716 recipe
files and cannot receive a silent performance regression. Any correctness-first
fallback is measured against representative consumers and requires the release
approval specified in section 4.3.

## Baseline gates

All normative baseline commands ran against original HEAD in a detached clean
worktree. The local ignored `.python-version` selects the `3.12` minor line and
is not an exact evidence pin. Every W0 evidence command therefore names
`--python 3.12.10` explicitly; artifacts also record the full patch version.
The current dirty candidate was measured separately with CPython 3.12.10,
pytest 9.0.3, Ruff 0.15.12, mypy 1.20.2, and httpx 0.28.1.

| Source | Command | Result |
|---|---|---|
| Original HEAD, CPython 3.12.10 | `uv run pytest` | 54 collected, 54 passed in 2.53s. |
| Original HEAD | `uv run ruff check .` | Failed with 3 existing test findings: `PERF401` at `api_test.py:108`, `PLR2004` at lines 146 and 147. |
| Original HEAD | `uv run mypy b24api` | Failed with 24 existing `union-attr` errors at `api_test.py:145-147`. |
| Dirty candidate, CPython 3.12.10 | `uv run pytest` | 73 collected, 73 passed in 2.03s on the correction rerun. |
| Dirty candidate | `uv run ruff check .` | Passed. |
| Dirty candidate | `uv run mypy b24api` | Passed for 11 source files. |

W11 compares against the recorded failures as well as the passing pytest
baseline. A new gate may not hide an original failure by weakening lint or type
configuration.

## Preregistered evidence boundaries

The schemas and semantic validator beside this ledger lock the W9/W10 evidence
shape before any live measurement:

- `dataset-plan.schema.json`: read-only estimate and explicit write authorization;
- `disposable-entity-profiles.schema.json` and
  `disposable-entity-profiles.json`: content-addressed reviewed allowlist for
  exact disposable create/read/delete/marker/scope tuples;
- `dataset-manifest-record.schema.json`: append-only, resumable ownership records;
- `oracle-record.schema.json`: qualification, expected set/multiset/order, and mutation state;
- `benchmark-plan.schema.json`: preregistered cases, controls, repetitions, and gates;
- `evidence-artifact.schema.json`: same-SHA outcome and metric envelope.
- `batch-chaining-probe.schema.json`: exact runner, request shape, and bounded response summary.
- `validate_contracts.py`: cross-field equality/order/uniqueness/lineage checks and recursive secret-pattern scanning that JSON Schema cannot express.

Live credentials are never schema fields. Runtime selects exactly one credential
through `BITRIX24_API_WEBHOOK_URL`; artifacts retain only the host and an
HMAC-SHA-256 fingerprint whose key is never stored. The fingerprint input is a
canonical tuple of public host, declared credential role, and the current
principal ID observed by the read-only `profile` command. The ID itself is
discarded. This distinguishes roles/principals on one portal without hashing a
webhook token. The host is intentionally public metadata required by section
16.3; paths and tokens are not retained. The four supplied roles are represented
only as `admin_full`, `admin_limited`, `employee_full`, and `employee_limited`.

The schemas bound and type free-text surfaces and reject known credential forms,
but they are not claimed to prove redaction by themselves. The semantic
validator recursively scans every string and already rejects URL,
query/bearer, JSON `auth`, `AUTH_ID`, and `APPLICATION_TOKEN` forms. W1 owns
canonical redaction, and W9 must run the repository/artifact leak scanner before
accepting any artifact. W1 regression input explicitly includes a bare token,
`{"auth": "..."}`, and `AUTH_ID=... APPLICATION_TOKEN=...`; bare-token
detection must be context-aware so that lineage hashes are not false positives.

No large seeding is authorized by W0. A plan is valid only when every cell
matches the content-addressed disposable allowlist, its declared entity estimate
covers `max(target_count, base_count)` per cell, and each cell remains within the
reviewed `max_entities_per_cell`. Writes additionally require a reviewed
generator SHA, a concrete plan instance, dry-run estimates, explicit user write
authorization, and a resumable cleanup path.

## Read-only batch command-chaining probe

At `2026-08-19T20:06:59Z`, committed runner
`cf153551640934a215a465959831a401a83358ff` made one request using the
`admin_full` role. It ran `profile` followed by `user.get` whose `ID` came
from `$result[who][ID]`.
The HTTP status was 200, the envelope shape was recognized, neither the envelope
nor either command reported an error, no unexpected command-error key existed,
and exactly one dependent row matched the profile identity. Exact IDs and
response bodies were discarded. The runner deliberately accepts the Bitrix/PHP
wire polymorphism where an empty PHP array becomes JSON `[]` and the same array
with associative command keys becomes a JSON object; only exactly empty `[]` or
an object is valid for `result_error`.

The runner used CPython 3.12.10 and httpx 0.28.1. Its committed source SHA-256
is `8bda6eae912a956871c9106e1ed209751823a1db4e09b4a6e3cdb92c9f8bab75`.
The artifact validates against `batch-chaining-probe.schema.json`. Its portal
fingerprint uses `HMAC-SHA-256(key, canonical_json([host, role, principal_id]))`
under algorithm ID `hmac-sha256-portal-role-principal-v1`; the principal ID,
one-way key, and webhook were not retained. The runner handles unexpected
exceptions with a fixed message that cannot render the webhook.

This proves only that dependent command substitution works for this portal and
query shape. It does not authorize a traversal plan or strengthen assurance.
Per sections 27.3 and 27.11, a fixed-depth chained-keyset candidate requires a
separate specification, correctness proof, implementation review, and benefit
gates before use.

## Known gaps and dependent decisions

- Corpus counts are lexical against the current dirty recipe tree. W7 still
  needs executable signature, import, wire, and yield characterization tests.
- No endpoint profile or optimized plan is authorized. The single runtime probe
  can falsify an assumption but cannot promote assurance.
- The W0 disposable allowlist contains task and CRM-deal method tuples, but no
  concrete dataset plan, manifest, or oracle instance exists yet, and no live
  write is authorized. Any additional family requires another reviewed allowlist
  revision and content hash.
- Original HEAD has recorded Ruff and mypy failures; later work must repair them
  without weakening policy.
- The approved specification remains a user-owned untracked input. Its locked
  hash, rather than an implicit copy, governs this branch.

Approval of this packet authorizes implementation of W1 and W2 only. It does not
authorize live writes, trust retry/batch measurements, choose a traversal
default, admit an optimization, or accept a release candidate. Those decisions
remain behind their section 21 checkpoints.
