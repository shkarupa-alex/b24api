# W7-W9 human review packet v3

## Blocking decision

Review the exact immutable pre-live candidate
`d8c2987d64dd75b3ef415da53e8053cac25a0099` and either accept it or return
prioritized findings against that SHA.

Acceptance does not authorize webhook use, live access, writes, cleanup, W10,
profile/default publication, release actions, or performance claims. This
packet is a later documentation-only commit and is not the review subject.

## Inputs

- branch: `codex/bitrix24-client-benchmarks`;
- rejected predecessor: `e5ef566bcc1e3d3c3b8669170ee185a2c85e97b1`;
- review subject: `d8c2987d64dd75b3ef415da53e8053cac25a0099`;
- normative specification:
  `spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md`;
- specification SHA-256:
  `2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7`.

Review in a detached clean worktree. Do not use live webhooks or print/inspect
credential-bearing Git remote URLs.

```bash
git worktree add --detach /tmp/b24api-human-review \
  d8c2987d64dd75b3ef415da53e8053cac25a0099
cd /tmp/b24api-human-review
git diff e5ef566bcc1e3d3c3b8669170ee185a2c85e97b1..d8c2987d64dd75b3ef415da53e8053cac25a0099 -- \
  b24api tests tools/b24api_evidence
```

## What changed after the original rejection

### Pagination and compatibility facade

- counted traversal, including direct head and batched tail, is owned by the
  shared `PaginationDriver` rather than a second facade traversal engine;
- remaining physical and logical budgets include head retries before any row
  is emitted;
- partially acquired page reservations roll back on cancellation;
- explicit legacy controls, including explicit values equal to public
  defaults, are distinguished from omission and checked against plan/profile
  before I/O;
- nested filter/order paths come from the selected plan;
- evidence anchors must share one candidate SHA and terminal reports preserve
  that provenance;
- early close on a multi-row counted page counts only rows actually emitted.

### Live evidence admission

- `scope` and `app.info` reject hostile semantic types and missing build data;
- plan self-claims are insufficient: the caller must repeat the exact final
  plan content hash and exact external review SHA;
- the review SHA must be a Git commit, not a blob/tree/tag, and its trailer
  binds a stable canonical review subject (only the circular
  `plan_review_sha` pointer is projected to `null`);
- final executable plan confirmation still binds the real review commit SHA;
- exact clean HEAD is checked both before and after the append-only dispatch
  journal record, immediately before each create/delete call;
- a post-journal refusal records `create_cancelled` or `delete_cancelled`,
  proves the adapter was not called, and permits a safe retry.

## Independent clean-room result

An independent subagent reviewed the exact subject in a detached worktree,
without network, live systems, credentials, or edits.

```text
Verdict: ACCEPT
Exact SHA: d8c2987d64dd75b3ef415da53e8053cac25a0099
P1/P2/P3: 0/0/0
pytest: 377 passed in 111.87s
ruff check: PASS
ruff format --check: 41 files already formatted
mypy --strict: PASS, 25 source files
git diff --check: PASS
```

The reviewer explicitly rechecked the original findings plus the review-hash
fixed point, non-commit Git objects, pre/post-journal HEAD drift, resumability
of cancelled create/delete dispatches, and multi-row early-close accounting.

## Human reviewer attack list

Verify the implementation rather than this summary, especially:

- self-forged approval, unrelated hashes, and blob/tree/tag review objects;
- semantic coverage of the stable plan review hash and final plan hash;
- dirty-tree and clean-HEAD drift before and during dispatch journaling;
- absence of create/delete calls after a post-journal guard refusal;
- resumability and allowed manifest transitions for both cancelled states;
- hostile scope/build values, including bool/int aliasing;
- retry accounting and refusal before counted head emission;
- partial reservation cancellation and shared-context liveness;
- explicit default controls versus omitted defaults for plan/profile paths;
- nested profile applicability and mixed candidate provenance;
- facade delegation to the shared traversal driver;
- unique/emitted/buffered row and physical/logical request accounting.

## Copy-paste review prompt

```text
Проведи независимое read-only ревью exact SHA
d8c2987d64dd75b3ef415da53e8053cac25a0099 в отдельном detached worktree.

Нормативный документ:
spec/2026-08-19-bitrix24-client-benchmarks/spec-review.md
SHA-256 документа:
2fb7acb73cb2f5d1d203b0bf53af3ecb14a23218f71226c28fa67f178af5c7e7

Не используй сеть, live Bitrix24, вебхуки или credentials; ничего не меняй.
Сначала проверь все пункты attack list из
docs/bitrix24-client-2.0/w7-w9/review-packet-v3.md, затем самостоятельно ищи
расхождения со спецификацией. Запусти pytest, Ruff check, Ruff format --check,
strict mypy и git diff --check.

Верни findings сначала: P1/P2/P3, exact file/line, минимальный reproducer и
нарушенный пункт спецификации. Если actionable findings нет, явно напиши
P1/P2/P3 = 0/0/0 и verdict:
ACCEPT d8c2987d64dd75b3ef415da53e8053cac25a0099
Иначе:
REJECT d8c2987d64dd75b3ef415da53e8053cac25a0099

После verdict остановись. Не выполняй live/W10/release действия.
```

## Required stop

After the human verdict, stop. Live portal validation is a separate checkpoint
that requires explicit authorization and a separately reviewed dataset plan.
