# W1 acceptance — redaction and errors

## Immutable subject

- W1 implementation SHA: `7fc7b221b91636e13231540d062f824398c80421`
- W0 approved packet HEAD: `90a449e9ba6f7cbc88ee577ef7d996db498cba9b`
- Original baseline SHA: `08277c4d921b83b9252177b3e72a21a4c0c86109`
- Branch: `codex/bitrix24-client-benchmarks`

## Outputs

- `b24api/redaction.py`: one recursive, bounded redactor for nested keys,
  configured paths/PII, textual webhook/query/bearer/cookie/JSON/env forms, and
  context-aware bare credentials while preserving 40/64-character lineage
  hashes.
- `b24api/models.py`: frozen `RequestSummary`, `ResponseEvidence`, and
  `BatchCommandEvidence` safe values with explicit serialization.
- `b24api/error.py`: complete typed hierarchy, safe default text/repr/dicts,
  original/committed/normalized API codes, and import-compatible
  `RetryApiResponseError` / `RetryHTTPStatusError`.
- `b24api/protocol.py`: structured Bitrix body before generic HTTP status,
  bounded previews, safe-header allowlist, retry-code classification, and typed
  gateway/protocol fallbacks.
- `docs/bitrix24-client-2.0/w9/review-obligations.md`: all findings accepted as
  mandatory W9 or pre-live-probe gates.

The canonical protocol layer is intentionally not wired into the legacy facade
inside W1. W3 owns transport integration; W7 owns compatibility wrappers. This
keeps the package boundary independently testable without committing unrelated
user-owned `api.py` or `entity.py` candidate changes.

## Fixture matrix

The W1 tests cover:

- nested authorization/token/cookie fields;
- webhook URLs, bearer/query secrets, JSON `auth`, environment dumps, and a bare
  credential;
- configured secret paths and PII fields;
- recursion, item, depth, string, and body-preview bounds;
- lineage hash preservation and request summaries without values;
- structured errors on success, 4xx, and 5xx before status handling;
- numeric and mixed-case code snapshots;
- retry classification;
- HTML/plain gateway bodies, malformed JSON, empty success bodies, safe request
  IDs/headers, and redacted exception serialization;
- the complete hierarchy and committed aliases.

## Checks

| Check | Result |
|---|---|
| CPython 3.12.10 pytest | 87/87 passed in 2.22s. |
| Ruff 0.15.12 full tree, no fix | Passed. |
| mypy 1.20.2 `b24api` | Passed for 16 source files. |
| W1 webhook-pattern scan | Passed. |
| `git diff --check` | Passed. |

W1 deliberately supersedes the pre-existing user candidate `b24api/error.py`
(`7e4cb5d8...`) with canonical file SHA-256
`404d7ab87c245d406a6b8bac03af992df585ace1f1e4256153a189ca41b999c4`.
The other seven W0-protected dirty files remain byte-identical. No webhook or
credential was committed.
