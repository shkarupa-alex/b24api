# W3/W4 acceptance — instrumentation foundation

## Accepted subject

- Implementation SHA: `521f0eb7cb107ec948c693496154f94e57dbf7c9`
- Review packet commit: `d1cb857d2877f59256776b0da0720423c570309e`
- Branch: `codex/bitrix24-client-benchmarks`

Two independent reviews accepted the subject after three iterations. The final
review reproduced 119 clean tests, 35 focused W3/W4 tests, scoped Ruff and
strict mypy, 138 protected-overlay tests, full overlay Ruff/mypy, overlay
hashes, typed model-contract errors, and tolerant-batch total correlation.

## Authorization boundary

The SHA is accepted only as the instrumentation foundation for W5/W6. It does
not authorize any strategy or default. Pagination, reference dispatch,
prefetch, proactive rate governance, and automatic selection retain their own
correctness evidence and review gates.

The obligations in `docs/bitrix24-client-2.0/w5-w7/review-obligations.md` and
`docs/bitrix24-client-2.0/w9/review-obligations.md` remain binding.
