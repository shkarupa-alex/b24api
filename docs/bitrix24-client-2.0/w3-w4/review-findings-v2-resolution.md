# W3/W4 review findings v2 — resolution

Accepted-by-one-reviewer subject:
`5bec599ef1e47e375ada56f67c856f4c11963719`.

Independent review found one remaining blocker, so the replacement subject is:
`521f0eb7cb107ec948c693496154f94e57dbf7c9`.

## Blocking finding

`_decode_success()` parsed the HTTP envelope but allowed validation failures
from `Response` and `ResponseTime` to escape as raw `ValueError`. This occurred
for at least:

- `next=-1`;
- a JSON number such as `1e400` that Python materializes as infinity;
- negative server duration/processing values.

For a batch envelope, the overflow could terminate `batch_outcomes()` as
`FAILED` rather than synthesize the required per-command `BatchFailure`.

## Resolution

- The decoder creates safe HTTP evidence once and passes it through every
  success-contract error.
- Request summary is retained without parameter values.
- `ResponseTime` parsing raises internal `TypeError`/`ValueError` for invalid
  model inputs.
- All `Response`/`ResponseTime` construction `TypeError`, `ValueError`, and
  `OverflowError` are wrapped as `ProtocolError` with request summary and HTTP
  evidence.
- Malformed JSON and missing envelopes remain typed `HTTPGatewayError` cases.
- Tolerant batch catches the resulting `B24ApiError` at chunk scope and emits a
  separately correlated failure for every unresolved command.

## Regressions

Three direct success-envelope cases assert `ProtocolError`, status 200 evidence,
safe request summary, and an explicit model-validation cause:

1. negative continuation;
2. overflowed result number;
3. negative server duration.

A fourth regression puts `1e400` inside a successful outer batch result. The
stream finishes operationally, returns one correlated `BatchFailure` carrying
`ProtocolError`, and retains command index zero.

The non-blocking dead `_PhaseTracker.observed` field was removed. The other two
review notes are recorded as W5/W7 obligations rather than protocol claims.
