# W5/W7 obligations inherited from W3/W4 review

These items do not block the reviewed W3/W4 mechanisms, but they constrain
later work.

## W5 — `total=-1` evidence boundary

The canonical response deliberately preserves the received sentinel `-1`; it
does not infer what that value means. The claim that a specific endpoint such as
`im.recent.list` returns this form is currently an assumption from repository
history, not a standalone W3/W4 evidence artifact.

Before W5 relies on `-1` for an endpoint/profile decision, it must either:

- attach deterministic fixture/profile provenance that authorizes
  `TotalSemantics.IGNORE` or `ADVISORY`; or
- produce a bounded read-only sanitized observation tied to the reviewed
  instrumentation SHA.

Until then, preserving `-1` prevents premature decoder failure but authorizes no
termination rule.

## W7 — source ownership compatibility

Early `BatchStream.aclose()` closes the exact sync or async iterator supplied by
the caller so its `finally` cleanup runs. The supplied generator therefore
cannot be resumed after the stream takes ownership and closes early.

W7 compatibility documentation and characterization must state this ownership
transfer explicitly and compare it with the legacy facade before public wiring.
