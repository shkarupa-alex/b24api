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

## W7 — item-cursor exposure

The admitted W5 cursor contract has an independent `cursor_coercion`, but it
still requires continued-page cursor values to be unique and strictly monotonic
in the declared direction. A nonunique field such as a timestamp must not be
offered as a cursor unless W7 introduces a deterministic tie-breaker or
composite cursor profile and sends that expanded contract through review.

Under the admitted strict ordering, `cursor_take=min/max` are observational
aliases of `first/last`. W7 must either expose only the two distinct choices or
document the aliases explicitly; it must not imply four independent traversal
semantics.

`PROFILE_CURSOR_EXHAUSTED` treats a page whose cursor values are uniformly
missing or null as the delivered terminal page. Mixed present and exhausted
values are a typed pre-emission failure. W7 compatibility fixtures must preserve
that distinction.

## W7 — decoded-row budget wording

Batch arrays are charged by top-level element count, while an empty array has
weight zero. W7 documentation must not imply that an empty result consumes one
decoded row. The zero weight is bounded by the portal limit of 50 commands per
batch and does not relax command/outcome correlation or chunk ceilings.
