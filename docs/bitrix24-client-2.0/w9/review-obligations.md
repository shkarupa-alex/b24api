# Mandatory W9 and live-evidence review obligations

W0 v3 was approved for transition to W1/W2 with the following findings deferred
to the W9 evidence boundary. None of these is optional, and no affected live
write or benchmark admission may occur before its regression passes.

## Dataset authorization

- Pin the reviewed disposable profile set by both exact `profile_set_id` and an
  immutable reviewed SHA-256 constant. The validator must fail when the file is
  edited until a new human-reviewed digest is committed.
- Replace the plan-self-declared scale ceiling with a reviewed machine constant
  or content-addressed authorization profile containing a hard maximum. A plan
  may request less, never raise the reviewed maximum itself.
- For non-empty seeding, require conservative positive estimates for physical
  requests, batch commands where applicable, duration, and quota impact. Validate
  their relationship to entity counts and the chosen create/delete strategy.

## Manifest and oracle

- Define the marker hash algorithm and require
  `marker_hash == sha256(marker_value.encode("utf-8"))`.
- Formally derive or bind the marker to the record correlation key so recovery
  cannot reconcile two different ownership identities.
- A PASS with `frozen_manifest` requires non-null equal pre/post hashes, just as
  an independent pre/post oracle does.

## Evidence admission

- Every PASS, including `seed`, `cleanup`, and recovery operations, rejects
  blocking violations. Cleanup evidence additionally records typed
  `orphan_count` and `absence_verified`; PASS requires zero verified orphans and
  confirmed absence.
- Add `benchmark_plan_content_hash` to benchmark evidence. Record the applicable
  maximum RTT and operating ratios, recompute observed ratios from finite
  before/after measurements, and derive `drift_quarantined` rather than trusting
  a caller-provided boolean.
- Require `duplicates == raw_rows - unique_rows` and add any other algebraic
  metric identities used by correctness gates.
- State explicitly that draft benefit thresholds are non-normative, or constrain
  them to the normative domain if they can flow into later admission.

## Probe before reuse

- Require the `result_error` key to be present. Record its actual wire shape as
  `empty_array` or `associative_object`; absence, a non-empty array, and every
  other type are malformed and block PASS.
- Require `BITRIX24_EVIDENCE_FINGERPRINT_KEY` to encode at least 32 random bytes
  in one specified format. Record the format/strength assertion without the key,
  and reject weak keys before network I/O.

These obligations supplement, rather than replace, the W9 bundle-level lineage,
canonical redaction, repository/artifact leak scan, and human write-approval
gates already required by the specification.
