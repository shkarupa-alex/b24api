# Declared golden deltas

`baseline.json` was captured once on the unchanged base commit `c4cafdd` with
`uv run python -m tests.golden.capture --write` and is never recaptured. Every intentional change of a
projected field is one row below: `old` and `new` are exact JSON values (or `<absent>`), `field` is the
flattened projection path, and `evidence` names the test or live protocol that proves the new value.
`tests/golden_test.py` fails on any undeclared difference and on a row whose values no longer match.

| ID | fixture | field | old | new | reason | evidence |
|---|---|---|---|---|---|---|
| A12 | scenario:batch_early_close | streams.0.report.terminal_reason | "GeneratorExit" | "stream closed before exhaustion" | an early close is the kernel CANCELLED branch, not a failure named after `GeneratorExit` | tests/stream_lifecycle_test.py::test_early_close_is_reported_as_closed_before_exhaustion |
| A14 | scenario:batch_fail_fast | streams.0.report.terminal_reason | "_BatchWindowError" | "BatchCommandError" | the report names the public failure the private window carrier carries (`report_cause`) | tests/stream_lifecycle_test.py::test_fail_fast_batch_reports_the_command_failure_not_its_carrier |
| A14 | scenario:batch_fail_fast | streams.0.report.violations.0.1 | "internal_failure" | "batch_command_failure" | `classify_failure` follows `report_cause` to the failed command | tests/stream_lifecycle_test.py::test_fail_fast_batch_reports_the_command_failure_not_its_carrier |
