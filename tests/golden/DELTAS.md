# Declared golden deltas

`baseline.json` was captured once on the unchanged base commit `c4cafdd` with
`uv run python -m tests.golden.capture --write` and is never recaptured. Every intentional change of a
projected field is one row below: `old` and `new` are exact JSON values (or `<absent>`), `field` is the
flattened projection path, and `evidence` names the test or live protocol that proves the new value.
`tests/golden_test.py` fails on any undeclared difference and on a row whose values no longer match.

| ID | fixture | field | old | new | reason | evidence |
|---|---|---|---|---|---|---|
