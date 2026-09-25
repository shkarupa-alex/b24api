# Local QC runs every blocking CI check except the internal benches (pytest marker `slow`: harness
# contracts and 50k/100k-scale runs, several minutes). Those run in `make bench` and in the blocking
# CI job `slow`; react to them when that job fails rather than running them on every change.

.PHONY: qc lint types test bench

qc: lint types test

lint:
	uv run --locked ruff check --no-fix
	uv run --locked ruff format --check

types:
	uv run --locked mypy b24api
	uv run --locked python .github/scripts/mypy_ratchet.py

# The default addopts in pyproject.toml exclude the `slow` and `benchmark` markers.
test:
	uv run --locked pytest

bench:
	uv run --locked pytest -m slow
