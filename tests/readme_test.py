"""Executable integrity checks for the v2 README surface."""

from __future__ import annotations
import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
TEST_REFERENCE = re.compile(r"<!-- tested: ([^:]+\.py)::([A-Za-z0-9_]+) -->")
CONSOLE_TEST_REFERENCE = re.compile(r"<!-- tested-console: ([^:]+\.py)::([A-Za-z0-9_]+) -->")
PYTHON_BLOCK = re.compile(r"```python\n(.*?)\n```", re.DOTALL)


def test_every_readme_python_example_is_syntax_checked_and_names_an_executable_test() -> None:
    text = README.read_text(encoding="utf-8")
    blocks = PYTHON_BLOCK.findall(text)
    references = TEST_REFERENCE.findall(text)

    assert len(blocks) == len(references)
    for source in blocks:
        compile(source, str(README), "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
    for relative, test_name in references:
        test_text = (ROOT / relative).read_text(encoding="utf-8")
        assert re.search(rf"^(?:async )?def {re.escape(test_name)}\b", test_text, re.MULTILINE)

    for relative, test_name in CONSOLE_TEST_REFERENCE.findall(text):
        test_text = (ROOT / relative).read_text(encoding="utf-8")
        assert re.search(rf"^(?:async )?def {re.escape(test_name)}\b", test_text, re.MULTILINE)


def test_readme_contains_no_removed_import_or_shape_changing_call_examples() -> None:
    text = README.read_text(encoding="utf-8")
    examples = "\n".join(PYTHON_BLOCK.findall(text))

    assert "from b24api.models" not in examples
    assert "from b24api.plans" not in examples
    assert "raw=True" not in examples
    assert "with_payload=True" not in examples
    assert "errors=" not in examples
