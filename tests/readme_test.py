"""Executable integrity checks for the v2 README surface."""

from __future__ import annotations
import ast
import re
from collections.abc import AsyncIterator, Iterable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Self, cast

import pytest

import b24api
from b24api import (
    Command,
    CommandSuccess,
    IdentitySpec,
    ReferenceComplete,
    ReferenceItem,
    ReplaySafety,
    Request,
    Response,
    ResultSelector,
)
from b24api.contracts import IdentityCoercion

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
TEST_REFERENCE = re.compile(r"<!-- tested: ([^:]+\.py)::([A-Za-z0-9_]+) -->")
CONSOLE_TEST_REFERENCE = re.compile(r"<!-- tested-console: ([^:]+\.py)::([A-Za-z0-9_]+) -->")
PYTHON_BLOCK = re.compile(r"```python\n(.*?)\n```", re.DOTALL)


class _ExampleStream[T](AsyncIterator[T]):
    """Small deterministic public-stream stand-in for exact documentation execution."""

    def __init__(self, values: Iterable[T]) -> None:
        self._values = iter(values)
        self.report: SimpleNamespace | None = None

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        try:
            return next(self._values)
        except StopIteration:
            self.report = SimpleNamespace(partial=False)
            raise StopAsyncIteration from None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self.report is None:
            self.report = SimpleNamespace(partial=True)

    async def first(self) -> SimpleNamespace:
        value: tuple[T, ...]
        try:
            value = (await anext(self),)
        except StopAsyncIteration:
            value = ()
        self.report = SimpleNamespace(partial=True)
        return SimpleNamespace(value=value, report=self.report)

    async def collect(self, *, limit: int) -> SimpleNamespace:
        values: list[T] = []
        while len(values) < limit:
            try:
                values.append(await anext(self))
            except StopAsyncIteration:
                break
        self.report = SimpleNamespace(partial=True)
        return SimpleNamespace(value=values, report=self.report)


class _ExampleClient:
    """Method-agnostic no-I/O facade sufficient to execute README snippets exactly."""

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def call(self, request: Request, **_kwargs: object) -> object:
        return request.copy_parameters()

    async def call_response(self, _request: Request, **_kwargs: object) -> Response:
        return Response({"ok": True})

    def batch(self, commands: Iterable[Command[object]], **_kwargs: object) -> _ExampleStream[CommandSuccess[object]]:
        return _ExampleStream(
            CommandSuccess(index, command.correlation, command.request.summary, Response({"ok": True}))
            for index, command in enumerate(commands)
        )

    def batch_outcomes(
        self,
        commands: Iterable[Command[object]],
        **_kwargs: object,
    ) -> _ExampleStream[CommandSuccess[object]]:
        return self.batch(commands)

    def iter_list(self, _request: Request, **_kwargs: object) -> _ExampleStream[object]:
        return _ExampleStream(({"ID": 1},))

    def iter_list_counted(self, request: Request, **kwargs: object) -> _ExampleStream[object]:
        return self.iter_list(request, **kwargs)

    def iter_list_keyset(self, request: Request, **kwargs: object) -> _ExampleStream[object]:
        return self.iter_list(request, **kwargs)

    def iter_list_cursor(self, request: Request, **kwargs: object) -> _ExampleStream[object]:
        return self.iter_list(request, **kwargs)

    def iter_references(
        self,
        _request: Request,
        bindings: Iterable[object],
        **_kwargs: object,
    ) -> _ExampleStream[object]:
        binding = next(iter(bindings))
        correlation = cast("Any", binding).correlation
        return _ExampleStream(
            (
                ReferenceItem(0, correlation, 0, {"ID": 1}),
                ReferenceComplete(0, correlation, 1),
            ),
        )


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
    assert "from b24api.traversal.plans" not in examples
    assert "raw=True" not in examples
    assert "with_payload=True" not in examples
    assert "errors=" not in examples


@pytest.mark.asyncio
async def test_every_readme_python_example_executes_exactly_without_io(monkeypatch: pytest.MonkeyPatch) -> None:
    """Execute exact snippets; named tests separately prove their production semantics."""
    client = _ExampleClient()
    monkeypatch.setattr(b24api, "Bitrix24", _ExampleClient)

    for source in PYTHON_BLOCK.findall(README.read_text(encoding="utf-8")):
        commands = (Command(Request("example.item.get", {"id": value}, ReplaySafety.SAFE), value) for value in (1, 2))
        namespace: dict[str, object] = {
            "client": client,
            "commands": commands,
            "consume": lambda *_args: None,
            "handle": lambda *_args: None,
            "identity": IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER),
            "parent_ids": (1, 2),
            "record_completion": lambda *_args: None,
            "request": Request("example.item.list", replay_safety=ReplaySafety.SAFE),
            "CommandSuccess": CommandSuccess,
            "IdentityCoercion": IdentityCoercion,
            "ReplaySafety": ReplaySafety,
            "Request": Request,
            "ResultSelector": ResultSelector,
            "source_ids": (1, 2),
        }
        code = compile(source, str(README), "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
        result = eval(code, namespace)  # noqa: S307 - exact trusted repository documentation source
        if result is not None:
            await cast("Any", result)
