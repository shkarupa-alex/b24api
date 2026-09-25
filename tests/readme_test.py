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
    Bitrix24,
    ExecutionPolicy,
    IdentitySpec,
    ReplaySafety,
    Request,
    ResultSelector,
    Settings,
    TerminalState,
)
from b24api.contracts import Command, CommandSuccess, IdentityCoercion, ReferenceComplete, ReferenceItem, Response
from b24api.contracts.request import RouteKind
from b24api.testing import ScriptedExchange, ScriptedTransport
from tests.real_signature import real_signature

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
RECIPES = ROOT / "docs" / "recipes.md"
TEST_REFERENCE = re.compile(r"<!-- tested: ([^:]+\.py)::([A-Za-z0-9_]+) -->")
CONSOLE_TEST_REFERENCE = re.compile(r"<!-- tested-console: ([^:]+\.py)::([A-Za-z0-9_]+) -->")
PYTHON_BLOCK = re.compile(r"```python\n(.*?)\n```", re.DOTALL)
PORTAL = "portal.example.invalid"
PORTAL_WEBHOOK = f"https://{PORTAL}/rest/1/token/"


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
            self.report = SimpleNamespace(partial=False, state=TerminalState.COMPLETED)
            raise StopAsyncIteration from None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self.report is None:
            self.report = SimpleNamespace(partial=True, state=TerminalState.EARLY_CLOSED)

    async def first(self) -> SimpleNamespace:
        value: tuple[T, ...]
        try:
            value = (await anext(self),)
        except StopAsyncIteration:
            value = ()
        self.report = SimpleNamespace(partial=True, state=TerminalState.EARLY_CLOSED)
        return SimpleNamespace(value=value, report=self.report)

    async def collect(self, *, limit: int) -> SimpleNamespace:
        values: list[T] = []
        while len(values) < limit:
            try:
                values.append(await anext(self))
            except StopAsyncIteration:
                break
        self.report = SimpleNamespace(partial=True, state=TerminalState.EARLY_CLOSED)
        return SimpleNamespace(value=values, report=self.report)


class _ExampleClient:
    """No-I/O facade for README snippets; every call is bound against the real ``Bitrix24`` method."""

    @real_signature
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        return None

    @classmethod
    @real_signature
    def from_webhook(cls, *_args: object, **_kwargs: object) -> Self:
        return cls()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    @real_signature
    async def call(self, request: Request, **_kwargs: object) -> object:
        return request.copy_parameters()

    @real_signature
    async def call_response(self, _request: Request, **_kwargs: object) -> Response:
        return Response({"ok": True})

    @real_signature
    async def call_bytes(self, _request: Request, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(body=b"", content_type="application/octet-stream")

    @real_signature
    def batch(self, commands: Iterable[Command[object]], **_kwargs: object) -> _ExampleStream[CommandSuccess[object]]:
        return self._successes(commands)

    @staticmethod
    def _successes(commands: Iterable[Command[object]]) -> _ExampleStream[CommandSuccess[object]]:
        return _ExampleStream(
            CommandSuccess(index, command.correlation, command.request.summary, Response({"ok": True}))
            for index, command in enumerate(commands)
        )

    @real_signature
    def batch_outcomes(
        self,
        commands: Iterable[Command[object]],
        **_kwargs: object,
    ) -> _ExampleStream[CommandSuccess[object]]:
        return self._successes(commands)

    @real_signature
    def iter_list(self, _request: Request, **_kwargs: object) -> _ExampleStream[object]:
        return self._rows()

    @staticmethod
    def _rows() -> _ExampleStream[object]:
        return _ExampleStream(({"ID": 1, "TITLE": "Example"},))

    @real_signature
    def iter_list_counted(self, _request: Request, **_kwargs: object) -> _ExampleStream[object]:
        return self._rows()

    @real_signature
    def iter_list_keyset(self, _request: Request, **_kwargs: object) -> _ExampleStream[object]:
        return self._rows()

    @real_signature
    def iter_list_cursor(self, _request: Request, **_kwargs: object) -> _ExampleStream[object]:
        return self._rows()

    @real_signature
    def iter_cursors(
        self,
        _request: Request,
        bindings: Iterable[object],
        **_kwargs: object,
    ) -> _ExampleStream[object]:
        return self._references(bindings)

    @real_signature
    async def verify_keyset_capability(self, _request: Request, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(verdict="verified")

    @real_signature
    def iter_references(
        self,
        _request: Request,
        bindings: Iterable[object],
        **_kwargs: object,
    ) -> _ExampleStream[object]:
        return self._references(bindings)

    @staticmethod
    def _references(bindings: Iterable[object]) -> _ExampleStream[object]:
        binding = next(iter(bindings))
        correlation = cast("Any", binding).correlation
        return _ExampleStream(
            (
                ReferenceItem(0, correlation, 0, {"ID": 1}),
                ReferenceComplete(0, correlation, 1),
            ),
        )


def test_example_client_rejects_calls_the_real_client_would_reject() -> None:
    client = _ExampleClient()
    request = Request("example.item.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)

    with pytest.raises(TypeError, match="page_sise"):
        client.iter_list(request, page_sise=10)
    with pytest.raises(TypeError, match="missing a required argument"):
        client.iter_references(request)  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="too many positional arguments"):
        client.iter_list_counted(request, 1)  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="unexpected keyword"):
        _ExampleClient(Settings(webhook_url="https://example.invalid/rest/1/doc/"), transprot=None)


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
    monkeypatch.setenv("BITRIX24_API_WEBHOOK_URL", PORTAL_WEBHOOK)

    for source in PYTHON_BLOCK.findall(README.read_text(encoding="utf-8")):
        commands = (
            Command(Request("example.item.get", {"id": value}, ReplaySafety.SAFE, route=RouteKind.BARE), value)
            for value in (1, 2)
        )
        namespace: dict[str, object] = {
            "client": client,
            "chat_ids": ("chat-1", "chat-2"),
            "commands": commands,
            "consume": lambda *_args: None,
            "handle": lambda *_args: None,
            "identity": IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER),
            "parent_ids": (1, 2),
            "record_completion": lambda *_args: None,
            "request": Request("example.item.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
            "settings": Settings(webhook_url="https://example.invalid/rest/1/doc/"),
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


@pytest.mark.asyncio
async def test_every_recipe_python_example_executes_exactly_without_io() -> None:
    """Keep copyable endpoint recipes executable as public contracts evolve."""
    client = _ExampleClient()
    for source in PYTHON_BLOCK.findall(RECIPES.read_text(encoding="utf-8")):
        namespace: dict[str, object] = {
            "client": client,
            "checkpoints": {1: 0, 2: 10},
            "cursor": object(),
            "identity": IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER),
            "keyset": object(),
            "parent_ids": (1, 2),
            "request": Request("example.item.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE),
            "write_file": lambda *_args, **_kwargs: None,
        }
        code = compile(source, str(RECIPES), "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
        result = eval(code, namespace)  # noqa: S307 - exact trusted repository documentation source
        if result is not None:
            await cast("Any", result)


def _quickstart_blocks() -> list[str]:
    text = README.read_text(encoding="utf-8")
    return PYTHON_BLOCK.findall(text[text.index("## Quickstart") : text.index("## Direct calls")])


@pytest.mark.asyncio
async def test_quickstart_runs_exactly_against_a_scripted_portal(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Run both quickstart snippets on the real client; only the portal behind the webhook is scripted."""
    listing, single_call = _quickstart_blocks()
    deals = {"select": ["ID", "TITLE"]}
    transport = ScriptedTransport(
        (
            ScriptedExchange.json(
                Request.bare("crm.deal.list", {**deals, "start": 0}),
                {"result": [{"ID": "1", "TITLE": "Kitchen"}, {"ID": "2", "TITLE": "Office"}], "total": 2},
            ),
            ScriptedExchange.json(Request.bare("crm.deal.list", {**deals, "start": 2}), {"result": [], "total": 2}),
            ScriptedExchange.json(Request.bare("user.get", {"ID": 1}), {"result": [{"ID": "1", "NAME": "Ada"}]}),
        ),
        host=PORTAL,
    )
    webhooks: list[str] = []

    def from_webhook(
        cls: type[Bitrix24], url: str, *, http_timeout: float | None = None, policy: ExecutionPolicy | None = None
    ) -> Bitrix24:
        del http_timeout
        webhooks.append(url)
        return cls(Settings(webhook_url=url), transport=transport, policy=policy)

    monkeypatch.setattr(Bitrix24, "from_webhook", classmethod(real_signature(from_webhook)))
    monkeypatch.setenv("BITRIX24_API_WEBHOOK_URL", PORTAL_WEBHOOK)
    namespace: dict[str, object] = {}
    result = eval(compile(listing, str(README), "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT), namespace)  # noqa: S307 - exact trusted repository documentation source
    await cast("Any", result)

    assert webhooks == [PORTAL_WEBHOOK]
    assert capsys.readouterr().out.splitlines() == ["1 Kitchen", "2 Office", "completed"]
    async with Bitrix24(Settings(webhook_url=PORTAL_WEBHOOK), transport=transport) as client:
        namespace["client"] = client
        result = eval(compile(single_call, str(README), "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT), namespace)  # noqa: S307 - exact trusted repository documentation source
        await cast("Any", result)
    assert namespace["users"] == [{"ID": "1", "NAME": "Ada"}]
    transport.assert_exhausted()


def test_quickstart_listing_needs_four_concepts_client_request_iter_list_and_report() -> None:
    tree = ast.parse(_quickstart_blocks()[0])
    imports = [node for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module == "b24api"]
    imported = {alias.name for node in imports for alias in node.names}
    attributes = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)} - {"environ"}

    assert imported == {"Bitrix24", "Request"}
    assert attributes == {"from_webhook", "bare", "iter_list", "report", "state"}
