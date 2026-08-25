"""Installed CLI boundary and exact JSON-to-contract routing."""

from __future__ import annotations
import io
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, NoReturn, Self

import pytest

from b24api import (
    CursorSpec,
    IdentitySpec,
    KeysetSpec,
    OffsetSpec,
    OperationReport,
    ParameterPath,
    ReplaySafety,
    Request,
    Response,
    ResultSelector,
    TerminalState,
    TraversalAssurance,
    cli,
)
from b24api.contracts import IdentityCoercion
from b24api.errors import CapabilityError, IncompleteTraversalError, ProtocolError

if TYPE_CHECKING:
    from pathlib import Path

_USAGE = 2
_UNAVAILABLE = 3
_CORRECTNESS = 4
_OUTPUT_CLOSED = 5
_INTERRUPTED = 130


@dataclass
class _Call:
    name: str
    request: Request
    kwargs: dict[str, object]


class _Stream(AsyncIterator[object]):
    def __init__(self, items: list[object], *, assurance: TraversalAssurance | None = None) -> None:
        self._items = iter(items)
        self._assurance = assurance
        self._report: OperationReport | None = None
        self.close_calls = 0

    @property
    def report(self) -> OperationReport | None:
        return self._report

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> object:
        try:
            return next(self._items)
        except StopIteration:
            self._report = OperationReport(
                TerminalState.COMPLETED,
                "list",
                "empty_confirmation",
                assurance=self._assurance,
            )
            raise StopAsyncIteration from None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._report is None:
            await self.aclose()

    async def aclose(self) -> None:
        self.close_calls += 1
        if self._report is None:
            self._report = OperationReport(TerminalState.EARLY_CLOSED, "list", "caller_closed")


class _Client:
    def __init__(self, *, rows: list[object] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[_Call] = []
        self.streams: list[_Stream] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def call(self, request: Request) -> object:
        self.calls.append(_Call("call", request, {}))
        return {"ok": True}

    async def call_response(self, request: Request) -> Response:
        self.calls.append(_Call("call_response", request, {}))
        return Response({"ok": True}, total=1, next=2)

    def _listing(self, name: str, request: Request, kwargs: dict[str, object]) -> _Stream:
        self.calls.append(_Call(name, request, kwargs))
        assurance = (
            TraversalAssurance.MECHANICS_ONLY
            if name == "iter_list" and kwargs.get("identity") is None
            else TraversalAssurance.IDENTITY_EXACT
        )
        stream = _Stream(self.rows, assurance=assurance)
        self.streams.append(stream)
        return stream

    def iter_list(self, request: Request, **kwargs: object) -> _Stream:
        return self._listing("iter_list", request, kwargs)

    def iter_list_counted(self, request: Request, **kwargs: object) -> _Stream:
        return self._listing("iter_list_counted", request, kwargs)

    def iter_list_keyset(self, request: Request, **kwargs: object) -> _Stream:
        return self._listing("iter_list_keyset", request, kwargs)

    def iter_list_cursor(self, request: Request, **kwargs: object) -> _Stream:
        return self._listing("iter_list_cursor", request, kwargs)


class _BrokenOutput(io.StringIO):
    def write(self, _value: str) -> int:
        raise BrokenPipeError


class _IncompleteStream(_Stream):
    async def __anext__(self) -> object:
        self._report = OperationReport(TerminalState.INCOMPLETE, "list", "contradictory_page")
        raise IncompleteTraversalError(report=self._report)


def _run(monkeypatch: pytest.MonkeyPatch, client: _Client, args: list[str], *, stdin: str = "") -> tuple[int, str, str]:
    monkeypatch.setattr(cli, "Bitrix24", lambda: client)
    stdout = io.StringIO()
    stderr = io.StringIO()
    code = cli.main(args, stdin=io.StringIO(stdin), stdout=stdout, stderr=stderr)
    return code, stdout.getvalue(), stderr.getvalue()


def _contract(tmp_path: Path, value: dict[str, object]) -> str:
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return f"@{path}"


@pytest.mark.parametrize("safety", list(ReplaySafety))
def test_call_routes_replay_safety_and_keeps_success_data_on_stdout(
    monkeypatch: pytest.MonkeyPatch,
    safety: ReplaySafety,
) -> None:
    client = _Client()

    code, stdout, stderr = _run(
        monkeypatch,
        client,
        ["call", "test.get", "--params", '{"select":["ID"]}', "--replay-safety", safety.value],
    )

    assert code == 0
    assert json.loads(stdout) == {"ok": True}
    assert stderr == ""
    request = client.calls[0].request
    assert request.replay_safety is safety
    assert request.copy_parameters() == {"select": ["ID"]}


def test_raw_call_emits_one_full_response_object(monkeypatch: pytest.MonkeyPatch) -> None:
    code, stdout, stderr = _run(monkeypatch, _Client(), ["call", "test.get", "--raw"])

    assert code == 0
    assert json.loads(stdout) == {
        "result": {"ok": True},
        "total": 1,
        "next": 2,
        "time": None,
        "evidence": {
            "http_status": None,
            "request_id": None,
            "headers": {},
            "body_preview": None,
        },
    }
    assert stderr == ""


def test_success_stdout_preserves_token_like_application_data(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"access_token": "n1x2y3z4q5w6e7r8", "ok": True}

    class PayloadClient(_Client):
        async def call(self, request: Request) -> object:
            self.calls.append(_Call("call", request, {}))
            return payload

    code, stdout, stderr = _run(monkeypatch, PayloadClient(), ["call", "test.get"])

    assert code == 0
    assert json.loads(stdout) == payload
    assert stderr == ""


@pytest.mark.parametrize(
    ("params", "stdin"),
    [('{"ID":1}', ""), ("-", '{"ID":1}')],
)
def test_parameter_inline_and_stdin_sources(
    monkeypatch: pytest.MonkeyPatch,
    params: str,
    stdin: str,
) -> None:
    client = _Client()
    code, _, _ = _run(monkeypatch, client, ["call", "test.get", "--params", params], stdin=stdin)
    assert code == 0
    assert client.calls[0].request.copy_parameters() == {"ID": 1}


def test_parameter_file_source(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "params.json"
    path.write_text('{"ID":1}', encoding="utf-8")
    client = _Client()
    code, _, _ = _run(monkeypatch, client, ["call", "test.get", "--params", f"@{path}"])
    assert code == 0
    assert client.calls[0].request.copy_parameters() == {"ID": 1}


@pytest.mark.parametrize(
    "value",
    ['{"x":1,"x":2}', '{"x":1} {}', '{"x":NaN}', '{"x":Infinity}', "[]", "null", "1"],
)
def test_parameters_reject_duplicate_trailing_and_non_object_json(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    code, stdout, stderr = _run(monkeypatch, _Client(), ["call", "test.get", "--params", value])
    assert code == _USAGE
    assert stdout == ""
    assert json.loads(stderr)["kind"] == "usage_error"


@pytest.mark.parametrize("command", ["call", "list"])
def test_invalid_method_is_a_local_usage_error_before_client_entry(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    entered = False

    class UnusedClient(_Client):
        async def __aenter__(self) -> Self:
            nonlocal entered
            entered = True
            return self

    code, stdout, stderr = _run(monkeypatch, UnusedClient(), [command, "bad/method"])

    assert code == _USAGE
    assert stdout == ""
    assert json.loads(stderr)["kind"] == "usage_error"
    assert entered is False


def test_sequential_default_is_mechanics_only_and_reports_once(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _Client(rows=[{"ID": 1}, {"ID": 2}])
    code, stdout, stderr = _run(monkeypatch, client, ["list", "test.list"])

    assert code == 0
    assert [json.loads(line) for line in stdout.splitlines()] == [{"ID": 1}, {"ID": 2}]
    diagnostics = [json.loads(line) for line in stderr.splitlines()]
    assert [item["kind"] for item in diagnostics] == ["warning", "report"]
    assert diagnostics[0]["code"] == "mechanics_only"
    assert diagnostics[1]["assurance"] == "mechanics_only"
    assert stderr.count('"kind":"report"') == 1
    assert client.calls[0].request.replay_safety is ReplaySafety.UNKNOWN
    assert client.calls[0].kwargs == {
        "selector": ResultSelector.root(),
        "identity": None,
        "page_size": 50,
        "offset": OffsetSpec(),
    }


@pytest.mark.parametrize(
    ("strategy", "contract_value", "method_name", "expected"),
    [
        (
            "sequential",
            {
                "version": 1,
                "selector": ["items"],
                "page_size": 25,
                "identity": {
                    "item_path": ["ID"],
                    "filter_key": "ID",
                    "order_key": "ID",
                    "coercion": "decimal_string_integer",
                },
                "offset": {
                    "parameter_path": ["start"],
                    "limit_path": ["LIMIT"],
                    "allow_create_controls": False,
                },
            },
            "iter_list",
            OffsetSpec(
                ParameterPath(("start",)),
                ParameterPath(("LIMIT",)),
                allow_create_controls=False,
            ),
        ),
        (
            "counted",
            {
                "version": 1,
                "selector": ["items"],
                "identity": {
                    "item_path": ["ID"],
                    "filter_key": "ID",
                    "order_key": "ID",
                    "coercion": "exact_integer",
                },
                "offset": {"parameter_path": ["start"]},
            },
            "iter_list_counted",
            OffsetSpec(),
        ),
        (
            "keyset",
            {
                "version": 1,
                "selector": ["items"],
                "identity": {
                    "item_path": ["ID"],
                    "filter_key": "ID",
                    "order_key": "ID",
                    "coercion": "exact_integer",
                },
                "keyset": {
                    "filter_path": ["filter"],
                    "order_path": ["order"],
                    "start_suppression_path": ["start"],
                    "limit_path": ["LIMIT"],
                    "direction": "descending",
                    "allow_create_controls": False,
                },
            },
            "iter_list_keyset",
            KeysetSpec(
                ParameterPath(("filter",)),
                ParameterPath(("order",)),
                ParameterPath(("start",)),
                ParameterPath(("LIMIT",)),
                "descending",
                allow_create_controls=False,
            ),
        ),
        (
            "cursor",
            {
                "version": 1,
                "selector": ["items"],
                "cursor": {
                    "parameter_path": ["LAST_ID"],
                    "item_path": ["ID"],
                    "coercion": "exact_string",
                    "direction": "ascending",
                    "take": "first",
                    "limit_path": ["LIMIT"],
                    "allow_create_controls": False,
                },
            },
            "iter_list_cursor",
            CursorSpec(
                ParameterPath(("LAST_ID",)),
                ("ID",),
                IdentityCoercion.EXACT_STRING,
                "ascending",
                "first",
                ParameterPath(("LIMIT",)),
                allow_create_controls=False,
            ),
        ),
    ],
)
def test_all_list_contract_fields_route_one_to_one_to_public_mechanics(  # noqa: PLR0913
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    strategy: str,
    contract_value: dict[str, object],
    method_name: str,
    expected: object,
) -> None:
    client = _Client()
    code, _, stderr = _run(
        monkeypatch,
        client,
        ["list", "test.list", "--strategy", strategy, "--contract", _contract(tmp_path, contract_value)],
    )

    assert code == 0
    assert client.calls[0].name == method_name
    key = {"sequential": "offset", "counted": "offset", "keyset": "keyset", "cursor": "cursor"}[strategy]
    assert client.calls[0].kwargs[key] == expected
    assert stderr.count('"kind":"report"') == 1
    if "identity" in contract_value:
        assert isinstance(client.calls[0].kwargs["identity"], IdentitySpec)


@pytest.mark.parametrize(
    "contract_value",
    [
        {"version": 1, "limit_path": ["LIMIT"]},
        {"version": 1, "allow_create_controls": True},
        {"version": 1, "keyset": {"terminal": "empty_confirmation"}},
        {"version": 1, "unknown": 1},
    ],
)
def test_contract_rejects_orphan_and_unknown_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    contract_value: dict[str, object],
) -> None:
    code, stdout, stderr = _run(
        monkeypatch,
        _Client(),
        ["list", "test.list", "--contract", _contract(tmp_path, contract_value)],
    )
    assert code == _USAGE
    assert stdout == ""
    assert json.loads(stderr)["kind"] == "usage_error"


def test_invalid_contract_is_rejected_before_client_construction(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    constructed = False

    def unused_client() -> _Client:
        nonlocal constructed
        constructed = True
        return _Client()

    monkeypatch.setattr(cli, "Bitrix24", unused_client)
    stderr = io.StringIO()
    code = cli.main(
        ["list", "test.list", "--contract", _contract(tmp_path, {"version": 1, "unknown": True})],
        stdin=io.StringIO(),
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert code == _USAGE
    assert json.loads(stderr.getvalue())["kind"] == "usage_error"
    assert constructed is False


def test_advanced_strategy_requires_contract_file(monkeypatch: pytest.MonkeyPatch) -> None:
    code, stdout, stderr = _run(monkeypatch, _Client(), ["list", "test.list", "--strategy", "counted"])
    assert code == _USAGE
    assert stdout == ""
    assert "requires --contract" in stderr


def test_control_collision_fails_at_public_cli_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    class CollisionClient(_Client):
        def iter_list(self, request: Request, **kwargs: object) -> _Stream:
            del request, kwargs
            raise CapabilityError("request parameter path collides with traversal controls")

    code, stdout, stderr = _run(monkeypatch, CollisionClient(), ["list", "test.list"])
    assert code == _CORRECTNESS
    assert stdout == ""
    assert json.loads(stderr)["type"] == "CapabilityError"


def test_incomplete_traversal_emits_its_report_once_before_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class IncompleteClient(_Client):
        def iter_list(self, request: Request, **kwargs: object) -> _Stream:
            del request, kwargs
            stream = _IncompleteStream([])
            self.streams.append(stream)
            return stream

    code, stdout, stderr = _run(monkeypatch, IncompleteClient(), ["list", "test.list"])
    assert code == _CORRECTNESS
    assert stdout == ""
    diagnostics = [json.loads(line) for line in stderr.splitlines()]
    assert [item["kind"] for item in diagnostics] == ["report", "error"]
    assert diagnostics[0]["state"] == "incomplete"
    assert diagnostics[1]["type"] == "IncompleteTraversalError"
    assert stderr.count('"kind":"report"') == 1


def test_protocol_failure_is_safely_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    credential = "n1x2y3z4q5w6e7r8"

    class FailingClient(_Client):
        async def call(self, _request: Request) -> object:
            raise ProtocolError(f"https://portal.invalid/rest/1/{credential}/")

    code, stdout, stderr = _run(monkeypatch, FailingClient(), ["call", "test.get"])
    assert code == _CORRECTNESS
    assert stdout == ""
    diagnostic = json.loads(stderr)
    assert diagnostic["type"] == "ProtocolError"
    assert credential not in stderr
    assert "/rest/1/" not in stderr


def test_configuration_failure_maps_to_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    configuration_error = type("ValidationError", (Exception,), {"__module__": "pydantic_core"})

    def fail() -> NoReturn:
        raise configuration_error("contains a credential")

    monkeypatch.setattr(cli, "Bitrix24", fail)
    stderr = io.StringIO()
    code = cli.main(["call", "test.get"], stdin=io.StringIO(), stdout=io.StringIO(), stderr=stderr)
    assert code == _UNAVAILABLE
    assert json.loads(stderr.getvalue()) == {"kind": "unavailable", "message": "configuration unavailable"}


def test_broken_pipe_closes_stream_and_uses_exit_five(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _Client(rows=[{"ID": 1}])
    monkeypatch.setattr(cli, "Bitrix24", lambda: client)
    stderr = io.StringIO()

    code = cli.main(
        ["list", "test.list"],
        stdin=io.StringIO(),
        stdout=_BrokenOutput(),
        stderr=stderr,
    )

    assert code == _OUTPUT_CLOSED
    assert client.streams[0].close_calls == 1
    diagnostics = stderr.getvalue()
    assert diagnostics.count('"kind":"report"') == 1
    assert json.loads(diagnostics.splitlines()[-1])["state"] == "early_closed"


def test_keyboard_interrupt_maps_to_130(monkeypatch: pytest.MonkeyPatch) -> None:
    class InterruptedClient(_Client):
        async def __aenter__(self) -> Self:
            raise KeyboardInterrupt

    code, stdout, stderr = _run(monkeypatch, InterruptedClient(), ["call", "test.get"])
    assert code == _INTERRUPTED
    assert stdout == stderr == ""
