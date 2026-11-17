"""The CLI's process boundary: its settings source, its input and output streams, its signals and ``counted``."""

from __future__ import annotations
import io
import json
import signal
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from b24api import Bitrix24, Settings, cli
from tests.scripting import BatchPortal, ResponderTransport, client_for

if TYPE_CHECKING:
    from tests.scripting import Envelope, Query

_USAGE = 2
_OUTPUT_CLOSED = 5
_ROOT = Path(__file__).resolve().parents[1]
_ROWS = 120
_COUNTED_CONTRACT = {
    "version": 1,
    "identity": {"item_path": ["ID"], "filter_key": "ID", "order_key": "ID", "coercion": "exact_integer"},
}


def _main(
    args: list[str], *, stdin: io.TextIOBase | None = None, stdout: io.TextIOBase | None = None
) -> tuple[int, str]:
    stderr = io.StringIO()
    code = cli.main(
        args,
        stdin=stdin or io.StringIO(),  # type: ignore[arg-type]
        stdout=stdout or io.StringIO(),  # type: ignore[arg-type]
        stderr=stderr,
    )
    return code, stderr.getvalue()


def _counted_rows(_method: str, query: Query) -> Envelope:
    offset = int(query.get("start", "0"))
    rows = [{"ID": row} for row in range(offset, min(offset + 50, _ROWS))]
    return {"result": rows, "total": _ROWS, **({"next": offset + 50} if offset + 50 < _ROWS else {})}


def test_list_strategy_counted_runs_a_direct_head_and_a_batched_tail(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    portal = BatchPortal(_counted_rows)
    monkeypatch.setattr(cli, "_open_client", lambda: client_for(portal))
    contract = tmp_path / "counted.json"
    contract.write_text(json.dumps(_COUNTED_CONTRACT))
    stdout = io.StringIO()

    code, stderr = _main(
        ["list", "crm.deal.list", "--route", "bare", "--strategy", "counted", "--contract", f"@{contract}"],
        stdout=stdout,
    )

    assert code == 0, stderr
    assert [json.loads(line)["ID"] for line in stdout.getvalue().splitlines()] == list(range(_ROWS))
    assert portal.waves == [1, 2]  # a direct head, then one batch of the two remaining pages
    assert json.loads(stderr.splitlines()[-1])["state"] == "completed"


def test_the_cli_reads_a_dotenv_file_in_its_working_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    credential = "n1x2y3z4q5w6e7r8"
    (tmp_path / ".env").write_text(
        f"BITRIX24_API_WEBHOOK_URL=https://dotenv.invalid/rest/1/{credential}/\nBITRIX24_API_HTTP_TIMEOUT=7\n"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BITRIX24_API_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("BITRIX24_API_HTTP_TIMEOUT", raising=False)
    opened: list[Settings] = []

    def open_client(settings: Settings) -> Bitrix24:
        opened.append(settings)
        return client_for(ResponderTransport(lambda _request: {"result": 1}))

    monkeypatch.setattr(cli, "Bitrix24", open_client)

    code, stderr = _main(["call", "user.current", "--route", "bare"])

    assert code == 0, stderr
    assert [(settings.webhook_url.host, settings.http_timeout) for settings in opened] == [("dotenv.invalid", 7.0)]


@pytest.mark.parametrize("closed", ["stdout", "stderr"])
def test_a_standard_stream_closed_at_startup_is_a_gone_reader(monkeypatch: pytest.MonkeyPatch, closed: str) -> None:
    # Python leaves sys.stdout or sys.stderr unset when its descriptor was closed before the process started.
    monkeypatch.setattr(sys, closed, None)
    monkeypatch.setattr(cli, "_open_client", lambda: client_for(ResponderTransport(lambda _request: {"result": 1})))

    # A call writes its result to stdout; a malformed parameter is reported on stderr.
    args = ["call", "test.get", "--route", "bare"] + ([] if closed == "stdout" else ["--params", "[]"])
    code = cli.main(args, stdin=io.StringIO())

    assert code == _OUTPUT_CLOSED


def test_a_result_reaches_a_stdout_that_cannot_encode_it_as_escaped_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli, "_open_client", lambda: client_for(ResponderTransport(lambda _request: {"result": {"NAME": "привет 😀"}}))
    )
    raw = io.BytesIO()
    stdout = io.TextIOWrapper(raw, encoding="ascii", write_through=True)

    code, stderr = _main(["call", "test.get", "--route", "bare"], stdout=stdout)

    assert code == 0, stderr
    assert json.loads(raw.getvalue().decode("ascii")) == {"NAME": "привет 😀"}


@pytest.mark.parametrize(
    ("params", "stdin"),
    [
        ("-", io.TextIOWrapper(io.BytesIO(b'{"a": "\xff"}'), encoding="utf-8")),
        ('{"a": ' + "9" * 5000 + "}", None),
        ('{"a": ' * 100_000 + "1" + "}" * 100_000, None),
        ("@", None),
    ],
    ids=["non-utf8-stdin", "integer-past-digit-limit", "nesting-past-recursion", "empty-file-name"],
)
def test_malformed_local_input_is_a_usage_error(params: str, stdin: io.TextIOBase | None) -> None:
    code, stderr = _main(["call", "test.get", "--route", "bare", "--params", params], stdin=stdin)

    assert code == _USAGE
    assert json.loads(stderr)["kind"] == "usage_error"


def test_a_parameter_file_that_is_not_utf8_is_a_usage_error(tmp_path: Path) -> None:
    path = tmp_path / "latin1.json"
    path.write_bytes(b'{"a":"\xe9"}')

    code, stderr = _main(["call", "test.get", "--route", "bare", "--params", f"@{path}"])

    assert code == _USAGE
    assert json.loads(stderr) == {"kind": "usage_error", "message": "cannot read parameters file as UTF-8"}


def test_a_contract_is_read_from_stdin_as_the_help_says(monkeypatch: pytest.MonkeyPatch) -> None:
    portal = BatchPortal(_counted_rows)
    monkeypatch.setattr(cli, "_open_client", lambda: client_for(portal))
    contract = json.dumps(_COUNTED_CONTRACT)
    args = ["list", "crm.deal.list", "--route", "bare", "--strategy", "counted", "--contract", "-"]

    code, _ = _main(args, stdin=io.StringIO(contract))
    both, stderr = _main([*args, "--params", "-"], stdin=io.StringIO(contract))

    assert code == 0
    assert both == _USAGE
    assert "cannot both read stdin" in stderr


_SIGNAL_CHILD = """
import asyncio, sys
from b24api import Bitrix24, Settings, cli
from tests.scripting import ResponderTransport, client_for

async def respond(request):
    if request.copy_parameters().get("start", 0):
        await asyncio.Event().wait()  # the second page never comes
    return {"result": [{"ID": 1}], "next": 1}

cli._open_client = lambda: client_for(ResponderTransport(respond))
sys.exit(cli.main())
"""


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGHUP], ids=["SIGTERM", "SIGHUP"])
def test_a_termination_signal_ends_a_list_with_its_report_and_the_signal_exit_code(signum: int) -> None:
    pipe = subprocess.PIPE
    with subprocess.Popen(  # noqa: S603 - fixed interpreter and in-repository child script
        [sys.executable, "-c", _SIGNAL_CHILD, "list", "test.list", "--route", "bare"],
        cwd=_ROOT,
        env={"PYTHONUNBUFFERED": "1"},
        stdin=subprocess.DEVNULL,
        stdout=pipe,
        stderr=pipe,
    ) as child:
        assert child.stdout is not None
        assert json.loads(child.stdout.readline()) == {"ID": 1}
        child.send_signal(signum)
        _, stderr = child.communicate(timeout=30)

    assert child.returncode == 128 + signum, stderr
    assert json.loads(stderr.splitlines()[-1])["kind"] == "report"
