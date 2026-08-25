"""Small JSONL console boundary over the public b24api v2 contracts."""

from __future__ import annotations
import argparse
import asyncio
import dataclasses
import json
import sys
from typing import TYPE_CHECKING, NoReturn, TextIO

from b24api import (
    B24ApiError,
    Bitrix24,
    ReplaySafety,
    Request,
    Response,
    TerminalState,
    TraversalAssurance,
)
from b24api.cli_contract import (
    CliUsageError,
    ListContractRoute,
    cli_request,
    default_contract,
    list_stream,
    parse_list_contract,
    read_json_source,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from b24api.contracts import OperationReport

_SUCCESS = 0
_USAGE = 2
_UNAVAILABLE = 3
_CORRECTNESS = 4
_OUTPUT_CLOSED = 5
_INTERRUPTED = 130


def _compact(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _response_json(response: Response) -> dict[str, object]:
    return {
        "result": response.result,
        "total": response.total,
        "next": response.next,
        "time": dataclasses.asdict(response.time) if response.time is not None else None,
        "evidence": response.evidence.to_dict(),
    }


def _report_json(report: OperationReport) -> dict[str, object]:
    return {"kind": "report", **dataclasses.asdict(report)}


def _safe_error(error: BaseException) -> dict[str, object]:
    if isinstance(error, B24ApiError):
        return {"kind": "error", **error.to_safe_dict()}
    return {"kind": "error", "type": type(error).__name__, "message": "operation failed"}


def _write_json(stream: TextIO, value: object) -> None:
    stream.write(_compact(value))
    stream.write("\n")
    stream.flush()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="b24api")
    subparsers = parser.add_subparsers(dest="command", required=True)
    call = subparsers.add_parser("call")
    call.add_argument("method")
    call.add_argument("--params")
    call.add_argument("--raw", action="store_true")
    call.add_argument(
        "--replay-safety",
        choices=tuple(safety.value for safety in ReplaySafety),
        default=ReplaySafety.UNKNOWN.value,
    )
    listing = subparsers.add_parser("list")
    listing.add_argument("method")
    listing.add_argument("--params")
    listing.add_argument(
        "--strategy",
        choices=("sequential", "counted", "keyset", "cursor"),
        default="sequential",
    )
    listing.add_argument("--contract")
    return parser


async def _call(args: argparse.Namespace, request: Request, stdout: TextIO) -> None:
    async with Bitrix24() as client:
        result = await client.call_response(request) if args.raw else await client.call(request)
    _write_json(stdout, _response_json(result) if isinstance(result, Response) else result)


async def _list(
    request: Request,
    route: ListContractRoute,
    stdout: TextIO,
    stderr: TextIO,
) -> None:
    async with Bitrix24() as client:
        stream = list_stream(
            client,
            request=request,
            route=route,
        )
        primary: BaseException | None = None
        try:
            async with stream:
                async for item in stream:
                    _write_json(stdout, item)
        except BaseException as error:  # noqa: BLE001 - preserve typed primary after reporting
            primary = error
        report = stream.report
    if report is None:
        if primary is not None:
            raise primary
        raise RuntimeError("list stream terminated without a report")
    if report.assurance is TraversalAssurance.MECHANICS_ONLY:
        _write_json(
            stderr,
            {
                "kind": "warning",
                "code": "mechanics_only",
                "message": "completion proves pagination mechanics, not exact row identity",
            },
        )
    _write_json(stderr, _report_json(report))
    if primary is not None:
        raise primary
    if report.state is not TerminalState.COMPLETED:
        raise RuntimeError("list traversal did not complete successfully")


def main(  # noqa: PLR0911 - stable process-code boundary
    argv: Sequence[str] | None = None,
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Run one CLI operation and return its stable process code."""
    input_stream = stdin or sys.stdin
    output_stream = stdout or sys.stdout
    error_stream = stderr or sys.stderr
    try:
        args = _parser().parse_args(argv)
        params = read_json_source(args.params, label="parameters", stdin=input_stream)
        if args.command == "call":
            request = cli_request(args.method, params, ReplaySafety(args.replay_safety))
            asyncio.run(_call(args, request, output_stream))
        else:
            contract = default_contract(args.strategy, args.contract, input_stream)
            route = parse_list_contract(args.strategy, contract)
            request = cli_request(args.method, params, ReplaySafety.SAFE)
            asyncio.run(_list(request, route, output_stream, error_stream))
    except KeyboardInterrupt:
        return _INTERRUPTED
    except SystemExit as error:
        return int(error.code) if isinstance(error.code, int) else _USAGE
    except CliUsageError as error:
        _write_json(error_stream, {"kind": "usage_error", "message": str(error)})
        return _USAGE
    except BrokenPipeError:
        return _OUTPUT_CLOSED
    except B24ApiError as error:
        _write_json(error_stream, _safe_error(error))
        return _CORRECTNESS
    except (asyncio.CancelledError, GeneratorExit):
        return _INTERRUPTED
    except Exception as error:  # noqa: BLE001 - configuration and output boundary
        if type(error).__module__.startswith(("pydantic", "pydantic_settings")):
            _write_json(error_stream, {"kind": "unavailable", "message": "configuration unavailable"})
            return _UNAVAILABLE
        _write_json(error_stream, _safe_error(error))
        return _CORRECTNESS
    return _SUCCESS


def _entrypoint() -> NoReturn:
    raise SystemExit(main())


if __name__ == "__main__":
    _entrypoint()
