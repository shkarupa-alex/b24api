"""Small JSONL console boundary over the public b24api v2 contracts."""

from __future__ import annotations
import argparse
import asyncio
import dataclasses
import json
import os
import sys
from typing import TYPE_CHECKING, NoReturn, TextIO

from b24api import (
    B24ApiError,
    Bitrix24,
    KeysetCapabilityError,
    ReplaySafety,
    Request,
    RouteKind,
    TerminalState,
    TraversalAssurance,
)
from b24api.cli_contract import (
    CliUsageError,
    ListContractRoute,
    VerifyKeysetContractRoute,
    cli_request,
    default_contract,
    list_stream,
    parse_list_contract,
    parse_verify_keyset_contract,
    read_json_source,
)
from b24api.contracts import Response

if TYPE_CHECKING:
    from collections.abc import Sequence

    from _typeshed import SupportsWrite

    from b24api.contracts import OperationReport

_SUCCESS = 0
_USAGE = 2
_UNAVAILABLE = 3
_CORRECTNESS = 4
_OUTPUT_CLOSED = 5
_KEYSET_UNSUPPORTED = 6
_KEYSET_INCONCLUSIVE = 7
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
    result: dict[str, object] = {
        "kind": "report",
        "state": report.state,
        "operation": report.operation,
        "terminal_reason": report.terminal_reason,
        "assurance": report.assurance,
        "admitted": report.admitted,
        "emitted": report.emitted,
        "successes": report.successes,
        "failures": report.failures,
        "not_executed": report.not_executed,
        "unknown": report.unknown,
        "unique_rows": report.unique_rows,
        "physical_requests": report.physical_requests,
        "logical_pages": report.logical_pages,
        "batch_requests": report.batch_requests,
        "batch_commands": report.batch_commands,
        "retries": report.retries,
        "cooldown_seconds": report.cooldown_seconds,
        "buffered_commands_high_water": report.buffered_commands_high_water,
        "buffered_rows_high_water": report.buffered_rows_high_water,
        "active_references_high_water": report.active_references_high_water,
        "violations": tuple(item.to_safe_dict() for item in report.violations),
    }
    if report.keyset_execution is not None:
        result["keyset_execution"] = dataclasses.asdict(report.keyset_execution)
    if report.keyset_selection is not None:
        result["keyset_selection"] = dataclasses.asdict(report.keyset_selection)
    return result


def _safe_error(error: BaseException) -> dict[str, object]:
    if isinstance(error, B24ApiError):
        return {"kind": "error", **error.to_safe_dict()}
    return {"kind": "error", "type": type(error).__name__, "message": "operation failed"}


def _write_json(stream: TextIO, value: object) -> None:
    stream.write(_compact(value))
    stream.write("\n")
    stream.flush()


def _emit(stream: SupportsWrite[str] | None, text: str) -> None:
    # Python leaves a standard stream unset when its descriptor was closed at startup; like argparse,
    # fall back to stderr and drop the message when that is unset too.
    target = stream or sys.stderr
    if target is None:
        return
    target.write(text)
    flush = getattr(target, "flush", None)
    if flush is not None:
        flush()


class _Parser(argparse.ArgumentParser):
    """Argument parser whose help, usage and error writes reach the caller, failures included.

    ``ArgumentParser`` neither flushes its writes nor lets an ``OSError`` from them escape, so a reader
    that has gone away would only show up in whatever stayed buffered. Each message is written and
    flushed here, and a ``BrokenPipeError`` reaches the CLI's output boundary whatever the buffering.
    """

    def print_usage(self, file: SupportsWrite[str] | None = None) -> None:
        """Write and flush the usage line (stdout by default)."""
        _emit(file or sys.stdout, self.format_usage())

    def print_help(self, file: SupportsWrite[str] | None = None) -> None:
        """Write and flush the help text (stdout by default)."""
        _emit(file or sys.stdout, self.format_help())

    def exit(self, status: int = 0, message: str | None = None) -> NoReturn:
        """Write and flush ``message`` to stderr, then exit with ``status``."""
        if message:
            _emit(sys.stderr, message)
        sys.exit(status)


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="b24api", description="Call Bitrix24 methods and stream list rows as JSONL.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    call = subparsers.add_parser("call", help="execute one method and print one JSON value")
    call.add_argument("method", help="Bitrix24 REST method name")
    call.add_argument("--route", choices=tuple(route.value for route in RouteKind), required=True)
    call.add_argument("--params", help="JSON object, @file, or - for stdin")
    call.add_argument("--raw", action="store_true", help="print the immutable response envelope")
    call.add_argument(
        "--replay-safety",
        choices=tuple(safety.value for safety in ReplaySafety),
        default=ReplaySafety.UNKNOWN.value,
        help="operator assertion controlling post-dispatch replay (default: unknown)",
    )
    listing = subparsers.add_parser("list", help="stream one list traversal as JSONL")
    listing.add_argument("method", help="Bitrix24 REST method name")
    listing.add_argument("--route", choices=tuple(route.value for route in RouteKind), required=True)
    listing.add_argument("--params", help="JSON object, @file, or - for stdin")
    listing.add_argument(
        "--strategy",
        choices=("sequential", "counted", "keyset", "cursor"),
        default="sequential",
        help="traversal mechanics (default: sequential)",
    )
    listing.add_argument("--contract", help="closed v1 traversal contract as @file or -")
    verify = subparsers.add_parser("verify-keyset", help="verify strict keyset bounds for one portal method")
    verify.add_argument("method", help="Bitrix24 REST method name")
    verify.add_argument("--route", choices=tuple(route.value for route in RouteKind), required=True)
    verify.add_argument("--params", help="JSON object, @file, or - for stdin")
    verify.add_argument("--contract", required=True, help="closed v1 verifier contract as @file")
    return parser


async def _call(args: argparse.Namespace, request: Request, stdout: TextIO) -> None:
    async with Bitrix24() as client:
        result = await client.call_response(request) if args.raw else await client.call(request)
    _write_json(stdout, _response_json(result) if isinstance(result, Response) else result)


def _write_report(stderr: TextIO, report: OperationReport) -> None:
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
    try:
        _write_report(stderr, report)
    except OSError as error:
        if isinstance(primary, BrokenPipeError):  # a gone stdout reader outranks a failing stderr
            raise primary from error
        raise
    if primary is not None:
        raise primary
    if report.state is not TerminalState.COMPLETED:
        raise RuntimeError("list traversal did not complete successfully")


async def _verify_keyset(request: Request, route: VerifyKeysetContractRoute, stdout: TextIO) -> None:
    async with Bitrix24() as client:
        report = await client.verify_keyset_capability(
            request,
            selector=route.selector,
            identity=route.identity,
            collection_shape=route.collection_shape,
            page_size=route.page_size,
            keyset=route.keyset,
        )
    _write_json(stdout, report.to_dict())


def _discard(stream: TextIO) -> None:
    """Point a standard stream that cannot be flushed at devnull, so the interpreter's own flush succeeds.

    CPython flushes ``sys.stdout`` and ``sys.stderr`` at shutdown and exits with 120 when bytes a failed
    write left behind still cannot be delivered. A stream the caller injected is the caller's to close.
    """
    if stream is not sys.stdout and stream is not sys.stderr:
        return
    try:
        descriptor = stream.fileno()
    except (AttributeError, OSError, ValueError):
        return
    devnull = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(devnull, descriptor)
    finally:
        os.close(devnull)


def _settle(code: int, *streams: TextIO | None) -> int:
    """Deliver what is still buffered and fold an undeliverable stream into the process code."""
    for stream in streams:
        # A stream its owner already closed has nothing left to deliver; its writes failed in the run.
        if stream is None or getattr(stream, "closed", False):
            continue
        try:
            stream.flush()
        except BrokenPipeError:
            code = _OUTPUT_CLOSED
        except OSError:  # e.g. a full disk: a failure, but never a traceback
            code = code or _CORRECTNESS
        else:
            continue
        _discard(stream)
    return code


def main(
    argv: Sequence[str] | None = None,
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Run one CLI operation and return its stable process code.

    A reader of stdout or stderr that has gone away at any write yields the output-closed code, which
    takes priority over every other outcome; any other failed write yields the generic failure code.
    """
    input_stream = stdin or sys.stdin
    output_stream = stdout or sys.stdout
    error_stream = stderr or sys.stderr
    try:
        code = _run(argv, input_stream, output_stream, error_stream)
    except BrokenPipeError:
        code = _OUTPUT_CLOSED
    except OSError:  # a diagnostic or report that could not be written, e.g. to a full disk
        code = _CORRECTNESS
    except ValueError:
        # Only a write to a stream its owner already closed is a failed write; anything else is a bug.
        if not any(getattr(stream, "closed", False) for stream in (output_stream, error_stream)):
            raise
        code = _CORRECTNESS
    return _settle(code, output_stream, error_stream)


def _run(  # noqa: C901, PLR0911 - stable process-code boundary
    argv: Sequence[str] | None,
    input_stream: TextIO,
    output_stream: TextIO,
    error_stream: TextIO,
) -> int:
    try:
        args = _parser().parse_args(argv)
        params = read_json_source(args.params, label="parameters", stdin=input_stream)
        if args.command == "call":
            request = cli_request(args.method, params, ReplaySafety(args.replay_safety), RouteKind(args.route))
            asyncio.run(_call(args, request, output_stream))
        elif args.command == "list":
            contract = default_contract(args.strategy, args.contract, input_stream)
            route = parse_list_contract(args.strategy, contract)
            request = cli_request(args.method, params, ReplaySafety.UNKNOWN, RouteKind(args.route))
            asyncio.run(_list(request, route, output_stream, error_stream))
        else:
            contract = default_contract("verify-keyset", args.contract, input_stream)
            verify_route = parse_verify_keyset_contract(contract)
            request = cli_request(args.method, params, ReplaySafety.UNKNOWN, RouteKind(args.route))
            asyncio.run(_verify_keyset(request, verify_route, output_stream))
    except KeyboardInterrupt:
        return _INTERRUPTED
    except SystemExit as error:
        return int(error.code) if isinstance(error.code, int) else _USAGE
    except CliUsageError as error:
        _write_json(error_stream, {"kind": "usage_error", "message": str(error)})
        return _USAGE
    except BrokenPipeError:
        raise  # the output boundary in main owns a closed reader, not the generic failure arm below
    except KeysetCapabilityError as error:
        _write_json(output_stream, error.report.to_dict())
        return _KEYSET_UNSUPPORTED if error.verdict.value == "unsupported" else _KEYSET_INCONCLUSIVE
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
