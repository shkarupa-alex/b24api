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
    KeysetCapabilityError,
    ReplaySafety,
    Request,
    Response,
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

if TYPE_CHECKING:
    from collections.abc import Sequence

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
    return result


def _safe_error(error: BaseException) -> dict[str, object]:
    if isinstance(error, B24ApiError):
        return {"kind": "error", **error.to_safe_dict()}
    return {"kind": "error", "type": type(error).__name__, "message": "operation failed"}


def _write_json(stream: TextIO, value: object) -> None:
    stream.write(_compact(value))
    stream.write("\n")
    stream.flush()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="b24api", description="Call Bitrix24 methods and stream list rows as JSONL.")
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


def main(  # noqa: C901, PLR0911, PLR0912 - stable process-code boundary
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
        return _OUTPUT_CLOSED
    except KeysetCapabilityError as error:
        try:
            _write_json(output_stream, error.report.to_dict())
        except BrokenPipeError:
            return _OUTPUT_CLOSED
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
