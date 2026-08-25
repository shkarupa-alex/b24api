"""Small JSONL console boundary over the public b24api v2 contracts."""

from __future__ import annotations
import argparse
import asyncio
import dataclasses
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NoReturn, TextIO, cast

from b24api import (
    B24ApiError,
    Bitrix24,
    CursorSpec,
    IdentitySpec,
    KeysetSpec,
    OffsetSpec,
    ParameterPath,
    ReplaySafety,
    Request,
    Response,
    ResultSelector,
    TerminalState,
    TraversalAssurance,
)
from b24api.contracts import IdentityCoercion, JsonValue, OperationReport, OperationStream

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

_SUCCESS = 0
_USAGE = 2
_UNAVAILABLE = 3
_CORRECTNESS = 4
_OUTPUT_CLOSED = 5
_INTERRUPTED = 130
_CONTRACT_VERSION = 1
_PORTAL_BATCH_CAP = 50


class _DuplicateKeyError(ValueError):
    pass


class _CliUsageError(ValueError):
    pass


def _reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _decode_one_object(text: str, *, label: str) -> dict[str, object]:
    decoder = json.JSONDecoder(object_pairs_hook=_reject_duplicates)
    try:
        value, end = decoder.raw_decode(text.lstrip())
    except (json.JSONDecodeError, _DuplicateKeyError) as error:
        raise _CliUsageError(f"{label} is not valid closed JSON") from error
    leading = len(text) - len(text.lstrip())
    if text[leading + end :].strip():
        raise _CliUsageError(f"{label} contains trailing JSON data")
    if not isinstance(value, dict):
        raise _CliUsageError(f"{label} must be exactly one JSON object")
    return cast("dict[str, object]", value)


def _read_json_source(raw: str | None, *, label: str, stdin: TextIO) -> dict[str, object]:
    if raw is None:
        return {}
    if raw == "-":
        text = stdin.read()
    elif raw.startswith("@"):
        path = raw[1:]
        if not path:
            raise _CliUsageError(f"{label} file path is empty")
        try:
            text = Path(path).read_text(encoding="utf-8")
        except OSError as error:
            raise _CliUsageError(f"cannot read {label} file") from error
    else:
        text = raw
    return _decode_one_object(text, label=label)


def _closed(mapping: Mapping[str, object], allowed: set[str], *, label: str) -> None:
    unknown = set(mapping) - allowed
    if unknown:
        raise _CliUsageError(f"{label} contains unknown fields: {sorted(unknown)}")


def _path(raw: object, *, label: str, optional: bool = False) -> ParameterPath | None:
    if raw is None and optional:
        return None
    if not isinstance(raw, list):
        raise _CliUsageError(f"{label} must be a JSON path array")
    try:
        return ParameterPath(tuple(raw))
    except (TypeError, ValueError) as error:
        raise _CliUsageError(f"{label} is not a valid parameter path") from error


def _selector(raw: object, *, required: bool) -> ResultSelector:
    if raw is None and not required:
        return ResultSelector.root()
    if not isinstance(raw, list):
        raise _CliUsageError("selector must be a JSON path array")
    try:
        return ResultSelector(tuple(raw))
    except (TypeError, ValueError) as error:
        raise _CliUsageError("selector is not a valid result path") from error


def _identity(raw: object, *, required: bool) -> IdentitySpec | None:
    if raw is None and not required:
        return None
    if not isinstance(raw, dict):
        raise _CliUsageError("identity is required and must be an object")
    _closed(raw, {"item_path", "filter_key", "order_key", "coercion"}, label="identity")
    if not isinstance(raw.get("item_path"), list):
        raise _CliUsageError("identity.item_path must be a path array")
    filter_key = raw.get("filter_key")
    order_key = raw.get("order_key")
    if not isinstance(filter_key, str) or not isinstance(order_key, str):
        raise _CliUsageError("identity filter_key and order_key must be strings")
    coercion_raw = raw.get("coercion", IdentityCoercion.EXACT_STRING.value)
    try:
        coercion = IdentityCoercion(coercion_raw)
        return IdentitySpec(tuple(raw["item_path"]), filter_key, order_key, coercion)
    except (TypeError, ValueError) as error:
        raise _CliUsageError("identity contract is invalid") from error


def _common(
    contract: Mapping[str, object],
    *,
    selector_required: bool,
) -> tuple[ResultSelector, int]:
    if contract.get("version") != _CONTRACT_VERSION:
        raise _CliUsageError("contract.version must equal 1")
    selector = _selector(contract.get("selector"), required=selector_required)
    page_size = contract.get("page_size", _PORTAL_BATCH_CAP)
    if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size < 1:
        raise _CliUsageError("contract.page_size must be a positive integer")
    return selector, page_size


def _control_options(raw: Mapping[str, object], *, label: str) -> tuple[ParameterPath | None, bool]:
    limit_path = _path(raw.get("limit_path"), label=f"{label}.limit_path", optional=True)
    allow_create = raw.get("allow_create_controls", True)
    if not isinstance(allow_create, bool):
        raise _CliUsageError(f"{label}.allow_create_controls must be boolean")
    return limit_path, allow_create


def _offset(raw: object) -> OffsetSpec:
    if raw is None:
        return OffsetSpec()
    if not isinstance(raw, dict):
        raise _CliUsageError("offset must be an object")
    _closed(raw, {"parameter_path", "limit_path", "allow_create_controls"}, label="offset")
    path = _path(raw.get("parameter_path"), label="offset.parameter_path")
    limit_path, allow_create = _control_options(raw, label="offset")
    return OffsetSpec(cast("ParameterPath", path), limit_path, allow_create)


def _keyset(raw: object) -> KeysetSpec:
    if not isinstance(raw, dict):
        raise _CliUsageError("keyset is required and must be an object")
    _closed(
        raw,
        {
            "filter_path",
            "order_path",
            "start_suppression_path",
            "limit_path",
            "direction",
            "allow_create_controls",
        },
        label="keyset",
    )
    direction = raw.get("direction", "ascending")
    if direction not in {"ascending", "descending"}:
        raise _CliUsageError("keyset.direction is invalid")
    filter_path = _path(raw.get("filter_path"), label="keyset.filter_path")
    order_path = _path(raw.get("order_path"), label="keyset.order_path")
    start = _path(
        raw.get("start_suppression_path"),
        label="keyset.start_suppression_path",
        optional=True,
    )
    limit_path, allow_create = _control_options(raw, label="keyset")
    return KeysetSpec(
        cast("ParameterPath", filter_path),
        cast("ParameterPath", order_path),
        start,
        limit_path,
        cast("Literal['ascending', 'descending']", direction),
        allow_create,
    )


def _cursor(raw: object) -> CursorSpec:
    if not isinstance(raw, dict):
        raise _CliUsageError("cursor is required and must be an object")
    _closed(
        raw,
        {
            "parameter_path",
            "item_path",
            "coercion",
            "direction",
            "take",
            "limit_path",
            "allow_create_controls",
        },
        label="cursor",
    )
    item_path = raw.get("item_path")
    if not isinstance(item_path, list):
        raise _CliUsageError("cursor.item_path must be a path array")
    direction = raw.get("direction", "ascending")
    take = raw.get("take", "last")
    if direction not in {"ascending", "descending"} or take not in {"first", "last"}:
        raise _CliUsageError("cursor direction or take is invalid")
    try:
        limit_path, allow_create = _control_options(raw, label="cursor")
        return CursorSpec(
            cast("ParameterPath", _path(raw.get("parameter_path"), label="cursor.parameter_path")),
            tuple(item_path),
            IdentityCoercion(raw.get("coercion", IdentityCoercion.EXACT_STRING.value)),
            cast("Literal['ascending', 'descending']", direction),
            cast("Literal['first', 'last']", take),
            limit_path,
            allow_create,
        )
    except (TypeError, ValueError) as error:
        raise _CliUsageError("cursor contract is invalid") from error


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


async def _call(args: argparse.Namespace, params: dict[str, object], stdout: TextIO) -> None:
    request = Request(args.method, params, ReplaySafety(args.replay_safety))
    async with Bitrix24() as client:
        result = await client.call_response(request) if args.raw else await client.call(request)
    _write_json(stdout, _response_json(result) if isinstance(result, Response) else result)


def _list_stream(
    client: Bitrix24,
    args: argparse.Namespace,
    params: dict[str, object],
    contract: dict[str, object],
) -> OperationStream[JsonValue]:
    strategy = cast("str", args.strategy)
    allowed_common = {"version", "selector", "page_size"}
    allowed_by_strategy = {
        "sequential": allowed_common | {"offset", "identity"},
        "counted": allowed_common | {"offset", "identity"},
        "keyset": allowed_common | {"identity", "keyset"},
        "cursor": allowed_common | {"identity", "cursor"},
    }
    _closed(contract, allowed_by_strategy[strategy], label=f"{strategy} contract")
    required_selector = strategy in {"keyset", "cursor"}
    selector, page_size = _common(contract, selector_required=required_selector)
    identity = _identity(contract.get("identity"), required=strategy in {"counted", "keyset"})
    request = Request(args.method, params, ReplaySafety.SAFE)
    if strategy == "sequential":
        return client.iter_list(
            request,
            selector=selector,
            identity=identity,
            page_size=page_size,
            offset=_offset(contract.get("offset")),
        )
    if strategy == "counted":
        return client.iter_list_counted(
            request,
            selector=selector,
            identity=cast("IdentitySpec", identity),
            page_size=page_size,
            offset=_offset(contract.get("offset")),
        )
    if strategy == "keyset":
        return client.iter_list_keyset(
            request,
            selector=selector,
            identity=cast("IdentitySpec", identity),
            page_size=page_size,
            keyset=_keyset(contract.get("keyset")),
        )
    return client.iter_list_cursor(
        request,
        selector=selector,
        cursor=_cursor(contract.get("cursor")),
        identity=identity,
        page_size=page_size,
    )


async def _list(
    args: argparse.Namespace,
    params: dict[str, object],
    contract: dict[str, object],
    stdout: TextIO,
    stderr: TextIO,
) -> None:
    async with Bitrix24() as client:
        stream = _list_stream(client, args, params, contract)
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


def _default_contract(strategy: str, raw: str | None, stdin: TextIO) -> dict[str, object]:
    if raw is None:
        if strategy != "sequential":
            raise _CliUsageError(f"{strategy} strategy requires --contract @FILE")
        return {"version": 1}
    if not raw.startswith("@"):
        raise _CliUsageError("--contract must use @FILE")
    return _read_json_source(raw, label="contract", stdin=stdin)


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
        params = _read_json_source(args.params, label="parameters", stdin=input_stream)
        if args.command == "call":
            asyncio.run(_call(args, params, output_stream))
        else:
            contract = _default_contract(args.strategy, args.contract, input_stream)
            asyncio.run(_list(args, params, contract, output_stream, error_stream))
    except KeyboardInterrupt:
        return _INTERRUPTED
    except SystemExit as error:
        return int(error.code) if isinstance(error.code, int) else _USAGE
    except _CliUsageError as error:
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
