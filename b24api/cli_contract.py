"""Closed JSON parsing and routing for the installed CLI."""

from __future__ import annotations
import json
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NoReturn, TextIO, cast

from b24api import (
    Bitrix24,
    CursorSpec,
    IdentitySpec,
    KeysetSpec,
    OffsetSpec,
    ParameterPath,
    ReplaySafety,
    Request,
    ResultSelector,
)
from b24api.contracts import IdentityCoercion, JsonValue, OperationStream

if TYPE_CHECKING:
    from collections.abc import Mapping

_CONTRACT_VERSION = 1
_PORTAL_BATCH_CAP = 50


class CliUsageError(ValueError):
    """A bounded local CLI JSON or contract error."""


class _DuplicateKeyError(ValueError):
    pass


class _NonFiniteNumberError(ValueError):
    pass


def _reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_non_finite_number(value: str) -> NoReturn:
    raise _NonFiniteNumberError(f"non-finite JSON number: {value}")


def decode_one_object(text: str, *, label: str) -> dict[str, object]:
    """Decode exactly one closed JSON object without duplicate keys."""
    decoder = json.JSONDecoder(
        object_pairs_hook=_reject_duplicates,
        parse_constant=_reject_non_finite_number,
    )
    try:
        value, end = decoder.raw_decode(text.lstrip())
    except (json.JSONDecodeError, _DuplicateKeyError, _NonFiniteNumberError) as error:
        raise CliUsageError(f"{label} is not valid closed JSON") from error
    leading = len(text) - len(text.lstrip())
    if text[leading + end :].strip():
        raise CliUsageError(f"{label} contains trailing JSON data")
    if not isinstance(value, dict):
        raise CliUsageError(f"{label} must be exactly one JSON object")
    return cast("dict[str, object]", value)


def cli_request(method: str, parameters: Mapping[str, object], replay_safety: ReplaySafety) -> Request:
    """Construct one request while preserving the CLI's local-error boundary."""
    try:
        return Request(method, parameters, replay_safety)
    except (TypeError, ValueError) as error:
        raise CliUsageError("request method or parameters are invalid") from error


def read_json_source(raw: str | None, *, label: str, stdin: TextIO) -> dict[str, object]:
    """Read inline, file-backed or stdin JSON without exposing credentials."""
    if raw is None:
        return {}
    if raw == "-":
        text = stdin.read()
    elif raw.startswith("@"):
        path = raw[1:]
        if not path:
            raise CliUsageError(f"{label} file path is empty")
        try:
            text = Path(path).read_text(encoding="utf-8")
        except OSError as error:
            raise CliUsageError(f"cannot read {label} file") from error
    else:
        text = raw
    return decode_one_object(text, label=label)


def default_contract(strategy: str, raw: str | None, stdin: TextIO) -> dict[str, object]:
    """Return the closed default sequential contract or read an explicit file."""
    if raw is None:
        if strategy != "sequential":
            raise CliUsageError(f"{strategy} strategy requires --contract @FILE")
        return {"version": 1}
    if not raw.startswith("@"):
        raise CliUsageError("--contract must use @FILE")
    return read_json_source(raw, label="contract", stdin=stdin)


def _closed(mapping: Mapping[str, object], allowed: set[str], *, label: str) -> None:
    unknown = set(mapping) - allowed
    if unknown:
        raise CliUsageError(f"{label} contains unknown fields: {sorted(unknown)}")


def _path(raw: object, *, label: str, optional: bool = False) -> ParameterPath | None:
    if raw is None and optional:
        return None
    if not isinstance(raw, list):
        raise CliUsageError(f"{label} must be a JSON path array")
    try:
        return ParameterPath(tuple(raw))
    except (TypeError, ValueError) as error:
        raise CliUsageError(f"{label} is not a valid parameter path") from error


def _selector(raw: object, *, required: bool) -> ResultSelector:
    if raw is None and not required:
        return ResultSelector.root()
    if not isinstance(raw, list):
        raise CliUsageError("selector must be a JSON path array")
    try:
        return ResultSelector(tuple(raw))
    except (TypeError, ValueError) as error:
        raise CliUsageError("selector is not a valid result path") from error


def _identity(raw: object, *, required: bool) -> IdentitySpec | None:
    if raw is None and not required:
        return None
    if not isinstance(raw, dict):
        raise CliUsageError("identity is required and must be an object")
    _closed(raw, {"item_path", "filter_key", "order_key", "coercion"}, label="identity")
    if not isinstance(raw.get("item_path"), list):
        raise CliUsageError("identity.item_path must be a path array")
    filter_key = raw.get("filter_key")
    order_key = raw.get("order_key")
    if not isinstance(filter_key, str) or not isinstance(order_key, str):
        raise CliUsageError("identity filter_key and order_key must be strings")
    try:
        coercion = IdentityCoercion(raw.get("coercion", IdentityCoercion.EXACT_STRING.value))
        return IdentitySpec(tuple(raw["item_path"]), filter_key, order_key, coercion)
    except (TypeError, ValueError) as error:
        raise CliUsageError("identity contract is invalid") from error


def _common(contract: Mapping[str, object], *, selector_required: bool) -> tuple[ResultSelector, int]:
    if contract.get("version") != _CONTRACT_VERSION:
        raise CliUsageError("contract.version must equal 1")
    selector = _selector(contract.get("selector"), required=selector_required)
    page_size = contract.get("page_size", _PORTAL_BATCH_CAP)
    if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size < 1:
        raise CliUsageError("contract.page_size must be a positive integer")
    return selector, page_size


def _control_options(raw: Mapping[str, object], *, label: str) -> tuple[ParameterPath | None, bool]:
    limit_path = _path(raw.get("limit_path"), label=f"{label}.limit_path", optional=True)
    allow_create = raw.get("allow_create_controls", True)
    if not isinstance(allow_create, bool):
        raise CliUsageError(f"{label}.allow_create_controls must be boolean")
    return limit_path, allow_create


def _offset(raw: object) -> OffsetSpec:
    if raw is None:
        return OffsetSpec()
    if not isinstance(raw, dict):
        raise CliUsageError("offset must be an object")
    _closed(raw, {"parameter_path", "limit_path", "allow_create_controls"}, label="offset")
    path = _path(raw.get("parameter_path"), label="offset.parameter_path")
    limit_path, allow_create = _control_options(raw, label="offset")
    return OffsetSpec(cast("ParameterPath", path), limit_path, allow_create)


def _keyset(raw: object) -> KeysetSpec:
    if not isinstance(raw, dict):
        raise CliUsageError("keyset is required and must be an object")
    _closed(
        raw,
        {"filter_path", "order_path", "start_suppression_path", "limit_path", "direction", "allow_create_controls"},
        label="keyset",
    )
    direction = raw.get("direction", "ascending")
    if direction not in {"ascending", "descending"}:
        raise CliUsageError("keyset.direction is invalid")
    filter_path = _path(raw.get("filter_path"), label="keyset.filter_path")
    order_path = _path(raw.get("order_path"), label="keyset.order_path")
    start = _path(raw.get("start_suppression_path"), label="keyset.start_suppression_path", optional=True)
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
        raise CliUsageError("cursor is required and must be an object")
    _closed(
        raw,
        {"parameter_path", "item_path", "coercion", "direction", "take", "limit_path", "allow_create_controls"},
        label="cursor",
    )
    item_path = raw.get("item_path")
    if not isinstance(item_path, list):
        raise CliUsageError("cursor.item_path must be a path array")
    direction = raw.get("direction", "ascending")
    take = raw.get("take", "last")
    if direction not in {"ascending", "descending"} or take not in {"first", "last"}:
        raise CliUsageError("cursor direction or take is invalid")
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
        raise CliUsageError("cursor contract is invalid") from error


def list_stream(
    client: Bitrix24,
    *,
    request: Request,
    strategy: str,
    contract: dict[str, object],
) -> OperationStream[JsonValue]:
    """Route one closed contract to exactly one public list operation."""
    allowed_common = {"version", "selector", "page_size"}
    allowed_by_strategy = {
        "sequential": allowed_common | {"offset", "identity"},
        "counted": allowed_common | {"offset", "identity"},
        "keyset": allowed_common | {"identity", "keyset"},
        "cursor": allowed_common | {"identity", "cursor"},
    }
    _closed(contract, allowed_by_strategy[strategy], label=f"{strategy} contract")
    selector, page_size = _common(contract, selector_required=strategy in {"keyset", "cursor"})
    identity = _identity(contract.get("identity"), required=strategy in {"counted", "keyset"})
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


__all__ = ["CliUsageError", "cli_request", "decode_one_object", "default_contract", "list_stream", "read_json_source"]
