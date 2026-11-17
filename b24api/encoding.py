"""Shared PHP-compatible bracket encoding, and the one rule for what it carries as direct JSON does.

A form body and a classic batch command name each leaf by its bracket path (``fields[TITLE]=x``),
which PHP's ``parse_str`` reads back. That form has no ``null``, no empty array or object, and no way
to name a key that holds a bracket; PHP also renames a top-level dot or space to ``_``. A classic batch
command is further read by the batch handler, which substitutes ``$result[...]`` and its siblings inside
every value. :func:`bracket_obstacle` names the first value the encoding would change, so every path
that sends bracket encoding refuses it before any request instead of sending a different query.

Scalars keep the meaning PHP gives them: every scalar arrives as a string, and ``True``/``False``
arrive as ``1``/``0``, exactly as PHP's ``http_build_query`` writes them.
"""

from __future__ import annotations
import re
from collections.abc import Iterable, Mapping, Sequence
from enum import StrEnum
from urllib.parse import quote_plus


class BracketObstacle(StrEnum):
    """Why PHP bracket encoding would not carry a parameter mapping as direct JSON does."""

    NONE_VALUE = "a None value, which bracket encoding drops"
    EMPTY_CONTAINER = "an empty array or object, which bracket encoding drops"
    KEY = "a key that is empty or holds a bracket or NUL, or a top-level key with a dot or space, which PHP renames"
    REFERENCE = "a $result reference pattern, which the batch handler substitutes inside any value"


# Every name the classic batch handler substitutes (``$result``, ``$result_error``, ``$result_total``,
# ``$result_next``, ``$result_time``), followed by its opening bracket, in any letter case.
_BATCH_REFERENCE = re.compile(r"\$result(?:_error|_total|_next|_time)?\[", re.IGNORECASE)
_NESTED_KEY_FORBIDDEN = frozenset("[]\x00")
_TOP_KEY_FORBIDDEN = _NESTED_KEY_FORBIDDEN | frozenset(". ")


def bracket_obstacle(parameters: Mapping[str, object], *, batch: bool) -> BracketObstacle | None:
    """Return the first value bracket encoding would carry differently from direct JSON, or ``None``.

    ``batch`` adds the classic batch handler's own reading: a value holding a ``$result[`` reference
    pattern would be replaced by another command's result.
    """
    pending: list[tuple[Mapping[str, object] | Sequence[object], bool]] = [(parameters, True)]
    while pending:
        container, top = pending.pop()
        values: Iterable[object] = container
        if isinstance(container, Mapping):
            forbidden = _TOP_KEY_FORBIDDEN if top else _NESTED_KEY_FORBIDDEN
            if any(not key or not forbidden.isdisjoint(key) for key in container):
                return BracketObstacle.KEY
            values = container.values()
        for value in values:
            obstacle = _value_obstacle(value, batch=batch)
            if obstacle is not None:
                return obstacle
            if isinstance(value, Mapping | Sequence) and not isinstance(value, str):
                pending.append((value, False))
    return None


def _value_obstacle(value: object, *, batch: bool) -> BracketObstacle | None:
    if value is None:
        return BracketObstacle.NONE_VALUE
    if isinstance(value, str):
        return BracketObstacle.REFERENCE if batch and _BATCH_REFERENCE.search(value) else None
    if isinstance(value, Mapping | Sequence) and not value:
        return BracketObstacle.EMPTY_CONTAINER
    return None


def encode_php_query(parameters: Mapping[str | int, object], path: str = "%s") -> str:
    """Encode frozen JSON values using PHP bracket semantics.

    A value :func:`bracket_obstacle` names is not representable; callers refuse it before encoding.
    """
    return _encode_mapping(parameters, root_template=path)


def _encode_mapping(
    parameters: Mapping[str | int, object],
    *,
    prefix: str | None = None,
    root_template: str = "%s",
) -> str:
    parts: list[str] = []
    for key, raw_value in parameters.items():
        if raw_value is None:
            continue
        value: object = (
            dict(enumerate(raw_value))
            if isinstance(raw_value, Sequence) and not isinstance(raw_value, str)
            else raw_value
        )
        key_text = str(key)
        nested_path = root_template % key_text if prefix is None else f"{prefix}[{key_text}]"
        if isinstance(value, Mapping):
            nested = _encode_mapping(value, prefix=nested_path)
            if nested:
                parts.append(nested)
            continue
        rendered = "1" if value is True else "0" if value is False else str(value)
        parts.append(f"{quote_plus(nested_path)}={quote_plus(rendered)}")
    return "&".join(parts)


__all__ = ["BracketObstacle", "bracket_obstacle", "encode_php_query"]
