"""Shared PHP-compatible bracket encoding."""

from __future__ import annotations
from collections.abc import Mapping, Sequence
from urllib.parse import quote_plus


def encode_php_query(parameters: Mapping[str | int, object], path: str = "%s") -> str:
    """Encode frozen JSON values using PHP bracket semantics."""
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


__all__ = ["encode_php_query"]
