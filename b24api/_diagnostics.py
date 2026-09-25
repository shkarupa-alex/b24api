"""Private per-request diagnostic context: local field aliases and the request's own exact secrets.

A caller field name is not safe to print merely because it sits in a field position: an unknown
credential can occupy the same slot. The context therefore replaces every name from ``filter``
(without its operator prefix), ``order``, ``fields`` and the string items of ``select`` with a
request-local alias ``field#1``, ``field#2`` ... in first-appearance order, and never exposes the raw
names: it has no serialization, its representation carries only a count, and pickling is refused.
It is derived from one canonical request, is used only while rendering that request's errors and is
never retained by an error, a report or a shared execution context.
"""

from __future__ import annotations
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import NoReturn

from b24api.redaction import DEFAULT_SENSITIVE_KEYS, is_secret, replace_secrets, secret_values

_ALIASED_PARAMETERS = frozenset({"filter", "order", "fields", "select"})
_SENSITIVE_NAMES = frozenset(name.casefold().replace("-", "_") for name in DEFAULT_SENSITIVE_KEYS)
_OPERATOR_PREFIX = re.compile(r"^[^A-Za-z0-9_]+")
_HAS_ALPHANUMERIC = re.compile(r"[A-Za-z0-9]")
# The bare-credential heuristic treats this class as one token; an alias never splits such a run.
_RUN_CHARACTERS = "A-Za-z0-9_-"
_V3_CONDITION_MINIMUM = 2
_MAX_SECRET_DEPTH = 32


class DiagnosticContext:
    """Aliases and exact secrets of one canonical request, resolved on first use."""

    __slots__ = ("_aliases", "_headers", "_parameters", "_pattern", "_secrets")

    def __init__(
        self,
        parameters: Mapping[str, object] | None = None,
        *,
        headers: Iterable[tuple[str, str]] = (),
    ) -> None:
        """Bind the immutable request parameters and scoped headers without scanning them yet."""
        self._parameters = parameters
        self._headers = tuple(headers)
        self._aliases: dict[str, str] | None = None
        self._pattern: re.Pattern[str] | None = None
        self._secrets: frozenset[str] = frozenset()

    def __repr__(self) -> str:
        """Return a representation that never contains a field name or a secret."""
        return f"DiagnosticContext(fields={len(self._resolved())})"

    def __reduce__(self) -> NoReturn:
        """Refuse serialization: the context holds raw caller field names."""
        raise TypeError("DiagnosticContext cannot be serialized")

    def alias_text(self, text: str) -> str:
        """Replace whole-token occurrences of declared field names with their aliases."""
        aliases = self._resolved()
        if self._pattern is None:
            return text
        return self._pattern.sub(lambda match: aliases[match.group(0)], text)

    def is_known_secret(self, value: str, *, ignore_case: bool = False) -> bool:
        """Report whether a whole value equals one of the request's own exact secrets."""
        if self._aliases is None:
            self._resolved()
        return is_secret(value, self._secrets, ignore_case=ignore_case)

    def replace_secrets(self, text: str) -> str:
        """Replace the request's own exact secrets wherever they occur."""
        self._resolved()
        return replace_secrets(text, self._secrets)

    def _resolved(self) -> dict[str, str]:
        if self._aliases is not None:
            return self._aliases
        secrets = [value for name, value in self._headers if name.casefold().replace("-", "_") in _SENSITIVE_NAMES]
        secrets.extend(_sensitive_values(self._parameters or {}, depth=0))
        self._secrets = secret_values(secrets)
        aliases: dict[str, str] = {}
        for name in _field_names(self._parameters or {}):
            # An exact secret stays an exact secret: it is redacted, never merely aliased.
            if name not in aliases and name not in self._secrets:
                aliases[name] = f"field#{len(aliases) + 1}"
        if aliases:
            names = "|".join(re.escape(name) for name in sorted(aliases, key=len, reverse=True))
            self._pattern = re.compile(rf"(?<![{_RUN_CHARACTERS}])(?:{names})(?![{_RUN_CHARACTERS}])")
        self._aliases = aliases
        return aliases


def _field_names(parameters: Mapping[str, object]) -> Iterable[str]:
    """Yield declared field names in parameter order, then in each container's own order."""
    for key, value in parameters.items():
        if key.casefold() not in _ALIASED_PARAMETERS:
            continue
        if isinstance(value, Mapping):
            names: Iterable[object] = (
                _OPERATOR_PREFIX.sub("", name) if key.casefold() == "filter" and isinstance(name, str) else name
                for name in value
            )
        elif isinstance(value, Sequence) and not isinstance(value, str):
            # ``select`` lists names; a REST 3.0 ``filter`` lists ``[field, operator, value]`` conditions.
            names = (
                item[0] if _is_v3_condition(item) else item
                for item in value
                if isinstance(item, str) or _is_v3_condition(item)
            )
        else:
            continue
        # A name without a letter or digit, such as ``*``, cannot carry a credential and stays readable.
        yield from (name for name in names if isinstance(name, str) and _HAS_ALPHANUMERIC.search(name))


def _is_v3_condition(item: object) -> bool:
    return (
        isinstance(item, Sequence)
        and not isinstance(item, str)
        and len(item) >= _V3_CONDITION_MINIMUM
        and isinstance(item[0], str)
    )


def _sensitive_values(value: object, *, depth: int) -> Iterable[str]:
    """Yield string values stored under sensitive parameter names anywhere in the request."""
    if depth > _MAX_SECRET_DEPTH:
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if isinstance(key, str) and key.casefold().replace("-", "_") in _SENSITIVE_NAMES and isinstance(item, str):
                yield item
            else:
                yield from _sensitive_values(item, depth=depth + 1)
    elif isinstance(value, Sequence) and not isinstance(value, str):
        for item in value:
            yield from _sensitive_values(item, depth=depth + 1)


__all__ = ["DiagnosticContext"]
