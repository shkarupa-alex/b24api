"""Private per-request diagnostic context: local field aliases and the request's own exact secrets.

A caller field name is not safe to print merely because it sits in a field position: an unknown
credential can occupy the same slot. The context therefore replaces every name from ``filter``
(without its operator prefix), ``order``, ``fields`` and the string items of ``select`` with a
request-local alias ``field#1``, ``field#2`` ... in first-appearance order, and never exposes the raw
names: it has no serialization, its representation carries only a count, and pickling is refused.
Bitrix field names ignore letter case and the server echoes them in its own, so a name is aliased in
any letter case, letter for letter (``ß`` does not match ``SS``), and spellings that match each other this
way, such as ``TITLE`` and ``title``, share one alias.
It is derived from one canonical request, is used only while rendering that request's errors and is
never retained by an error, a report or a shared execution context.
"""

from __future__ import annotations
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import NoReturn

from b24api.redaction import (
    DEFAULT_SENSITIVE_KEYS,
    REDACTED,
    contains_secret,
    is_secret,
    replace_secrets,
    secret_values,
)

_ALIASED_PARAMETERS = frozenset({"filter", "order", "fields", "select"})
_SENSITIVE_NAMES = frozenset(name.casefold().replace("-", "_") for name in DEFAULT_SENSITIVE_KEYS)
_OPERATOR_PREFIX = re.compile(r"^[^A-Za-z0-9_]+")
_HAS_ALPHANUMERIC = re.compile(r"[A-Za-z0-9]")
# The bare-credential heuristic treats this class as one token; an alias never splits such a run.
_RUN_CHARACTERS = "A-Za-z0-9_-"
# The mark free-text rendering puts in place of what it hid before it aliases; a field named ``redacted`` must
# not relabel it.
_MARK = re.escape(REDACTED)
_V3_CONDITION_MINIMUM = 2
_MAX_SECRET_DEPTH = 32


class _AliasTable:
    """Aliases of declared spellings, found as ``re`` ignores letter case, so one name has one alias.

    ``re`` compares letter for letter, so two spellings can match only at equal length, and two ASCII ones
    exactly when they lower-case alike: that case is one dictionary lookup. A spelling or text outside ASCII
    (the dotless i, the long s, the Kelvin sign) is compared by ``re`` itself against the same-length
    spellings it could match.
    """

    __slots__ = ("_ascii", "_folded", "_other", "count")

    def __init__(self) -> None:
        self._folded: dict[str, str] = {}
        self._ascii: dict[int, list[tuple[str, str]]] = {}
        self._other: dict[int, list[tuple[str, str]]] = {}
        self.count = 0

    def add(self, name: str) -> None:
        """Give ``name`` the alias of a spelling it matches, or the next alias in declaration order."""
        alias = self.find(name)
        if alias is None:
            self.count += 1
            alias = f"field#{self.count}"
        if name.isascii():
            self._folded.setdefault(name.lower(), alias)
            self._ascii.setdefault(len(name), []).append((name, alias))
        else:
            self._other.setdefault(len(name), []).append((name, alias))

    def find(self, text: str) -> str | None:
        """Return the alias of a declared spelling that ``text`` matches ignoring letter case, if any."""
        if text.isascii() and (alias := self._folded.get(text.lower())) is not None:
            return alias
        candidates = self._other.get(len(text), [])
        if not text.isascii():
            candidates = [*self._ascii.get(len(text), []), *candidates]
        if candidates:
            probe = re.compile(re.escape(text), re.IGNORECASE)
            return next((alias for name, alias in candidates if probe.fullmatch(name)), None)
        return None


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
        self._aliases: _AliasTable | None = None
        self._pattern: re.Pattern[str] | None = None
        self._secrets: frozenset[str] = frozenset()

    def __repr__(self) -> str:
        """Return a representation that never contains a field name or a secret."""
        return f"DiagnosticContext(fields={self._resolved().count})"

    def __reduce__(self) -> NoReturn:
        """Refuse serialization: the context holds raw caller field names."""
        raise TypeError("DiagnosticContext cannot be serialized")

    def alias_text(self, text: str) -> str:
        """Replace whole-token occurrences of declared field names, in any letter case, with their aliases."""
        aliases = self._resolved()
        if self._pattern is None:
            return text
        return self._pattern.sub(lambda match: _replacement(match.group(0), aliases), text)

    def is_known_secret(self, value: str, *, ignore_case: bool = False) -> bool:
        """Report whether a whole value equals one of the request's own exact secrets."""
        if self._aliases is None:
            self._resolved()
        return is_secret(value, self._secrets, ignore_case=ignore_case)

    def contains_known_secret(self, text: str) -> bool:
        """Report whether any of the request's own exact secrets occurs in ``text``, ignoring letter case."""
        self._resolved()
        return contains_secret(text, self._secrets)

    def replace_secrets(self, text: str) -> str:
        """Replace the request's own exact secrets wherever they occur."""
        self._resolved()
        return replace_secrets(text, self._secrets)

    def _resolved(self) -> _AliasTable:
        if self._aliases is not None:
            return self._aliases
        secrets = [value for name, value in self._headers if name.casefold().replace("-", "_") in _SENSITIVE_NAMES]
        secrets.extend(_sensitive_values(self._parameters or {}, depth=0))
        self._secrets = secret_values(secrets)
        # An exact secret stays an exact secret: it is redacted, never merely aliased.
        spellings = list(
            dict.fromkeys(name for name in _field_names(self._parameters or {}) if name not in self._secrets)
        )
        aliases = _AliasTable()
        for name in spellings:
            aliases.add(name)
        if spellings:
            # Longest first, so a longer spelling wins over its own prefix at the same position. Only the names
            # ignore case: under a global flag ``[A-Za-z]`` would also take the dotless i, the long s or the
            # Kelvin sign, and a name beside one of them would stop standing as a whole token.
            names = "|".join(re.escape(name) for name in sorted(spellings, key=lambda name: (-len(name), name)))
            self._pattern = re.compile(rf"{_MARK}|(?<![{_RUN_CHARACTERS}])(?i:{names})(?![{_RUN_CHARACTERS}])")
        self._aliases = aliases
        return aliases


def _replacement(matched: str, aliases: _AliasTable) -> str:
    """Keep the mark; give a matched name its alias, and hide it should no alias be found."""
    if matched == REDACTED:
        return matched
    return aliases.find(matched) or REDACTED


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
