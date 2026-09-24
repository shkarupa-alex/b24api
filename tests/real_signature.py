"""Tie a documentation stub's methods to the real ``Bitrix24`` signatures (B13).

README, recipe and migration examples run against no-I/O stand-ins, which accept anything. Decorating
a stand-in method with :func:`real_signature` binds each call against the ``Bitrix24`` method of the
same name first, so an example that passes an unknown keyword, misses a required argument or passes
too many positionals fails the way the real call would, with ``TypeError``.
"""

from __future__ import annotations
import functools
import inspect
from typing import TYPE_CHECKING

from b24api import Bitrix24

if TYPE_CHECKING:
    from collections.abc import Callable


def _real(name: str) -> inspect.Signature:
    declared = inspect.getattr_static(Bitrix24, name)
    function = declared.__func__ if isinstance(declared, classmethod | staticmethod) else declared
    return inspect.signature(function)


def real_signature[**P, R](function: Callable[P, R]) -> Callable[P, R]:
    """Bind every call against ``Bitrix24.<same name>`` before running the stand-in."""
    signature = _real(function.__name__)

    @functools.wraps(function)
    def checked(*args: P.args, **kwargs: P.kwargs) -> R:
        signature.bind(*args, **kwargs)
        return function(*args, **kwargs)

    return checked
