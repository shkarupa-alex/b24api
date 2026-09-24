"""The admission audit retains violations under the report's bound, not one per command (A16)."""

from __future__ import annotations
from typing import TYPE_CHECKING

import pytest

from b24api import Command, ReplaySafety, Request, RouteKind
from b24api._audit import _AsyncAuditSource, _SyncAuditSource
from b24api.contracts.report import Violation, ViolationSeverity
from b24api.contracts.violation import MAX_RETAINED_VIOLATIONS

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

COMMANDS = 20_000


def _commands() -> list[Command[int]]:
    request = Request("x.get", replay_safety=ReplaySafety.UNKNOWN, route=RouteKind.BARE)
    return [Command(request, index) for index in range(COMMANDS)]


def _audit(_request: Request) -> Violation:
    return Violation(ViolationSeverity.WARNING, "unknown_request", "request was not declared")


def test_sync_audit_source_retains_bounded_violations() -> None:
    source = _SyncAuditSource(_commands(), _audit)
    assert sum(1 for _ in source) == COMMANDS
    violations = source.violations
    assert len(violations) == MAX_RETAINED_VIOLATIONS
    assert violations[-1].code == "violations_truncated"


@pytest.mark.asyncio
async def test_async_audit_source_retains_bounded_violations() -> None:
    async def commands() -> AsyncIterator[Command[int]]:
        for command in _commands():
            yield command

    source = _AsyncAuditSource(commands(), _audit)
    assert len([command async for command in source]) == COMMANDS
    violations = source.violations
    assert len(violations) == MAX_RETAINED_VIOLATIONS
    assert violations[-1].code == "violations_truncated"
