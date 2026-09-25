"""Golden trace projection: the stable public behavior refactors must preserve.

The projection is captured through public seams only: every request a transport observes, every
item a public ``OperationStream`` yields, and the stable fields of each frozen ``OperationReport``.
It deliberately ignores wall-clock values and internal identifiers so a structural refactor that keeps
behavior produces an identical projection. An intentional change of any projected field must be
declared in ``tests/golden/DELTAS.md``; ``tests/golden_test.py`` rejects an undeclared delta.
"""

from __future__ import annotations
import dataclasses
import enum
import hashlib
import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self

from b24api.contracts.report import OperationReport
from b24api.contracts.request import Request

if TYPE_CHECKING:
    from collections.abc import Iterator

# Every report field compared by the golden gate. Fields left out are time dependent (cooldown_seconds)
# or diagnostic payloads whose layout is not a behavioral contract (keyset_execution internals).
REPORT_FIELDS = (
    "state",
    "operation",
    "terminal_reason",
    "exhausted",
    "assurance",
    "admitted",
    "emitted",
    "successes",
    "failures",
    "not_executed",
    "unknown",
    "unique_rows",
    "physical_requests",
    "logical_pages",
    "batch_requests",
    "batch_commands",
    "retries",
    "buffered_commands_high_water",
    "buffered_rows_high_water",
    "active_references_high_water",
    "page_trace_truncated",
)
# The only values excluded from comparison; kept in one place so the exclusion list is reviewable.
EXCLUDED_REPORT_FIELDS = ("cooldown_seconds", "keyset_execution")
ABSENT = "<absent>"


def canonical(value: object) -> object:  # noqa: PLR0911 - one branch per value family
    """Return a deterministic JSON-compatible form without memory addresses or wall-clock values."""
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, enum.Enum):
        return f"{type(value).__name__}.{value.name}"
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest(), "length": len(value)}
    if isinstance(value, BaseException):
        return {"error": type(value).__name__}
    if isinstance(value, OperationReport):
        return report_projection(value)
    if isinstance(value, Mapping):
        return {str(key): canonical(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [canonical(item) for item in value]
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            "type": type(value).__name__,
            **{field.name: canonical(getattr(value, field.name)) for field in dataclasses.fields(value)},
        }
    return {"type": type(value).__name__}


def request_projection(request: object) -> dict[str, object]:
    """Project one observed request onto method, route and normalized wire parameters."""
    if not isinstance(request, Request):
        return {"type": type(request).__name__}
    positional = request.positional
    return {
        "method": request.method,
        "route": request.route.value,
        "replay_safety": request.replay_safety.value,
        "parameters": canonical(request.to_wire_parameters()) if positional is None else None,
        "positional": canonical(positional.to_wire_slots()) if positional is not None else None,
    }


def report_projection(report: OperationReport) -> dict[str, object]:
    """Project the stable fields of a frozen public report."""
    projected: dict[str, object] = {name: canonical(getattr(report, name)) for name in REPORT_FIELDS}
    projected["violations"] = [[item.severity.value, item.code] for item in report.violations]
    projected["page_trace"] = [
        [record.dispatch.value, record.outcome.value, record.rejection_code, record.rows_admitted]
        for record in report.page_trace
    ]
    projected["page_trace"] = canonical(projected["page_trace"])
    return projected


def items_digest(items: list[object]) -> dict[str, object]:
    """Summarize yielded items by count and a digest of their canonical sequence."""
    encoded = json.dumps([canonical(item) for item in items], sort_keys=True, ensure_ascii=False).encode()
    return {"count": len(items), "sha256": hashlib.sha256(encoded).hexdigest()}


class RecordedStream:
    """Transparent proxy that records what one public operation stream yields."""

    def __init__(self, stream: Any, sink: list[RecordedStream]) -> None:  # noqa: ANN401 - any public stream
        """Wrap without pulling."""
        self._stream = stream
        self.items: list[object] = []
        self.error: BaseException | None = None
        sink.append(self)

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401 - transparent delegation
        """Delegate every other attribute to the wrapped stream."""
        return getattr(self._stream, name)

    def __aiter__(self) -> Self:
        """Return this proxy as the iterator so every item is observed."""
        return self

    async def __anext__(self) -> object:
        """Pull through the wrapped stream and record the item or terminal error."""
        try:
            item = await anext(self._stream)
        except StopAsyncIteration:
            raise
        except BaseException as error:
            self.error = error
            raise
        self.items.append(item)
        return item

    async def __aenter__(self) -> Self:
        """Enter the wrapped stream."""
        await self._stream.__aenter__()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Exit the wrapped stream."""
        await self._stream.__aexit__(*exc)

    async def first(self) -> object:
        """Record the bounded first-item helper."""
        result = await self._stream.first()
        self.items.extend(result.value)
        return result

    async def collect(self, *, limit: int) -> object:
        """Record the bounded collection helper."""
        result = await self._stream.collect(limit=limit)
        self.items.extend(result.value)
        return result

    def projection(self) -> dict[str, object]:
        """Return the yielded items digest, terminal error class and frozen report."""
        report = self._stream.report
        return {
            "items": items_digest(self.items),
            "error": None if self.error is None else type(self.error).__name__,
            "report": None if report is None else report_projection(report),
        }


def flatten(value: object, prefix: str = "") -> Iterator[tuple[str, object]]:
    """Flatten a projection into ``dotted.path -> scalar`` pairs for field-level deltas."""
    if isinstance(value, dict):
        if not value:
            yield prefix, {}
        for key, item in value.items():
            yield from flatten(item, f"{prefix}.{key}" if prefix else str(key))
    elif isinstance(value, list):
        if not value:
            yield prefix, []
        for index, item in enumerate(value):
            yield from flatten(item, f"{prefix}.{index}" if prefix else str(index))
    else:
        yield prefix, value
