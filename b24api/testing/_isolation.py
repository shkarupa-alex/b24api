"""Disposable thread boundary for untrusted conformance cases."""

from __future__ import annotations
import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

_POLL_SECONDS = 0.01


class IsolationDeadlineError(Exception):
    """Value-free deadline raised after abandoning an isolated daemon thread."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class IsolationAbortError(Exception):
    """Value-free marker for a BaseException raised by isolated code."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


@dataclass(slots=True)
class IsolationController:
    """Thread-safe phase and result state shared with the caller loop."""

    deadline: float
    detail: str = "CaseDeadlineExceeded"
    completed: threading.Event = field(default_factory=threading.Event)
    result: object | None = None
    error: BaseException | None = None

    def enter_cleanup(self, seconds: float, *, preceding_detail: str | None) -> None:
        """Start an independent cleanup budget."""
        self.deadline = time.monotonic() + seconds
        self.detail = preceding_detail or "CleanupDeadlineExceeded"


async def run_isolated[T](
    operation: Callable[[IsolationController], Awaitable[T]],
    *,
    case_seconds: float,
) -> T:
    """Run one operation on a disposable daemon-thread event loop."""
    controller = IsolationController(time.monotonic() + case_seconds)

    async def publish() -> None:
        try:
            controller.result = await operation(controller)
        except Exception as error:  # noqa: BLE001 - transport exception crosses as an outcome
            controller.error = error
        except BaseException as error:  # noqa: BLE001 - convert isolated aborts to outcomes
            controller.error = IsolationAbortError(type(error).__name__)
        finally:
            controller.completed.set()

    def worker() -> None:
        try:
            asyncio.run(publish())
        except BaseException as error:  # noqa: BLE001  # pragma: no cover - bootstrap failure
            controller.error = IsolationAbortError(type(error).__name__)
            controller.completed.set()

    threading.Thread(target=worker, name="b24api-conformance-case", daemon=True).start()
    while not controller.completed.is_set():
        remaining = controller.deadline - time.monotonic()
        if remaining <= 0:
            raise IsolationDeadlineError(controller.detail)
        await asyncio.sleep(min(_POLL_SECONDS, remaining))
    if controller.error is not None:
        raise controller.error
    return controller.result  # type: ignore[return-value]
