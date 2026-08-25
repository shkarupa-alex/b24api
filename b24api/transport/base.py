"""Transport lifecycle, replay-aware retries, and shared rate coordination."""

from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from b24api.contracts.request import Request

_HTTP_STATUS_MINIMUM = 100
_HTTP_STATUS_MAXIMUM = 599


@dataclass(frozen=True, slots=True)
class WireResponse:
    """Complete bounded transport response consumed by the protocol layer."""

    status_code: int
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        if not _HTTP_STATUS_MINIMUM <= self.status_code <= _HTTP_STATUS_MAXIMUM:
            raise ValueError("HTTP status must be between 100 and 599")
        object.__setattr__(self, "headers", tuple(self.headers))
        object.__setattr__(self, "body", bytes(self.body))

    @property
    def header_map(self) -> dict[str, str]:
        """Return the header map."""
        return {name.casefold(): value for name, value in self.headers}


class Transport(Protocol):
    """One cancellable attempt that honors ``attempt_timeout`` and classifies failures.

    Implementations must not suppress cancellation indefinitely. The executor's
    operation deadline is only a hard public bound when the injected transport
    cooperates with cancellation or returns within ``attempt_timeout``.
    """

    @property
    def host(self) -> str:
        """Return the normalized portal host without credentials."""
        ...

    async def send(
        self,
        request: Request,
        *,
        attempt_timeout: float,
        max_response_bytes: int,
    ) -> WireResponse:
        """Send one transport request attempt."""
        ...
