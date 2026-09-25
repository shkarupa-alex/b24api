"""Bounded request identity and route family, free of policy and error imports."""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum

from b24api.contracts.wire import BodyEncoding
from b24api.redaction import DEFAULT_REDACTOR


class RouteKind(StrEnum):
    """REST endpoint family selected by the caller."""

    BARE = "bare"
    JSON = "json"
    API_V3 = "api_v3"


@dataclass(frozen=True, slots=True)
class RequestSummary:
    """Bounded request identity that intentionally excludes parameter values."""

    method: str
    parameter_keys: tuple[str, ...] = ()
    encoding: BodyEncoding = BodyEncoding.JSON
    header_names: tuple[str, ...] = ()
    route: RouteKind = RouteKind.BARE

    def __post_init__(self) -> None:
        """Validate and normalize instance state."""
        object.__setattr__(self, "method", DEFAULT_REDACTOR.redact_text(self.method))
        object.__setattr__(
            self,
            "parameter_keys",
            tuple(DEFAULT_REDACTOR.redact_text(str(key)) for key in self.parameter_keys[: DEFAULT_REDACTOR.max_items]),
        )
        if not isinstance(self.encoding, BodyEncoding):
            raise TypeError("encoding must be a BodyEncoding")
        if not isinstance(self.route, RouteKind):
            raise TypeError("route must be a RouteKind")
        object.__setattr__(
            self,
            "header_names",
            tuple(
                sorted(
                    DEFAULT_REDACTOR.redact_text(str(name).casefold())
                    for name in self.header_names[: DEFAULT_REDACTOR.max_items]
                ),
            ),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the to dict representation."""
        return {
            "method": self.method,
            "parameter_keys": list(self.parameter_keys),
            "encoding": self.encoding.value,
            "header_names": list(self.header_names),
            "route": self.route.value,
        }


__all__ = ["RequestSummary", "RouteKind"]
