"""The driver surface that page strategies use, so mypy checks every strategy call (§3.6 step 1)."""

from __future__ import annotations
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from b24api.completion.recorder import CompletionSink
    from b24api.contracts.json import FrozenJson, JsonValue
    from b24api.contracts.policy import ConfirmationPolicy
    from b24api.contracts.report import PageDispatch
    from b24api.contracts.request import IdentitySpec, Request, ResultSelector
    from b24api.contracts.response import Response
    from b24api.execution import ExecutionContext, Executor
    from b24api.execution.snapshot import KernelReport
    from b24api.traversal.identity import PageFetch
    from b24api.traversal.page_adaptation import _SourcePageState
    from b24api.traversal.plans import ListPlan
    from b24api.traversal.values import IdentityValue


class StrategyContext(Protocol):
    """One traversal's state and page transaction, as the strategies see it."""

    executor: Executor
    request: Request
    plan: ListPlan
    selector: ResultSelector
    context: ExecutionContext
    completion_recorder: CompletionSink | None
    fetch_override: PageFetch | None
    single_result_as_item: bool
    source_page: _SourcePageState
    initial_cursor: IdentityValue | None
    validated_rows: int
    terminal_reason: str | None
    cursor_state: JsonValue | IdentityValue
    batch_report: KernelReport | None

    @property
    def page_trace_count(self) -> int:
        """Return how many page records the traversal has produced."""
        ...

    @property
    def expected_total(self) -> int | None:
        """Return the exact total the traversal has committed to, if any."""
        ...

    @property
    def confirmation_policy(self) -> ConfirmationPolicy:
        """Return the effective completion confirmation policy."""
        ...

    @property
    def page_offset(self) -> int | None:
        """Return the logical offset of the scheduled page."""
        ...

    def schedule_page(self, *, offset: int | None, dispatch: PageDispatch, batch_index: int | None = None) -> None:
        """Set value-free provenance before one logical page is decoded."""
        ...

    def set_page_dispatch(self, *, dispatch: PageDispatch, batch_index: int | None = None) -> None:
        """Attach physical dispatch provenance without changing the logical cursor."""
        ...

    def record_unknown_page(self, *, dispatch: PageDispatch, batch_index: int | None, error: BaseException) -> None:
        """Record a scheduled page whose rows could not be decoded."""
        ...

    def select_page(self, response: Response, *, single: bool = False) -> tuple[FrozenJson, ...]:
        """Select one scheduled page."""
        ...

    def reject_external_page(self, items: tuple[FrozenJson, ...], response: Response, error: BaseException) -> None:
        """Record a pre-commit rejection exactly once."""
        ...

    def begin_external_validation(self) -> None:
        """Start canonical validation for an external page dispatcher."""
        ...

    def validate_external_page(
        self,
        items: tuple[FrozenJson, ...],
        response: Response,
        *,
        terminal: bool = False,
        empty_source: bool = False,
    ) -> None:
        """Validate one externally dispatched page."""
        ...

    def close_external_validation(self) -> None:
        """Release the identity store retained by external validation."""
        ...

    def validate_page(  # noqa: PLR0913 - mirrors the driver's page transaction
        self,
        items: tuple[FrozenJson, ...],
        *,
        response: Response,
        qualified_count: int | None = None,
        terminal: bool = False,
        identities: list[IdentityValue] | None = None,
        empty_source: bool = False,
    ) -> list[IdentityValue]:
        """Evaluate a page transactionally and commit only after all checks pass."""
        ...

    def extract_identities(self, items: tuple[FrozenJson, ...]) -> list[IdentityValue]:
        """Return the identities of the page's items."""
        ...

    def require_identity(self, plan_name: str) -> IdentitySpec:
        """Return the scalar identity a plan requires, or reject the plan."""
        ...

    def empty_source_head_eligible(
        self,
        response: Response,
        source: tuple[FrozenJson, ...],
        adapted: tuple[FrozenJson, ...],
    ) -> bool:
        """Return whether an unvalidated head may witness an empty source."""
        ...

    async def fetch(self, request: Request) -> Response:
        """Fetch one page directly, accounting for it in the operation budget."""
        ...


__all__ = ["StrategyContext"]
