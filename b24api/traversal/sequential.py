"""Single-response and sequential offset traversal strategies."""

# ruff: noqa: TRY301 - rejected-page evidence is recorded at this transaction boundary

from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast

from b24api.completion.closure import QUALIFIED_TOTAL_REACHED, SINGLE_RESPONSE_COMPLETE
from b24api.contracts.completion import CommandSettlement
from b24api.contracts.report import PageDispatch, PageRejectionCode
from b24api.contracts.traversal import OffsetContinuation
from b24api.errors import BudgetExceededError, CapabilityError, PaginationError
from b24api.execution import (
    WorkClass,
)
from b24api.traversal.identity import (
    _initial_offset,
    _next_offset,
    _offset_terminal,
    _Page,
    _PageRejectionError,
    _request_with_controls,
)
from b24api.traversal.sparse import sparse_page_terminal

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts.json import FrozenJson, JsonValue
    from b24api.contracts.request import ParameterPath, Request
    from b24api.contracts.response import Response
    from b24api.traversal.plans import (
        CountedOffsetPlan,
        OffsetSequentialPlan,
        SingleResponsePlan,
    )


class _SequentialMixin:
    terminal_reason: str | None
    cursor_state: JsonValue

    async def _single(self: Any, plan: SingleResponsePlan) -> AsyncGenerator[_Page]:
        response = await self._fetch(self.request)
        trace_count = self.page_trace_count
        items: tuple[FrozenJson, ...] = ()
        try:
            frozen_result = response._frozen_result()  # noqa: SLF001 - whole-result fan-out stays immutable internally
            qualified_count = (
                len(frozen_result)
                if self._single_result_as_item and self.selector.path == () and isinstance(frozen_result, tuple)
                else None
            )
            items = (
                (frozen_result,)
                if self._single_result_as_item and self.selector.path == ()
                else self.select_page(response, single=True)
            )
            if qualified_count is None:
                qualified_count = len(items)
            if plan.reject_continuation and response.next is not None:
                raise CapabilityError("single-response plan observed a continuation")
            if (
                plan.reject_positive_total_over_result
                and response.total is not None
                and response.total > qualified_count
            ):
                raise CapabilityError("single-response plan observed a larger qualified total")
            self._validate_page(items, response=response, qualified_count=qualified_count, terminal=True)
        except BaseException as error:
            if self.page_trace_count == trace_count:
                self.reject_external_page(items, response, error)
            raise
        self.terminal_reason = SINGLE_RESPONSE_COMPLETE
        item_weights = (qualified_count,) if self._single_result_as_item else (1,) * len(items)
        yield _Page(tuple(items), response, item_weights, continuing=False)

    async def _offset(  # noqa: C901, PLR0912 - one ordered page transaction with two closure variants
        self: Any,
        plan: OffsetSequentialPlan,
    ) -> AsyncGenerator[_Page]:
        offset = _initial_offset(self.request, plan.offset_path, default=plan.initial_control)
        sparse = plan.sparse_raw_bound
        if sparse is not None and offset != 0:
            raise CapabilityError("sparse raw traversal requires the complete range from offset zero")
        expected_raw_total: int | None = None
        pending_short_window = False
        short_page_width = plan.short_page_width or plan.fixed_step
        self.cursor_state = offset
        visited_offsets: set[int] = set()
        while True:
            if sparse is not None and len(visited_offsets) >= sparse.max_pages:
                raise BudgetExceededError("sparse raw page budget exhausted")
            if offset in visited_offsets:
                raise PaginationError("offset cycle detected")
            visited_offsets.add(offset)
            self.schedule_page(offset=offset, dispatch=PageDispatch.DIRECT)
            updates: dict[ParameterPath, object] = {plan.offset_path: offset}
            if plan.limit_path is not None and plan.requested_page_size is not None:
                updates[plan.limit_path] = plan.requested_page_size
            response = await self._fetch(
                _request_with_controls(
                    self.request,
                    updates,
                    allow_create=plan.allow_create_controls,
                    replace=frozenset({plan.offset_path}),
                ),
            )
            trace_count = self.page_trace_count
            items: tuple[FrozenJson, ...] = ()
            try:
                items = self.select_page(response)
                if pending_short_window and (items or not plan.allow_empty_after_short_window):
                    raise PaginationError("fixed-step traversal cannot prove closure after a short page")
                if sparse is None:
                    terminal = _offset_terminal(
                        plan,
                        response,
                        page_size=len(items),
                        accepted=self.validated_rows + len(items),
                        confirmation=self._confirmation_policy,
                    )
                else:
                    terminal, expected_raw_total = sparse_page_terminal(
                        sparse,
                        response,
                        offset=offset,
                        previous_total=expected_raw_total,
                    )
                if (
                    sparse is None
                    and plan.page_stride is not None
                    and terminal is None
                    and len(items) < plan.page_stride.max_decoded_rows
                ):
                    raise PaginationError("fixed-stride traversal observed an unexplained short page")
                if (
                    sparse is None
                    and plan.page_stride is None
                    and plan.continuation is OffsetContinuation.FIXED_STEP
                    and terminal is None
                    and short_page_width is not None
                    and len(items) < short_page_width
                ):
                    pending_short_window = True
                next_offset = (
                    None if terminal is not None else _next_offset(plan, response, current=offset, observed=len(items))
                )
                if next_offset is not None and next_offset <= offset:
                    raise PaginationError("offset did not advance")
                self._validate_page(items, response=response, terminal=terminal is not None)
            except BaseException as error:
                if self.page_trace_count == trace_count:
                    self.reject_external_page(items, response, error)
                raise
            if items:
                yield _Page(tuple(items), response, (1,) * len(items), terminal is None)
            if terminal is not None:
                self.terminal_reason = terminal
                return
            if next_offset is None:
                raise RuntimeError("non-terminal offset page lacks its validated next offset")
            offset = next_offset
            self.cursor_state = offset

    async def _counted(self: Any, plan: CountedOffsetPlan) -> AsyncGenerator[_Page]:  # noqa: C901
        offset = 0
        self.cursor_state = offset
        visited_offsets: set[int] = set()
        while True:
            if offset in visited_offsets:
                raise PaginationError("counted offset cycle detected")
            visited_offsets.add(offset)
            self.schedule_page(offset=offset, dispatch=PageDispatch.DIRECT)
            updates: dict[ParameterPath, object] = {plan.offset_path: offset}
            if plan.limit_path is not None and plan.requested_page_size is not None:
                updates[plan.limit_path] = plan.requested_page_size
            response = await self._fetch(
                _request_with_controls(
                    self.request,
                    updates,
                    allow_create=plan.allow_create_controls,
                ),
            )
            trace_count = self.page_trace_count
            items: tuple[FrozenJson, ...] = ()
            try:
                items = self.select_page(response)
                prospective_rows = self.validated_rows + len(items)
                effective_total = (
                    response.total if response.total is not None and response.total >= 0 else self._expected_total
                )
                terminal = effective_total is not None and prospective_rows == effective_total
                if not terminal and not items:
                    raise _PageRejectionError(
                        "counted traversal ended before its exact total",
                        PageRejectionCode.RANGE_CONTRADICTION,
                    )
                next_offset = None if terminal else _next_offset(plan, response, current=offset, observed=len(items))
                if next_offset is not None and next_offset <= offset:
                    raise PaginationError("counted offset did not advance")
                self._validate_page(items, response=response, terminal=terminal)
            except BaseException as error:
                if self.page_trace_count == trace_count:
                    self.reject_external_page(items, response, error)
                raise
            if items:
                yield _Page(tuple(items), response, (1,) * len(items), not terminal)
            if self._expected_total is not None and self.validated_rows == self._expected_total:
                self.terminal_reason = QUALIFIED_TOTAL_REACHED
                return
            if next_offset is None:
                raise RuntimeError("non-terminal counted page lacks its validated next offset")
            offset = next_offset
            self.cursor_state = offset

    async def _fetch(self: Any, request: Request) -> Response:
        if self._fetch_override is not None:
            return cast("Response", await self._fetch_override(request))
        recorder = self.completion_recorder
        if recorder is not None:
            recorder.scheduled()
        reservation = None
        try:
            reservation = await self.context.reserve_page()
            response = await self.executor.execute(
                request,
                context=self.context,
                work_class=WorkClass.TRAVERSAL_DIRECT,
            )
            self.context.commit_page(reservation)
            if recorder is not None:
                recorder.settled(CommandSettlement.SUCCESS)
        except BaseException as error:
            if recorder is not None:
                recorder.settled(
                    CommandSettlement.UNKNOWN
                    if bool(getattr(error, "_b24api_dispatch_started", False))
                    else CommandSettlement.NOT_EXECUTED,
                )
            if reservation is not None:
                self.context.release_page(reservation)
            if bool(getattr(error, "_b24api_dispatch_started", False)):
                self.set_page_dispatch(dispatch=PageDispatch.DIRECT)
                self.record_unknown_page(
                    dispatch=PageDispatch.DIRECT,
                    batch_index=None,
                    error=error,
                )
            raise
        return cast("Response", response)
