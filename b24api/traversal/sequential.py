"""Single-response and sequential offset traversal strategies."""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast

from b24api.errors import CapabilityError, PaginationError
from b24api.execution import (
    WorkClass,
)
from b24api.traversal.identity import (
    _next_offset,
    _offset_terminal,
    _Page,
    _request_with_controls,
)
from b24api.traversal.values import _response_items

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from b24api.contracts.json import JsonValue
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
        qualified_count = (
            len(response.result)
            if self._single_result_as_item and self.selector.path == () and isinstance(response.result, list)
            else None
        )
        items = (
            [response.result]
            if self._single_result_as_item and self.selector.path == ()
            else _response_items(response, self.selector, single=True)
        )
        if qualified_count is None:
            qualified_count = len(items)
        if plan.reject_continuation and response.next is not None:
            raise CapabilityError("single-response plan observed a continuation")
        if plan.reject_positive_total_over_result and response.total is not None and response.total > qualified_count:
            raise CapabilityError("single-response plan observed a larger qualified total")
        self._validate_page(items, response=response, qualified_count=qualified_count)
        self._validate_terminal_total()
        self.terminal_reason = "single response complete"
        item_weights = (qualified_count,) if self._single_result_as_item else (1,) * len(items)
        yield _Page(tuple(items), response, item_weights)

    async def _offset(self: Any, plan: OffsetSequentialPlan) -> AsyncGenerator[_Page]:
        offset = 0
        self.cursor_state = offset
        visited_offsets: set[int] = set()
        while True:
            if offset in visited_offsets:
                raise PaginationError("offset cycle detected")
            visited_offsets.add(offset)
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
            items = _response_items(response, self.selector)
            self._validate_page(items, response=response)
            terminal = _offset_terminal(
                plan,
                response,
                page_size=len(items),
                accepted=self.validated_rows,
                confirmation=self._confirmation_policy,
            )
            if terminal is not None:
                self._validate_terminal_total()
            if items:
                yield _Page(tuple(items), response, (1,) * len(items))
            if terminal is not None:
                self.terminal_reason = terminal
                return
            next_offset = _next_offset(plan, response, current=offset, observed=len(items))
            if next_offset <= offset:
                raise PaginationError("offset did not advance")
            offset = next_offset
            self.cursor_state = offset

    async def _counted(self: Any, plan: CountedOffsetPlan) -> AsyncGenerator[_Page]:
        offset = 0
        self.cursor_state = offset
        visited_offsets: set[int] = set()
        while True:
            if offset in visited_offsets:
                raise PaginationError("counted offset cycle detected")
            visited_offsets.add(offset)
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
            items = _response_items(response, self.selector)
            self._validate_page(items, response=response)
            if items:
                yield _Page(tuple(items), response, (1,) * len(items))
            if self._expected_total is not None and self.validated_rows == self._expected_total:
                self._validate_terminal_total()
                self.terminal_reason = "qualified total reached"
                return
            if not items:
                raise PaginationError("counted traversal ended before its exact total")
            next_offset = response.next if response.next is not None else offset + len(items)
            if next_offset <= offset:
                raise PaginationError("counted offset did not advance")
            offset = next_offset
            self.cursor_state = offset

    async def _fetch(self: Any, request: Request) -> Response:
        if self._fetch_override is not None:
            return cast("Response", await self._fetch_override(request))
        reservation = await self.context.reserve_page()
        try:
            response = await self.executor.execute(
                request,
                context=self.context,
                work_class=WorkClass.TRAVERSAL_DIRECT,
            )
            self.context.commit_page(reservation)
        except BaseException:
            self.context.release_page(reservation)
            raise
        return cast("Response", response)
