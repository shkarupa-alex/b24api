"""Support helper."""

from collections.abc import AsyncGenerator, AsyncIterable, Generator, Iterable
from itertools import repeat
from typing import Any, Literal

from b24api.entity import LegacyRequest as Request
from b24api.entity import ListRequest, Response
from b24api.type import ApiTypes


class _NoCountHelperBase:
    """Shared scaffolding for no-count list strategies that page by id range instead of `start=N`."""

    def __init__(
        self,
        request: ListRequest | dict[str, Any],
        id_key: str,
        list_size: int,
        batch_size: int,
    ) -> None:
        """Capture the shared knobs every no-count strategy needs and ensure id is selected."""
        self.request = ListRequest.model_validate(request)
        self.id_key = id_key
        self.list_size = list_size
        self.batch_size = batch_size
        self.id_from = f">{id_key}"
        self.id_to = f"<{id_key}"

        select_ = self.request.parameters.select
        if "*" not in select_ and id_key not in select_:
            self.request.parameters.select.append(id_key)

    def _get_id(self, item: ApiTypes) -> int:
        """Read the configured id key from a result item so it can drive the next page filter."""
        if not isinstance(item, dict):
            raise TypeError(f"Expecting dict, got {type(item)}")
        return int(item[self.id_key])

    def _max_head_id(self, head_result: Response) -> int | None:
        """Largest id in the head page, used as the lower bound for body chunk windows."""
        return max(map(self._get_id, head_result.list_result), default=None)


class BatchedNoCountHelper(_NoCountHelperBase):
    """Plan a single-table no-count gather: head + tail boundaries, then mid-range body chunks."""

    def __init__(self, request: ListRequest | dict[str, Any], id_key: str, list_size: int, batch_size: int) -> None:
        """Reserve `>ID`/`<ID` filter and ordering so the no-count algorithm controls them."""
        super().__init__(request, id_key, list_size, batch_size)

        filter_ = self.request.parameters.filter
        if filter_ and (self.id_from in filter_ or self.id_to in filter_):
            raise ValueError(
                f"Filter parameters `{self.id_from}` and `{self.id_to}` are reserved in `list_batched_no_count`",
            )

        if self.request.parameters.order:
            raise ValueError("Ordering parameters are reserved in `list_batched_no_count`")

    def head_request(self) -> ListRequest:
        """Boundary request for the lowest ids; anchors the bottom of the range."""
        request = self.request.model_copy(deep=True)
        request.parameters.start = -1
        request.parameters.order = {self.id_key: "ASC"}
        return request

    def tail_request(self) -> ListRequest:
        """Boundary request for the highest ids; anchors the top of the range."""
        request = self.request.model_copy(deep=True)
        request.parameters.start = -1
        request.parameters.order = {self.id_key: "DESC"}
        return request

    def body_requests(self, head_result: Response, tail_result: Response) -> Generator[ListRequest]:
        """Mid-range requests filling the gap between head and tail in `list_size` strides."""
        max_head_id = self._max_head_id(head_result)
        min_tail_id = min(map(self._get_id, tail_result.list_result), default=None)

        if max_head_id and min_tail_id and max_head_id < min_tail_id:
            for start in range(max_head_id, min_tail_id, self.list_size):
                body_request = self.head_request()
                body_request.parameters.filter[self.id_from] = start
                body_request.parameters.filter[self.id_to] = min(start + self.list_size + 1, min_tail_id)
                yield body_request

    def tail_results(self, head_result: Response, tail_result: Response) -> Generator[ApiTypes]:
        """Tail-page items above the head boundary, so callers append without overlap."""
        max_head_id = self._max_head_id(head_result)
        for item in reversed(tail_result.list_result):
            if max_head_id is not None and self._get_id(item) > max_head_id:
                yield item


class ReferenceNoCountHelper(_NoCountHelperBase):
    """Plan a no-count gather across many references in parallel via `>ID` continuation filters."""

    def __init__(  # noqa: PLR0913
        self,
        request: ListRequest | dict[str, Any],
        updates: (
            Iterable[dict[str, Any] | tuple[dict[str, Any], Any]]
            | AsyncIterable[dict[str, Any] | tuple[dict[str, Any], Any]]
        ),
        id_key: str,
        list_size: int,
        batch_size: int,
        with_payload: bool,  # noqa: FBT001
    ) -> None:
        """Reserve `>ID` filter and ordering, and remember the per-reference update stream."""
        super().__init__(request, id_key, list_size, batch_size)

        filter_ = self.request.parameters.filter
        if filter_ and self.id_from in filter_:
            raise ValueError(
                f"Filter parameters `{self.id_from}` is reserved in `reference_batched_no_count`",
            )

        if self.request.parameters.order:
            raise ValueError("Ordering parameters are reserved in `reference_batched_no_count`")

        self.updates = updates
        self.with_payload = with_payload

    def tail_requests(self) -> Generator[ListRequest | tuple[ListRequest, Any]]:
        """Per-reference first-page requests when references are a synchronous iterable."""
        if isinstance(self.updates, AsyncIterable):
            raise TypeError("Use `atail_requests` to get asynchronous tail requests")

        for update in self.updates:
            yield self._updated_request(update)

    async def atail_requests(self) -> AsyncGenerator[ListRequest | tuple[ListRequest, Any]]:
        """Per-reference first-page requests when references arrive as an `AsyncIterable`."""
        if isinstance(self.updates, Iterable):
            raise TypeError("Use `tail_requests` to get synchronous tail requests")

        async for update in self.updates:
            yield self._updated_request(update)

    def _updated_request(
        self,
        update: dict[str, Any] | tuple[dict[str, Any], Any],
    ) -> ListRequest | tuple[ListRequest, Any]:
        """Merge one reference update into a fresh tail request, preserving payload if requested."""
        payload: Any = None
        if self.with_payload:
            update, payload = update  # type: ignore[assignment]

        if not isinstance(update, dict):
            raise TypeError(f"Expecting dict update, got {type(update)}")

        if self.id_from in update:
            raise ValueError(
                f"Filter parameters `{self.id_from}` is reserved in `reference_batched_no_count`",
            )

        tail_request = self.request.model_copy(deep=True)
        tail_request.parameters.filter |= update
        tail_request.parameters.start = -1
        tail_request.parameters.order = {self.id_key: "ASC"}

        if self.with_payload:
            return tail_request, payload

        return tail_request

    def head_requests(
        self,
        body_requests: tuple[ListRequest | tuple[ListRequest, Any], ...],
        body_results: list[ApiTypes | tuple[ApiTypes, Any]],
    ) -> tuple[ListRequest | tuple[ListRequest, Any], ...]:
        """Continuation requests for references whose previous page was full (more pages remain)."""
        requests: list[ListRequest | tuple[ListRequest, Any]] = []
        for body_request_, body_result_ in zip(body_requests, body_results, strict=True):
            if self.with_payload:
                body_request, body_payload = body_request_
                if not isinstance(body_result_, tuple):
                    raise TypeError(f"Expecting tuple (result, payload), got {type(body_result_)}")
                body_result, _ = body_result_
            else:
                body_request, body_result, body_payload = body_request_, body_result_, None  # type: ignore[assignment]

            if not isinstance(body_result, list):
                continue

            if len(body_result) == self.list_size:
                max_id = max(map(self._get_id, body_result))
                if not isinstance(body_request, ListRequest):
                    raise TypeError(f"Expecting ListRequest, got {type(body_request)}")
                head_request = body_request.model_copy(deep=True)
                head_request.parameters.filter[self.id_from] = max_id
                if self.with_payload:
                    requests.append((head_request, body_payload))
                else:
                    requests.append(head_request)

        return tuple(requests)

    def body_results(
        self,
        results: list[ApiTypes | tuple[ApiTypes, Any]],
    ) -> Generator[ApiTypes | tuple[ApiTypes, Any]]:
        """Flatten and re-pair batch list results so callers see one item at a time."""
        if self.with_payload:
            for item in results:
                if not isinstance(item, tuple):
                    raise TypeError(f"Expecting tuple (result, payload), got {type(item)}")
                result, payload = item
                if isinstance(result, list):
                    yield from zip(result, repeat(payload, len(result)), strict=False)
        else:
            for item in results:
                if isinstance(item, list):
                    yield from item


class CursorNoCountHelper:
    """Plan a no-count gather across many references in parallel via per-reference cursor parameters."""

    def __init__(  # noqa: PLR0913
        self,
        request: Request | dict[str, Any],
        updates: (
            Iterable[dict[str, Any] | tuple[dict[str, Any], Any]]
            | AsyncIterable[dict[str, Any] | tuple[dict[str, Any], Any]]
        ),
        cursor_param: str,
        cursor_field: str,
        cursor_take: Literal["max", "min"],
        list_size: int,
        list_size_param: str,
        batch_size: int,
        result_key: str | None,
        with_payload: bool,  # noqa: FBT001
    ) -> None:
        """Validate reserved cursor params and store config."""
        self.base_request = Request.model_validate(request)
        if cursor_param in self.base_request.parameters:
            raise ValueError(
                f"Parameter `{cursor_param}` is reserved in `reference_cursor_no_count`",
            )
        if cursor_take not in ("max", "min"):
            raise ValueError(f"`cursor_take` must be 'max' or 'min', got {cursor_take!r}")
        self.updates = updates
        self.cursor_param = cursor_param
        self.cursor_field = cursor_field
        self.cursor_take = cursor_take
        self.list_size = list_size
        self.list_size_param = list_size_param
        self.batch_size = batch_size
        self.result_key = result_key
        self.with_payload = with_payload

    def first_request(
        self,
        update: dict[str, Any] | tuple[dict[str, Any], Any],
    ) -> tuple[Request, Any]:
        """First-page request for one reference."""
        payload: Any = None
        if self.with_payload:
            if not isinstance(update, tuple):
                raise TypeError(f"Expecting (dict, payload) tuple, got {type(update)}")
            update, payload = update
        if not isinstance(update, dict):
            raise TypeError(f"Expecting dict update, got {type(update)}")
        if self.cursor_param in update:
            raise ValueError(
                f"Parameter `{self.cursor_param}` is reserved in `reference_cursor_no_count`",
            )

        params: dict[str, Any] = dict(self.base_request.parameters)
        params.update(update)
        params[self.list_size_param] = self.list_size
        return Request(method=self.base_request.method, parameters=params), payload

    def items_from_result(self, result: ApiTypes) -> list[ApiTypes]:
        """Items list from a raw result."""
        if self.result_key is not None:
            if not isinstance(result, dict):
                raise TypeError(f"Expecting dict result with key `{self.result_key}`, got {type(result)}")
            if self.result_key not in result:
                raise KeyError(f"Result does not contain key `{self.result_key}`: {sorted(result)}")
            value = result[self.result_key]
            if not isinstance(value, list):
                raise TypeError(f"Result key `{self.result_key}` must hold a list, got {type(value)}")
            return value

        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            if len(result) != 1:
                raise ValueError(
                    f"Cannot auto-detect items list in multi-key result {sorted(result)}; pass `result_key`",
                )
            value = next(iter(result.values()))
            if not isinstance(value, list):
                raise TypeError(f"Single-key result must hold a list, got {type(value)}")
            return value
        raise TypeError(f"Expecting list or dict result, got {type(result)}")

    def continuation(
        self,
        prev_request: Request,
        items: list[ApiTypes],
        payload: Any,  # noqa: ANN401
    ) -> tuple[Request, Any] | None:
        """Next-page request when the previous page was full."""
        if len(items) < self.list_size:
            return None

        ids: list[int] = []
        for item in items:
            if not isinstance(item, dict):
                raise TypeError(f"Expecting dict item to read cursor field `{self.cursor_field}`, got {type(item)}")
            if self.cursor_field not in item:
                raise KeyError(
                    f"Item does not contain cursor field `{self.cursor_field}`: {sorted(item)}",
                )
            ids.append(int(item[self.cursor_field]))

        cursor_value = max(ids) if self.cursor_take == "max" else min(ids)
        next_params = dict(prev_request.parameters)
        next_params[self.cursor_param] = cursor_value
        return Request(method=prev_request.method, parameters=next_params), payload
