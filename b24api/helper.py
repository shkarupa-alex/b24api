from collections.abc import AsyncGenerator, AsyncIterable, Generator, Iterable
from itertools import repeat
from typing import Any

from b24api.entity import ListRequest, Response
from b24api.type import ApiTypes


class _NoCountHelperBase:
    def __init__(
        self,
        request: ListRequest | dict[str, Any],
        id_key: str,
        list_size: int,
        batch_size: int,
    ) -> None:
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
        if not isinstance(item, dict):
            raise TypeError(f"Expecting dict, got {type(item)}")
        return int(item[self.id_key])

    def _max_head_id(self, head_result: Response) -> int | None:
        return max(map(self._get_id, head_result.list_result), default=None)


class BatchedNoCountHelper(_NoCountHelperBase):
    def __init__(self, request: ListRequest | dict[str, Any], id_key: str, list_size: int, batch_size: int) -> None:
        super().__init__(request, id_key, list_size, batch_size)

        filter_ = self.request.parameters.filter
        if filter_ and (self.id_from in filter_ or self.id_to in filter_):
            raise ValueError(
                f"Filter parameters `{self.id_from}` and `{self.id_to}` are reserved in `list_batched_no_count`",
            )

        if self.request.parameters.order:
            raise ValueError("Ordering parameters are reserved in `list_batched_no_count`")

    def head_request(self) -> ListRequest:
        request = self.request.model_copy(deep=True)
        request.parameters.start = -1
        request.parameters.order = {self.id_key: "ASC"}
        return request

    def tail_request(self) -> ListRequest:
        request = self.request.model_copy(deep=True)
        request.parameters.start = -1
        request.parameters.order = {self.id_key: "DESC"}
        return request

    def body_requests(self, head_result: Response, tail_result: Response) -> Generator[ListRequest]:
        max_head_id = self._max_head_id(head_result)
        min_tail_id = min(map(self._get_id, tail_result.list_result), default=None)

        if max_head_id and min_tail_id and max_head_id < min_tail_id:
            for start in range(max_head_id, min_tail_id, self.list_size):
                body_request = self.head_request()
                body_request.parameters.filter[self.id_from] = start
                body_request.parameters.filter[self.id_to] = min(start + self.list_size + 1, min_tail_id)
                yield body_request

    def tail_results(self, head_result: Response, tail_result: Response) -> Generator[ApiTypes]:
        max_head_id = self._max_head_id(head_result)
        for item in reversed(tail_result.list_result):
            if max_head_id is not None and self._get_id(item) > max_head_id:
                yield item


class ReferenceNoCountHelper(_NoCountHelperBase):
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
        if isinstance(self.updates, AsyncIterable):
            raise TypeError("Use `atail_requests` to get asynchronous tail requests")

        for update in self.updates:
            yield self._updated_request(update)

    async def atail_requests(self) -> AsyncGenerator[ListRequest | tuple[ListRequest, Any]]:
        if isinstance(self.updates, Iterable):
            raise TypeError("Use `tail_requests` to get synchronous tail requests")

        async for update in self.updates:
            yield self._updated_request(update)

    def _updated_request(
        self, update: dict[str, Any] | tuple[dict[str, Any], Any],
    ) -> ListRequest | tuple[ListRequest, Any]:
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
