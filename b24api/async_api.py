from collections.abc import AsyncGenerator, Iterable
from itertools import batched, islice
from operator import itemgetter
from typing import Any

from httpx import AsyncClient
from tenacity import AsyncRetrying

from b24api.base_api import BaseBitrix24
from b24api.entity import ApiTypes, ListRequest, Request, Response


class AsyncBitrix24(BaseBitrix24):
    @staticmethod
    def _http() -> type[AsyncClient]:
        return AsyncClient

    @staticmethod
    def _retry() -> type[AsyncRetrying]:
        return AsyncRetrying

    async def call(self, request: Request | dict) -> ApiTypes:
        """Call any method (with retries) and return `result` from response."""
        return (await self.retry(self._call, request)).result

    async def _call(self, request: Request | dict) -> Response:
        """Call any method and return full response."""
        request = Request.model_validate(request)
        self.logger.debug("Sending request: %s", request)

        response = await self.http.post(
            f"{self.settings.webhook_url}{request.method}",
            headers={"Content-Type": "application/json"},
            json=request.model_dump(mode="json")["parameters"],
        )
        response = self._estimate_call_response(request, response)
        self.logger.debug("Received response: %s", response)

        return response

    async def batch(
        self,
        requests: Iterable[Request | dict | tuple[Request | dict, Any]],
        *,
        batch_size: int | None = None,
        with_payload: bool = False,
    ) -> AsyncGenerator[ApiTypes | tuple[ApiTypes, Any]]:
        """Call unlimited sequence of methods within batches and return `result` from responses."""
        batch_size = batch_size or self.settings.batch_size

        for batched_requests in batched(requests, batch_size):
            if with_payload:
                batched_requests, batched_payloads = zip(*batched_requests, strict=True)
            else:
                batched_payloads = None

            batched_responses = await self.retry(self._batch, batched_requests)
            for i, response in enumerate(batched_responses):
                if with_payload:
                    yield response.result, batched_payloads[i]
                else:
                    yield response.result

    async def _batch(self, requests: Iterable[Request | dict]) -> list[Response]:
        """Call limited batch of methods and return full responses."""
        commands, request = self._estimate_batch_requests(requests)

        result = (await self._call(request)).result

        return self._estimate_batch_responses(commands, result)

    async def list_sequential(
        self,
        request: Request | dict,
        *,
        list_size: int | None = None,
    ) -> AsyncGenerator[ApiTypes]:
        """Call `list` method and return full `result`.

        Slow (sequential tail) list gathering for methods without `filter` parameter (e.g. `department.get`).
        """
        request = Request.model_validate(request)
        list_size = list_size or self.settings.list_size

        head_request = request.model_copy(deep=True)
        head_request.parameters["start"] = 0

        head_response = await self.retry(self._call, head_request)
        for item in self._fix_list_result(head_response.result):
            yield item

        tail_requests = self._estimate_list_tail_requests(head_request, head_response, list_size=list_size)
        for tail_request in tail_requests:
            tail_response = await self.retry(self._call, tail_request)

            start = tail_request.parameters["start"]
            if tail_response.next and tail_response.next != start + list_size:
                raise ValueError(
                    f"Expecting next list chunk to start at {start + list_size}. Got: {tail_response.next}",
                )

            for item in self._fix_list_result(tail_response.result):
                yield item

    async def list_batched(
        self,
        request: Request | dict,
        *,
        list_size: int | None = None,
        batch_size: int | None = None,
    ) -> AsyncGenerator[ApiTypes]:
        """Call `list` method and return full `result`.

        Faster (batched tail) list gathering for methods without `filter` parameter (e.g. `department.get`).
        """
        request = Request.model_validate(request)
        list_size = list_size or self.settings.list_size
        batch_size = batch_size or self.settings.batch_size

        head_request = request.model_copy(deep=True)
        head_request.parameters["start"] = 0

        head_response = await self.retry(self._call, head_request)
        for item in self._fix_list_result(head_response.result):
            yield item

        tail_requests = self._estimate_list_tail_requests(head_request, head_response, list_size=list_size)
        async for tail_response in self.batch(tail_requests, batch_size=batch_size):
            for item in self._fix_list_result(tail_response):
                yield item

    async def list_batched_no_count(
        self,
        request: ListRequest | dict,
        *,
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
    ) -> AsyncGenerator[ApiTypes]:
        """Call `list` method and return full `result`.

        Fastest (batched, no count) list gathering for methods with `filter` parameter (e.g. `crm.lead.list`).
        """
        list_size = list_size or self.settings.list_size
        batch_size = batch_size or self.settings.batch_size

        head_request, tail_request = self._estimate_list_batched_no_count_boundary_requests(request, id_key)

        boundary_results = [r async for r in self.batch([head_request, tail_request], batch_size=batch_size)]
        head_result, tail_result = tuple(map(self._fix_list_result, boundary_results))
        for item in head_result:
            yield item

        body_requests = self._estimate_list_batched_no_count_body_requests(
            head_request,
            head_result,
            tail_result,
            id_key,
            list_size,
        )
        async for body_result in self.batch(body_requests, batch_size=batch_size):
            body_result = self._fix_list_result(body_result)
            for item in body_result:
                yield item

        get_id = itemgetter(id_key)
        max_head_id = max(map(int, map(get_id, head_result)), default=None)
        for item in reversed(tail_result):
            if int(get_id(item)) > max_head_id:
                yield item

    async def reference_batched_no_count(
        self,
        request: ListRequest | dict,
        updates: Iterable[dict | tuple[dict, Any]],
        *,
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
        with_payload: bool = False,
    ) -> AsyncGenerator[ApiTypes | tuple[ApiTypes, Any]]:
        """Call `list` method with reference `updates` for `filter` and return full `result`.

        Fastest (batched, no count) list gathering for methods with `filter` parameter and required `reference`
        (e.g. `crm.timeline.comment.list`).
        """
        list_size = list_size or self.settings.list_size
        batch_size = batch_size or self.settings.batch_size

        head_requests = []
        tail_requests = iter(
            self._estimate_reference_batched_no_count_tail_requests(
                request,
                updates,
                id_key,
                with_payload,
            ),
        )
        while body_requests := head_requests + list(islice(tail_requests, batch_size - len(head_requests))):
            if with_payload:
                body_requests, body_payloads = zip(*body_requests, strict=True)
            else:
                body_payloads = None

            head_requests = []

            i = 0
            async for body_result in self.batch(body_requests, batch_size=batch_size):
                body_request = body_requests[i]
                body_payload = body_payloads[i] if with_payload else None
                head_requests, body_result = self._estimate_reference_batched_no_count_result_next(
                    body_request,
                    body_result,
                    body_payload,
                    id_key,
                    list_size,
                    with_payload,
                )
                for item in body_result:
                    yield item
                i += 1
