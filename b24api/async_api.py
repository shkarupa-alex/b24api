from collections.abc import AsyncGenerator, AsyncIterable, Iterable
from itertools import batched, islice
from typing import Any

from aioitertools.itertools import batched as abatched
from aioitertools.itertools import chain as achain
from httpx import AsyncClient
from tenacity import AsyncRetrying

from b24api.base_api import BaseBitrix24, BatchedNoCountHelper, ReferenceNoCountHelper
from b24api.entity import ApiTypes, ListRequest, Request, Response


class AsyncBitrix24(BaseBitrix24):
    @staticmethod
    def _http() -> type[AsyncClient]:
        return AsyncClient

    @staticmethod
    def _retry() -> type[AsyncRetrying]:
        return AsyncRetrying

    async def _call(self, request: Request | dict) -> Response:
        """Call any method and return full response."""
        request = Request.model_validate(request)
        self.logger.debug("Sending request: %s", request)

        response = await self.http.post(
            f"{self.settings.webhook_url}{request.method}",
            headers={"Content-Type": "application/json"},
            json=request.model_dump(mode="json")["parameters"],
        )
        response = self._validate_call_response(request, response)
        self.logger.debug("Received response: %s", response)

        return response

    async def call(self, request: Request | dict) -> ApiTypes:
        """Call any method (with retries) and return `result` from response."""
        return (await self.retry(self._call, request)).result

    async def _batch(self, requests: tuple[Request | dict, ...]) -> list[Response]:
        """Call limited batch of methods and return full responses."""
        commands, request = self._batch_requests(requests)

        response = await self._call(request)

        return self._batch_responses(commands, response)

    async def batch(
        self,
        requests: Iterable[Request | dict | tuple[Request | dict, Any]]
        | AsyncIterable[Request | dict | tuple[Request | dict, Any]],
        *,
        batch_size: int | None = None,
        list_method: bool = False,
        with_payload: bool = False,
    ) -> AsyncGenerator[ApiTypes | tuple[ApiTypes, Any]]:
        """Call unlimited sequence of methods within batches and return `result` from responses."""
        batch_size = batch_size or self.settings.batch_size

        if isinstance(requests, AsyncIterable):
            async for batched_requests_ in abatched(requests, batch_size):
                async for item in self._batch_common_body(batched_requests_, list_method, with_payload):
                    yield item
        else:
            for batched_requests_ in batched(requests, batch_size):
                async for item in self._batch_common_body(batched_requests_, list_method, with_payload):
                    yield item

    async def _batch_common_body(
        self,
        requests: tuple[Request | dict | tuple[Request | dict, Any], ...],
        list_method: bool = False,  # noqa: FBT001, FBT002
        with_payload: bool = False,  # noqa: FBT001, FBT002
    ) -> AsyncGenerator[ApiTypes | tuple[ApiTypes, Any]]:
        if with_payload:
            batched_requests, batched_payloads = zip(*requests, strict=True)
        else:
            batched_requests, batched_payloads = requests, None

        batched_responses = await self.retry(self._batch, batched_requests)

        for i, response in enumerate(batched_responses):
            result = response.list_result if list_method else response.result
            if with_payload:
                yield result, batched_payloads[i]
            else:
                yield result

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
        for item in head_response.list_result:
            yield item

        tail_requests = self._list_tail_requests(head_request, head_response, list_size=list_size)
        for tail_request in tail_requests:
            tail_response = await self.retry(self._call, tail_request)

            start = tail_request.parameters["start"]
            if tail_response.next and tail_response.next != start + list_size:
                raise ValueError(
                    f"Expecting next list chunk to start at {start + list_size}. Got: {tail_response.next}",
                )

            for item in tail_response.list_result:
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
        for item in head_response.list_result:
            yield item

        tail_requests = self._list_tail_requests(head_request, head_response, list_size=list_size)
        tail_responses = self.batch(tail_requests, batch_size=batch_size, list_method=True)
        async for tail_response in tail_responses:
            for item in tail_response:
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

        batched_helper = BatchedNoCountHelper(request, id_key, list_size, batch_size)

        boundary_requests = batched_helper.head_request(), batched_helper.tail_request()
        head_result, tail_result = await self.retry(self._batch, boundary_requests)
        for item in head_result.list_result:
            yield item

        body_requests = batched_helper.body_requests(head_result, tail_result)
        body_results = self.batch(body_requests, batch_size=batch_size, list_method=True)
        async for item in achain.from_iterable(body_results):
            yield item

        for item in batched_helper.tail_results(head_result, tail_result):
            yield item

    async def reference_batched_no_count(  # noqa: PLR0913
        self,
        request: ListRequest | dict,
        updates: Iterable[dict | tuple[dict, Any]] | AsyncIterable[dict | tuple[dict, Any]],
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

        reference_helper = ReferenceNoCountHelper(request, updates, id_key, list_size, batch_size, with_payload)

        if isinstance(updates, AsyncIterable):
            async for item in self._reference_batched_no_count_async_updates(reference_helper):
                yield item
        else:
            async for item in self._reference_batched_no_count_sync_updates(reference_helper):
                yield item

    async def _reference_batched_no_count_async_updates(
        self,
        reference_helper: ReferenceNoCountHelper,
    ) -> AsyncGenerator[ApiTypes | tuple[ApiTypes, Any]]:
        head_requests, body_requests = (), []
        async for tail_request in reference_helper.atail_requests():
            body_requests.append(tail_request)
            if len(head_requests) + len(body_requests) < reference_helper.batch_size:
                continue

            body_results, head_requests = await self._reference_batched_no_count_async_updates_batch(
                reference_helper,
                body_requests,
                head_requests,
            )
            body_requests = []
            for item in reference_helper.body_results(body_results):
                yield item

        while head_requests or body_requests:
            body_results, head_requests = await self._reference_batched_no_count_async_updates_batch(
                reference_helper,
                body_requests,
                head_requests,
            )
            body_requests = []
            for item in reference_helper.body_results(body_results):
                yield item

    async def _reference_batched_no_count_async_updates_batch(
        self,
        reference_helper: ReferenceNoCountHelper,
        body_requests: list[ListRequest],
        head_requests: tuple[ListRequest, ...],
    ) -> tuple[list[ApiTypes], tuple[ListRequest, ...]]:
        body_requests = head_requests + tuple(body_requests)
        body_results = [
            r
            async for r in self.batch(
                body_requests,
                batch_size=reference_helper.batch_size,
                list_method=True,
                with_payload=reference_helper.with_payload,
            )
        ]
        head_requests = reference_helper.head_requests(body_requests, body_results)

        return body_results, head_requests

    async def _reference_batched_no_count_sync_updates(
        self,
        reference_helper: ReferenceNoCountHelper,
    ) -> AsyncGenerator[ApiTypes | tuple[ApiTypes, Any]]:
        head_requests = ()
        tail_requests = iter(reference_helper.tail_requests())
        while body_requests := head_requests + tuple(
            islice(tail_requests, reference_helper.batch_size - len(head_requests)),
        ):
            body_results = [
                r
                async for r in self.batch(
                    body_requests,
                    batch_size=reference_helper.batch_size,
                    list_method=True,
                    with_payload=reference_helper.with_payload,
                )
            ]
            head_requests = reference_helper.head_requests(body_requests, body_results)
            for item in reference_helper.body_results(body_results):
                yield item
