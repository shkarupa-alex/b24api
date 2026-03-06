import contextlib
import logging
from collections.abc import AsyncGenerator, AsyncIterable, Generator, Iterable
from itertools import batched, islice
from types import TracebackType
from typing import Any, Self

import h2
import httpx
from aioitertools.itertools import batched as abatched
from fast_depends import inject
from pydantic import ValidationError
from tenacity import AsyncRetrying, before_sleep_log, retry_if_exception_type, stop_after_attempt, wait_exponential

from b24api.entity import BatchResult, ErrorResponse, ListRequest, Request, Response
from b24api.error import RetryApiResponseError, RetryHTTPStatusError
from b24api.helper import BatchedNoCountHelper, ReferenceNoCountHelper
from b24api.settings import ApiSettings
from b24api.type import ApiTypes


class Bitrix24:
    @inject
    def __init__(self, settings: ApiSettings) -> None:
        self._settings = settings

        self._logger = logging.getLogger(self._settings.logger_name)
        self._http = httpx.AsyncClient(http2=True, timeout=self._settings.http_timeout)
        self._retry = AsyncRetrying(
            retry=retry_if_exception_type(
                (
                    httpx.TransportError,
                    h2.exceptions.ProtocolError,
                    RetryHTTPStatusError,
                    RetryApiResponseError,
                ),
            ),
            wait=wait_exponential(multiplier=self._settings.retry_delay, exp_base=self._settings.retry_backoff),
            stop=stop_after_attempt(self._settings.retry_attempts),
            before_sleep=before_sleep_log(self._logger, logging.WARNING),
            reraise=True,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    @property
    def host(self) -> str:
        host = self._settings.webhook_url.host
        if host is None:
            raise ValueError("webhook_url must have a host")
        return host

    async def call(self, request: Request | dict[str, Any]) -> ApiTypes:
        """Call any method (with retries) and return `result` from response."""
        response: Response = await self._retry(self._call, request)
        return response.result

    async def batch(
        self,
        requests: Iterable[Request | dict[str, Any] | tuple[Request | dict[str, Any], Any]]
        | AsyncIterable[Request | dict[str, Any] | tuple[Request | dict[str, Any], Any]],
        *,
        batch_size: int | None = None,
        list_method: bool = False,
        with_payload: bool = False,
    ) -> AsyncGenerator[ApiTypes | tuple[ApiTypes, Any]]:
        """Call unlimited sequence of methods within batches and return `result` from responses."""
        batch_size = batch_size or self._settings.batch_size

        if isinstance(requests, AsyncIterable):
            async for batched_requests_ in abatched(requests, batch_size):
                async for item in self._batch_common_body(batched_requests_, list_method, with_payload):
                    yield item
        else:
            for batched_requests_ in batched(requests, batch_size):
                async for item in self._batch_common_body(batched_requests_, list_method, with_payload):
                    yield item

    async def list_sequential(
        self,
        request: ListRequest | dict[str, Any],
        *,
        list_size: int | None = None,
    ) -> AsyncGenerator[ApiTypes]:
        """Call `list` method and return full `result`.

        Slow (sequential tail) list gathering for methods without `filter` parameter (e.g. `department.get`).
        """
        request = ListRequest.model_validate(request)
        list_size = list_size or self._settings.list_size

        head_request = request.model_copy(deep=True)
        head_request.parameters.start = 0

        head_response: Response = await self._retry(self._call, head_request)
        for item in head_response.list_result:
            yield item

        tail_requests = self._list_tail_requests(head_request, head_response, list_size=list_size)
        for tail_request in tail_requests:
            tail_response: Response = await self._retry(self._call, tail_request)

            start = tail_request.parameters.start
            if start is None:
                raise ValueError("Expecting `start` parameter in tail request")
            if tail_response.next and tail_response.next != start + list_size:
                raise ValueError(
                    f"Expecting next list chunk to start at {start + list_size}. Got: {tail_response.next}",
                )

            for item in tail_response.list_result:
                yield item

    async def list_batched(
        self,
        request: ListRequest | dict[str, Any],
        *,
        list_size: int | None = None,
        batch_size: int | None = None,
    ) -> AsyncGenerator[ApiTypes]:
        """Call `list` method and return full `result`.

        Faster (batched tail) list gathering for methods without `filter` parameter (e.g. `department.get`).
        """
        request = ListRequest.model_validate(request)
        list_size = list_size or self._settings.list_size
        batch_size = batch_size or self._settings.batch_size

        head_request = request.model_copy(deep=True)
        head_request.parameters.start = 0

        head_response: Response = await self._retry(self._call, head_request)
        for item in head_response.list_result:
            yield item

        tail_requests = self._list_tail_requests(head_request, head_response, list_size=list_size)
        tail_responses = self.batch(tail_requests, batch_size=batch_size, list_method=True)
        async for tail_response in tail_responses:
            if isinstance(tail_response, list):
                for item in tail_response:
                    yield item

    async def list_batched_no_count(
        self,
        request: ListRequest | dict[str, Any],
        *,
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
    ) -> AsyncGenerator[ApiTypes]:
        """Call `list` method and return full `result`.

        Fastest (batched, no count) list gathering for methods with `filter` parameter (e.g. `crm.lead.list`).
        """
        list_size = list_size or self._settings.list_size
        batch_size = batch_size or self._settings.batch_size

        batched_helper = BatchedNoCountHelper(request, id_key, list_size, batch_size)

        boundary_requests = batched_helper.head_request(), batched_helper.tail_request()
        boundary_results: list[Response] = await self._retry(self._batch, boundary_requests)
        head_result, tail_result = boundary_results[0], boundary_results[1]
        for item in head_result.list_result:
            yield item

        body_requests = batched_helper.body_requests(head_result, tail_result)
        body_results = self.batch(body_requests, batch_size=batch_size, list_method=True)
        async for body_result in body_results:
            if isinstance(body_result, list):
                for item in body_result:
                    yield item

        for item in batched_helper.tail_results(head_result, tail_result):
            yield item

    async def reference_batched_no_count(  # noqa: PLR0913
        self,
        request: ListRequest | dict[str, Any],
        updates: Iterable[dict[str, Any] | tuple[dict[str, Any], Any]]
        | AsyncIterable[dict[str, Any] | tuple[dict[str, Any], Any]],
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
        list_size = list_size or self._settings.list_size
        batch_size = batch_size or self._settings.batch_size

        reference_helper = ReferenceNoCountHelper(request, updates, id_key, list_size, batch_size, with_payload)

        if isinstance(updates, AsyncIterable):
            async for item in self._reference_batched_no_count_async_updates(reference_helper):
                yield item
        else:
            async for item in self._reference_batched_no_count_sync_updates(reference_helper):
                yield item

    async def _call(self, request: Request | dict[str, Any]) -> Response:
        """Call any method and return full response."""
        request = Request.model_validate(request)
        self._logger.debug("Sending request: %s", request)

        http_response = await self._http.post(
            f"{self._settings.webhook_url}{request.method}",
            headers={"Content-Type": "application/json"},
            json=request.model_dump(mode="json")["parameters"],
        )

        # Checking more informative errors first (content may exist with 5xx status)
        with contextlib.suppress(httpx.ResponseNotRead, ValidationError):
            ErrorResponse.model_validate_json(http_response.content).raise_error(request, self._settings)

        try:
            http_response.raise_for_status()
        except httpx.HTTPStatusError as error:
            if http_response.status_code in self._settings.retry_statuses:
                raise RetryHTTPStatusError(
                    str(error),
                    request=error.request,
                    response=error.response,
                ) from error
            raise

        parsed_response = Response.model_validate_json(http_response.content)

        self._logger.debug("Received response: %s", parsed_response)

        return parsed_response

    async def _batch(self, requests: tuple[Request | dict[str, Any], ...]) -> list[Response]:
        """Call limited batch of methods and return full responses."""
        # Using string keys with equal length to keep requests order and simplify errors extraction
        width = len(str(len(requests)))
        commands = {f"_{i:0>{width}d}": Request.model_validate(request) for i, request in enumerate(requests)}
        request = Request(
            method="batch",
            parameters={
                "halt": True,
                "cmd": {key: request.query for key, request in commands.items()},
            },
        )

        response = await self._call(request)

        result = BatchResult.model_validate(response.result)

        responses: list[Response] = []
        for key, command in commands.items():
            if key in result.result_error:
                ErrorResponse.model_validate(result.result_error[key]).raise_error(
                    command,
                    self._settings,
                )
            if key not in result.result:
                raise ValueError(
                    f"Expecting `result` to contain result for command {{'{key}': '{command}'}}. Got: {result}",
                )
            if key not in result.result_time:
                raise ValueError(
                    f"Expecting `result_time` to contain result for command {{'{key}': '{command}'}}. Got: {result}",
                )

            responses.append(
                Response(
                    result=result.result[key],
                    time=result.result_time[key],
                    total=result.result_total.get(key, None),
                    next=result.result_next.get(key, None),
                ),
            )
        return responses

    @staticmethod
    def _list_tail_requests(
        head_request: ListRequest,
        head_response: Response,
        *,
        list_size: int,
    ) -> Generator[ListRequest]:
        if head_response.next and head_response.next != list_size:
            raise ValueError(f"Expecting list chunk size to be {list_size}. Got: {head_response.next}")

        total = head_response.total or 0
        for start in range(list_size, total, list_size):
            tail_request = head_request.model_copy(deep=True)
            tail_request.parameters.start = start
            yield tail_request

    async def _batch_common_body(
        self,
        requests: tuple[Request | dict[str, Any] | tuple[Request | dict[str, Any], Any], ...],
        list_method: bool = False,  # noqa: FBT001, FBT002
        with_payload: bool = False,  # noqa: FBT001, FBT002
    ) -> AsyncGenerator[ApiTypes | tuple[ApiTypes, Any]]:
        batched_payloads: tuple[Any, ...] | None
        if with_payload:
            batched_requests, batched_payloads = zip(*requests, strict=True)
        else:
            batched_requests, batched_payloads = requests, None

        batched_responses: list[Response] = await self._retry(self._batch, batched_requests)

        for i, response in enumerate(batched_responses):
            result = response.list_result if list_method else response.result
            if with_payload and batched_payloads is not None:
                yield result, batched_payloads[i]
            else:
                yield result

    async def _reference_batched_no_count_async_updates(
        self,
        reference_helper: ReferenceNoCountHelper,
    ) -> AsyncGenerator[ApiTypes | tuple[ApiTypes, Any]]:
        head_requests: tuple[ListRequest | tuple[ListRequest, Any], ...] = ()
        body_requests: list[ListRequest | tuple[ListRequest, Any]] = []
        async for tail_request in reference_helper.atail_requests():
            body_requests.append(tail_request)
            if len(head_requests) + len(body_requests) < reference_helper.batch_size:
                continue

            body_results, head_requests = await self._reference_batched_no_count_updates_batch(
                reference_helper,
                body_requests,
                head_requests,
            )
            body_requests = []
            for item in reference_helper.body_results(body_results):
                yield item

        while head_requests or body_requests:
            body_results, head_requests = await self._reference_batched_no_count_updates_batch(
                reference_helper,
                body_requests,
                head_requests,
            )
            body_requests = []
            for item in reference_helper.body_results(body_results):
                yield item

    async def _reference_batched_no_count_updates_batch(
        self,
        reference_helper: ReferenceNoCountHelper,
        body_requests: list[ListRequest | tuple[ListRequest, Any]],
        head_requests: tuple[ListRequest | tuple[ListRequest, Any], ...],
    ) -> tuple[list[ApiTypes | tuple[ApiTypes, Any]], tuple[ListRequest | tuple[ListRequest, Any], ...]]:
        all_requests = head_requests + tuple(body_requests)
        body_results: list[ApiTypes | tuple[ApiTypes, Any]] = [
            r
            async for r in self.batch(
                all_requests,
                batch_size=reference_helper.batch_size,
                list_method=True,
                with_payload=reference_helper.with_payload,
            )
        ]
        new_head_requests = reference_helper.head_requests(all_requests, body_results)

        return body_results, new_head_requests

    async def _reference_batched_no_count_sync_updates(
        self,
        reference_helper: ReferenceNoCountHelper,
    ) -> AsyncGenerator[ApiTypes | tuple[ApiTypes, Any]]:
        head_requests: tuple[ListRequest | tuple[ListRequest, Any], ...] = ()
        tail_requests = iter(reference_helper.tail_requests())
        while body_requests := head_requests + tuple(
            islice(tail_requests, reference_helper.batch_size - len(head_requests)),
        ):
            body_results: list[ApiTypes | tuple[ApiTypes, Any]] = [
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
