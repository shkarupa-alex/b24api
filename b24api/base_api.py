import contextlib
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterable, Generator, Iterable
from itertools import chain
from operator import itemgetter
from typing import Any

import h2.exceptions
import httpx
from fast_depends import inject
from pydantic import ValidationError
from tenacity import (
    AsyncRetrying,
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from b24api.entity import BatchResult, ErrorResponse, ListRequest, Request, Response
from b24api.error import RetryApiResponseError, RetryHTTPStatusError
from b24api.settings import ApiSettings
from b24api.type import ApiTypes


class BaseBitrix24(ABC):
    @inject
    def __init__(self, settings: ApiSettings) -> None:
        self.settings = settings

        self.logger = logging.getLogger(self.settings.logger_name)
        self.http = self._http()(http2=True, timeout=self.settings.http_timeout)
        self.retry = self._retry()(
            retry=retry_if_exception_type(
                (
                    httpx.TransportError,
                    h2.exceptions.ProtocolError,
                    RetryHTTPStatusError,
                    RetryApiResponseError,
                ),
            ),
            wait=wait_exponential(multiplier=self.settings.retry_delay, exp_base=self.settings.retry_backoff),
            stop=stop_after_attempt(self.settings.retry_attempts),
            before_sleep=before_sleep_log(self.logger, logging.WARNING),
            reraise=True,
        )

    @staticmethod
    @abstractmethod
    def _http() -> type[httpx.Client | httpx.AsyncClient]:
        pass

    @staticmethod
    @abstractmethod
    def _retry() -> type[Retrying | AsyncRetrying]:
        pass

    @property
    def host(self) -> str:
        return self.settings.webhook_url.host

    def _validate_call_response(self, request: Request, response: httpx.Response) -> Response:
        # Checking more informative errors first (content may exist with 5xx status)
        with contextlib.suppress(httpx.ResponseNotRead, ValidationError):
            ErrorResponse.model_validate_json(response.content).raise_error(request, self.settings)

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            if response.status_code in self.settings.retry_statuses:
                raise RetryHTTPStatusError(
                    str(error),
                    request=error.request,
                    response=error.response,
                ) from error
            raise

        return Response.model_validate_json(response.content)

    @staticmethod
    def _batch_requests(requests: tuple[Request | dict, ...]) -> tuple[dict[str, Request], Request]:
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

        return commands, request

    def _batch_responses(
        self,
        commands: dict[str, Request],
        response: Response,
    ) -> list[Response]:
        result = BatchResult.model_validate(response.result)

        responses = []
        for key, command in commands.items():
            if key in result.result_error:
                ErrorResponse.model_validate(result.result_error[key]).raise_error(
                    command,
                    self.settings,
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
        head_request: Request,
        head_response: Response,
        *,
        list_size: int,
    ) -> Generator[Request]:
        if head_response.next and head_response.next != list_size:
            raise ValueError(f"Expecting list chunk size to be {list_size}. Got: {head_response.next}")

        total = head_response.total or 0
        for start in range(list_size, total, list_size):
            tail_request = head_request.model_copy(deep=True)
            tail_request.parameters["start"] = start
            yield tail_request


class BatchedNoCountHelper:
    def __init__(self, request: ListRequest | dict, id_key: str, list_size: int, batch_size: int) -> None:
        request = ListRequest.model_validate(request)

        select_ = request.parameters.select
        if "*" not in select_ and id_key not in select_:
            request.select.append(id_key)

        id_from, id_to = f">{id_key}", f"<{id_key}"

        filter_ = request.parameters.filter
        if filter_ and (id_from in filter_ or id_to in filter_):
            raise ValueError(
                f"Filter parameters `{id_from}` and `{id_to}` are reserved in `list_batched_no_count`",
            )

        if request.parameters.order:
            raise ValueError("Ordering parameters are reserved in `list_batched_no_count`")

        self.request = request
        self.id_key = id_key
        self.list_size = list_size
        self.batch_size = batch_size

        self.get_id = itemgetter(id_key)

        self.id_from = f">{self.id_key}"
        self.id_to = f"<{self.id_key}"

    def head_request(self) -> ListRequest:
        request = self.request.model_copy(deep=True)
        request.parameters.start = -1
        request.parameters.order = {"ID": "ASC"}
        return request

    def tail_request(self) -> ListRequest:
        request = self.request.model_copy(deep=True)
        request.parameters.start = -1
        request.parameters.order = {"ID": "DESC"}
        return request

    def body_requests(self, head_result: Response, tail_result: Response) -> Generator[ListRequest]:
        max_head_id = max(map(int, map(self.get_id, head_result.list_result)), default=None)
        min_tail_id = min(map(int, map(self.get_id, tail_result.list_result)), default=None)

        if max_head_id and min_tail_id and max_head_id < min_tail_id:
            for start in range(max_head_id, min_tail_id, self.list_size):
                body_request = self.head_request()
                body_request.parameters.filter[self.id_from] = start
                body_request.parameters.filter[self.id_to] = min(start + self.list_size + 1, min_tail_id)
                yield body_request

    def tail_results(self, head_result: Response, tail_result: Response) -> Generator[ApiTypes]:
        max_head_id = max(map(int, map(self.get_id, head_result.list_result)), default=None)
        for item in reversed(tail_result.list_result):
            if int(self.get_id(item)) > max_head_id:
                yield item


class ReferenceNoCountHelper:
    def __init__(  # noqa: PLR0913
        self,
        request: ListRequest | dict,
        updates: Iterable[dict | tuple[dict, Any]] | AsyncIterable[dict | tuple[dict, Any]],
        id_key: str,
        list_size: int,
        batch_size: int,
        with_payload: bool,  # noqa: FBT001
    ) -> None:
        request = ListRequest.model_validate(request)

        select_ = request.parameters.select
        if "*" not in select_ and id_key not in select_:
            request.select.append(id_key)

        id_from = f">{id_key}"
        filter_ = request.parameters.filter
        if filter_ and id_from in filter_:
            raise ValueError(
                f"Filter parameters `{id_from}` is reserved in `reference_batched_no_count`",
            )

        if request.parameters.order:
            raise ValueError("Ordering parameters are reserved `order`in `reference_batched_no_count`")

        self.request = request
        self.updates = updates
        self.id_key = id_key
        self.list_size = list_size
        self.batch_size = batch_size
        self.with_payload = with_payload

        self.get_id = itemgetter(id_key)

        self.id_from = f">{self.id_key}"
        self.id_to = f"<{self.id_key}"

    def tail_requests(self) -> Generator[ListRequest | tuple[ListRequest, Any]]:
        if isinstance(self.updates, AsyncIterable):
            raise TypeError("Use `atail_requests` to get asynchronous tail requests")

        for update in self.updates:
            yield self._updated_request(update)

    async def atail_requests(self) -> AsyncGenerator[ListRequest | tuple[ListRequest, Any]]:
        if isinstance(self.updates, Iterable):
            raise TypeError("Use `atail_requests` to get asynchronous tail requests")

        async for update in self.updates:
            yield self._updated_request(update)

    def _updated_request(self, update: dict | tuple[dict, Any]) -> ListRequest | tuple[ListRequest, Any]:
        if self.with_payload:
            update, payload = update
        else:
            payload = None

        if self.id_from in update:
            raise ValueError(
                f"Filter parameters `{self.id_from}` is reserved in `reference_batched_no_count`",
            )

        tail_request = self.request.model_copy(deep=True)
        tail_request.parameters.filter |= update
        tail_request.parameters.start = -1
        tail_request.parameters.order = {"ID": "ASC"}

        if self.with_payload:
            return tail_request, payload

        return tail_request

    def head_requests(
        self,
        body_requests: tuple[ListRequest | tuple[ListRequest, Any], ...],
        body_results: list[ApiTypes | tuple[ApiTypes, Any]],
    ) -> tuple[ListRequest | tuple[ListRequest, Any], ...]:
        requests = []
        for body_request_, body_result_ in zip(body_requests, body_results, strict=True):
            if self.with_payload:
                body_request, body_payload = body_request_
                body_result, _ = body_result_
            else:
                body_request, body_result, body_payload = body_request_, body_result_, None

            id_from = f">{self.id_key}"

            if len(body_result) == self.list_size:
                max_id = max(map(int, map(self.get_id, body_result)))
                head_request = body_request.model_copy(deep=True)
                head_request.parameters.filter[id_from] = max_id
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
            for result, payload in results:
                yield from zip(result, [payload] * len(result), strict=False)
        else:
            yield from chain.from_iterable(results)
