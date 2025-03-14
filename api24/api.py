import contextlib
from collections.abc import Generator, Iterable
from itertools import batched, chain
from operator import itemgetter

import httpx
from pydantic import ValidationError
from retry import retry

from api24.entity import BaseType, ErrorResponse, ListRequest, Request, ResultBatch, ResultResponse
from api24.error import RetryApiResponseError, RetryHTTPStatusError


class API:
    RETRY_STATUS_CODES: tuple[int] = (
        httpx.codes.LOCKED,
        httpx.codes.TOO_EARLY,
        httpx.codes.TOO_MANY_REQUESTS,
        httpx.codes.INTERNAL_SERVER_ERROR,
        httpx.codes.BAD_GATEWAY,
        httpx.codes.SERVICE_UNAVAILABLE,
        httpx.codes.INSUFFICIENT_STORAGE,
    )
    RETRY_ERROR_CODES: tuple[str] = ("query_limit_exceeded", "operation_time_limit")
    MAX_BATCH_SIZE: int = 50
    MAX_CHUNK_SIZE: int = 50

    def __init__(self, webhook: str) -> None:
        self.webhook = webhook
        self.httpx = httpx.Client(http2=True)

    @retry(RetryApiResponseError, tries=5, delay=5, backoff=2)
    def call(self, request: Request) -> BaseType:
        """Call any method and return just `result` value."""
        response = self._call(request)

        return response.result

    @retry(RetryHTTPStatusError, tries=5, delay=5, backoff=2)
    def _call(self, request: Request) -> ResultResponse:
        """Call any method (with retries) and return full response."""
        http_response = self.httpx.post(
            f"{self.webhook}{request.method}",
            headers={"Content-Type": "application/json"},
            json=request.parameters,
        )

        try:
            json_response = http_response.raise_for_status().json()
        except httpx.HTTPStatusError as error:
            if http_response.status_code in self.RETRY_STATUS_CODES:
                raise RetryHTTPStatusError(
                    str(error),
                    request=error.request,
                    response=error.response,
                ) from error
            raise

        with contextlib.suppress(ValidationError):
            ErrorResponse.model_validate(json_response)

        return ResultResponse.model_validate(json_response)

    def batch(
        self,
        requests: Iterable[Request],
        batch_size: int = MAX_BATCH_SIZE,
    ) -> Generator[BaseType, None, None]:
        """Call sequence of methods within batches and return just `result`s."""
        for requests_batch in batched(requests, batch_size):
            for response in self._batch(requests_batch):
                yield response.result

    def _batch(self, requests: Iterable[Request]) -> Generator[ResultResponse, None, None]:
        """Call batch of methods and return full responses."""
        commands = {f"_{i}": request for i, request in enumerate(requests)}
        request = Request(
            method="batch",
            parameters={
                "halt": True,
                "cmd": {key: request.flat for key, request in commands.items()},
            },
        )

        result = self.call(request)
        for fix_key in ["result_error", "result_total", "result_next"]:
            if isinstance(result[fix_key], list) and not result[fix_key]:
                result[fix_key] = dict(result[fix_key])
        result = ResultBatch.model_validate(result)

        for i in range(len(commands)):
            key = f"_{i}"

            if key in result.result_error:
                error = result.result_error[key]
                ErrorResponse(error=error.error, error_description=error.error_description)

            command = commands[key]
            if key not in result.result:
                raise ValueError(
                    f"Expecting batch `result.result` to contain result for command {{`{key}`: {command}}}. "
                    f"Got: {result}",
                )
            if key not in result.result_time:
                raise ValueError(
                    f"Expecting batch `result.result_time` to contain result for command {{`{key}`: {command}}}. "
                    f"Got: {result}",
                )

            yield ResultResponse(
                result=result.result[key],
                time=result.result_time[key],
                total=result.result_total.get(key, None),
                next=result.result_next.get(key, None),
            )

    def list_sequential(
        self,
        head_request: Request,
        chunk_size: int = MAX_CHUNK_SIZE,
    ) -> Generator[BaseType, None, None]:
        """Call `list` method and return just `result`.

        Slow (sequential tail) fallback for methods without `filter` parameter (e.g. `department.get`).
        """
        head_response = self._call(head_request)
        yield from self._normalize_list(head_response.result)

        if head_response.next and head_response.next != chunk_size:
            raise ValueError(f"Expecting chunk size to be {chunk_size}. Got: {head_response.next}")

        total = head_response.total or 0
        for start in range(chunk_size, total, chunk_size):
            tail_request = head_request.model_copy(deep=True)
            tail_request.parameters |= {"start": start}
            tail_response = self._call(tail_request)

            if tail_response.next and tail_response.next != start + chunk_size:
                raise ValueError(f"Expecting next chunk to start at {start + chunk_size}. Got: {tail_response.next}")

            yield from self._normalize_list(tail_response.result)

    def list_batched(
        self,
        head_request: Request,
        chunk_size: int = MAX_CHUNK_SIZE,
        batch_size: int = MAX_BATCH_SIZE,
    ) -> Generator[BaseType, None, None]:
        """Call `list` method and return just `result`.

        Faster (batched tail) list gathering for methods without `filter` parameter (e.g. `department.get`).
        """
        head_response = self._call(head_request)
        yield from self._normalize_list(head_response.result)

        if head_response.next and head_response.next != chunk_size:
            raise ValueError(f"Expecting chunk size to be {chunk_size}. Got: {head_response.next}")

        def _tail_requests() -> Generator[Request, None, None]:
            total = head_response.total or 0
            for start in range(chunk_size, total, chunk_size):
                tail_request = head_request.model_copy(deep=True)
                tail_request.parameters = (tail_request.parameters or {}) | {"start": start}
                yield tail_request

        tail_responses = self.batch(_tail_requests(), batch_size)
        tail_responses = map(self._normalize_list, tail_responses)
        tail_responses = chain.from_iterable(tail_responses)

        yield from tail_responses

    def list_batched_no_count(
        self,
        request: ListRequest,
        id_key: str = "ID",
        chunk_size: int = MAX_CHUNK_SIZE,
        batch_size: int = MAX_BATCH_SIZE,
    ) -> Generator[BaseType, None, None]:
        select_ = request.parameters.select
        if "*" not in select_ and id_key not in select_:
            request.select.append(id_key)

        id_from, id_to = f">={id_key}", f"<{id_key}"

        filter_ = request.parameters.filter
        if filter_ and (id_from in filter_ or id_to in filter_):
            raise ValueError(f"Fast list gathering reserves `{id_from}` and `{id_to}` filters.")

        if request.parameters.order:
            raise ValueError("Fast list gathering reserves `order` parameter.")

        head_request = request.model_copy(deep=True)
        head_request.parameters.start = -1
        head_request.parameters.order = {"ID": "ASC"}

        tail_request = request.model_copy(deep=True)
        tail_request.parameters.start = -1
        tail_request.parameters.order = {"ID": "DESC"}

        head_tail = self.batch([head_request, tail_request])
        head, tail = tuple(map(self._normalize_list, head_tail))

        get_id = itemgetter(id_key)
        max_head = max(map(int, map(get_id, head)), default=None)
        min_tail = min(map(int, map(get_id, tail)), default=None)

        yield from head

        if max_head < min_tail:

            def _body_requests() -> Generator[Request, None, None]:
                for start in range(max_head + 1, min_tail, chunk_size):
                    body_request = request.model_copy(deep=True)
                    body_request.parameters.start = -1
                    body_request.parameters.filter[id_from] = start
                    body_request.parameters.filter[id_to] = min(start + chunk_size, min_tail)
                    body_request.parameters.order = {"ID": "DESC"}
                    yield body_request

            body = self.batch(_body_requests(), batch_size)
            body = map(self._normalize_list, body)
            body = chain.from_iterable(body)

            yield from body

        for item in reversed(tail):
            if int(get_id(item)) > max_head:
                yield item

    @staticmethod
    def _normalize_list(result: list | dict) -> list:
        """Normalize `list` method result to list of items structure.

        There are two kinds of what `list` method `result` may contain:
        - a list of items (e.g. `department-get` and `disk.folder.getchildren`),
        - a dictionary with single item that contains desired list of items
            (e.g. `tasks` in `tasks.task.list`).
        """
        if not isinstance(result, list | dict):
            raise TypeError(f"Expecting `result` to be a `list` or a `dict`. Got: {result}")

        if not result:
            return []

        if isinstance(result, list):
            return result

        if len(result) != 1:
            raise TypeError(
                f"If `result` is a `dict`, expecting single item. Got: {result}",
            )

        key = next(iter(result))
        value = result[key]

        if not isinstance(value, list):
            raise TypeError(f"If `result` is a `dict`, expecting single `list` item. Got: {result}")

        return value
