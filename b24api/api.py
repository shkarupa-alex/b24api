import contextlib
from collections.abc import Generator, Iterable
from itertools import chain
from operator import itemgetter

import httpx
from fast_depends import inject
from pydantic import ValidationError
from retry.api import retry_call

from b24api.entity import ApiTypes, BatchResult, ErrorResponse, ListRequest, Request, ResultResponse
from b24api.error import RetryApiResponseError, RetryHTTPStatusError
from b24api.future import batched
from b24api.http import HttpxClient
from b24api.settings import ApiSettings


class Bitrix24:
    @inject
    def __init__(self, client: HttpxClient, settings: ApiSettings) -> None:
        self.client = client
        self.settings = settings

    def call(self, request: Request) -> ApiTypes:
        """Call any method (with retries) and return just `result`."""
        response = retry_call(
            self._call,
            fargs=[request],
            exceptions=(RetryHTTPStatusError, RetryApiResponseError),
            tries=self.settings.retry_tries,
            delay=self.settings.retry_delay,
            backoff=self.settings.retry_backoff,
        )

        return response.result

    def _call(self, request: Request) -> ResultResponse:
        """Call any method and return full response."""
        http_response = self.client.post(
            f"{self.settings.webhook_url}{request.method}",
            headers={"Content-Type": "application/json"},
            json=request.model_dump()["parameters"],
        )

        try:
            json_response = http_response.raise_for_status().json()
        except httpx.HTTPStatusError as error:
            if http_response.status_code in self.settings.retry_statuses:
                raise RetryHTTPStatusError(
                    str(error),
                    request=error.request,
                    response=error.response,
                ) from error
            raise

        with contextlib.suppress(ValidationError):
            ErrorResponse.model_validate(json_response).raise_error(self.settings.retry_errors)

        return ResultResponse.model_validate(json_response)

    def batch(
        self,
        requests: Iterable[Request],
        batch_size: int | None = None,
    ) -> Generator[ApiTypes, None, None]:
        """Call infinite sequence of methods within batches and return just `result`s."""
        batch_size = batch_size or self.settings.batch_size

        for batched_requests in batched(requests, batch_size):
            for response in self._batch(batched_requests):
                yield response.result

    def _batch(self, requests: Iterable[Request]) -> Generator[ResultResponse, None, None]:
        """Call batch of methods and return full responses."""
        commands = {f"_{i}": request for i, request in enumerate(requests)}
        request = Request(
            method="batch",
            parameters={
                "halt": True,
                "cmd": {key: request.query for key, request in commands.items()},
            },
        )

        result = self.call(request)

        for fix_key in ["result_error", "result_total", "result_next"]:
            if isinstance(result[fix_key], list) and not result[fix_key]:
                result[fix_key] = dict(result[fix_key])

        result = BatchResult.model_validate(result)

        for i in range(len(commands)):
            key = f"_{i}"

            if key in result.result_error:
                ErrorResponse.model_validate(result.result_error[key]).raise_error(self.settings.retry_errors)

            command = commands[key]
            if key not in result.result:
                raise ValueError(
                    f"Expecting `result` to contain result for command {{`{key}`: {command}}}. Got: {result}",
                )
            if key not in result.result_time:
                raise ValueError(
                    f"Expecting `result_time` to contain result for command {{`{key}`: {command}}}. Got: {result}",
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
        list_size: int | None = None,
    ) -> Generator[ApiTypes, None, None]:
        """Call `list` method and return full `result`.

        Slow (sequential tail) list gathering for methods without `filter` parameter (e.g. `department.get`).
        """
        list_size = list_size or self.settings.list_size

        head_response = self._call(head_request)
        yield from self._normalize_list(head_response.result)

        if head_response.next and head_response.next != list_size:
            raise ValueError(f"Expecting list chunk size to be {list_size}. Got: {head_response.next}")

        total = head_response.total or 0
        for start in range(list_size, total, list_size):
            tail_request = head_request.model_copy(deep=True)
            tail_request.parameters |= {"start": start}
            tail_response = self._call(tail_request)

            if tail_response.next and tail_response.next != start + list_size:
                raise ValueError(
                    f"Expecting next list chunk to start at {start + list_size}. Got: {tail_response.next}",
                )

            yield from self._normalize_list(tail_response.result)

    def list_batched(
        self,
        head_request: Request,
        list_size: int | None = None,
        batch_size: int | None = None,
    ) -> Generator[ApiTypes, None, None]:
        """Call `list` method and return full `result`.

        Faster (batched tail) list gathering for methods without `filter` parameter (e.g. `department.get`).
        """
        list_size = list_size or self.settings.list_size
        batch_size = batch_size or self.settings.batch_size

        head_response = self._call(head_request)
        yield from self._normalize_list(head_response.result)

        if head_response.next and head_response.next != list_size:
            raise ValueError(f"Expecting chunk size to be {list_size}. Got: {head_response.next}")

        def _tail_requests() -> Generator[Request, None, None]:
            total = head_response.total or 0
            for start in range(list_size, total, list_size):
                tail_request = head_request.model_copy(deep=True)
                tail_request.parameters |= {"start": start}
                yield tail_request

        tail_responses = self.batch(_tail_requests(), batch_size)
        tail_responses = map(self._normalize_list, tail_responses)
        tail_responses = chain.from_iterable(tail_responses)

        yield from tail_responses

    def list_batched_no_count(
        self,
        request: ListRequest,
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
    ) -> Generator[ApiTypes, None, None]:
        """Call `list` method and return full `result`.

        Fastest (batched, no count) list gathering for methods with `filter` parameter (e.g. `crm.lead.list`).
        """
        list_size = list_size or self.settings.list_size
        batch_size = batch_size or self.settings.batch_size

        select_ = request.parameters.select
        if "*" not in select_ and id_key not in select_:
            request.select.append(id_key)

        id_from, id_to = f">={id_key}", f"<{id_key}"

        filter_ = request.parameters.filter
        if filter_ and (id_from in filter_ or id_to in filter_):
            raise ValueError(
                f"Filter parameters `{id_from}` and `{id_to}` are reserved in `list_batched_no_count`",
            )

        if request.parameters.order:
            raise ValueError("Ordering parameters are reserved `order`in `list_batched_no_count`")

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
                for start in range(max_head + 1, min_tail, list_size):
                    body_request = request.model_copy(deep=True)
                    body_request.parameters.start = -1
                    body_request.parameters.filter[id_from] = start
                    body_request.parameters.filter[id_to] = min(start + list_size, min_tail)
                    body_request.parameters.order = {"ID": "ASC"}
                    yield body_request

            body = self.batch(_body_requests(), batch_size)
            body = map(self._normalize_list, body)
            body = chain.from_iterable(body)

            yield from body

        for item in reversed(tail):
            if int(get_id(item)) > max_head:
                yield item

    @staticmethod
    def _normalize_list(result: list | dict[str, list]) -> list:
        """Normalize `list` method result to `list of items` structure.

        There are two kinds of what `list` method `result` may contain:
        - a list of items (e.g. `department-get` and `disk.folder.getchildren`),
        - a dictionary with single item that contains the desired list of items
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
