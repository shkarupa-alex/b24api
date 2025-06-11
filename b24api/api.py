import contextlib
import json
import logging
from collections.abc import Generator, Iterable
from functools import partial
from itertools import chain, islice
from operator import itemgetter

import h2.exceptions
import httpx
from fast_depends import inject
from pydantic import ValidationError
from retry import retry

from b24api.entity import ApiTypes, BatchResult, ErrorResponse, ListRequest, Request, Response
from b24api.error import RetryApiResponseError, RetryHTTPStatusError
from b24api.settings import ApiSettings
from b24api.transport import HttpxClient


class Bitrix24:
    @inject
    def __init__(self, client: HttpxClient, settings: ApiSettings) -> None:
        self.client = client
        self.settings = settings
        self.logger = logging.getLogger("b24api")

        retry_ = partial(
            retry,
            exceptions=(
                httpx.TransportError,
                h2.exceptions.ProtocolError,
                RetryHTTPStatusError,
                RetryApiResponseError,
            ),
            tries=self.settings.retry_tries,
            delay=self.settings.retry_delay,
            backoff=self.settings.retry_backoff,
            logger=self.logger,
        )
        self._call_retry = retry_()(self._call)
        self._batch_retry = retry_()(self._batch)

    def call(self, request: Request | dict) -> ApiTypes:
        """Call any method (with retries) and return just `result`."""
        return self._call_retry(request).result

    def _call(self, request: Request | dict) -> Response:
        """Call any method and return full response."""
        request = Request.model_validate(request)

        self.logger.debug("Sending request: %s", request)

        http_response = self.client.post(
            f"{self.settings.webhook_url}{request.method}",
            headers={"Content-Type": "application/json"},
            json=request.model_dump(mode="json")["parameters"],
        )

        with contextlib.suppress(httpx.ResponseNotRead, json.JSONDecodeError, ValidationError):
            ErrorResponse.model_validate(http_response.json()).raise_error(self.settings.retry_errors)

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

        response = Response.model_validate(json_response)

        self.logger.debug("Received response: %s", response)

        return response

    def batch(
        self,
        requests: Iterable[Request | dict],
        batch_size: int | None = None,
    ) -> Generator[ApiTypes, None, None]:
        """Call infinite sequence of methods within batches and return just `result`s."""
        batch_size = batch_size or self.settings.batch_size

        tail_requests = iter(requests)
        while batched_requests := list(islice(tail_requests, batch_size)):
            for response in self._batch_retry(batched_requests):
                yield response.result

    def _batch(self, requests: Iterable[Request | dict]) -> list[Response]:
        """Call batch of methods and return full responses."""
        commands = {f"_{i}": Request.model_validate(request) for i, request in enumerate(requests)}
        request = Request(
            method="batch",
            parameters={
                "halt": True,
                "cmd": {key: request.query for key, request in commands.items()},
            },
        )

        result = self._call(request).result
        result = BatchResult.model_validate(result)

        responses = []
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

            responses.append(
                Response(
                    result=result.result[key],
                    time=result.result_time[key],
                    total=result.result_total.get(key, None),
                    next=result.result_next.get(key, None),
                ),
            )

        return responses

    def list_sequential(
        self,
        request: Request | dict,
        list_size: int | None = None,
    ) -> Generator[ApiTypes, None, None]:
        """Call `list` method and return full `result`.

        Slow (sequential tail) list gathering for methods without `filter` parameter (e.g. `department.get`).
        """
        request = Request.model_validate(request)
        list_size = list_size or self.settings.list_size

        head_request = request.model_copy(deep=True)
        head_request.parameters["start"] = 0

        head_response = self._call_retry(head_request)
        yield from self._fix_list_result(head_response.result)

        if head_response.next and head_response.next != list_size:
            raise ValueError(f"Expecting list chunk size to be {list_size}. Got: {head_response.next}")

        total = head_response.total or 0
        for start in range(list_size, total, list_size):
            tail_request = head_request.model_copy(deep=True)
            tail_request.parameters["start"] = start
            tail_response = self._call_retry(tail_request)

            if tail_response.next and tail_response.next != start + list_size:
                raise ValueError(
                    f"Expecting next list chunk to start at {start + list_size}. Got: {tail_response.next}",
                )
            yield from self._fix_list_result(tail_response.result)

    def list_batched(
        self,
        request: Request | dict,
        list_size: int | None = None,
        batch_size: int | None = None,
    ) -> Generator[ApiTypes, None, None]:
        """Call `list` method and return full `result`.

        Faster (batched tail) list gathering for methods without `filter` parameter (e.g. `department.get`).
        """
        request = Request.model_validate(request)
        list_size = list_size or self.settings.list_size
        batch_size = batch_size or self.settings.batch_size

        head_request = request.model_copy(deep=True)
        head_request.parameters["start"] = 0

        head_response = self._call_retry(head_request)
        yield from self._fix_list_result(head_response.result)

        if head_response.next and head_response.next != list_size:
            raise ValueError(f"Expecting chunk size to be {list_size}. Got: {head_response.next}")

        def _tail_requests() -> Generator[Request, None, None]:
            total = head_response.total or 0
            for start in range(list_size, total, list_size):
                tail_request = head_request.model_copy(deep=True)
                tail_request.parameters["start"] = start
                yield tail_request

        tail_responses = self.batch(_tail_requests(), batch_size)
        tail_responses = map(self._fix_list_result, tail_responses)
        tail_responses = chain.from_iterable(tail_responses)
        yield from tail_responses

    def list_batched_no_count(
        self,
        request: ListRequest | dict,
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
    ) -> Generator[ApiTypes, None, None]:
        """Call `list` method and return full `result`.

        Fastest (batched, no count) list gathering for methods with `filter` parameter (e.g. `crm.lead.list`).
        """
        request = ListRequest.model_validate(request)
        list_size = list_size or self.settings.list_size
        batch_size = batch_size or self.settings.batch_size

        select_ = request.parameters.select
        if "*" not in select_ and id_key not in select_:
            request.select.append(id_key)

        id_from, id_to = f">{id_key}", f"<{id_key}"
        get_id = itemgetter(id_key)

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

        head_tail_result = self.batch([head_request, tail_request])
        head_result, tail_result = tuple(map(self._fix_list_result, head_tail_result))
        yield from head_result

        max_head_id = max(map(int, map(get_id, head_result)), default=None)
        min_tail_id = min(map(int, map(get_id, tail_result)), default=None)

        def _body_requests() -> Generator[ListRequest, None, None]:
            for start in range(max_head_id, min_tail_id, list_size):
                body_request = head_request.model_copy(deep=True)
                body_request.parameters.filter[id_from] = start
                body_request.parameters.filter[id_to] = min(start + list_size + 1, min_tail_id)
                yield body_request

        if max_head_id and min_tail_id and max_head_id < min_tail_id:
            body = self.batch(_body_requests(), batch_size)
            body = map(self._fix_list_result, body)
            body = chain.from_iterable(body)
            yield from body

        for item in reversed(tail_result):
            if int(get_id(item)) > max_head_id:
                yield item

    def reference_batched_no_count(
        self,
        request: ListRequest | dict,
        updates: Iterable[dict],
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
    ) -> Generator[ApiTypes, None, None]:
        """Call `list` method with reference `updates` to filter and return full `result`.

        Fastest (batched, no count) list gathering for methods with `filter` parameter and required `reference`
        (e.g. `crm.timeline.comment.list`).
        """
        request = ListRequest.model_validate(request)
        list_size = list_size or self.settings.list_size
        batch_size = batch_size or self.settings.batch_size

        select_ = request.parameters.select
        if "*" not in select_ and id_key not in select_:
            request.select.append(id_key)

        id_from = f">{id_key}"
        get_id = itemgetter(id_key)

        filter_ = request.parameters.filter
        if filter_ and id_from in filter_:
            raise ValueError(
                f"Filter parameters `{id_from}` is reserved in `reference_batched_no_count`",
            )

        if request.parameters.order:
            raise ValueError("Ordering parameters are reserved `order`in `reference_batched_no_count`")

        def _tail_requests() -> Generator[ListRequest, None, None]:
            for update in updates:
                if id_from in update:
                    raise ValueError(
                        f"Filter parameters `{id_from}` is reserved in `reference_batched_no_count`",
                    )
                tail_request = request.model_copy(deep=True)
                tail_request.parameters.filter |= update
                tail_request.parameters.start = -1
                tail_request.parameters.order = {"ID": "ASC"}
                yield tail_request

        head_requests = []
        tail_requests = iter(_tail_requests())
        while body_requests := head_requests + list(islice(tail_requests, batch_size - len(head_requests))):
            body_results = self.batch(body_requests, batch_size)
            body_results = map(self._fix_list_result, body_results)

            head_requests = []
            for body_request, body_result in zip(body_requests, body_results, strict=True):
                if len(body_result) == list_size:
                    max_id = max(map(int, map(get_id, body_result)), default=None)
                    head_request = body_request.model_copy(deep=True)
                    head_request.parameters.filter[id_from] = max_id
                    head_requests.append(head_request)

                yield from body_result

    @staticmethod
    def _fix_list_result(result: list | dict[str, list]) -> list:
        """Fix `list` method result to `list of items` structure.

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
