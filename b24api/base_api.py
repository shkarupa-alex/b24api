import contextlib
import logging
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable
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

    def _estimate_call_response(self, request: Request, response: httpx.Response) -> Response:
        # Checking more informative errors first (content may exist with 5xx status)
        with contextlib.suppress(httpx.ResponseNotRead, ValidationError):
            ErrorResponse.model_validate_json(response.content).raise_error(request, self.settings.retry_errors)

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
    def _estimate_batch_requests(requests: Iterable[Request | dict]) -> tuple[dict[str, Request], Request]:
        # Using string keys to simplify errors extraction
        commands = {f"_{i}": Request.model_validate(request) for i, request in enumerate(requests)}
        request = Request(
            method="batch",
            parameters={
                "halt": True,
                "cmd": {key: request.query for key, request in commands.items()},
            },
        )

        return commands, request

    def _estimate_batch_responses(self, commands: dict[str, Request], result: Any) -> list[Response]:
        result = BatchResult.model_validate(result)

        responses = []
        for key, command in commands.items():
            if key in result.result_error:
                ErrorResponse.model_validate(result.result_error[key]).raise_error(
                    command,
                    self.settings.retry_errors,
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
    def _estimate_list_tail_requests(
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

    @staticmethod
    def _estimate_list_batched_no_count_boundary_requests(
        request: ListRequest | dict,
        id_key: str,
    ) -> tuple[ListRequest, ListRequest]:
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

        head_request = request.model_copy(deep=True)
        head_request.parameters.start = -1
        head_request.parameters.order = {"ID": "ASC"}

        tail_request = request.model_copy(deep=True)
        tail_request.parameters.start = -1
        tail_request.parameters.order = {"ID": "DESC"}

        return head_request, tail_request

    @staticmethod
    def _estimate_list_batched_no_count_body_requests(
        head_request: ListRequest,
        head_result: list,
        tail_result: list,
        id_key: str,
        list_size: int,
    ) -> Generator[ListRequest]:
        get_id = itemgetter(id_key)
        max_head_id = max(map(int, map(get_id, head_result)), default=None)
        min_tail_id = min(map(int, map(get_id, tail_result)), default=None)

        if max_head_id and min_tail_id and max_head_id < min_tail_id:
            id_from, id_to = f">{id_key}", f"<{id_key}"

            for start in range(max_head_id, min_tail_id, list_size):
                body_request = head_request.model_copy(deep=True)
                body_request.parameters.filter[id_from] = start
                body_request.parameters.filter[id_to] = min(start + list_size + 1, min_tail_id)
                yield body_request

    @staticmethod
    def _estimate_reference_batched_no_count_tail_requests(
        request: ListRequest | dict,
        updates: Iterable[dict | tuple[dict, Any]],
        id_key: str,
        with_payload: bool,
    ) -> Generator[ListRequest | tuple[ListRequest, Any]]:
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

        for update in updates:
            if with_payload:
                update, payload = update
            else:
                payload = None

            if id_from in update:
                raise ValueError(
                    f"Filter parameters `{id_from}` is reserved in `reference_batched_no_count`",
                )

            tail_request = request.model_copy(deep=True)
            tail_request.parameters.filter |= update
            tail_request.parameters.start = -1
            tail_request.parameters.order = {"ID": "ASC"}

            if with_payload:
                yield tail_request, payload
            else:
                yield tail_request

    @staticmethod
    def _estimate_reference_batched_no_count_result_next(
        body_request: ListRequest,
        body_result: list,
        body_payload: Any,
        id_key: str,
        list_size: int,
        with_payload: bool,
    ) -> tuple[[list[ListRequest | tuple[ListRequest, Any]]], Iterable[Any | tuple[Any, Any]]]:
        id_from = f">{id_key}"
        get_id = itemgetter(id_key)

        head_requests = []
        body_result = BaseBitrix24._fix_list_result(body_result)

        if len(body_result) == list_size:
            max_id = max(map(int, map(get_id, body_result)), default=None)
            head_request = body_request.model_copy(deep=True)
            head_request.parameters.filter[id_from] = max_id
            if with_payload:
                head_requests.append((head_request, body_payload))
            else:
                head_requests.append(head_request)

        if with_payload:
            body_payload = [body_payload] * len(body_result)
            body_result = zip(body_result, body_payload, strict=True)

        return head_requests, body_result

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
            raise TypeError(f"If `result` is a `dict`, expecting single item to be a `list`. Got: {result}")

        return value
