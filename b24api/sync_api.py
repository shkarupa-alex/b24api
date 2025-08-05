from collections.abc import Generator, Iterable
from itertools import batched, chain, islice
from typing import Any

from httpx import Client
from tenacity import Retrying

from b24api.base_api import BaseBitrix24, BatchedNoCountHelper, ReferenceNoCountHelper
from b24api.entity import ApiTypes, ListRequest, Request, Response


class SyncBitrix24(BaseBitrix24):
    @staticmethod
    def _http() -> type[Client]:
        return Client

    @staticmethod
    def _retry() -> type[Retrying]:
        return Retrying

    def _call(self, request: Request | dict) -> Response:
        """Call any method and return full response."""
        request = Request.model_validate(request)
        self.logger.debug("Sending request: %s", request)

        response = self.http.post(
            f"{self.settings.webhook_url}{request.method}",
            headers={"Content-Type": "application/json"},
            json=request.model_dump(mode="json")["parameters"],
        )
        response = self._validate_call_response(request, response)
        self.logger.debug("Received response: %s", response)

        return response

    def call(self, request: Request | dict) -> ApiTypes:
        """Call any method (with retries) and return `result` from response."""
        return self.retry(self._call, request).result

    def _batch(self, requests: tuple[Request | dict, ...]) -> list[Response]:
        """Call limited batch of methods and return full responses."""
        commands, request = self._batch_requests(requests)

        response = self._call(request)

        return self._batch_responses(commands, response)

    def batch(
        self,
        requests: Iterable[Request | dict | tuple[Request | dict, Any]],
        *,
        batch_size: int | None = None,
        list_method: bool = False,
        with_payload: bool = False,
    ) -> Generator[ApiTypes | tuple[ApiTypes, Any]]:
        """Call unlimited sequence of methods within batches and return `result` from responses."""
        batch_size = batch_size or self.settings.batch_size

        for batched_requests_ in batched(requests, batch_size):
            if with_payload:
                batched_requests, batched_payloads = zip(*batched_requests_, strict=True)
            else:
                batched_requests, batched_payloads = batched_requests_, None

            batched_responses = self.retry(self._batch, batched_requests)

            for i, response in enumerate(batched_responses):
                result = response.list_result if list_method else response.result
                if with_payload:
                    yield result, batched_payloads[i]
                else:
                    yield result

    def list_sequential(
        self,
        request: Request | dict,
        *,
        list_size: int | None = None,
    ) -> Generator[ApiTypes]:
        """Call `list` method and return full `result`.

        Slow (sequential tail) list gathering for methods without `filter` parameter (e.g. `department.get`).
        """
        request = Request.model_validate(request)
        list_size = list_size or self.settings.list_size

        head_request = request.model_copy(deep=True)
        head_request.parameters["start"] = 0

        head_response = self.retry(self._call, head_request)
        yield from head_response.list_result

        tail_requests = self._list_tail_requests(head_request, head_response, list_size=list_size)
        for tail_request in tail_requests:
            tail_response = self.retry(self._call, tail_request)

            start = tail_request.parameters["start"]
            if tail_response.next and tail_response.next != start + list_size:
                raise ValueError(
                    f"Expecting next list chunk to start at {start + list_size}. Got: {tail_response.next}",
                )

            yield from tail_response.list_result

    def list_batched(
        self,
        request: Request | dict,
        *,
        list_size: int | None = None,
        batch_size: int | None = None,
    ) -> Generator[ApiTypes]:
        """Call `list` method and return full `result`.

        Faster (batched tail) list gathering for methods without `filter` parameter (e.g. `department.get`).
        """
        request = Request.model_validate(request)
        list_size = list_size or self.settings.list_size
        batch_size = batch_size or self.settings.batch_size

        head_request = request.model_copy(deep=True)
        head_request.parameters["start"] = 0

        head_response = self.retry(self._call, head_request)
        yield from head_response.list_result

        tail_requests = self._list_tail_requests(head_request, head_response, list_size=list_size)
        tail_responses = self.batch(tail_requests, batch_size=batch_size, list_method=True)
        tail_responses = chain.from_iterable(tail_responses)
        yield from tail_responses

    def list_batched_no_count(
        self,
        request: ListRequest | dict,
        *,
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
    ) -> Generator[ApiTypes]:
        """Call `list` method and return full `result`.

        Fastest (batched, no count) list gathering for methods with `filter` parameter (e.g. `crm.lead.list`).
        """
        list_size = list_size or self.settings.list_size
        batch_size = batch_size or self.settings.batch_size

        batched_helper = BatchedNoCountHelper(request, id_key, list_size, batch_size)

        boundary_requests = batched_helper.head_request(), batched_helper.tail_request()
        head_result, tail_result = self.retry(self._batch, boundary_requests)
        yield from head_result.list_result

        body_requests = batched_helper.body_requests(head_result, tail_result)
        body_results = self.batch(body_requests, batch_size=batch_size, list_method=True)
        yield from chain.from_iterable(body_results)

        yield from batched_helper.tail_results(head_result, tail_result)

    def reference_batched_no_count(  # noqa: PLR0913
        self,
        request: ListRequest | dict,
        updates: Iterable[dict | tuple[dict, Any]],
        *,
        id_key: str = "ID",
        list_size: int | None = None,
        batch_size: int | None = None,
        with_payload: bool = False,
    ) -> Generator[ApiTypes | tuple[ApiTypes, Any]]:
        """Call `list` method with reference `updates` for `filter` and return full `result`.

        Fastest (batched, no count) list gathering for methods with `filter` parameter and required `reference`
        (e.g. `crm.timeline.comment.list`).
        """
        list_size = list_size or self.settings.list_size
        batch_size = batch_size or self.settings.batch_size

        reference_helper = ReferenceNoCountHelper(request, updates, id_key, list_size, batch_size, with_payload)

        head_requests = ()
        tail_requests = iter(reference_helper.tail_requests())
        while body_requests := head_requests + tuple(islice(tail_requests, batch_size - len(head_requests))):
            body_results = self.batch(body_requests, batch_size=batch_size, list_method=True, with_payload=with_payload)
            body_results = list(body_results)
            head_requests = reference_helper.head_requests(body_requests, body_results)
            yield from reference_helper.body_results(body_results)
