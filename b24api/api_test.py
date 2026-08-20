"""HTTP boundary characterization for the W7 public facade."""

from __future__ import annotations
import json
from typing import TYPE_CHECKING

import httpx
import pytest

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock

from b24api.api import Bitrix24
from b24api.error import ApiResponseError, HTTPGatewayError, IncompleteTraversalError
from b24api.models import ReplaySafety, Request, Response
from b24api.settings import Settings

_WEBHOOK = "https://bitrix24.com/rest/0/test/"
_NEXT_OFFSET = 50
_QUALIFIED_TOTAL = 123
_RETRIED_REQUEST_COUNT = 2
_TIME = {
    "start": 1.0,
    "finish": 2.0,
    "duration": 1.0,
    "processing": 0.5,
    "date_start": "2026-08-20T00:00:00+00:00",
    "date_finish": "2026-08-20T00:00:01+00:00",
}


def _client(*, attempts: int = 1) -> Bitrix24:
    return Bitrix24(
        Settings(
            webhook_url=_WEBHOOK,
            retry_attempts=attempts,
            retry_delay=0,
            retry_backoff=1,
        ),
    )


@pytest.mark.asyncio
async def test_zero_explicit_argument_construction_remains_valid() -> None:
    client = Bitrix24()
    try:
        assert client.host == "bitrix24.com"
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_call_preserves_direct_wire_and_default_result_shape(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_WEBHOOK}profile",
        match_headers={"Content-Type": "application/json"},
        match_json={},
        json={"result": {"ID": "1"}, "time": _TIME},
    )

    async with _client() as client:
        assert client.host == "bitrix24.com"
        assert await client.call({"method": "profile"}) == {"ID": "1"}


@pytest.mark.asyncio
async def test_call_raw_returns_canonical_response_envelope(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_WEBHOOK}crm.item.list",
        match_json={"start": 0},
        json={"result": [{"ID": 1}], "next": _NEXT_OFFSET, "total": _QUALIFIED_TOTAL, "time": _TIME},
    )

    async with _client() as client:
        response = await client.call(
            {"method": "crm.item.list", "parameters": {"start": 0}},
            raw=True,
        )

    assert isinstance(response, Response)
    assert response.result == [{"ID": 1}]
    assert response.next == _NEXT_OFFSET
    assert response.total == _QUALIFIED_TOTAL


@pytest.mark.asyncio
async def test_structured_error_preserves_committed_code_and_safe_request_summary(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_WEBHOOK}crm.item.add",
        json={"error": "ACCESS_DENIED", "error_description": "Denied"},
    )

    async with _client() as client:
        with pytest.raises(ApiResponseError) as captured:
            await client.call(
                {
                    "method": "crm.item.add",
                    "parameters": {"fields": {"TITLE": "must-not-appear"}},
                },
            )

    error = captured.value
    assert error.code == "access_denied"
    assert error.original_code == "ACCESS_DENIED"
    assert error.normalized_code == "access_denied"
    assert error.request_summary is not None
    assert error.request_summary.method == "crm.item.add"
    assert error.request_summary.parameter_keys == ("fields",)
    assert "must-not-appear" not in repr(error)


@pytest.mark.asyncio
async def test_unknown_call_does_not_retry_post_response_gateway_failure(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_WEBHOOK}tasks.task.add",
        status_code=httpx.codes.SERVICE_UNAVAILABLE,
    )

    async with _client(attempts=2) as client:
        with pytest.raises(HTTPGatewayError):
            await client.call({"method": "tasks.task.add"}, retry=True)

    assert len(httpx_mock.get_requests()) == 1


@pytest.mark.asyncio
async def test_explicit_safe_call_retries_eligible_gateway_failure(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_WEBHOOK}profile",
        status_code=httpx.codes.SERVICE_UNAVAILABLE,
    )
    httpx_mock.add_response(
        method="POST",
        url=f"{_WEBHOOK}profile",
        json={"result": {"ID": "1"}},
    )

    async with _client(attempts=2) as client:
        assert await client.call(Request("profile", replay_safety=ReplaySafety.SAFE), retry=True) == {"ID": "1"}

    assert len(httpx_mock.get_requests()) == _RETRIED_REQUEST_COUNT


@pytest.mark.asyncio
async def test_batch_wire_snapshot_uses_reviewed_php_shape_and_preserves_order(httpx_mock: HTTPXMock) -> None:
    expected_cmd = {
        "c000000000000": "profile",
        "c000000000001": "department.get?ID=1",
    }
    httpx_mock.add_response(
        method="POST",
        url=f"{_WEBHOOK}batch",
        match_json={"halt": 1, "cmd": expected_cmd},
        json={
            "result": {
                "result": {
                    "c000000000000": {"ID": "1"},
                    "c000000000001": [{"ID": "2"}],
                },
                "result_error": [],
            },
        },
    )

    async with _client() as client:
        result = [
            item
            async for item in client.batch(
                [
                    {"method": "profile"},
                    {"method": "department.get", "parameters": {"ID": 1}},
                ],
            )
        ]

    assert result == [{"ID": "1"}, [{"ID": "2"}]]
    assert json.loads(httpx_mock.get_requests()[0].content) == {"halt": 1, "cmd": expected_cmd}


@pytest.mark.asyncio
async def test_list_wrapper_marks_unset_request_safe_for_eligible_retry(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_WEBHOOK}department.get",
        status_code=httpx.codes.SERVICE_UNAVAILABLE,
    )
    httpx_mock.add_response(
        method="POST",
        url=f"{_WEBHOOK}department.get",
        json={"result": [], "total": 0},
    )

    async with _client(attempts=2) as client:
        assert [item async for item in client.list_sequential({"method": "department.get"})] == []

    assert len(httpx_mock.get_requests()) == _RETRIED_REQUEST_COUNT


@pytest.mark.asyncio
async def test_list_wrapper_does_not_override_explicit_unsafe_request(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_WEBHOOK}tasks.task.add",
        status_code=httpx.codes.SERVICE_UNAVAILABLE,
    )

    async with _client(attempts=2) as client:
        with pytest.raises(IncompleteTraversalError) as captured:
            _ = [
                item
                async for item in client.list_sequential(
                    Request("tasks.task.add", replay_safety=ReplaySafety.UNSAFE),
                )
            ]

    assert len(httpx_mock.get_requests()) == 1
    assert isinstance(captured.value.__cause__, HTTPGatewayError)


@pytest.mark.asyncio
async def test_mapping_request_rejects_unknown_fields_before_http(httpx_mock: HTTPXMock) -> None:
    async with _client() as client:
        with pytest.raises(ValueError, match="unknown request fields"):
            await client.call({"method": "profile", "token": "forbidden"})

    assert httpx_mock.get_requests() == []
