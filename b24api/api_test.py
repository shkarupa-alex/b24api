import asyncio
import json
import math
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs

import httpx
import pytest
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from b24api.api import Bitrix24
from b24api.entity import ListRequest
from b24api.error import ApiResponseError, RetryApiResponseError, RetryHTTPStatusError


@pytest.mark.asyncio
async def test_call(httpx_mock: HTTPXMock) -> None:
    result = _DEFAULT_PROFILE
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/profile",
        match_headers={"Content-Type": "application/json"},
        match_json={},
        json={
            "result": result,
            "time": _DEFAULT_TIME,
        },
    )

    api = Bitrix24()
    response = await api.call({"method": "profile"})
    assert response == result


@pytest.mark.asyncio
async def test_call_list(httpx_mock: HTTPXMock) -> None:
    result = _DEFAULT_LEADS
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/crm.lead.list",
        match_headers={"Content-Type": "application/json"},
        match_json={"select": ["ID", "STATUS_ID"], "filter": {">DATE_CREATE": "2024-01-02T03:04:00+03:00"}},
        json={
            "result": result,
            "next": 3,
            "total": 10,
            "time": _DEFAULT_TIME,
        },
    )

    api = Bitrix24()
    response = await api.call(
        {
            "method": "crm.lead.list",
            "parameters": {
                "select": ["ID", "STATUS_ID"],
                "filter": {">DATE_CREATE": datetime(2024, 1, 2, 3, 4, tzinfo=timezone(timedelta(hours=3)))},
            },
        },
    )
    assert response == result


@pytest.mark.asyncio
async def test_call_status_error(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/profile",
        match_headers={"Content-Type": "application/json"},
        match_json={},
        status_code=httpx.codes.NOT_EXTENDED,
        content=b"",
    )

    api = Bitrix24()
    with pytest.raises(httpx.HTTPStatusError):
        await api.call({"method": "profile"})


@pytest.mark.asyncio
async def test_call_retry_status_error(httpx_mock: HTTPXMock, mocker: MockerFixture) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/profile",
        match_headers={"Content-Type": "application/json"},
        match_json={},
        status_code=httpx.codes.TOO_MANY_REQUESTS,
        content=b"",
        is_reusable=True,
    )
    sleep_mock = mocker.patch("asyncio.sleep")

    api = Bitrix24()
    with pytest.raises(RetryHTTPStatusError):
        await api.call({"method": "profile"})

    num_retries = 5
    assert sleep_mock.call_count == num_retries - 1


@pytest.mark.asyncio
async def test_call_api_error(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/profile",
        match_headers={"Content-Type": "application/json"},
        match_json={},
        json={
            "error": "ACCESS_DENIED",
            "error_description": "Method is blocked due to operation time limit.",
        },
        is_reusable=True,
    )

    api = Bitrix24()
    with pytest.raises(ApiResponseError):
        await api.call({"method": "profile"})


@pytest.mark.asyncio
async def test_call_retry_api_error(httpx_mock: HTTPXMock, mocker: MockerFixture) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/profile",
        match_headers={"Content-Type": "application/json"},
        match_json={},
        json={
            "error": "operation_time_limit".upper(),
            "error_description": "Method is blocked due to operation time limit.",
        },
        is_reusable=True,
    )
    sleep_mock = mocker.patch("asyncio.sleep")

    api = Bitrix24()
    with pytest.raises(RetryApiResponseError):
        await api.call({"method": "profile"})

    num_retries = 5
    assert sleep_mock.call_count == num_retries - 1


@pytest.mark.asyncio
async def test_call_status_and_api_error(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/profile",
        match_headers={"Content-Type": "application/json"},
        match_json={},
        status_code=httpx.codes.FORBIDDEN,
        json={
            "error": "ACCESS_DENIED",
            "error_description": "REST API is available only on commercial plans",
        },
    )

    api = Bitrix24()
    with pytest.raises(ApiResponseError):
        await api.call({"method": "profile"})


@pytest.mark.asyncio
async def test_call_retry_status_and_api_error(httpx_mock: HTTPXMock, mocker: MockerFixture) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/profile",
        match_headers={"Content-Type": "application/json"},
        match_json={},
        status_code=httpx.codes.FORBIDDEN,
        json={
            "error": "operation_time_limit".upper(),
            "error_description": "Method is blocked due to operation time limit.",
        },
        is_reusable=True,
    )
    sleep_mock = mocker.patch("asyncio.sleep")

    api = Bitrix24()
    with pytest.raises(RetryApiResponseError):
        await api.call({"method": "profile"})

    num_retries = 5
    assert sleep_mock.call_count == num_retries - 1


@pytest.mark.asyncio
async def test_batch(httpx_mock: HTTPXMock) -> None:
    result = [
        _DEFAULT_PROFILE,
        {"items": _DEFAULT_LEADS},
        [{"ID": "1", "NAME": "Main department", "SORT": 500, "UF_HEAD": "1"}],
    ]
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/batch",
        match_headers={"Content-Type": "application/json"},
        match_json={
            "halt": True,
            "cmd": {
                "_0": "profile",
                "_1": "crm.lead.list?select%5B0%5D=ID&select%5B1%5D=STATUS_ID&start=-1",
                "_2": "department.get?ID=1",
            },
        },
        json={
            "result": {
                "result": {f"_{i}": r for i, r in enumerate(result)},
                "result_error": [],
                "result_total": {"_1": 2, "_2": 1},
                "result_next": [],
                "result_time": {f"_{i}": _DEFAULT_TIME for i in range(3)},
            },
            "time": _DEFAULT_TIME,
        },
    )

    api = Bitrix24()
    response = [
        r
        async for r in api.batch(
            [
                {"method": "profile"},
                {"method": "crm.lead.list", "parameters": {"select": ["ID", "STATUS_ID"], "start": -1}},
                {"method": "department.get", "parameters": {"ID": 1}},
            ],
        )
    ]
    assert response == result


@pytest.mark.asyncio
async def test_batch_payload(httpx_mock: HTTPXMock) -> None:
    result = [
        _DEFAULT_PROFILE,
        {"items": _DEFAULT_LEADS},
        [{"ID": "1", "NAME": "Main department", "SORT": 500, "UF_HEAD": "1"}],
    ]
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/batch",
        match_headers={"Content-Type": "application/json"},
        match_json={
            "halt": True,
            "cmd": {
                "_0": "profile",
                "_1": "crm.lead.list?select%5B0%5D=ID&select%5B1%5D=STATUS_ID&start=-1",
                "_2": "department.get?ID=1",
            },
        },
        json={
            "result": {
                "result": {f"_{i}": r for i, r in enumerate(result)},
                "result_error": [],
                "result_total": {"_1": 2, "_2": 1},
                "result_next": [],
                "result_time": {f"_{i}": _DEFAULT_TIME for i in range(3)},
            },
            "time": _DEFAULT_TIME,
        },
    )

    api = Bitrix24()
    response = [
        r
        async for r in api.batch(
            [
                ({"method": "profile"}, {"payload": 0}),
                (
                    {"method": "crm.lead.list", "parameters": {"select": ["ID", "STATUS_ID"], "start": -1}},
                    {"payload": 1},
                ),
                ({"method": "department.get", "parameters": {"ID": 1}}, {"payload": 2}),
            ],
            with_payload=True,
        )
    ]
    assert response == [(r, {"payload": i}) for i, r in enumerate(result)]


@pytest.mark.asyncio
async def test_batch_api_error(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/batch",
        match_headers={"Content-Type": "application/json"},
        match_json={
            "halt": True,
            "cmd": {"_0": "profile", "_1": "telephony.externalLine.get", "_2": "department.get?ID=1"},
        },
        json={
            "result": {
                "result": {
                    "_0": _DEFAULT_PROFILE,
                },
                "result_error": {"_1": {"error": "insufficient_scope", "error_description": ""}},
                "result_total": [],
                "result_next": [],
                "result_time": {
                    "_0": _DEFAULT_TIME,
                },
            },
            "time": _DEFAULT_TIME,
        },
    )

    api = Bitrix24()
    with pytest.raises(ApiResponseError):
        async for _ in api.batch(
            [
                {"method": "profile"},
                {"method": "telephony.externalLine.get"},
                {"method": "department.get", "parameters": {"ID": 1}},
            ],
        ):
            pass


@pytest.mark.asyncio
async def test_batch_retry_api_error(httpx_mock: HTTPXMock, mocker: MockerFixture) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/batch",
        match_headers={"Content-Type": "application/json"},
        match_json={
            "halt": True,
            "cmd": {"_0": "profile", "_1": "telephony.externalLine.get", "_2": "department.get?ID=1"},
        },
        json={
            "result": {
                "result": {
                    "_0": _DEFAULT_PROFILE,
                },
                "result_error": {"_1": {"error": "operation_time_limit", "error_description": ""}},
                "result_total": [],
                "result_next": [],
                "result_time": {
                    "_0": _DEFAULT_TIME,
                },
            },
            "time": _DEFAULT_TIME,
        },
        is_reusable=True,
    )
    sleep_mock = mocker.patch("asyncio.sleep")

    api = Bitrix24()
    with pytest.raises(RetryApiResponseError):
        async for _ in api.batch(
            [
                {"method": "profile"},
                {"method": "telephony.externalLine.get"},
                {"method": "department.get", "parameters": {"ID": 1}},
            ],
        ):
            pass

    num_retries = 5
    assert sleep_mock.call_count == num_retries - 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("total_items", "list_size"), [(150, 50), (155, 50), (10, 50), (45, 20)])
async def test_list_sequential(httpx_mock: HTTPXMock, total_items: int, list_size: int) -> None:
    result = [{"ID": str(i), "STATUS_ID": "1"} for i in range(total_items)]
    for start in range(0, total_items, list_size):
        httpx_mock.add_response(
            method="POST",
            url="https://bitrix24.com/rest/0/test/crm.lead.list",
            match_headers={"Content-Type": "application/json"},
            match_json={"select": [], "filter": {}, "order": {}, "start": start},
            json={
                "result": result[start : start + list_size],
                "total": total_items,
                "time": _DEFAULT_TIME,
            }
            | ({} if start + list_size >= total_items else {"next": start + list_size}),
        )

    api = Bitrix24()
    response = [
        r
        async for r in api.list_sequential(
            {"method": "crm.lead.list"},
            list_size=list_size,
        )
    ]
    assert response == result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("total_items", "list_size", "batch_size"),
    [(150, 50, 1), (155, 50, 1), (10, 50, 50), (5500, 50, 50)],
)
async def test_list_batched(httpx_mock: HTTPXMock, total_items: int, list_size: int, batch_size: int) -> None:
    result = [{"ID": str(i), "STATUS_ID": "1"} for i in range(total_items)]
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/crm.lead.list",
        match_headers={"Content-Type": "application/json"},
        match_json={"select": [], "filter": {}, "order": {}, "start": 0},
        json={
            "result": result[:list_size],
            "total": total_items,
            "time": _DEFAULT_TIME,
        },
    )
    for batch_start in range(list_size, total_items, list_size * batch_size):
        max_chunks = math.ceil((total_items - batch_start) / batch_size)
        commands, results, times = {}, {}, {}
        for chunk in range(min(batch_size, max_chunks)):
            width = len(str(min(batch_size, max_chunks)))
            start = batch_start + chunk * list_size
            commands[f"_{chunk:0>{width}d}"] = f"crm.lead.list?start={start}"
            results[f"_{chunk:0>{width}d}"] = result[start : start + list_size]
            times[f"_{chunk:0>{width}d}"] = _DEFAULT_TIME
        httpx_mock.add_response(
            method="POST",
            url="https://bitrix24.com/rest/0/test/batch",
            match_headers={"Content-Type": "application/json"},
            match_json={"halt": True, "cmd": commands},
            json={
                "result": {
                    "result": results,
                    "result_error": [],
                    "result_total": [],  # not used
                    "result_next": [],  # not used
                    "result_time": times,
                },
                "total": total_items,
                "time": _DEFAULT_TIME,
            },
        )

    api = Bitrix24()
    response = [
        r
        async for r in api.list_batched(
            {"method": "crm.lead.list"},
            list_size=list_size,
            batch_size=batch_size,
        )
    ]
    assert response == result


@pytest.mark.parametrize(
    ("total_items", "list_size", "batch_size"),
    [(150, 50, 1), (155, 50, 1), (10, 50, 50), (5500, 50, 50)],
)
@pytest.mark.asyncio
async def test_list_batched_no_count(httpx_mock: HTTPXMock, total_items: int, list_size: int, batch_size: int) -> None:
    result = [{"ID": i, "STATUS_ID": "1"} for i in range(total_items)]

    def custom_response(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://bitrix24.com/rest/0/test/batch"

        output = {}
        for key, value in json.loads(request.content)["cmd"].items():
            method, command = value.split("?")
            assert method == "crm.lead.list"

            command = parse_qs(command)
            assert command.pop("select[0]", None) == ["ID"]
            assert command.pop("select[1]", None) == ["STATUS_ID"]
            assert command.pop("filter[>DATE]", None) == ["2025-03-14T14:00:17+03:00"]
            assert command.pop("start", None) == ["-1"]

            reverse = command.pop("order[ID]", None) == ["DESC"]

            from_id = command.pop("filter[>ID]", [-1])
            assert from_id
            assert len(from_id) == 1

            to_id = command.pop("filter[<ID]", [total_items])
            assert to_id
            assert len(to_id) == 1

            assert not command

            from_id = int(from_id[0])
            to_id = int(to_id[0])

            data = [r for r in result if from_id < r["ID"] < to_id]
            data = data[::-1] if reverse else data
            output[key] = data[:list_size]

        return httpx.Response(
            status_code=200,
            json={
                "result": {
                    "result": output,
                    "result_error": [],
                    "result_total": [],
                    "result_next": [],
                    "result_time": dict.fromkeys(output, _DEFAULT_TIME),
                },
                "time": _DEFAULT_TIME,
            },
        )

    httpx_mock.add_callback(custom_response, is_reusable=True)

    api = Bitrix24()
    response = [
        r
        async for r in api.list_batched_no_count(
            {
                "method": "crm.lead.list",
                "parameters": {
                    "select": ["ID", "STATUS_ID"],
                    "filter": {
                        ">DATE": datetime(2025, 3, 14, 14, 0, 17, tzinfo=timezone(timedelta(hours=3))),
                    },
                },
            },
            list_size=list_size,
            batch_size=batch_size,
        )
    ]
    assert response == result


@pytest.mark.parametrize(
    ("total_items", "list_size", "batch_size"),
    [(150, 50, 1), (155, 50, 1), (10, 50, 50)],
)
@pytest.mark.asyncio
async def test_reference_batched_no_count(
    httpx_mock: HTTPXMock,
    total_items: int,
    list_size: int,
    batch_size: int,
) -> None:
    result = [
        {"ID": i + j * total_items, "ENTITY_TYPE": "deal", "ENTITY_ID": j}
        for i in range(total_items)
        for j in range(total_items - i)
    ]
    result = sorted(result, key=lambda r: r["ID"])

    def custom_response(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://bitrix24.com/rest/0/test/batch"

        output = {}
        for key, value in json.loads(request.content)["cmd"].items():
            method, command = value.split("?")
            assert method == "crm.timeline.comment.list"

            command = parse_qs(command)
            assert command.pop("select[0]", None) == ["ID"]
            assert command.pop("select[1]", None) == ["ENTITY_ID"]
            assert command.pop("filter[=ENTITY_TYPE]", None) == ["deal"]
            assert command.pop("order[ID]", None) == ["ASC"]
            assert command.pop("start", None) == ["-1"]

            entity_id = command.pop("filter[=ENTITY_ID]", [-1])
            assert entity_id
            assert len(entity_id) == 1

            from_id = command.pop("filter[>ID]", [-1])
            assert from_id
            assert len(from_id) == 1

            assert not command

            entity_id = int(entity_id[0])
            from_id = int(from_id[0])

            data = [r for r in result if r["ENTITY_ID"] == entity_id and r["ID"] > from_id]
            output[key] = data[:list_size]

        return httpx.Response(
            status_code=200,
            json={
                "result": {
                    "result": output,
                    "result_error": [],
                    "result_total": [],
                    "result_next": [],
                    "result_time": dict.fromkeys(output, _DEFAULT_TIME),
                },
                "time": _DEFAULT_TIME,
            },
        )

    httpx_mock.add_callback(custom_response, is_reusable=True)

    api = Bitrix24()
    response = [
        r
        async for r in api.reference_batched_no_count(
            {
                "method": "crm.timeline.comment.list",
                "parameters": {"select": ["ID", "ENTITY_ID"], "filter": {"=ENTITY_TYPE": "deal"}},
            },
            ({"=ENTITY_ID": i} for i in range(total_items)),
            list_size=list_size,
            batch_size=batch_size,
        )
    ]
    assert sorted(response, key=lambda r: r["ID"]) == result


@pytest.mark.parametrize(
    ("total_items", "list_size", "batch_size"),
    [(150, 50, 1), (155, 50, 1), (10, 50, 50)],
)
@pytest.mark.asyncio
async def test_reference_batched_no_count_payload(
    httpx_mock: HTTPXMock,
    total_items: int,
    list_size: int,
    batch_size: int,
) -> None:
    result = [
        {"ID": i + j * total_items, "ENTITY_TYPE": "deal", "ENTITY_ID": j}
        for i in range(total_items)
        for j in range(total_items - i)
    ]
    result = sorted(result, key=lambda r: r["ID"])

    def custom_response(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://bitrix24.com/rest/0/test/batch"

        output = {}
        for key, value in json.loads(request.content)["cmd"].items():
            method, command = value.split("?")
            assert method == "crm.timeline.comment.list"

            command = parse_qs(command)
            assert command.pop("select[0]", None) == ["ID"]
            assert command.pop("select[1]", None) == ["ENTITY_ID"]
            assert command.pop("filter[=ENTITY_TYPE]", None) == ["deal"]
            assert command.pop("order[ID]", None) == ["ASC"]
            assert command.pop("start", None) == ["-1"]

            entity_id = command.pop("filter[=ENTITY_ID]", [-1])
            assert entity_id
            assert len(entity_id) == 1

            from_id = command.pop("filter[>ID]", [-1])
            assert from_id
            assert len(from_id) == 1

            assert not command

            entity_id = int(entity_id[0])
            from_id = int(from_id[0])

            data = [r for r in result if r["ENTITY_ID"] == entity_id and r["ID"] > from_id]
            output[key] = data[:list_size]

        return httpx.Response(
            status_code=200,
            json={
                "result": {
                    "result": output,
                    "result_error": [],
                    "result_total": [],
                    "result_next": [],
                    "result_time": dict.fromkeys(output, _DEFAULT_TIME),
                },
                "time": _DEFAULT_TIME,
            },
        )

    httpx_mock.add_callback(custom_response, is_reusable=True)

    api = Bitrix24()
    response = [
        r
        async for r in api.reference_batched_no_count(
            {
                "method": "crm.timeline.comment.list",
                "parameters": {"select": ["ID", "ENTITY_ID"], "filter": {"=ENTITY_TYPE": "deal"}},
            },
            (({"=ENTITY_ID": i}, {"payload": i}) for i in range(total_items)),
            list_size=list_size,
            batch_size=batch_size,
            with_payload=True,
        )
    ]
    response = list(response)
    assert response
    assert len(response[0]) == 2  # noqa: PLR2004

    response, payload = zip(*response, strict=False)
    assert sorted(response, key=lambda r: r["ID"]) == result
    assert payload[0] == {"payload": 0}


@pytest.mark.asyncio
async def test_batch_async_requests(httpx_mock: HTTPXMock) -> None:
    result = [
        _DEFAULT_PROFILE,
        {"items": _DEFAULT_LEADS},
        [{"ID": "1", "NAME": "Main department", "SORT": 500, "UF_HEAD": "1"}],
    ]
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/batch",
        match_headers={"Content-Type": "application/json"},
        match_json={
            "halt": True,
            "cmd": {
                "_0": "profile",
                "_1": "crm.lead.list?select%5B0%5D=ID&select%5B1%5D=STATUS_ID&start=-1",
                "_2": "department.get?ID=1",
            },
        },
        json={
            "result": {
                "result": {f"_{i}": r for i, r in enumerate(result)},
                "result_error": [],
                "result_total": {"_1": 2, "_2": 1},
                "result_next": [],
                "result_time": {f"_{i}": _DEFAULT_TIME for i in range(3)},
            },
            "time": _DEFAULT_TIME,
        },
    )

    async def _requests() -> AsyncGenerator[dict]:
        requests = [
            {"method": "profile"},
            {"method": "crm.lead.list", "parameters": {"select": ["ID", "STATUS_ID"], "start": -1}},
            {"method": "department.get", "parameters": {"ID": 1}},
        ]
        for request in requests:
            await asyncio.sleep(0.0001)
            yield request

    api = Bitrix24()
    response = [r async for r in api.batch(_requests())]
    assert response == result


@pytest.mark.parametrize(
    ("total_items", "list_size", "batch_size"),
    [(150, 50, 1), (155, 50, 10), (10, 50, 50)],
)
@pytest.mark.asyncio
async def test_reference_batched_no_count_async_updates(
    httpx_mock: HTTPXMock,
    total_items: int,
    list_size: int,
    batch_size: int,
) -> None:
    result = [
        {"ID": i + j * total_items, "ENTITY_TYPE": "deal", "ENTITY_ID": j}
        for i in range(total_items)
        for j in range(total_items - i)
    ]
    result = sorted(result, key=lambda r: r["ID"])

    def custom_response(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://bitrix24.com/rest/0/test/batch"

        output = {}
        for key, value in json.loads(request.content)["cmd"].items():
            method, command = value.split("?")
            assert method == "crm.timeline.comment.list"

            command = parse_qs(command)
            assert command.pop("select[0]", None) == ["ID"]
            assert command.pop("select[1]", None) == ["ENTITY_ID"]
            assert command.pop("filter[=ENTITY_TYPE]", None) == ["deal"]
            assert command.pop("order[ID]", None) == ["ASC"]
            assert command.pop("start", None) == ["-1"]

            entity_id = command.pop("filter[=ENTITY_ID]", [-1])
            assert entity_id
            assert len(entity_id) == 1

            from_id = command.pop("filter[>ID]", [-1])
            assert from_id
            assert len(from_id) == 1

            assert not command

            entity_id = int(entity_id[0])
            from_id = int(from_id[0])

            data = [r for r in result if r["ENTITY_ID"] == entity_id and r["ID"] > from_id]
            output[key] = data[:list_size]

        return httpx.Response(
            status_code=200,
            json={
                "result": {
                    "result": output,
                    "result_error": [],
                    "result_total": [],
                    "result_next": [],
                    "result_time": dict.fromkeys(output, _DEFAULT_TIME),
                },
                "time": _DEFAULT_TIME,
            },
        )

    httpx_mock.add_callback(custom_response, is_reusable=True)

    async def _updates() -> AsyncGenerator[dict]:
        for i in range(total_items):
            await asyncio.sleep(0.0001)
            yield {"=ENTITY_ID": i}

    api = Bitrix24()
    response = [
        r
        async for r in api.reference_batched_no_count(
            {
                "method": "crm.timeline.comment.list",
                "parameters": {"select": ["ID", "ENTITY_ID"], "filter": {"=ENTITY_TYPE": "deal"}},
            },
            _updates(),
            list_size=list_size,
            batch_size=batch_size,
        )
    ]
    assert sorted(response, key=lambda r: r["ID"]) == result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dialogs", "list_size", "batch_size"),
    [({"chat1": 5, "chat2": 3, "chat3": 7}, 2, 2), ({"only": 4}, 4, 2), ({"a": 1, "b": 1}, 5, 1)],
)
async def test_reference_cursor_no_count(
    httpx_mock: HTTPXMock,
    dialogs: dict[str, int],
    list_size: int,
    batch_size: int,
) -> None:
    messages = {d: [{"id": i + 1, "text": f"{d}-{i + 1}"} for i in range(n)] for d, n in dialogs.items()}

    def custom_response(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://bitrix24.com/rest/0/test/batch"
        output = {}
        for key, value in json.loads(request.content)["cmd"].items():
            method, command = value.split("?")
            assert method == "im.dialog.messages.get"
            command = parse_qs(command)

            dialog_id = command.pop("DIALOG_ID")[0]
            assert int(command.pop("LIMIT")[0]) == list_size
            last_id_raw = command.pop("LAST_ID", None)
            assert not command

            msgs = messages[dialog_id]
            if last_id_raw is None:
                page = list(reversed(msgs[-list_size:]))
            else:
                older = [m for m in msgs if m["id"] < int(last_id_raw[0])]
                page = list(reversed(older[-list_size:]))
            output[key] = {"messages": page}

        return httpx.Response(
            status_code=200,
            json={
                "result": {
                    "result": output,
                    "result_error": [],
                    "result_total": [],
                    "result_next": [],
                    "result_time": dict.fromkeys(output, _DEFAULT_TIME),
                },
                "time": _DEFAULT_TIME,
            },
        )

    httpx_mock.add_callback(custom_response, is_reusable=True)

    api = Bitrix24()
    response = [
        r
        async for r in api.reference_cursor_no_count(
            {"method": "im.dialog.messages.get", "parameters": {}},
            ({"DIALOG_ID": d} for d in dialogs),
            cursor_param="LAST_ID",
            cursor_field="id",
            cursor_take="min",
            list_size=list_size,
            list_size_param="LIMIT",
            batch_size=batch_size,
            result_key="messages",
        )
    ]

    expected = sorted(f"{d}-{i + 1}" for d, n in dialogs.items() for i in range(n))
    assert sorted(str(r["text"]) for r in response) == expected


@pytest.mark.asyncio
async def test_reference_cursor_no_count_with_payload(httpx_mock: HTTPXMock) -> None:
    list_size = 2
    messages = [{"id": i + 1, "text": f"m-{i + 1}"} for i in range(3)]

    def custom_response(request: httpx.Request) -> httpx.Response:
        output = {}
        for key, value in json.loads(request.content)["cmd"].items():
            command = parse_qs(value.split("?")[1])
            assert command.pop("DIALOG_ID") == ["chat"]
            assert int(command.pop("LIMIT")[0]) == list_size
            last_id_raw = command.pop("LAST_ID", None)

            if last_id_raw is None:
                page = list(reversed(messages[-list_size:]))
            else:
                older = [m for m in messages if m["id"] < int(last_id_raw[0])]
                page = list(reversed(older[-list_size:]))
            output[key] = {"messages": page}

        return httpx.Response(
            status_code=200,
            json={
                "result": {
                    "result": output,
                    "result_error": [],
                    "result_total": [],
                    "result_next": [],
                    "result_time": dict.fromkeys(output, _DEFAULT_TIME),
                },
                "time": _DEFAULT_TIME,
            },
        )

    httpx_mock.add_callback(custom_response, is_reusable=True)

    api = Bitrix24()
    response = [
        r
        async for r in api.reference_cursor_no_count(
            {"method": "im.dialog.messages.get", "parameters": {}},
            [({"DIALOG_ID": "chat"}, {"tag": "p"})],
            cursor_param="LAST_ID",
            cursor_field="id",
            cursor_take="min",
            list_size=list_size,
            list_size_param="LIMIT",
            result_key="messages",
            with_payload=True,
        )
    ]
    assert len(response) == len(messages)
    items, payloads = zip(*response, strict=True)
    assert sorted(item["id"] for item in items) == [m["id"] for m in messages]
    assert all(p == {"tag": "p"} for p in payloads)


@pytest.mark.asyncio
async def test_reference_cursor_no_count_async_updates(httpx_mock: HTTPXMock) -> None:
    list_size = 2
    dialogs = {"a": 3, "b": 2}
    messages = {d: [{"id": i + 1, "text": f"{d}-{i + 1}"} for i in range(n)] for d, n in dialogs.items()}

    def custom_response(request: httpx.Request) -> httpx.Response:
        output = {}
        for key, value in json.loads(request.content)["cmd"].items():
            command = parse_qs(value.split("?")[1])
            dialog_id = command.pop("DIALOG_ID")[0]
            command.pop("LIMIT")
            last_id_raw = command.pop("LAST_ID", None)

            msgs = messages[dialog_id]
            if last_id_raw is None:
                page = list(reversed(msgs[-list_size:]))
            else:
                older = [m for m in msgs if m["id"] < int(last_id_raw[0])]
                page = list(reversed(older[-list_size:]))
            output[key] = {"messages": page}

        return httpx.Response(
            status_code=200,
            json={
                "result": {
                    "result": output,
                    "result_error": [],
                    "result_total": [],
                    "result_next": [],
                    "result_time": dict.fromkeys(output, _DEFAULT_TIME),
                },
                "time": _DEFAULT_TIME,
            },
        )

    httpx_mock.add_callback(custom_response, is_reusable=True)

    async def _updates() -> AsyncGenerator[dict]:
        for d in dialogs:
            await asyncio.sleep(0.0001)
            yield {"DIALOG_ID": d}

    api = Bitrix24()
    response = [
        r
        async for r in api.reference_cursor_no_count(
            {"method": "im.dialog.messages.get", "parameters": {}},
            _updates(),
            cursor_param="LAST_ID",
            cursor_field="id",
            cursor_take="min",
            list_size=list_size,
            list_size_param="LIMIT",
            result_key="messages",
        )
    ]
    expected = sorted(f"{d}-{i + 1}" for d, n in dialogs.items() for i in range(n))
    assert sorted(str(r["text"]) for r in response) == expected


@pytest.mark.asyncio
async def test_reference_cursor_no_count_reserved_in_base() -> None:
    api = Bitrix24()
    gen = api.reference_cursor_no_count(
        {"method": "im.dialog.messages.get", "parameters": {"LAST_ID": 100}},
        [{"DIALOG_ID": "x"}],
        cursor_param="LAST_ID",
        cursor_field="id",
        cursor_take="min",
        result_key="messages",
    )
    with pytest.raises(ValueError, match=r"`LAST_ID` is reserved"):
        async for _ in gen:
            pass


@pytest.mark.asyncio
async def test_reference_cursor_no_count_reserved_in_update() -> None:
    api = Bitrix24()
    gen = api.reference_cursor_no_count(
        {"method": "im.dialog.messages.get", "parameters": {}},
        [{"DIALOG_ID": "x", "LAST_ID": 100}],
        cursor_param="LAST_ID",
        cursor_field="id",
        cursor_take="min",
        result_key="messages",
    )
    with pytest.raises(ValueError, match=r"`LAST_ID` is reserved"):
        async for _ in gen:
            pass


@pytest.mark.asyncio
async def test_reference_cursor_no_count_invalid_result_key(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/batch",
        json={
            "result": {
                "result": {"_0": {"unexpected": []}},
                "result_error": [],
                "result_total": [],
                "result_next": [],
                "result_time": {"_0": _DEFAULT_TIME},
            },
            "time": _DEFAULT_TIME,
        },
    )

    api = Bitrix24()
    with pytest.raises(KeyError, match="messages"):
        async for _ in api.reference_cursor_no_count(
            {"method": "im.dialog.messages.get", "parameters": {}},
            [{"DIALOG_ID": "x"}],
            cursor_param="LAST_ID",
            cursor_field="id",
            cursor_take="min",
            list_size=10,
            list_size_param="LIMIT",
            result_key="messages",
        ):
            pass


@pytest.mark.asyncio
async def test_reference_cursor_no_count_missing_cursor_field(httpx_mock: HTTPXMock) -> None:
    list_size = 2
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/batch",
        json={
            "result": {
                "result": {"_0": {"messages": [{"text": "no id"}, {"text": "still no id"}]}},
                "result_error": [],
                "result_total": [],
                "result_next": [],
                "result_time": {"_0": _DEFAULT_TIME},
            },
            "time": _DEFAULT_TIME,
        },
    )

    api = Bitrix24()
    with pytest.raises(KeyError, match="`id`"):
        async for _ in api.reference_cursor_no_count(
            {"method": "im.dialog.messages.get", "parameters": {}},
            [{"DIALOG_ID": "x"}],
            cursor_param="LAST_ID",
            cursor_field="id",
            cursor_take="min",
            list_size=list_size,
            list_size_param="LIMIT",
            result_key="messages",
        ):
            pass


@pytest.mark.asyncio
async def test_reference_cursor_no_count_invalid_cursor_take() -> None:
    api = Bitrix24()
    with pytest.raises(ValueError, match=r"cursor_take"):
        async for _ in api.reference_cursor_no_count(
            {"method": "im.dialog.messages.get", "parameters": {}},
            [{"DIALOG_ID": "x"}],
            cursor_param="LAST_ID",
            cursor_field="id",
            cursor_take="oops",
            result_key="messages",
        ):
            pass


_DEFAULT_TIME = {
    "start": 1741699660.029826,
    "finish": 1741699660.111687,
    "duration": 0.08186101913452148,
    "processing": 0.0500180721282959,
    "date_start": "2025-03-11T16:27:40+03:00",
    "date_finish": "2025-03-11T16:27:40+03:00",
    "operating_reset_at": 1741700260,
    "operating": 1.8415930271148682,
}
_DEFAULT_PROFILE = {
    "ID": "12",
    "ADMIN": False,
    "NAME": "First",
    "LAST_NAME": "Last",
    "PERSONAL_GENDER": "",
    "TIME_ZONE": "",
    "TIME_ZONE_OFFSET": 10800,
}
_DEFAULT_LEADS = [{"ID": "38945", "STATUS_ID": "1"}, {"ID": "43595", "STATUS_ID": "1"}]


# --- Regression tests for fixed bugs ---


@pytest.mark.asyncio
async def test_call_integer_error_code(httpx_mock: HTTPXMock) -> None:
    """ErrorResponse.error can be an integer (e.g. 0). Must be normalized to str."""
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/profile",
        match_headers={"Content-Type": "application/json"},
        match_json={},
        json={
            "error": 0,
            "error_description": "Unknown error",
        },
    )

    api = Bitrix24()
    with pytest.raises(ApiResponseError, match=r"API error \[0\]"):
        await api.call({"method": "profile"})


@pytest.mark.asyncio
async def test_list_sequential_accepts_list_request(httpx_mock: HTTPXMock) -> None:
    """list_sequential must work when passed a ListRequest instance (not just dict)."""
    result = [{"ID": str(i), "STATUS_ID": "1"} for i in range(3)]
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/crm.lead.list",
        match_headers={"Content-Type": "application/json"},
        json={
            "result": result,
            "total": 3,
            "time": _DEFAULT_TIME,
        },
    )

    request = ListRequest.model_validate({"method": "crm.lead.list"})
    api = Bitrix24()
    response = [r async for r in api.list_sequential(request)]
    assert response == result


@pytest.mark.asyncio
async def test_list_batched_no_count_custom_id_key(httpx_mock: HTTPXMock) -> None:
    """id_key != 'ID' must use that key in both filter and order."""
    total_items = 10
    result = [{"ELEMENT_ID": i, "VALUE": "x"} for i in range(total_items)]

    def custom_response(request: httpx.Request) -> httpx.Response:
        output = {}
        for key, value in json.loads(request.content)["cmd"].items():
            _method, command = value.split("?")

            command = parse_qs(command)
            assert command.pop("start", None) == ["-1"]

            order = command.pop("order[ELEMENT_ID]", None)
            assert order is not None, "order must use custom id_key, not hardcoded 'ID'"

            reverse = order == ["DESC"]

            from_id = int(command.pop("filter[>ELEMENT_ID]", ["-1"])[0])
            to_id = int(command.pop("filter[<ELEMENT_ID]", [str(total_items)])[0])

            data = [r for r in result if from_id < r["ELEMENT_ID"] < to_id]
            data = data[::-1] if reverse else data
            output[key] = data[:50]

        return httpx.Response(
            status_code=200,
            json={
                "result": {
                    "result": output,
                    "result_error": [],
                    "result_total": [],
                    "result_next": [],
                    "result_time": dict.fromkeys(output, _DEFAULT_TIME),
                },
                "time": _DEFAULT_TIME,
            },
        )

    httpx_mock.add_callback(custom_response, is_reusable=True)

    api = Bitrix24()
    response = [
        r
        async for r in api.list_batched_no_count(
            {"method": "crm.item.list", "parameters": {"select": ["ELEMENT_ID", "VALUE"]}},
            id_key="ELEMENT_ID",
        )
    ]
    assert response == result


@pytest.mark.asyncio
async def test_list_batched_no_count_id_not_in_select(httpx_mock: HTTPXMock) -> None:
    """If id_key is not in select, it must be appended automatically (was AttributeError)."""
    result = [{"ID": i, "STATUS_ID": "1"} for i in range(3)]

    def custom_response(request: httpx.Request) -> httpx.Response:
        output = {}
        for key, value in json.loads(request.content)["cmd"].items():
            _method, command = value.split("?")
            command = parse_qs(command)

            # ID must have been auto-appended to select
            select_keys = [k for k in command if k.startswith("select[")]
            assert any(command[k] == ["ID"] for k in select_keys), "ID must be auto-appended to select"

            command.pop("start", None)
            command.pop("order[ID]", None)
            for k in list(command):
                if k.startswith(("select[", "filter[")):
                    command.pop(k)

            from_id = -1
            to_id = len(result)
            data = [r for r in result if from_id < r["ID"] < to_id]
            output[key] = data[:50]

        return httpx.Response(
            status_code=200,
            json={
                "result": {
                    "result": output,
                    "result_error": [],
                    "result_total": [],
                    "result_next": [],
                    "result_time": dict.fromkeys(output, _DEFAULT_TIME),
                },
                "time": _DEFAULT_TIME,
            },
        )

    httpx_mock.add_callback(custom_response, is_reusable=True)

    api = Bitrix24()
    # select does NOT include "ID" — helper must append it
    response = [
        r
        async for r in api.list_batched_no_count(
            {"method": "crm.lead.list", "parameters": {"select": ["STATUS_ID"]}},
        )
    ]
    assert response == result
