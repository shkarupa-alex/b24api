import json
import math
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs

import httpx
import pytest
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from b24api.api import Bitrix24
from b24api.error import ApiResponseError, RetryApiResponseError, RetryHTTPStatusError


def test_call(httpx_mock: HTTPXMock) -> None:
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
    response = api.call({"method": "profile"})
    assert response == result


def test_call_list(httpx_mock: HTTPXMock) -> None:
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
    response = api.call(
        {
            "method": "crm.lead.list",
            "parameters": {
                "select": ["ID", "STATUS_ID"],
                "filter": {">DATE_CREATE": datetime(2024, 1, 2, 3, 4, tzinfo=timezone(timedelta(hours=3)))},
            },
        },
    )
    assert response == result


def test_call_status_error(httpx_mock: HTTPXMock) -> None:
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
        api.call({"method": "profile"})


def test_call_retry_status_error(httpx_mock: HTTPXMock, mocker: MockerFixture) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/profile",
        match_headers={"Content-Type": "application/json"},
        match_json={},
        status_code=httpx.codes.TOO_MANY_REQUESTS,
        content=b"",
        is_reusable=True,
    )
    sleep_mock = mocker.patch("time.sleep")

    api = Bitrix24()
    with pytest.raises(RetryHTTPStatusError):
        api.call({"method": "profile"})

    num_retries = 5
    assert sleep_mock.call_count == num_retries - 1


def test_call_api_error(httpx_mock: HTTPXMock) -> None:
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
        api.call({"method": "profile"})


def test_call_retry_api_error(httpx_mock: HTTPXMock, mocker: MockerFixture) -> None:
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
    sleep_mock = mocker.patch("time.sleep")

    api = Bitrix24()
    with pytest.raises(RetryApiResponseError):
        api.call({"method": "profile"})

    num_retries = 5
    assert sleep_mock.call_count == num_retries - 1


def test_call_status_and_api_error(httpx_mock: HTTPXMock) -> None:
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
        api.call({"method": "profile"})


def test_call_retry_status_and_api_error(httpx_mock: HTTPXMock, mocker: MockerFixture) -> None:
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
    sleep_mock = mocker.patch("time.sleep")

    api = Bitrix24()
    with pytest.raises(RetryApiResponseError):
        api.call({"method": "profile"})

    num_retries = 5
    assert sleep_mock.call_count == num_retries - 1


def test_batch(httpx_mock: HTTPXMock) -> None:
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
    response = api.batch(
        [
            {"method": "profile"},
            {"method": "crm.lead.list", "parameters": {"select": ["ID", "STATUS_ID"], "start": -1}},
            {"method": "department.get", "parameters": {"ID": 1}},
        ],
    )
    assert list(response) == result


def test_batch_api_error(httpx_mock: HTTPXMock) -> None:
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
        list(
            api.batch(
                [
                    {"method": "profile"},
                    {"method": "telephony.externalLine.get"},
                    {"method": "department.get", "parameters": {"ID": 1}},
                ],
            ),
        )


def test_batch_retry_api_error(httpx_mock: HTTPXMock, mocker: MockerFixture) -> None:
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
    sleep_mock = mocker.patch("time.sleep")

    api = Bitrix24()
    with pytest.raises(RetryApiResponseError):
        list(
            api.batch(
                [
                    {"method": "profile"},
                    {"method": "telephony.externalLine.get"},
                    {"method": "department.get", "parameters": {"ID": 1}},
                ],
            ),
        )

    num_retries = 5
    assert sleep_mock.call_count == num_retries - 1


@pytest.mark.parametrize(("total_items", "list_size"), [(150, 50), (155, 50), (10, 50), (45, 20)])
def test_list_sequential(httpx_mock: HTTPXMock, total_items: int, list_size: int) -> None:
    result = [{"ID": str(i), "STATUS_ID": "1"} for i in range(total_items)]
    for start in range(0, total_items, list_size):
        httpx_mock.add_response(
            method="POST",
            url="https://bitrix24.com/rest/0/test/crm.lead.list",
            match_headers={"Content-Type": "application/json"},
            match_json={"start": start},
            json={
                "result": result[start : start + list_size],
                "total": total_items,
                "time": _DEFAULT_TIME,
            }
            | ({} if start + list_size >= total_items else {"next": start + list_size}),
        )

    api = Bitrix24()
    response = api.list_sequential(
        {"method": "crm.lead.list"},
        list_size=list_size,
    )
    assert list(response) == result


@pytest.mark.parametrize(
    ("total_items", "list_size", "batch_size"),
    [(150, 50, 1), (155, 50, 1), (10, 50, 50), (5500, 50, 50)],
)
def test_list_batched(httpx_mock: HTTPXMock, total_items: int, list_size: int, batch_size: int) -> None:
    result = [{"ID": str(i), "STATUS_ID": "1"} for i in range(total_items)]
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/crm.lead.list",
        match_headers={"Content-Type": "application/json"},
        match_json={"start": 0},
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
            start = batch_start + chunk * list_size
            commands[f"_{chunk}"] = f"crm.lead.list?start={start}"
            results[f"_{chunk}"] = result[start : start + list_size]
            times[f"_{chunk}"] = _DEFAULT_TIME
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
    response = api.list_batched(
        {"method": "crm.lead.list"},
        list_size=list_size,
        batch_size=batch_size,
    )
    assert list(response) == result


@pytest.mark.parametrize(
    ("total_items", "list_size", "batch_size"),
    [(150, 50, 1), (155, 50, 1), (10, 50, 50), (5500, 50, 50)],
)
def test_list_batched_no_count(httpx_mock: HTTPXMock, total_items: int, list_size: int, batch_size: int) -> None:
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
    response = api.list_batched_no_count(
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
    assert list(response) == result


@pytest.mark.parametrize(
    ("total_items", "list_size", "batch_size"),
    [(150, 50, 1), (155, 50, 1), (10, 50, 50)],
)
def test_reference_batched_no_count(httpx_mock: HTTPXMock, total_items: int, list_size: int, batch_size: int) -> None:
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
    response = api.reference_batched_no_count(
        {
            "method": "crm.timeline.comment.list",
            "parameters": {"select": ["ID", "ENTITY_ID"], "filter": {"=ENTITY_TYPE": "deal"}},
        },
        ({"=ENTITY_ID": i} for i in range(total_items)),
        list_size=list_size,
        batch_size=batch_size,
    )
    assert sorted(response, key=lambda r: r["ID"]) == result


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
