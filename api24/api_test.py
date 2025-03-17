import math

import httpx
import pytest
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from api24.api import API
from api24.entity import ListRequest, ListRequestParameters, Request
from api24.error import ApiResponseError, RetryHTTPStatusError


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

    api = API(tmp=123)
    response = api.call(Request(method="profile"))
    assert response == result


def test_call_list(httpx_mock: HTTPXMock) -> None:
    result = _DEFAULT_LEADS
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/crm.lead.list",
        match_headers={"Content-Type": "application/json"},
        match_json={"select": ["ID", "STATUS_ID"]},
        json={
            "result": result,
            "next": 3,
            "total": 10,
            "time": _DEFAULT_TIME,
        },
    )

    api = API()
    response = api.call(Request(method="crm.lead.list", parameters={"select": ["ID", "STATUS_ID"]}))
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

    api = API()
    with pytest.raises(httpx.HTTPStatusError):
        api.call(Request(method="profile"))


def test_call_retry_error(httpx_mock: HTTPXMock, mocker: MockerFixture) -> None:
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

    api = API()
    with pytest.raises(RetryHTTPStatusError):
        api.call(Request(method="profile"))

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
            "error_description": "REST API is available only on commercial plans",
        },
    )

    api = API()
    with pytest.raises(ApiResponseError):
        api.call(Request(method="profile"))


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
                "_1": "crm.lead.list?select%5B0%5D=ID&select%5B1%5D=STATUS_ID",
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

    api = API()
    response = api.batch(
        [
            Request(method="profile"),
            ListRequest(method="crm.lead.list", parameters=ListRequestParameters(select=["ID", "STATUS_ID"])),
            Request(method="department.get", parameters={"ID": 1}),
        ],
    )
    assert list(response) == result


def test_batch_halt_on_error(httpx_mock: HTTPXMock) -> None:
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

    api = API()
    with pytest.raises(ApiResponseError):
        list(
            api.batch(
                [
                    Request(method="profile"),
                    Request(method="telephony.externalLine.get"),
                    Request(method="department.get", parameters={"ID": 1}),
                ],
            ),
        )


@pytest.mark.parametrize(("total_items", "list_size"), [(150, 50), (155, 50), (10, 50), (45, 20)])
def test_list_sequential(httpx_mock: HTTPXMock, total_items: int, list_size: int) -> None:
    result = [{"ID": str(i), "STATUS_ID": "1"} for i in range(total_items)]
    for start in range(0, total_items, list_size):
        httpx_mock.add_response(
            method="POST",
            url="https://bitrix24.com/rest/0/test/crm.lead.list",
            match_headers={"Content-Type": "application/json"},
            match_json={} if start == 0 else {"start": start},
            json={
                "result": result[start : start + list_size],
                "total": total_items,
                "time": _DEFAULT_TIME,
            }
            | ({} if start + list_size >= total_items else {"next": start + list_size}),
        )

    api = API()
    response = api.list_sequential(
        Request(method="crm.lead.list"),
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
        match_json={},
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

    api = API()
    response = api.list_batched(
        Request(method="crm.lead.list"),
        list_size=list_size,
        batch_size=batch_size,
    )
    assert list(response) == result


@pytest.mark.parametrize(
    ("total_items", "list_size", "batch_size"),
    [(150, 50, 1), (155, 50, 1), (10, 50, 50), (5500, 50, 50)],
)
def test_list_batched_no_count(httpx_mock: HTTPXMock, total_items: int, list_size: int, batch_size: int) -> None:
    result = [{"ID": str(i), "STATUS_ID": "1"} for i in range(total_items)]
    httpx_mock.add_response(
        method="POST",
        url="https://bitrix24.com/rest/0/test/batch",
        match_headers={"Content-Type": "application/json"},
        match_json={
            "halt": True,
            "cmd": {
                "_0": "crm.lead.list"
                "?select%5B0%5D=ID"
                "&select%5B1%5D=STATUS_ID"
                "&filter%5B%3EDATE%5D=2025-03-14T14%3A00%3A17%2B03%3A00"
                "&order%5BID%5D=ASC"
                "&start=-1",
                "_1": "crm.lead.list"
                "?select%5B0%5D=ID"
                "&select%5B1%5D=STATUS_ID"
                "&filter%5B%3EDATE%5D=2025-03-14T14%3A00%3A17%2B03%3A00"
                "&order%5BID%5D=DESC"
                "&start=-1",
            },
        },
        json={
            "result": {
                "result": {
                    "_0": result[:list_size],
                    "_1": result[-list_size:][::-1],
                },
                "result_error": [],
                "result_total": [],
                "result_next": [],
                "result_time": {"_0": _DEFAULT_TIME, "_1": _DEFAULT_TIME},
            },
            "time": _DEFAULT_TIME,
        },
    )
    for batch_start in range(list_size, total_items - list_size, list_size * batch_size):
        max_chunks = math.ceil((total_items - batch_start - list_size) / batch_size)
        commands, results, times = {}, {}, {}
        for chunk in range(min(batch_size, max_chunks)):
            start = batch_start + chunk * list_size
            stop = min(start + list_size, total_items - list_size)
            commands[f"_{chunk}"] = (
                "crm.lead.list"
                "?select%5B0%5D=ID"
                "&select%5B1%5D=STATUS_ID"
                "&filter%5B%3EDATE%5D=2025-03-14T14%3A00%3A17%2B03%3A00"
                f"&filter%5B%3E%3DID%5D={start}"
                f"&filter%5B%3CID%5D={stop}"
                "&order%5BID%5D=ASC"
                "&start=-1"
            )
            results[f"_{chunk}"] = result[start:stop]
            times[f"_{chunk}"] = _DEFAULT_TIME
        httpx_mock.add_response(
            method="POST",
            url="https://bitrix24.com/rest/0/test/batch",
            match_headers={"Content-Type": "application/json"},
            match_json={
                "halt": True,
                "cmd": commands,
            },
            json={
                "result": {
                    "result": results,
                    "result_error": [],
                    "result_total": [],
                    "result_next": [],
                    "result_time": times,
                },
                "total": total_items,
                "time": _DEFAULT_TIME,
            },
        )

    api = API()
    response = api.list_batched_no_count(
        ListRequest(
            method="crm.lead.list",
            parameters=ListRequestParameters(select=["ID", "STATUS_ID"], filter={">DATE": "2025-03-14T14:00:17+03:00"}),
        ),
        list_size=list_size,
        batch_size=batch_size,
    )
    assert list(response) == result


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
