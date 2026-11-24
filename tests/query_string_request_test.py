"""A ``QUERY_STRING`` request: a GET with its parameters in the query, or a POST of its raw content."""

from __future__ import annotations
import io
import logging
import string
from typing import TYPE_CHECKING, cast

import httpx
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from b24api import ReplaySafety, Request, RouteKind
from b24api.contracts import BodyEncoding, Command, CommandFailure, CommandSuccess, RawContent, RequestHeaders
from b24api.contracts.request import RequestSpec, canonical_request
from b24api.errors import B24ApiError, CapabilityError
from b24api.execution.executor import Executor
from b24api.testing import (
    ConformanceCase,
    ScriptedExchange,
    ScriptedTransport,
    run_transport_conformance,
)
from b24api.transport.base import TransportCapabilities, WireRequest, WireResponse
from b24api.transport.decoding import BOUNDED_ACCEPT_ENCODING
from b24api.transport.httpx import HttpxTransport
from tests.scripting import ResponderTransport, client_for, json_response

if TYPE_CHECKING:
    from collections.abc import Callable

_WEBHOOK = "https://portal.invalid/rest/1/token/"
_CHUNK = RawContent(b"\x00\x01chunk\xff", "application/octet-stream")
_QUERY_CASES = frozenset({ConformanceCase.QUERY_STRING_GET, ConformanceCase.QUERY_STRING_RAW_CONTENT})


def _httpx(respond: Callable[[httpx.Request], httpx.Response], webhook: str = _WEBHOOK) -> HttpxTransport:
    return HttpxTransport(webhook, client=httpx.AsyncClient(transport=httpx.MockTransport(respond)))


@pytest.mark.asyncio
async def test_a_query_string_request_without_content_is_a_get_with_its_parameters_in_the_query() -> None:
    sent: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, headers={"content-type": "image/png"}, content=b"\x89PNG")

    transport = _httpx(respond)
    request = Request.bare(
        "crm.site.fileuploader.preview",
        {"fileId": 7, "options": {"size": ["a b", True]}},
        ReplaySafety.SAFE,
        encoding=BodyEncoding.QUERY_STRING,
        headers=RequestHeaders({"X-Upload-Content-Name": "n.bin"}),
    )
    async with client_for(transport) as client:
        answer = await client.call_bytes(request)
    await transport.aclose()

    assert answer.body == b"\x89PNG"
    [wire] = sent
    assert wire.method == "GET"
    assert wire.url.raw_path == (
        b"/rest/1/token/crm.site.fileuploader.preview?fileId=7&options%5Bsize%5D%5B0%5D=a+b&options%5Bsize%5D%5B1%5D=1"
    )
    assert wire.content == b""
    assert "content-type" not in wire.headers
    assert wire.headers["x-upload-content-name"] == "n.bin"
    assert wire.headers["accept-encoding"] == BOUNDED_ACCEPT_ENCODING


@pytest.mark.asyncio
async def test_a_query_string_request_without_parameters_has_no_query() -> None:
    sent: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, json={"result": {"merged": True}})

    transport = _httpx(respond)
    async with client_for(transport) as client:
        answer = await client.call(Request.bare("crm.timeline.ai.mergefields", encoding=BodyEncoding.QUERY_STRING))
    await transport.aclose()

    assert answer == {"merged": True}
    assert [(wire.method, wire.url.raw_path) for wire in sent] == [
        ("GET", b"/rest/1/token/crm.timeline.ai.mergefields")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("chunk", [_CHUNK, RawContent(b"", "application/octet-stream")], ids=["bytes", "empty"])
async def test_raw_content_is_posted_byte_for_byte_with_its_type_and_the_parameters_in_the_query(
    chunk: RawContent,
) -> None:
    sent: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, json={"result": {"token": "next"}})

    transport = _httpx(respond)
    request = Request.bare(
        "crm.site.fileuploader.upload",
        {"controller": "crm.fileUploader", "token": "t"},
        ReplaySafety.UNSAFE,
        encoding=BodyEncoding.QUERY_STRING,
        headers=RequestHeaders({"X-Upload-Content-Name": "chunk.bin"}),
        content=chunk,
    )
    async with client_for(transport) as client:
        answer = await client.call(request)
    await transport.aclose()

    assert answer == {"token": "next"}
    [wire] = sent
    assert wire.method == "POST"
    assert wire.url.raw_path == b"/rest/1/token/crm.site.fileuploader.upload?controller=crm.fileUploader&token=t"
    assert wire.content == chunk.data
    assert wire.headers["content-type"] == "application/octet-stream"
    assert wire.headers["content-length"] == str(len(chunk.data))
    assert wire.headers["x-upload-content-name"] == "chunk.bin"


def test_raw_content_travels_only_with_query_string_encoding() -> None:
    for encoding in (BodyEncoding.JSON, BodyEncoding.FORM_URLENCODED):
        with pytest.raises(ValueError, match="raw content requires QUERY_STRING encoding"):
            Request.bare("crm.site.fileuploader.upload", encoding=encoding, content=_CHUNK)
    with pytest.raises(TypeError, match="content must be a RawContent"):
        Request.bare("x.y", encoding=BodyEncoding.QUERY_STRING, content=b"bytes")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="content must be a RawContent"):
        canonical_request(
            cast(
                "RequestSpec",
                {"method": "x.y", "route": RouteKind.BARE, "encoding": BodyEncoding.QUERY_STRING, "content": b"bytes"},
            )
        )
    with pytest.raises(ValueError, match="API_V3 requires JSON"):
        Request("x.y", encoding=BodyEncoding.QUERY_STRING, route=RouteKind.API_V3)


def test_query_string_parameters_carry_a_batch_reference_as_text_as_a_form_request_does() -> None:
    request = Request.bare("x.y", {"name": "$result[x]"}, encoding=BodyEncoding.QUERY_STRING)

    assert request.parameters["name"] == "$result[x]"


@pytest.mark.parametrize(
    ("parameters", "obstacle"),
    [({"filter": {"IDS": []}}, "empty"), ({"a": None}, "None")],
)
def test_query_string_parameters_refuse_what_php_query_encoding_would_drop(
    parameters: dict[str, object], obstacle: str
) -> None:
    with pytest.raises(ValueError, match=f"query-string parameters cannot carry .*{obstacle}"):
        Request.bare("x.y", parameters, encoding=BodyEncoding.QUERY_STRING)


@pytest.mark.parametrize(
    "content_type",
    [
        "",
        "octet-stream",
        "application/",
        "text/plain\r\nX-Evil: 1",
        " application/json",
        "application/json garbage",
        "text/plain; not-a-param",
        "text/plain; charset=",
        'text/plain; name="open',
        "text/plain; charset=utf-8 ",
        "text/plain; ",
        "a/" + "b" * 255,
        'a/b; n="x"y"',
        "/b",
        "a/",
        "a/b; =v",
        "a/b; n=",
    ],
)
def test_raw_content_refuses_a_content_type_that_is_not_a_media_type(content_type: str) -> None:
    with pytest.raises(ValueError, match="raw content type must be a media type"):
        RawContent(b"x", content_type)


@pytest.mark.parametrize(
    "content_type",
    [
        "application/octet-stream",
        "text/plain ; charset=utf-8",
        "text/plain;;",
        'text/plain; name="a \\"b\\";c"',
        "multipart/form-data; boundary=x-1",
        "image/svg+xml",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/ld+json",
        "text/plain\t;\tcharset=utf-8",
        'a/b; n="x\ty"',
        "a/" + "b" * 254,
        'a/b; n=""',
    ],
)
def test_raw_content_accepts_any_media_type_with_its_parameters_up_to_256_characters(content_type: str) -> None:
    assert RawContent(b"x", content_type).content_type == content_type


# RFC 9110: token characters, quoted-string text (no obs-text) and what a backslash may quote.
_TCHAR = frozenset("!#$%&'*+-.^_`|~0123456789" + string.ascii_letters)
_QDTEXT = frozenset("\t \x21" + "".join(map(chr, range(0x23, 0x5C))) + "".join(map(chr, range(0x5D, 0x7F))))
_QUOTABLE = frozenset("\t " + "".join(map(chr, range(0x21, 0x7F))))
_CHARACTERS = [chr(code) for code in range(0x80)] + ["\x80", "\xe9", "\u044f"]


def _accepts(content_type: str) -> bool:
    try:
        RawContent(b"", content_type)
    except ValueError:
        return False
    return True


_CONTENT_TYPE_MAXIMUM = 256


def _media_types() -> st.SearchStrategy[str]:
    """Media types written from RFC 9110's ABNF (visible ASCII), independently of the validator's expression.

    A field value has no whitespace at its end, so a last empty parameter after ``OWS ";" OWS`` keeps none.
    """
    token = st.text(alphabet=sorted(_TCHAR), min_size=1, max_size=6)
    quoted = st.lists(
        st.one_of(st.sampled_from(sorted(_QDTEXT)), st.sampled_from(sorted(_QUOTABLE)).map(lambda c: "\\" + c)),
        max_size=6,
    ).map(lambda parts: '"' + "".join(parts) + '"')
    named = st.builds(lambda name, value: f"{name}={value}", token, st.one_of(token, quoted))
    ows = st.sampled_from(["", " ", "\t", " \t "])
    parameters = st.lists(
        st.builds(lambda before, after, item: f"{before};{after}{item}", ows, ows, st.just("") | named)
    )
    return (
        st.builds(lambda kind, sub, rest: f"{kind}/{sub}{''.join(rest)}", token, token, parameters)
        .map(lambda content_type: content_type.rstrip(" \t"))
        .filter(lambda content_type: len(content_type) <= _CONTENT_TYPE_MAXIMUM)
    )


@given(_media_types())
@settings(max_examples=300)
def test_raw_content_accepts_every_media_type_rfc_9110_writes(content_type: str) -> None:
    assert RawContent(b"", content_type).content_type == content_type


@pytest.mark.parametrize(
    ("form", "allowed"),
    [
        ("a{}a/b", _TCHAR),
        ("a/b{}b", _TCHAR),
        ("a/b; n{}n=v", _TCHAR),
        ("a/b; n=v{}v", _TCHAR),
        ('a/b; n="x{}"', _QDTEXT),
        ('a/b; n="x\\{}y"', _QUOTABLE),
    ],
    ids=["type", "subtype", "parameter-name", "token-value", "quoted-text", "quoted-pair"],
)
def test_raw_content_type_admits_each_character_where_rfc_9110_does(form: str, allowed: frozenset[str]) -> None:
    accepted = {character for character in _CHARACTERS if _accepts(form.format(character))}

    assert accepted == set(allowed)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "forged", "error"),
    [("content_type", "text/plain\r\nX-Evil: 1", ValueError), ("data", "not bytes", TypeError)],
)
async def test_the_repository_transport_refuses_forged_raw_content_before_it_sends(
    field: str, forged: str, error: type[Exception]
) -> None:
    sent: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, json={"result": True})

    transport = _httpx(respond)
    content = RawContent(b"chunk", "application/octet-stream")
    object.__setattr__(content, field, forged)
    request = Request.bare("crm.site.fileuploader.upload", encoding=BodyEncoding.QUERY_STRING, content=content)
    try:
        with pytest.raises(error, match="raw content"):
            await transport.send_wire(WireRequest(request), attempt_timeout=1, max_response_bytes=1024)
    finally:
        await transport.aclose()

    assert sent == []


def test_raw_content_keeps_its_bytes_out_of_every_representation() -> None:
    secret = b"chunk-secret-bytes"
    content = RawContent(secret, "text/plain; charset=utf-8")
    request = Request.bare("x.y", {"a": 1}, encoding=BodyEncoding.QUERY_STRING, content=content)

    assert repr(content) == "RawContent(content_type='text/plain; charset=utf-8', byte_length=18)"
    for rendered in (repr(content), repr(request), repr(WireRequest(request)), repr(request.summary)):
        assert "chunk-secret" not in rendered
    with pytest.raises(TypeError, match="raw content data must be bytes"):
        RawContent(bytearray(secret), "text/plain")  # type: ignore[arg-type]


def test_a_request_keeps_its_raw_content_through_every_rebuild() -> None:
    request = Request.bare("x.y", {"a": 1}, encoding=BodyEncoding.QUERY_STRING, content=_CHUNK)
    spec = canonical_request(
        {
            "method": "x.y",
            "parameters": {"a": 1},
            "encoding": BodyEncoding.QUERY_STRING,
            "content": _CHUNK,
            "route": RouteKind.BARE,
        }
    )

    assert spec == request
    assert request.with_parameters({"a": 2}).content is _CHUNK
    assert WireRequest(request).content is _CHUNK
    assert request != Request.bare("x.y", {"a": 1}, encoding=BodyEncoding.QUERY_STRING)
    assert request != Request.bare(
        "x.y", {"a": 1}, encoding=BodyEncoding.QUERY_STRING, content=RawContent(_CHUNK.data, "image/png")
    )


@pytest.mark.asyncio
async def test_a_scripted_exchange_matches_raw_content_by_its_bytes_and_type() -> None:
    request = Request.bare("x.upload", {"t": 1}, encoding=BodyEncoding.QUERY_STRING, content=_CHUNK)
    other = Request.bare(
        "x.upload", {"t": 1}, encoding=BodyEncoding.QUERY_STRING, content=RawContent(b"other", _CHUNK.content_type)
    )
    transport = ScriptedTransport((ScriptedExchange.json(request, {"result": True}),))
    async with client_for(transport) as client:
        with pytest.raises(B24ApiError):
            await client.call(other)
        assert await client.call(request) is True

    assert [call.request for call in transport.unexpected] == [other]
    assert transport.calls == (other, request)


@pytest.mark.asyncio
async def test_a_send_only_transport_refuses_a_query_string_request_before_any_send() -> None:
    transport = ResponderTransport(lambda _request: json_response({"result": True}))

    with pytest.raises(CapabilityError, match="advanced request delivery"):
        await Executor(transport).execute(Request.bare("x.y", encoding=BodyEncoding.QUERY_STRING))

    assert transport.requests == []


@pytest.mark.asyncio
async def test_a_classic_batch_refuses_a_query_string_command_alone() -> None:
    def respond(request: Request) -> WireResponse:
        keys = tuple(request.copy_parameters()["cmd"])  # type: ignore[arg-type]
        return json_response({"result": {"result": dict.fromkeys(keys, True), "result_error": []}})

    transport = ResponderTransport(respond)
    query = Request.bare("x.get", {"id": 1}, ReplaySafety.SAFE, encoding=BodyEncoding.QUERY_STRING)
    async with client_for(transport) as client:
        outcomes = [
            outcome
            async for outcome in client.batch_outcomes(
                [Command(query, "query"), Command(Request.bare("x.list", replay_safety=ReplaySafety.SAFE), "list")]
            )
        ]

    assert isinstance(outcomes[0], CommandFailure)
    assert isinstance(outcomes[0].error, CapabilityError)
    assert isinstance(outcomes[1], CommandSuccess)
    [batch] = transport.requests
    commands = batch.copy_parameters()["cmd"]
    assert isinstance(commands, dict)
    assert [command.split("?")[0] for command in commands.values()] == ["x.list"]  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_httpx_logs_keep_the_path_of_a_query_string_request_and_drop_its_values() -> None:
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    transport = _httpx(
        lambda request: httpx.Response(200, content=b"bytes", request=request), "https://portal.invalid/rest/42/token/"
    )
    try:
        await transport.send_wire(
            WireRequest(
                Request.bare(
                    "x.download",
                    {"fileId": "value-marker", "owner": {"email": "second-marker"}, "name": "third-marker"},
                    encoding=BodyEncoding.QUERY_STRING,
                )
            ),
            attempt_timeout=1,
            max_response_bytes=1024,
        )
    finally:
        await transport.aclose()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)

    logged = output.getvalue()
    assert "x.download?[REDACTED]" in logged
    assert "marker" not in logged
    assert "owner" not in logged
    assert "/token/" not in logged


@pytest.mark.asyncio
async def test_the_repository_transport_passes_both_query_string_conformance_cases() -> None:
    report = await run_transport_conformance(HttpxTransport, cases=_QUERY_CASES)

    assert [(outcome.case, outcome.passed, outcome.skipped) for outcome in report.outcomes] == [
        (ConformanceCase.QUERY_STRING_GET, True, False),
        (ConformanceCase.QUERY_STRING_RAW_CONTENT, True, False),
    ]


class _PostsEveryQuery(HttpxTransport):
    """A transport that declares query strings but posts them as JSON, as one that ignored the encoding would."""

    async def send_wire(self, request: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        as_json = Request(request.method, parameters=request.copy_parameters(), route=RouteKind.BARE)
        return await super().send_wire(
            WireRequest(as_json), attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes
        )


@pytest.mark.asyncio
async def test_conformance_fails_a_transport_that_declares_query_strings_but_posts_json() -> None:
    report = await run_transport_conformance(_PostsEveryQuery, cases=_QUERY_CASES)

    assert [(outcome.case, outcome.passed) for outcome in report.outcomes] == [
        (ConformanceCase.QUERY_STRING_GET, False),
        (ConformanceCase.QUERY_STRING_RAW_CONTENT, False),
    ]


# Each flaw of a query-string request's URL, and of its body or content type, as (method, URL, type, body).
_URL_FLAWS: dict[str, Callable[[httpx.URL], httpx.URL]] = {
    "no_query": lambda url: url.copy_with(query=None),
    "other_nested_key": lambda url: url.copy_with(query=url.query.replace(b"items", b"item")),
    "space_as_percent": lambda url: url.copy_with(query=url.query.replace(b"+", b"%20")),
    "keys_reordered": lambda url: url.copy_with(query=b"&".join(reversed(url.query.split(b"&")))),
    "other_path": lambda url: url.copy_with(path=url.path + "x"),
}
_BODY_FLAWS: dict[tuple[str, str], Callable[[str | None, bytes], tuple[str | None, bytes]]] = {
    ("get_with_body", "GET"): lambda _type, _body: ("application/x-www-form-urlencoded", b"plain=a+b"),
    ("bytes_as_text", "POST"): lambda content_type, body: (content_type, body.decode("utf-8", "replace").encode()),
    ("other_bytes", "POST"): lambda content_type, _body: (content_type, b'{"plain":"a b"}'),
    ("other_content_type", "POST"): lambda _type, body: ("application/json", body),
}


class _Tampered(HttpxTransport):
    """A transport that builds its query-string requests right but for one ``flaw``."""

    def __init__(self, webhook_url: str, flaw: str) -> None:
        super().__init__(webhook_url)
        self.flaw = flaw

    def _build_request(
        self, request: WireRequest, headers: tuple[tuple[str, str], ...], method_url: str
    ) -> httpx.Request:
        built = super()._build_request(request, headers, method_url)
        kept = [
            (name, value) for name, value in built.headers.items() if name not in {"content-length", "content-type"}
        ]
        url = built.url
        if (move := _URL_FLAWS.get(self.flaw)) is not None:
            url = move(url)
        method = {"GET": "POST", "POST": "GET"}[built.method] if self.flaw == "other_method" else built.method
        content_type, body = built.headers.get("content-type"), built.content
        if (tamper := _BODY_FLAWS.get((self.flaw, built.method))) is not None:
            content_type, body = tamper(content_type, body)
        if content_type is not None:
            kept.append(("content-type", content_type))
        return httpx.Request(method, url, headers=kept, content=body)

    async def send_wire(self, request: WireRequest, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        answer = await super().send_wire(
            request, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes
        )
        if self.flaw == "other_status":
            return WireResponse(202, answer.headers, answer.body)
        return answer


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("flaw", "passed"),
    [
        ("none", (True, True)),
        ("space_as_percent", (True, True)),
        ("keys_reordered", (True, True)),
        ("no_query", (False, False)),
        ("other_path", (False, False)),
        ("other_method", (False, False)),
        ("other_nested_key", (False, False)),
        ("get_with_body", (False, True)),
        ("other_bytes", (True, False)),
        ("bytes_as_text", (True, False)),
        ("other_content_type", (True, False)),
        ("other_status", (False, False)),
    ],
)
async def test_conformance_checks_the_method_query_body_content_type_and_status_of_a_query_string_request(
    flaw: str, passed: tuple[bool, bool]
) -> None:
    report = await run_transport_conformance(lambda url: _Tampered(url, flaw), cases=_QUERY_CASES)

    assert [outcome.case for outcome in report.outcomes] == [
        ConformanceCase.QUERY_STRING_GET,
        ConformanceCase.QUERY_STRING_RAW_CONTENT,
    ]
    assert tuple(outcome.passed for outcome in report.outcomes) == passed


@pytest.mark.asyncio
async def test_conformance_skips_the_query_string_cases_for_a_transport_that_does_not_declare_them() -> None:
    report = await run_transport_conformance(
        lambda _url: ResponderTransport(lambda _request: json_response({"result": True})), cases=_QUERY_CASES
    )

    assert all(outcome.skipped for outcome in report.outcomes)


class _BodiesOnly(HttpxTransport):
    """A wire transport that declares only the body encodings."""

    capabilities = TransportCapabilities(encodings=frozenset({BodyEncoding.JSON, BodyEncoding.FORM_URLENCODED}))


@pytest.mark.asyncio
async def test_conformance_skips_the_query_string_cases_for_a_wire_transport_that_does_not_declare_them() -> None:
    report = await run_transport_conformance(_BodiesOnly, cases=_QUERY_CASES)

    assert [outcome.skipped for outcome in report.outcomes] == [True, True]


@pytest.mark.asyncio
@pytest.mark.parametrize("content", [None, _CHUNK])
async def test_an_injected_clients_own_query_parameters_join_a_query_string_requests_own(
    content: RawContent | None,
) -> None:
    sent: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, headers={"content-type": "application/octet-stream"}, content=b"x")

    injected = httpx.AsyncClient(transport=httpx.MockTransport(respond), params={"extra": "1", "c": "client"})
    transport = HttpxTransport(_WEBHOOK, client=injected)
    request = Request.bare(
        "x.get", {"a": {"b": ["x y", "z"]}, "c": "%2B"}, encoding=BodyEncoding.QUERY_STRING, content=content
    )
    async with client_for(transport) as client:
        await client.call_bytes(request)
    await transport.aclose()

    [wire] = sent
    assert wire.url.raw_path == b"/rest/1/token/x.get?extra=1&c=%252B&a%5Bb%5D%5B0%5D=x+y&a%5Bb%5D%5B1%5D=z"
