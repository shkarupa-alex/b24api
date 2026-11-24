"""#25 (A25): the Content-Disposition filename of a binary response, without leaking it into diagnostics."""

from __future__ import annotations
import hashlib
import logging
from dataclasses import fields

import httpx
import pytest

from b24api import Bitrix24, ExecutionPolicy, HttpxTransport, ReplaySafety, Request, RouteKind, Settings
from b24api.contracts import BinaryEvidence, BinaryResponse, FilenameStatus
from b24api.testing.scripted import ScriptedExchange, ScriptedTransport
from b24api.transport import WireResponse, disposition_filename

ABSENT = FilenameStatus.ABSENT
PARSED = FilenameStatus.PARSED
MALFORMED = FilenameStatus.MALFORMED
HEADER_MAXIMUM = 8192
FILENAME_MAXIMUM = 1024
PRIVATE_NAME = "quarterly-report-ZX81Q7.pdf"


def _one(value: str) -> tuple[tuple[str, str], ...]:
    return (("content-type", "application/octet-stream"), ("Content-Disposition", value))


# (case id, raw header value, expected name, expected status)
SINGLE_HEADER_CASES: tuple[tuple[str, str, str | None, FilenameStatus], ...] = (
    # no filename parameter at all
    ("type-only", "attachment", None, ABSENT),
    ("type-with-other-parameter", "inline; size=10", None, ABSENT),
    ("type-with-trailing-semicolon", "attachment;", None, ABSENT),
    # plain filename
    ("plain-quoted", 'attachment; filename="fixture.txt"', "fixture.txt", PARSED),
    ("plain-token", "attachment; filename=fixture.txt", "fixture.txt", PARSED),
    ("plain-case-insensitive-name", 'attachment; FileName="a.txt"', "a.txt", PARSED),
    ("plain-ows-around-equals", 'attachment; filename = "a.txt" ', "a.txt", PARSED),
    ("plain-inline-type", 'inline; filename="a.txt"', "a.txt", PARSED),
    ("plain-quoted-semicolon", 'attachment; filename="a;b.txt"', "a;b.txt", PARSED),
    ("plain-quoted-pair", r'attachment; filename="a\"b\\c.txt"', 'a"b\\c.txt', PARSED),
    ("plain-quoted-space", 'attachment; filename="my file.txt"', "my file.txt", PARSED),
    ("plain-no-basename", 'attachment; filename="../../etc/passwd"', "../../etc/passwd", PARSED),
    ("plain-no-normalization", 'attachment; filename="Café.txt"', "Café.txt", PARSED),
    ("plain-percent-stays-literal", "attachment; filename=a%20b.txt", "a%20b.txt", PARSED),
    ("plain-plus-stays-literal", "attachment; filename=a+b.txt", "a+b.txt", PARSED),
    ("plain-trailing-semicolon", 'attachment; filename="a.txt";', "a.txt", PARSED),
    ("plain-with-other-parameters", 'attachment; size=3; filename="a.txt"; x="y;z"', "a.txt", PARSED),
    ("plain-max-length", f'attachment; filename="{"n" * FILENAME_MAXIMUM}"', "n" * FILENAME_MAXIMUM, PARSED),
    # filename* (RFC 8187)
    ("ext-utf8", "attachment; filename*=UTF-8''%D1%84%D0%B0%D0%B9%D0%BB.txt", "файл.txt", PARSED),
    ("ext-utf8-lowercase-charset", "attachment; filename*=utf-8''a%20b.txt", "a b.txt", PARSED),
    ("ext-iso-8859-1", "attachment; filename*=ISO-8859-1''caf%E9.txt", "café.txt", PARSED),
    ("ext-language-tag", "attachment; filename*=UTF-8'en-US'a.txt", "a.txt", PARSED),
    ("ext-plus-not-space", "attachment; filename*=UTF-8''a+b.txt", "a+b.txt", PARSED),
    ("ext-lowercase-hex", "attachment; filename*=UTF-8''%c3%a9.txt", "é.txt", PARSED),
    ("ext-case-insensitive-name", "attachment; FILENAME*=UTF-8''a.txt", "a.txt", PARSED),
    # precedence: a valid filename* wins over a valid plain filename in either order
    ("ext-precedes-plain", "attachment; filename=\"plain.txt\"; filename*=UTF-8''ext.txt", "ext.txt", PARSED),
    ("ext-precedes-plain-first", "attachment; filename*=UTF-8''ext.txt; filename=\"plain.txt\"", "ext.txt", PARSED),
    # fallback: an unusable filename* falls back to a single valid plain filename
    ("fallback-unknown-charset", "attachment; filename*=KOI8-R''a.txt; filename=\"plain.txt\"", "plain.txt", PARSED),
    ("fallback-bad-triplet", "attachment; filename*=UTF-8''a%2.txt; filename=\"plain.txt\"", "plain.txt", PARSED),
    ("fallback-bad-utf8", "attachment; filename*=UTF-8''%FF.txt; filename=\"plain.txt\"", "plain.txt", PARSED),
    ("fallback-quoted-ext", "attachment; filename*=\"UTF-8''a.txt\"; filename=plain.txt", "plain.txt", PARSED),
    ("fallback-empty-ext", "attachment; filename*=UTF-8''; filename=plain.txt", "plain.txt", PARSED),
    ("fallback-control-ext", "attachment; filename*=UTF-8''a%00.txt; filename=plain.txt", "plain.txt", PARSED),
    # candidates present, none usable
    ("plain-empty-quoted", 'attachment; filename=""', None, MALFORMED),
    ("plain-empty-token", "attachment; filename=", None, MALFORMED),
    ("plain-unquoted-space", "attachment; filename=my file.txt", None, MALFORMED),
    ("plain-stray-quote", 'attachment; filename="a"b"', None, MALFORMED),
    ("plain-half-quoted", 'attachment; filename="a.txt"x', None, MALFORMED),
    ("plain-c1-control", 'attachment; filename="a\x85.txt"', None, MALFORMED),
    ("plain-surrogate", 'attachment; filename="a\ud800.txt"', None, MALFORMED),
    ("plain-overlong", f'attachment; filename="{"n" * (FILENAME_MAXIMUM + 1)}"', None, MALFORMED),
    ("ext-only-unknown-charset", "attachment; filename*=UTF-16''a.txt", None, MALFORMED),
    ("ext-only-missing-quotes", "attachment; filename*=UTF-8a.txt", None, MALFORMED),
    ("ext-only-bad-triplet", "attachment; filename*=UTF-8''a%G1.txt", None, MALFORMED),
    ("ext-only-truncated-triplet", "attachment; filename*=UTF-8''a%", None, MALFORMED),
    ("ext-only-raw-space", "attachment; filename*=UTF-8''a b.txt", None, MALFORMED),
    ("ext-only-raw-non-ascii", "attachment; filename*=UTF-8''файл.txt", None, MALFORMED),
    ("ext-only-invalid-utf8", "attachment; filename*=UTF-8''%C3%28", None, MALFORMED),
    ("ext-only-encoded-surrogate", "attachment; filename*=UTF-8''%ED%A0%80", None, MALFORMED),
    ("ext-only-control", "attachment; filename*=UTF-8''%0A.txt", None, MALFORMED),
    ("ext-only-overlong", f"attachment; filename*=UTF-8''{'n' * (FILENAME_MAXIMUM + 1)}", None, MALFORMED),
    ("both-unusable", "attachment; filename*=UTF-16''a; filename=\"\"", None, MALFORMED),
    # duplicates
    ("duplicate-plain", 'attachment; filename="a.txt"; filename="b.txt"', None, MALFORMED),
    ("duplicate-plain-case", 'attachment; filename="a.txt"; FILENAME="a.txt"', None, MALFORMED),
    ("duplicate-ext", "attachment; filename*=UTF-8''a.txt; filename*=UTF-8''b.txt", None, MALFORMED),
    # structural defects
    ("unclosed-quote", 'attachment; filename="a.txt', None, MALFORMED),
    ("unclosed-quote-escaped-close", 'attachment; filename="a.txt\\"', None, MALFORMED),
    ("unclosed-quote-in-other-parameter", 'attachment; x="y; filename="a.txt"', None, MALFORMED),
    ("control-newline", 'attachment; filename="a\n.txt"', None, MALFORMED),
    ("control-nul", 'attachment; filename="a\x00.txt"', None, MALFORMED),
    ("control-del", 'attachment; filename="a\x7f.txt"', None, MALFORMED),
    ("control-outside-filename", 'attachment; x="\x01"; filename="a.txt"', None, MALFORMED),
    ("c1-control-outside-filename", 'attachment; x="a\x85b"; filename=ok.txt', None, MALFORMED),
    ("c1-free-outside-filename", 'attachment; x="ab"; filename=ok.txt', "ok.txt", PARSED),
    ("parameter-without-equals", 'attachment; foo; filename="a.txt"', None, MALFORMED),
    ("parameter-without-name", 'attachment; ="a"; filename="a.txt"', None, MALFORMED),
    ("parameter-name-folds-to-filename", 'attachment; \ufb01lename="a.txt"', None, MALFORMED),
    ("missing-type", 'filename="a.txt"', None, MALFORMED),
    ("empty-type", '; filename="a.txt"', None, MALFORMED),
    ("empty-value", "", None, MALFORMED),
    # overflow of the whole header
    ("header-max-code-points", f"attachment; x={'v' * (HEADER_MAXIMUM - 26)}; filename=a", "a", PARSED),
    ("header-over-code-points", f"attachment; x={'v' * (HEADER_MAXIMUM - 25)}; filename=a", None, MALFORMED),
    ("header-over-bytes", f'attachment; x="{"é" * (HEADER_MAXIMUM // 2)}"; filename=a', None, MALFORMED),
)


def test_a25_single_header_case_table_is_exhaustive_over_statuses() -> None:
    assert {status for *_, status in SINGLE_HEADER_CASES} == set(FilenameStatus)
    assert len({case for case, *_ in SINGLE_HEADER_CASES}) == len(SINGLE_HEADER_CASES)
    assert len(SINGLE_HEADER_CASES[-3][1]) == HEADER_MAXIMUM
    assert len(SINGLE_HEADER_CASES[-2][1]) == HEADER_MAXIMUM + 1


@pytest.mark.parametrize(
    ("value", "name", "status"),
    [case[1:] for case in SINGLE_HEADER_CASES],
    ids=[case[0] for case in SINGLE_HEADER_CASES],
)
def test_a25_disposition_filename_single_header(value: str, name: str | None, status: FilenameStatus) -> None:
    assert disposition_filename(_one(value)) == (name, status)


@pytest.mark.parametrize(
    ("headers", "status"),
    [
        ((), ABSENT),
        ((("content-type", "text/plain"),), ABSENT),
        ((("content-disposition", 'attachment; filename="a"'), ("Content-Disposition", "attachment")), MALFORMED),
        ((("content-disposition", 'attachment; filename="a"'),) * 2, MALFORMED),
        ((("Content-Disposition", ""), ("content-disposition", 'attachment; filename="a"')), MALFORMED),
        ((("content-di\u017fposition", 'attachment; filename="a"'),), ABSENT),
    ],
    ids=[
        "no-headers",
        "absent",
        "repeated-header",
        "repeated-identical-header",
        "repeated-with-empty",
        "non-ascii-header-name",
    ],
)
def test_a25_disposition_filename_header_count(headers: tuple[tuple[str, str], ...], status: FilenameStatus) -> None:
    assert disposition_filename(headers) == (None, status)


def test_a25_evidence_defaults_to_absent_and_never_carries_the_name() -> None:
    evidence = BinaryEvidence(200, "text/plain", 1)

    assert evidence.filename_status is ABSENT
    assert [item.name for item in fields(BinaryEvidence)] == [
        "status_code",
        "content_type",
        "byte_length",
        "sha256",
        "filename_status",
    ]
    with pytest.raises(TypeError, match="FilenameStatus"):
        BinaryEvidence(200, None, 0, filename_status="parsed")  # type: ignore[arg-type]


def test_a25_binary_response_keeps_positional_signature_and_checks_status_consistency() -> None:
    parsed = BinaryEvidence(200, None, 1, filename_status=PARSED)
    absent = BinaryEvidence(200, None, 1)
    malformed = BinaryEvidence(200, None, 1, filename_status=MALFORMED)

    assert BinaryResponse(b"x", content_type=None, evidence=absent).filename is None
    assert BinaryResponse(b"x", content_type=None, evidence=parsed, filename="a.txt").filename == "a.txt"
    assert BinaryResponse(b"x", content_type=None, evidence=malformed).filename is None
    for evidence, filename in (
        (absent, "a.txt"),
        (malformed, "a.txt"),
        (parsed, None),
        (parsed, ""),
        (parsed, "a\x00"),
    ):
        with pytest.raises(ValueError, match="filename"):
            BinaryResponse(b"x", content_type=None, evidence=evidence, filename=filename)


def test_a25_filename_stays_out_of_repr_and_evidence() -> None:
    evidence = BinaryEvidence(200, "application/pdf", 3, filename_status=PARSED)
    response = BinaryResponse(b"pdf", content_type="application/pdf", evidence=evidence, filename=PRIVATE_NAME)

    assert response.filename == PRIVATE_NAME
    assert PRIVATE_NAME not in repr(response)
    assert PRIVATE_NAME not in repr(response.evidence)
    assert PRIVATE_NAME not in str(response)
    assert "filename" not in {item.name for item in fields(BinaryEvidence)} - {"filename_status"}


def _scripted_call(headers: tuple[tuple[str, str], ...], body: bytes) -> tuple[ScriptedTransport, Request]:
    request = Request.bare(
        "crm.item.getfile",
        {"entityTypeId": 2, "id": 1, "fieldName": "UF_CRM_FILE", "fileId": 7},
        ReplaySafety.SAFE,
    )
    transport = ScriptedTransport((ScriptedExchange(request, WireResponse(200, headers, body)),))
    return transport, request


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("disposition", "name", "status"),
    [
        (None, None, ABSENT),
        (f'attachment; filename="{PRIVATE_NAME}"', PRIVATE_NAME, PARSED),
        ("attachment; filename=\"fallback.txt\"; filename*=UTF-8''%D1%84.txt", "ф.txt", PARSED),
        ("attachment; filename*=UTF-16''a; filename=\"plain.txt\"", "plain.txt", PARSED),
        (f'attachment; filename="{PRIVATE_NAME}"; filename="b"', None, MALFORMED),
        (f'attachment; filename="{PRIVATE_NAME}', None, MALFORMED),
        (f'attachment; x="{"v" * HEADER_MAXIMUM}"; filename="{PRIVATE_NAME}"', None, MALFORMED),
    ],
    ids=["absent", "plain", "filename-star", "fallback", "duplicates", "malformed", "overflow"],
)
async def test_a25_call_bytes_via_scripted_transport_reports_status_and_keeps_body(
    disposition: str | None,
    name: str | None,
    status: FilenameStatus,
    caplog: pytest.LogCaptureFixture,
) -> None:
    body = bytes(range(256)) * 4
    headers: tuple[tuple[str, str], ...] = (("content-type", "application/pdf"),)
    if disposition is not None:
        headers += (("content-disposition", disposition),)
    transport, request = _scripted_call(headers, body)
    policy = ExecutionPolicy(binary_digest=True)

    caplog.set_level(logging.DEBUG)
    async with Bitrix24(
        Settings(webhook_url="https://fixture.invalid/rest/1/test/"), transport=transport, policy=policy
    ) as api:
        response = await api.call_bytes(request)

    transport.assert_exhausted()
    assert len(transport.calls) == 1
    assert (response.filename, response.evidence.filename_status) == (name, status)
    assert response.body == body
    assert response.evidence.sha256 == hashlib.sha256(body).hexdigest()
    assert response.byte_length == len(body)
    assert PRIVATE_NAME not in repr(response)
    assert PRIVATE_NAME not in repr(response.evidence)
    assert PRIVATE_NAME not in caplog.text


@pytest.mark.asyncio
async def test_a25_call_bytes_via_httpx_mock_transport_reads_raw_headers(caplog: pytest.LogCaptureFixture) -> None:
    body = b"%PDF-1.7 fixture"
    encoded_name = "%D0%BE%D1%82%D1%87%D1%91%D1%82-ZX81Q7.pdf"

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            headers=[
                ("Content-Type", "application/pdf"),
                ("Content-Disposition", f"attachment; filename=\"report.pdf\"; filename*=UTF-8''{encoded_name}"),
            ],
            content=body,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    webhook = "https://portal.invalid/rest/1/token/"
    transport = HttpxTransport(webhook, client=client)
    caplog.set_level(logging.DEBUG)
    try:
        async with Bitrix24(Settings(webhook_url=webhook), transport=transport) as api:
            response = await api.call_bytes(
                Request("crm.item.getfile", {"fileId": 7}, ReplaySafety.SAFE, route=RouteKind.BARE),
            )
    finally:
        await client.aclose()

    assert response.filename == "отчёт-ZX81Q7.pdf"
    assert response.evidence.filename_status is PARSED
    assert response.body == body
    assert "ZX81Q7" not in repr(response)
    assert "ZX81Q7" not in caplog.text


@pytest.mark.asyncio
async def test_a25_call_bytes_via_httpx_mock_transport_rejects_repeated_raw_headers() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            headers=[
                ("Content-Type", "application/pdf"),
                ("Content-Disposition", 'attachment; filename="a.pdf"'),
                ("Content-Disposition", 'attachment; filename="b.pdf"'),
            ],
            content=b"x",
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    webhook = "https://portal.invalid/rest/1/token/"
    try:
        async with Bitrix24(Settings(webhook_url=webhook), transport=HttpxTransport(webhook, client=client)) as api:
            response = await api.call_bytes(
                Request("crm.item.getfile", {"fileId": 7}, ReplaySafety.SAFE, route=RouteKind.BARE)
            )
    finally:
        await client.aclose()

    assert (response.filename, response.evidence.filename_status) == (None, MALFORMED)
    assert response.body == b"x"
