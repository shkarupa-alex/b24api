"""Scenario 16: exact binary bytes and typed JSON error from public call_bytes.

Offline fixture: readable disposable document and file IDs plus one denied file.
`crm.documentgenerator.document.getpdf` returns PDF bytes; `rest.file.get`
returns opaque bytes. A non-2xx JSON API error remains typed. No credential URL
or downloaded bytes are printed. Run: `uv run python -m examples.binary_download`.
"""

from __future__ import annotations
import asyncio
import json

from b24api import ApiResponseError, Bitrix24, ReplaySafety, Request, RouteKind, Settings, WireResponse
from b24api.testing import ScriptedExchange, ScriptedTransport
from examples._support.evidence import RecipeEvidence

EXPECTED_PDF = b"%PDF-1.4\nfixture\n%%EOF\n"
EXPECTED_FILE = bytes((0, 1, 2, 255))


def _request(method: str, identifier: str) -> Request:
    return Request(method, {"id": identifier}, replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _fixture() -> ScriptedTransport:
    pdf = _request("crm.documentgenerator.document.getpdf", "fixture-doc")
    file = _request("rest.file.get", "fixture-file")
    denied = _request("rest.file.get", "denied-file")
    return ScriptedTransport(
        (
            ScriptedExchange(pdf, WireResponse(200, (("content-type", "application/pdf"),), EXPECTED_PDF)),
            ScriptedExchange(file, WireResponse(200, (("content-type", "application/octet-stream"),), EXPECTED_FILE)),
            ScriptedExchange(
                denied,
                WireResponse(
                    403,
                    (("content-type", "application/json"),),
                    json.dumps({"error": "ACCESS_DENIED", "error_description": "fixture denied"}).encode(),
                ),
            ),
        )
    )


async def run() -> RecipeEvidence:
    """Check exact payload, media type, and failure classification offline."""
    transport = _fixture()
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        pdf = await client.call_bytes(_request("crm.documentgenerator.document.getpdf", "fixture-doc"))
        file = await client.call_bytes(_request("rest.file.get", "fixture-file"))
        if pdf.body != EXPECTED_PDF or pdf.content_type != "application/pdf":
            raise AssertionError("scenario 16 PDF bytes or media type differ from oracle")
        if file.body != EXPECTED_FILE or file.content_type != "application/octet-stream":
            raise AssertionError("scenario 16 file bytes or media type differ from oracle")
        try:
            await client.call_bytes(_request("rest.file.get", "denied-file"))
        except ApiResponseError as error:
            if error.original_code != "ACCESS_DENIED" or error.normalized_code != "access_denied":
                raise AssertionError("scenario 16 JSON API error code differs from oracle") from error
        else:
            raise AssertionError("scenario 16 JSON API error was treated as binary success")
    transport.assert_exhausted()
    return RecipeEvidence(len(pdf.body))


if __name__ == "__main__":
    asyncio.run(run())
