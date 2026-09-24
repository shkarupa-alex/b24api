"""Offline transport-shape regressions for the opt-in live harness."""

from __future__ import annotations
import base64
import gc
import gzip
import json
import logging
import tracemalloc
import weakref
import zlib
from typing import TYPE_CHECKING

import httpx
import pytest

from b24api.transport.logging_shield import HTTPX_LOG_SHIELD

from . import cli as cli_module
from . import live as live_module
from .contracts import ContractError, ExitCode
from .live import (
    ADAPTERS,
    MAX_RESPONSE_BYTES,
    LiveApiError,
    LiveCorrectnessError,
    LivePortal,
    LiveUnavailableError,
    _bounded_response_payload,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

EXPECTED_PREFLIGHT_CALLS = 2


def _portal(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]) -> LivePortal:
    key = base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip("=")
    monkeypatch.setenv("BITRIX24_EVIDENCE_FINGERPRINT_KEY", key)
    monkeypatch.setenv("BITRIX24_API_WEBHOOK_URL", "https://example.invalid" + "/rest/1/" + "not-a-secret/")
    portal = LivePortal(role="admin_full")
    portal._client.close()  # noqa: SLF001 - replace the network transport for an offline regression
    portal._client = httpx.Client(transport=httpx.MockTransport(handler))  # noqa: SLF001
    return portal


def _assert_live_error_redacted(error: BaseException, sensitive_fragment: str) -> None:
    assert sensitive_fragment not in str(error)
    assert sensitive_fragment not in repr(error.__dict__)
    assert error.__cause__ is None
    assert error.__context__ is None
    traceback = error.__traceback__
    while traceback is not None:
        if traceback.tb_frame.f_code.co_filename.endswith("harness/live.py"):
            for value in traceback.tb_frame.f_locals.values():
                assert sensitive_fragment not in repr(value)
                assert sensitive_fragment not in getattr(value, "_webhook_url", "")
                if isinstance(value, tuple):
                    for item in value:
                        assert sensitive_fragment not in getattr(item, "_webhook_url", "")
        traceback = traceback.tb_next


def test_exact_marker_scan_follows_every_continuation(monkeypatch: pytest.MonkeyPatch) -> None:
    starts: list[int | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        parameters = json.loads(request.content)
        starts.append(parameters.get("start"))
        if len(starts) == 1:
            return httpx.Response(200, json={"result": [{"ID": 1, "TITLE": "owned"}], "next": 2})
        return httpx.Response(200, json={"result": [{"ID": 2, "TITLE": "owned"}]})

    with _portal(monkeypatch, handler) as portal:
        assert ADAPTERS["tasks-task-v1"].find_exact_marker(portal, "owned") == ["1", "2"]
        assert portal.attempts == len(starts)
    assert starts == [None, 2]


def test_live_envelope_rejects_overflow_json_and_hides_unknown_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(
        (
            httpx.Response(200, content=b'{"result":1e400}'),
            httpx.Response(200, json={"error": "Bear" + "er should-never-be-rendered"}),
        ),
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    with _portal(monkeypatch, handler) as portal:
        with pytest.raises(LiveCorrectnessError, match="not JSON"):
            portal.call("scope")
        with pytest.raises(LiveCorrectnessError) as captured:
            portal.call("scope")
    assert "should-never-be-rendered" not in str(captured.value)


@pytest.mark.parametrize("payload_case", ["api_error", "malformed"])
def test_live_envelope_drops_hostile_payload_from_error_and_traceback(
    monkeypatch: pytest.MonkeyPatch,
    payload_case: str,
) -> None:
    sensitive_fragment = "synthetic-live-hostile-fragment"

    def handler(_request: httpx.Request) -> httpx.Response:
        if payload_case == "malformed":
            return httpx.Response(200, content=b'{"result":"' + sensitive_fragment.encode())
        return httpx.Response(200, json={"error": sensitive_fragment})

    expected = LiveCorrectnessError if payload_case == "malformed" else LiveApiError
    with _portal(monkeypatch, handler) as portal, pytest.raises(expected) as captured:
        portal.call("scope")

    error = captured.value
    _assert_live_error_redacted(error, sensitive_fragment)
    if isinstance(error, LiveApiError):
        assert error.code == "unexpected_api_error"


def test_live_transport_error_drops_credentialed_httpx_cause_and_locals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_fragment = "not-a-secret"

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("synthetic connection failure", request=request)

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveUnavailableError) as captured:
        portal.call("scope")

    _assert_live_error_redacted(captured.value, sensitive_fragment)


@pytest.mark.parametrize("configuration_case", ["fingerprint", "webhook"])
def test_live_portal_configuration_error_drops_credential_locals(
    monkeypatch: pytest.MonkeyPatch,
    configuration_case: str,
) -> None:
    sensitive_fragment = "synthetic-live-configuration-fragment"
    key = base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip("=")
    monkeypatch.setenv("BITRIX24_EVIDENCE_FINGERPRINT_KEY", key)
    monkeypatch.setenv(
        "BITRIX24_API_WEBHOOK_URL",
        f"https://example.invalid/rest/1/{sensitive_fragment}/",
    )
    if configuration_case == "fingerprint":
        monkeypatch.setenv("BITRIX24_EVIDENCE_FINGERPRINT_KEY", sensitive_fragment)
    else:
        monkeypatch.setenv(
            "BITRIX24_API_WEBHOOK_URL",
            f"https://example.invalid:bad/rest/1/{sensitive_fragment}/",
        )

    with pytest.raises(ContractError) as captured:
        LivePortal(role="admin_full")

    _assert_live_error_redacted(captured.value, sensitive_fragment)


def test_live_portal_drops_webhook_when_httpx_client_initialization_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_fragment = "synthetic-live-client-init-fragment"
    key = base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip("=")
    monkeypatch.setenv("BITRIX24_EVIDENCE_FINGERPRINT_KEY", key)
    monkeypatch.setenv(
        "BITRIX24_API_WEBHOOK_URL",
        f"https://example.invalid/rest/1/{sensitive_fragment}/",
    )

    def fail_client(*_args: object, **_kwargs: object) -> httpx.Client:
        raise httpx.InvalidURL("synthetic client initialization failure")

    monkeypatch.setattr(httpx, "Client", fail_client)

    with pytest.raises(LiveUnavailableError, match="client configuration") as captured:
        LivePortal(role="admin_full")

    _assert_live_error_redacted(captured.value, sensitive_fragment)


def test_live_portal_webhook_vault_is_opaque_and_gc_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    sensitive_fragment = "not-a-secret"
    portal = _portal(monkeypatch, lambda _request: httpx.Response(200, json={"result": []}))
    client = portal._client  # noqa: SLF001 - lifecycle regression
    handle = portal._webhook_handle  # noqa: SLF001 - lifecycle regression
    reference = weakref.ref(portal)

    assert sensitive_fragment not in repr(live_module.__dict__)
    del portal
    gc.collect()

    assert reference() is None
    with pytest.raises(LiveUnavailableError, match="credential is unavailable"):
        live_module._webhook_for(handle)  # noqa: SLF001 - lifecycle regression
    client.close()


@pytest.mark.parametrize("response_case", ["scope", "create", "missing_result"])
def test_live_semantic_errors_drop_hostile_response_locals(
    monkeypatch: pytest.MonkeyPatch,
    response_case: str,
) -> None:
    sensitive_fragment = "synthetic-live-semantic-fragment"

    def handler(_request: httpx.Request) -> httpx.Response:
        if response_case == "scope":
            return httpx.Response(200, json={"result": {sensitive_fragment: "N"}})
        if response_case == "create":
            return httpx.Response(200, json={"result": {"hostile": sensitive_fragment}})
        return httpx.Response(200, json={"hostile": sensitive_fragment})

    with _portal(monkeypatch, handler) as portal:

        def invoke() -> None:
            if response_case == "scope":
                portal.preflight(required_scopes={"task"})
            elif response_case == "create":
                ADAPTERS["crm-deal-v1"].create(portal, "owned-marker")
            else:
                portal.call("scope")

        with pytest.raises(LiveCorrectnessError) as captured:
            invoke()

    _assert_live_error_redacted(captured.value, sensitive_fragment)


def test_live_envelope_refuses_oversized_content_before_buffering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-length": str(MAX_RESPONSE_BYTES + 1)},
            content=b"{}",
        )

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveCorrectnessError, match="byte ceiling"):
        portal.call("scope")


def test_bounded_payload_error_drops_credentialed_response_local() -> None:
    sensitive_fragment = "synthetic-bounded-response-fragment"
    request = httpx.Request(
        "GET",
        f"https://example.invalid/rest/1/{sensitive_fragment}/scope",
    )
    response = httpx.Response(200, headers={"content-length": sensitive_fragment}, request=request)

    with pytest.raises(LiveCorrectnessError) as captured:
        _bounded_response_payload(response, method="scope")

    _assert_live_error_redacted(captured.value, sensitive_fragment)


def test_live_http_status_error_drops_credentialed_response_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_fragment = "not-a-secret"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"hostile": "ignored"})

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveUnavailableError) as captured:
        portal.call("scope")

    _assert_live_error_redacted(captured.value, sensitive_fragment)


@pytest.mark.parametrize(
    "scope_result",
    [
        {"task": "N"},
        ["", "task"],
    ],
)
def test_live_preflight_rejects_hostile_scope_types(
    monkeypatch: pytest.MonkeyPatch,
    scope_result: object,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": scope_result})

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveCorrectnessError):
        portal.preflight(required_scopes={"task"})


@pytest.mark.parametrize(
    "app_result",
    [
        {"SCOPE": ["task"], "LICENSE": "nfr"},
        {"VERSION": 4},
        {"version": "4"},
        {"VERSION": 4, "BUILD": "26.500.0"},
        {"BUILD": "26.500.0"},
        {"build": "26.500.0"},
    ],
)
def test_live_preflight_never_treats_app_info_fields_as_portal_build(
    monkeypatch: pytest.MonkeyPatch,
    app_result: object,
) -> None:
    results = iter((["task"], app_result))
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"result": next(results)})

    with _portal(monkeypatch, handler) as portal:
        assert portal.preflight(required_scopes={"task"}).build is None
    assert calls == EXPECTED_PREFLIGHT_CALLS


@pytest.mark.parametrize(
    "app_result",
    [
        {"SCOPE": ["task"], "LICENSE": "nfr"},
        {"VERSION": 4},
        {"version": "4"},
        {"VERSION": 4, "BUILD": "26.500.0"},
        {"BUILD": "26.500.0"},
        {"build": "26.500.0"},
    ],
)
def test_public_main_publishes_portal_identity_without_claiming_unknown_build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    app_result: object,
) -> None:
    results = iter((["task"], app_result))
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"result": next(results)})

    portal = _portal(monkeypatch, handler)

    def portal_factory(*, role: str) -> LivePortal:
        assert role == "admin_full"
        return portal

    monkeypatch.delenv("PYTEST_CURRENT_TEST")
    monkeypatch.setattr(cli_module, "LivePortal", portal_factory)
    monkeypatch.setattr(cli_module, "require_clean_tracked_tree", lambda _root: None)
    artifact_dir = tmp_path / "artifacts"

    result = cli_module.main(
        [
            "plan",
            "--artifact-dir",
            str(artifact_dir),
            "--live",
            "--credential-role",
            "admin_full",
            "--entity-profile",
            "tasks-task-v1",
            "--count",
            "5",
        ],
    )

    assert result == ExitCode.COMPLETED
    assert capsys.readouterr().err == ""
    plan = json.loads((artifact_dir / "dataset-plan.json").read_text())
    assert plan["portal"]["host"] == "example.invalid"
    assert plan["portal"]["build"] is None
    assert {path.name for path in artifact_dir.iterdir()} == {
        ".b24api-transaction-bundle.lock",
        "benchmark-plan.json",
        "dataset-plan.json",
        "model-fixture-manifest.json",
        "plan-evidence.json",
    }
    assert calls == EXPECTED_PREFLIGHT_CALLS


def test_live_preflight_maps_the_portal_empty_scope_sentinel_to_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"result": [""]})

    with (
        _portal(monkeypatch, handler) as portal,
        pytest.raises(
            LiveUnavailableError,
            match="required scope unavailable: task",
        ),
    ):
        portal.preflight(required_scopes={"task"})

    assert calls == 1


def test_streamed_response_has_no_per_chunk_retention_amplification() -> None:
    chunk_count = 100_000

    class OneByteStream(httpx.SyncByteStream):
        def __iter__(self) -> Iterator[bytes]:
            for _ in range(chunk_count):
                yield b"x"

    response = httpx.Response(200, stream=OneByteStream())
    tracemalloc.start()
    try:
        payload = _bounded_response_payload(response, method="scope")
        _, peak = tracemalloc.get_traced_memory()
    finally:
        response.close()
        tracemalloc.stop()
    assert len(payload) == chunk_count
    assert peak < chunk_count * 10


def test_point_read_classifies_typed_not_found_without_rendering_portal_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"error": "ERROR_NOT_FOUND", "error_description": "secret-like text"})

    with _portal(monkeypatch, handler) as portal:
        assert ADAPTERS["crm-deal-v1"].read(portal, "42") is None
        assert portal.attempts == 1


def test_non_ok_point_read_still_classifies_structured_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "ERROR_NOT_FOUND"})

    with _portal(monkeypatch, handler) as portal:
        assert ADAPTERS["crm-deal-v1"].read(portal, "42") is None


def test_crm_empty_error_code_with_exact_not_found_description_proves_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "", "error_description": "Not found"})

    with _portal(monkeypatch, handler) as portal:
        assert ADAPTERS["crm-deal-v1"].read(portal, "42") is None


@pytest.mark.parametrize("description", ["", "not found", "Access denied", None])
def test_crm_empty_error_code_with_any_other_description_remains_correctness_failure(
    monkeypatch: pytest.MonkeyPatch,
    description: str | None,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "", "error_description": description})

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveApiError):
        ADAPTERS["crm-deal-v1"].read(portal, "42")


@pytest.mark.parametrize("status_code", [200, 500])
def test_crm_empty_error_code_not_found_pair_requires_observed_http_status(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={"error": "", "error_description": "Not found"})

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveApiError):
        ADAPTERS["crm-deal-v1"].read(portal, "42")


def test_empty_error_code_not_found_pair_never_proves_tasks_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "", "error_description": "Not found"})

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveApiError):
        ADAPTERS["tasks-task-v1"].read(portal, "42")


@pytest.mark.parametrize("status_code", [600, 999])
def test_malformed_http_status_never_proves_point_read_absence(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={"error": "ERROR_NOT_FOUND"})

    with (
        _portal(monkeypatch, handler) as portal,
        pytest.raises(
            LiveCorrectnessError,
            match="invalid HTTP status",
        ),
    ):
        ADAPTERS["crm-deal-v1"].read(portal, "42")


@pytest.mark.parametrize("profile_id", ["crm-deal-v1", "tasks-task-v1"])
def test_successful_null_point_read_never_proves_absence(
    monkeypatch: pytest.MonkeyPatch,
    profile_id: str,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        result = None if profile_id == "crm-deal-v1" else {"task": None}
        return httpx.Response(200, json={"result": result})

    with (
        _portal(monkeypatch, handler) as portal,
        pytest.raises(
            LiveCorrectnessError,
            match="returned no entity",
        ),
    ):
        ADAPTERS[profile_id].read(portal, "42")


def test_invalid_content_encoding_is_correctness_not_unavailability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-encoding": "gzip", "content-type": "application/json"},
            content=b"not-a-gzip-stream",
        )

    with (
        _portal(monkeypatch, handler) as portal,
        pytest.raises(
            LiveCorrectnessError,
            match="decoding failed",
        ) as captured,
    ):
        portal.call("scope")
    assert not isinstance(captured.value, LiveUnavailableError)


def test_point_read_never_treats_not_found_substrings_as_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"error": "permission_not_found_but_entity_still_exists"})

    with _portal(monkeypatch, handler) as portal, pytest.raises(LiveApiError):
        ADAPTERS["crm-deal-v1"].read(portal, "42")


class _RawSyncStream(httpx.SyncByteStream):
    """Serve raw transfer bytes in fixed chunks and count what was read."""

    def __init__(self, raw: bytes, *, chunk_size: int = 65_536) -> None:
        self.raw = raw
        self.chunk_size = chunk_size
        self.served = 0

    def __iter__(self) -> Iterator[bytes]:
        for index in range(0, len(self.raw), self.chunk_size):
            chunk = self.raw[index : index + self.chunk_size]
            self.served += len(chunk)
            yield chunk


def _bounded_peak(response: httpx.Response) -> tuple[Exception | bytes, int]:
    tracemalloc.start()
    try:
        outcome: Exception | bytes = _bounded_response_payload(response, method="scope")
    except LiveCorrectnessError as error:
        outcome = error
    finally:
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        response.close()
    return outcome, peak


def test_live_gzip_bomb_is_refused_inside_the_bounded_decoder() -> None:
    bomb = gzip.compress(bytes(4 * MAX_RESPONSE_BYTES))
    response = httpx.Response(200, headers={"content-encoding": "gzip"}, stream=_RawSyncStream(bomb))

    outcome, peak = _bounded_peak(response)

    assert isinstance(outcome, LiveCorrectnessError)
    assert "byte ceiling" in str(outcome)
    assert peak < 2 * MAX_RESPONSE_BYTES + (1 << 20)


def test_live_stacked_gzip_cascade_is_refused_before_decompression() -> None:
    stream = _RawSyncStream(gzip.compress(gzip.compress(bytes(4 * MAX_RESPONSE_BYTES))))
    response = httpx.Response(200, headers={"content-encoding": "gzip, gzip"}, stream=stream)

    outcome, peak = _bounded_peak(response)

    assert isinstance(outcome, LiveCorrectnessError)
    assert "decoding failed" in str(outcome)
    assert stream.served == 0
    assert peak < 2 * MAX_RESPONSE_BYTES + (1 << 20)


@pytest.mark.parametrize("wbits", [zlib.MAX_WBITS, -zlib.MAX_WBITS], ids=["zlib", "raw"])
def test_live_deflate_fed_one_byte_at_a_time_decodes_once(wbits: int) -> None:
    payload = json.dumps({"result": ["x" * 1024]}).encode()
    compressor = zlib.compressobj(wbits=wbits)
    encoded = compressor.compress(payload) + compressor.flush()
    stream = _RawSyncStream(encoded, chunk_size=1)
    response = httpx.Response(200, headers={"content-encoding": "deflate"}, stream=stream)

    assert _bounded_response_payload(response, method="scope") == payload


@pytest.mark.parametrize("coding", ["br", "zstd", "unknown"])
def test_live_unbounded_codings_are_refused_as_decoding_failures(coding: str) -> None:
    stream = _RawSyncStream(b"opaque")
    response = httpx.Response(200, headers={"content-encoding": coding}, stream=stream)

    with pytest.raises(LiveCorrectnessError, match="decoding failed") as captured:
        _bounded_response_payload(response, method="scope")
    assert stream.served == 0
    assert captured.value.__context__ is None


def test_live_portal_advertises_only_bounded_codings(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("accept-encoding"))
        return httpx.Response(200, json={"result": []})

    with _portal(monkeypatch, handler) as portal:
        portal.call("scope")
    assert seen == ["gzip, deflate"]


_LIVE_TOKEN = "not-a-secret"  # noqa: S105 - the synthetic webhook token of ``_portal``
_FOREIGN_LIVE_URL = "https://other.invalid" + "/rest/7/" + "synthetic-foreign-live-secret/profile"


def test_live_portal_httpx_info_record_carries_no_webhook_token(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="httpx")

    with _portal(monkeypatch, lambda _request: httpx.Response(200, json={"result": []})) as portal:
        portal.call("profile")

    records = [record for record in caplog.records if record.name == "httpx"]
    assert len(records) == 1
    record = records[0]
    assert record.pathname.endswith("httpx/_client.py")
    assert _LIVE_TOKEN not in f"{record.msg!r} {record.args!r} {record.__dict__!r}"
    assert _LIVE_TOKEN not in record.getMessage()
    assert "[REDACTED]" in record.getMessage()
    assert _LIVE_TOKEN not in caplog.text


def test_live_portal_sync_attribution_leaves_foreign_records_on_the_same_logger_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A foreign send from inside the owned one: only attribution by the sync client's ``_send_handling_auth``
    # root can tell its record apart; without it the record would be scrubbed like an owned hop.
    caplog.set_level(logging.INFO, logger="httpx")
    logger = logging.getLogger("httpx")
    failure = RuntimeError("foreign failure without any registered secret")

    def foreign_hook(_response: httpx.Response) -> None:
        logger.error("foreign %s", _FOREIGN_LIVE_URL, exc_info=(RuntimeError, failure, None))

    foreign = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, request=request)),
        event_hooks={"response": [foreign_hook]},
    )

    def hook(_request: httpx.Request) -> None:
        foreign.get(_FOREIGN_LIVE_URL)

    with _portal(monkeypatch, lambda _request: httpx.Response(200, json={"result": []})) as portal:
        portal._client.event_hooks["request"] = [hook]  # noqa: SLF001 - inject a foreign send into the owned one
        portal.call("profile")
    foreign.close()

    records = [record for record in caplog.records if record.name == "httpx"]
    assert len(records) == 3  # noqa: PLR2004 - foreign send, foreign error, owned send
    foreign_send, foreign_error, owned_send = records
    assert isinstance(foreign_send.args, tuple)
    assert isinstance(foreign_send.args[1], httpx.URL)
    assert str(foreign_send.args[1]) == _FOREIGN_LIVE_URL
    assert foreign_error.args == (_FOREIGN_LIVE_URL,)
    assert foreign_error.exc_info == (RuntimeError, failure, None)
    assert _FOREIGN_LIVE_URL in caplog.text
    assert _LIVE_TOKEN not in owned_send.getMessage()
    assert _LIVE_TOKEN not in caplog.text


def test_live_portal_holds_its_shield_registration_exactly_for_its_lifetime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = HTTPX_LOG_SHIELD._transports  # noqa: SLF001 - registration lifecycle control
    portal = _portal(monkeypatch, lambda _request: httpx.Response(200, json={"result": []}))
    assert HTTPX_LOG_SHIELD._transports == initial + 1  # noqa: SLF001
    portal.close()
    assert HTTPX_LOG_SHIELD._transports == initial  # noqa: SLF001

    def fail_client(*_args: object, **_kwargs: object) -> httpx.Client:
        raise httpx.InvalidURL("synthetic client initialization failure")

    monkeypatch.setattr(httpx, "Client", fail_client)
    with pytest.raises(LiveUnavailableError, match="client configuration"):
        LivePortal(role="admin_full")
    assert HTTPX_LOG_SHIELD._transports == initial  # noqa: SLF001 - a failed initialization leaves none
