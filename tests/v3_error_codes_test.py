"""REST 3.0 error codes are shown verbatim, and recognizing one never decides whether it is retried (issue 31).

The server sends the upper-cased PHP class name of the thrown exception as ``error.code`` unless the class
overrides ``getRegistryCode()``, so the set is open and not limited to one prefix. The table below holds
both kinds, taken from a Bitrix module snapshot: every class-derived code listed in issue 31, the codes the
overriding classes return instead, and the load limiter's query time code.
"""

from __future__ import annotations
import json

import pytest

from b24api.contracts.policy import AmbiguityPolicy, ExecutionPolicy, RetryPolicy
from b24api.contracts.request import ReplaySafety, Request, RouteKind, diagnostic_context
from b24api.contracts.v3_codes import (
    KNOWN_V3_ERROR_CODES,
    METHOD_NOT_FOUND_ERROR_CODE,
    QUERY_TIME_LIMIT_ERROR_CODE,
    RATE_LIMIT_ERROR_CODE,
    VALIDATION_ERROR_CODE,
    is_v3_code_shape,
    render_code,
)
from b24api.errors import ApiResponseError
from b24api.execution import Executor, WireResponse
from b24api.redaction import REDACTED, Redactor, webhook_secrets
from b24api.transport.protocol import ProtocolCodec
from tests.scripting import ResponderTransport, replies

# The default rule: ``strtoupper(str_replace('\\', '_', static::class))``, as listed in issue 31.
CLASS_DERIVED_CODES = (
    "BITRIX_AIASSISTANT_INFRASTRUCTURE_REST_V3_EXCEPTION_SCHEDULEDTASKCONFLICTRESTEXCEPTION",
    "BITRIX_AIASSISTANT_INFRASTRUCTURE_REST_V3_EXCEPTION_SCHEDULEDTASKEMPTYUPDATERESTEXCEPTION",
    "BITRIX_AIASSISTANT_INFRASTRUCTURE_REST_V3_EXCEPTION_SCHEDULEDTASKLIMITRESTEXCEPTION",
    "BITRIX_AIASSISTANT_INFRASTRUCTURE_REST_V3_EXCEPTION_SCHEDULEDTASKSTATERESTEXCEPTION",
    "BITRIX_AIASSISTANT_INFRASTRUCTURE_REST_V3_EXCEPTION_SCHEDULEDTASKVALIDATIONRESTEXCEPTION",
    "BITRIX_BIZPROCDESIGNER_INFRASTRUCTURE_REST_EXCEPTION_BLOCKCATALOGUNAVAILABLEEXCEPTION",
    "BITRIX_BIZPROCDESIGNER_INFRASTRUCTURE_REST_EXCEPTION_BLOCKTYPENOTFOUNDEXCEPTION",
    "BITRIX_BIZPROCDESIGNER_INFRASTRUCTURE_REST_EXCEPTION_DRAFTSAVEFAILEDEXCEPTION",
    "BITRIX_BIZPROCDESIGNER_INFRASTRUCTURE_REST_EXCEPTION_GRAPHVALIDATIONEXCEPTION",
    "BITRIX_CALL_CONTROLLER_EXCEPTION_FOLLOWUPNOTACCESSIBLEEXCEPTION",
    "BITRIX_CALL_CONTROLLER_EXCEPTION_INVALIDDATERANGEEXCEPTION",
    "BITRIX_CALL_CONTROLLER_EXCEPTION_INVALIDORDEREXCEPTION",
    "BITRIX_CALL_CONTROLLER_EXCEPTION_INVALIDPAGINATIONEXCEPTION",
    "BITRIX_CALL_CONTROLLER_EXCEPTION_INVALIDSELECTFIELDEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_FORMMODULEUNAVAILABLEEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_FORMPARTIALLYCREATEDEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_FORMPHONEVERIFICATIONREQUIREDEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_FORMSIDEEFFECTEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_FORMVERSIONMISMATCHEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_FORMWRITERESTRICTEDEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_OPENLINEACCESSDENIEDEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_OPENLINESUNAVAILABLEEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_PHONEVERIFICATIONREQUIREDEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_SITEBUTTONACCESSDENIEDEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_SYSTEMFORMIMMUTABLEEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_UNKNOWNFORMENTITYEXCEPTION",
    "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_TIMELINE_ACTIVITY_EMAIL_CRMACTIVITYMAILRESTEXCEPTION",
    "BITRIX_LANDING_INFRASTRUCTURE_REST_EXCEPTION_AIMODULEUNAVAILABLEEXCEPTION",
    "BITRIX_LANDING_INFRASTRUCTURE_REST_EXCEPTION_AISITESDISABLEDEXCEPTION",
    "BITRIX_LANDING_INFRASTRUCTURE_REST_EXCEPTION_AISITESUNAVAILABLEEXCEPTION",
    "BITRIX_LANDING_INFRASTRUCTURE_REST_EXCEPTION_BINDINGUNAVAILABLEEXCEPTION",
    "BITRIX_LANDING_INFRASTRUCTURE_REST_EXCEPTION_FREEDRAFTUNAVAILABLEEXCEPTION",
    "BITRIX_LANDING_INFRASTRUCTURE_REST_EXCEPTION_GENERATIONNOTFOUNDEXCEPTION",
    "BITRIX_LANDING_INFRASTRUCTURE_REST_EXCEPTION_MODERATIONBLOCKLIMITEXCEPTION",
    "BITRIX_NOTE_INFRASTRUCTURE_REST_V3_EXCEPTIONS_COLLECTIONUNDERIMPORTEXCEPTION",
    "BITRIX_NOTE_INFRASTRUCTURE_REST_V3_EXCEPTIONS_DOCUMENTHASUNSAVEDCHANGESEXCEPTION",
    "BITRIX_NOTE_INFRASTRUCTURE_REST_V3_EXCEPTIONS_EMPTYUPDATEEXCEPTION",
    "BITRIX_NOTE_INFRASTRUCTURE_REST_V3_EXCEPTIONS_FILETOOLARGEEXCEPTION",
    "BITRIX_NOTE_INFRASTRUCTURE_REST_V3_EXCEPTIONS_FILETYPENOTALLOWEDEXCEPTION",
    "BITRIX_NOTE_INFRASTRUCTURE_REST_V3_EXCEPTIONS_INVALIDPARENTEXCEPTION",
    "BITRIX_NOTE_INFRASTRUCTURE_REST_V3_EXCEPTIONS_MARKDOWNTOOLARGEEXCEPTION",
    "BITRIX_NOTE_INFRASTRUCTURE_REST_V3_EXCEPTIONS_MOVEACCESSESCALATIONEXCEPTION",
    "BITRIX_NOTE_INFRASTRUCTURE_REST_V3_EXCEPTIONS_SEARCHQUERYTOOLONGEXCEPTION",
    "BITRIX_NOTE_INFRASTRUCTURE_REST_V3_EXCEPTIONS_SEARCHQUERYTOOSHORTEXCEPTION",
    "BITRIX_NOTE_INFRASTRUCTURE_REST_V3_EXCEPTIONS_TREETOOLARGEEXCEPTION",
    "BITRIX_PERFORMAN_INFRASTRUCTURE_REST_V3_EXCEPTION_STATECONFLICTEXCEPTION",
    "BITRIX_REST_INFRASTRUCTURE_REST_EXCEPTION_APPLICATIONNOTFOUNDEXCEPTION",
    "BITRIX_REST_INFRASTRUCTURE_REST_EXCEPTION_APPLICATIONNOTINSTALLEDEXCEPTION",
    "BITRIX_REST_INFRASTRUCTURE_REST_EXCEPTION_CANNOTPROLONGINCOMINGWEBHOOKEXCEPTION",
    "BITRIX_REST_INFRASTRUCTURE_REST_EXCEPTION_CANNOTREDUCEINCOMINGWEBHOOKEXPIRATIONEXCEPTION",
    "BITRIX_REST_INFRASTRUCTURE_REST_EXCEPTION_EMBEDDINGHANDLERALREADYBOUNDEXCEPTION",
    "BITRIX_REST_INFRASTRUCTURE_REST_EXCEPTION_INCOMINGWEBHOOKNOTFOUNDEXCEPTION",
    "BITRIX_REST_V3_DEFERREDBATCH_EXCEPTION_DEFERREDBATCHEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_ACCESSDENIEDEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_CLASSREQUIREATTRIBUTEEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_ENTITYALREADYEXISTSEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_ENTITYNOTFOUNDEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_FEATURENOTAVAILABLEONCURRENTPLANEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_HTTPSREQUIREDEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_IDEMPOTENCYKEYREUSEDEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_INSUFFICIENTSCOPEEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_INTERNAL_INTERNALEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_INTERNAL_ORMSAVEEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_INTERNAL_UNKNOWNDTOPROPERTYINTERNALEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_INVALIDCLASSINSTANCEPROVIDEDEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_INVALIDFILTEREXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_INVALIDIDEMPOTENCYKEYEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_INVALIDJSONEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_INVALIDORDEREXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_INVALIDPAGINATIONEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_INVALIDSELECTEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_LICENSEEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_LOGICEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_MARKETSUBSCRIPTIONREQUIREDEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_METHODNOTFOUNDEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_RATELIMITEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_RELATIONMETHODNOTFOUNDEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_RESTUNAVAILABLEEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_TOOMANYATTRIBUTESEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_UNKNOWNAGGREGATEFUNCTIONEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_UNKNOWNDTOPROPERTYEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_UNKNOWNFILTEROPERATOREXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_VALIDATION_DTOVALIDATIONEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_VALIDATION_INVALIDFILEEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_VALIDATION_REQUESTFILTERVALIDATIONEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_VALIDATION_REQUESTVALIDATIONEXCEPTION",
    "BITRIX_REST_V3_EXCEPTION_WRONGHTTPREQUESTMETHODEXCEPTION",
    "BITRIX_REST_V3_REALISATION_EXCEPTION_EVENT_ALREADYBOUNDEXCEPTION",
    "BITRIX_REST_V3_REALISATION_EXCEPTION_EVENT_INVALIDHANDLEREXCEPTION",
    "BITRIX_REST_V3_REALISATION_EXCEPTION_EVENT_NOTFOUNDEXCEPTION",
    "BITRIX_REST_V3_REALISATION_EXCEPTION_EVENT_WRONGAUTHTYPEEXCEPTION",
    "BITRIX_REST_V3_REALISATION_EXCEPTION_FIELDNOTFOUNDEXCEPTION",
    "BITRIX_REST_V3_REALISATION_EXCEPTION_FIELDWRONGTYPEEXCEPTION",
    "BITRIX_TASKS_DISPATCHER_PUBLICACTION_TASK_STAGES",
    QUERY_TIME_LIMIT_ERROR_CODE,
)
# Codes returned by ``getRegistryCode()`` overrides instead of the class name; ``call`` sends lower case.
OVERRIDDEN_CODES = (
    "AIASSISTANT_SCHEDULED_TASK_CONFLICT",
    "AIASSISTANT_SCHEDULED_TASK_EMPTY_UPDATE",
    "AIASSISTANT_SCHEDULED_TASK_INVALID_INPUT",
    "AIASSISTANT_SCHEDULED_TASK_INVALID_STATE",
    "AIASSISTANT_SCHEDULED_TASK_LIMIT_EXCEEDED",
    "BICONNECTOR_REST_BUILDER_EXCEPTION_RATE_LIMIT",
    "BIZPROCDESIGNER_FEATURE_UNAVAILABLE",
    "BIZPROCDESIGNER_PUBLISH_DRAFT_CHANGED",
    "BIZPROC_REST_OBSERVER_STORAGE_FAILURE",
    "CRM_FORM_MODULE_UNAVAILABLE",
    "CRM_FORM_SIDE_EFFECT_FAILED",
    "CRM_FORM_VERSION_MISMATCH",
    "DISK_SYNC_ERROR_CURSOR_EXPIRED",
    "FEATURE_NOT_AVAILABLE_ON_CURRENT_PLAN",
    "LANDING_AI_MODULE_UNAVAILABLE",
    "LANDING_AI_SITES_DISABLED",
    "LANDING_AI_SITES_MODE_DISABLED",
    "LANDING_AI_SITES_UNAVAILABLE",
    "LANDING_AI_SITE_BINDING_UNAVAILABLE",
    "LANDING_AI_SITE_FREE_DRAFT_UNAVAILABLE",
    "LANDING_AI_SITE_GENERATION_NOT_FOUND",
    "LANDING_AI_SITE_MODERATION_BLOCK_LIMIT",
    "NOTE_COLLECTION_IN_RECYCLE_BIN",
    "NOTE_COLLECTION_UNDER_IMPORT",
    "NOTE_DOCUMENT_HAS_UNSAVED_CHANGES",
    "NOTE_EMPTY_UPDATE",
    "NOTE_FILE_TOO_LARGE",
    "NOTE_FILE_TYPE_NOT_ALLOWED",
    "NOTE_INVALID_PARENT",
    "NOTE_MARKDOWN_TOO_LARGE",
    "NOTE_MOVE_ACCESS_ESCALATION",
    "NOTE_RESTORE_TARGET_REQUIRED",
    "NOTE_SEARCH_QUERY_TOO_LONG",
    "NOTE_SEARCH_QUERY_TOO_SHORT",
    "NOTE_TREE_TOO_LARGE",
    "OBJECT_NOT_FOUND",
    "PERFORMAN_STATE_CONFLICT",
    "SIGN_B2E_SIGNING_SERVICE_TEMPORARY_ERROR",
    "TASKS_STAGE_DIFFERENT_STAGES",
    "TASKS_STAGE_NOT_FOUND",
    "access_denied",
    "invalid_date_range",
    "invalid_order",
    "invalid_pagination",
    "invalid_select_field",
)
SERVER_CODES = CLASS_DERIVED_CODES + OVERRIDDEN_CODES
# The bare-credential heuristic hides a run of 16+ characters with a letter and a digit (``V3``, ``B2E``).
HEURISTIC_MINIMUM = 16
AUTH_VALUE = "plmoknijbuhvygct"


def _hidden_by_heuristic(code: str) -> bool:
    return len(code) >= HEURISTIC_MINIMUM and any(character.isdigit() for character in code)


def _v3_body(code: str, message: str = "refused") -> dict[str, object]:
    return {"error": {"code": code, "message": message, "validation": []}}


def _channels(error: ApiResponseError) -> str:
    return "\n".join((str(error), repr(error), json.dumps(error.to_safe_dict(), ensure_ascii=False)))


def _code_channels(error: ApiResponseError) -> str:
    """Every channel that renders the code itself; the body preview is free text and keeps its own rules."""
    safe = error.to_safe_dict()
    return "\n".join(
        (str(error), *(str(safe[key]) for key in ("message", "original_code", "code", "normalized_code", "wire_code")))
    )


def _decode(code: str, *, status: int = 400, codec: ProtocolCodec | None = None, **kwargs: object) -> ApiResponseError:
    error = (codec or ProtocolCodec()).error_from_http(
        status_code=status,
        body=_v3_body(code),
        retry_codes=RetryPolicy().transient_api_codes,
        **kwargs,  # type: ignore[arg-type]
    )
    assert isinstance(error, ApiResponseError)
    return error


def test_the_table_has_the_v3_shape_and_its_digit_bearing_codes_were_hidden_before() -> None:
    assert len(CLASS_DERIVED_CODES) == len(set(CLASS_DERIVED_CODES)) == 95  # noqa: PLR2004 - issue 31 table + 1
    assert all(is_v3_code_shape(code) for code in SERVER_CODES)
    hidden = [code for code in SERVER_CODES if _hidden_by_heuristic(code) and code not in KNOWN_V3_ERROR_CODES]
    assert all(render_code(code) == REDACTED for code in hidden)
    # The reproduction of issue 31.
    assert {
        "BITRIX_REST_V3_EXCEPTION_ACCESSDENIEDEXCEPTION",
        "BITRIX_REST_V3_EXCEPTION_INVALIDSELECTEXCEPTION",
        "BITRIX_CRM_V2_INFRASTRUCTURE_REST_EXCEPTION_FORM_UNKNOWNFORMENTITYEXCEPTION",
        "SIGN_B2E_SIGNING_SERVICE_TEMPORARY_ERROR",
    } <= set(hidden)


@pytest.mark.parametrize("code", SERVER_CODES)
def test_every_server_code_is_shown_verbatim_in_every_channel(code: str) -> None:
    error = _decode(code)

    assert render_code(code, v3_envelope=True) == code
    assert render_code(code.lower(), v3_envelope=True) == code.lower()
    assert str(error) == f"API error [{code}]: refused"
    safe = error.to_safe_dict()
    assert safe["wire_code"] == safe["original_code"] == safe["normalized_code"] == error.wire_code == code
    assert safe["code"] == code.lower()
    assert code in repr(error)
    assert REDACTED not in _code_channels(error)


@pytest.mark.parametrize(
    "code", ["SIGN_B2E_SIGNING_SERVICE_TEMPORARY_ERROR", RATE_LIMIT_ERROR_CODE.replace("RATE", "RAT3")]
)
def test_the_shape_is_trusted_only_in_the_v3_envelope_code(code: str) -> None:
    classic = ProtocolCodec().error_from_http(status_code=400, body={"error": code, "error_description": code})
    message = ProtocolCodec().error_from_http(status_code=400, body=_v3_body("NOTE_TREE_TOO_LARGE", f"see {code}"))

    assert render_code(code) == REDACTED
    assert isinstance(classic, ApiResponseError)
    assert classic.wire_code == REDACTED
    assert code.casefold() not in _channels(classic).casefold()
    assert isinstance(message, ApiResponseError)
    assert message.wire_code == "NOTE_TREE_TOO_LARGE"
    assert code not in _channels(message)


@pytest.mark.parametrize(
    "code",
    [
        "Note_Rest_V3_Empty_Update",  # mixed case
        "UF_CRM_1700000000123",  # a word that starts with a digit
        "NOTE-REST-V3-EMPTY-UPDATE",  # not snake case
        "NOTERESTV3EMPTYUPDATE",  # one word
        "NOTE_REST_V3__EMPTY_UPDATE",  # an empty word
        "NOTE_REST_V3_EMPTY_UPDATE_",  # a trailing empty word
    ],
)
def test_other_shapes_keep_the_free_text_redaction(code: str) -> None:
    error = _decode(code)

    assert not is_v3_code_shape(code)
    assert error.wire_code != code
    assert REDACTED in error.wire_code
    assert code not in _channels(error)
    assert code.lower() not in _channels(error)


def test_only_ascii_counts_for_the_shape() -> None:
    assert not is_v3_code_shape("NOTE_REST_V3_ÉMPTY_UPDATE")
    assert not is_v3_code_shape("ß_note_update")  # upper-cases to ASCII ``SS_NOTE_UPDATE``


def test_registered_and_request_secrets_beat_the_shape() -> None:
    webhook = webhook_secrets(f"https://portal.invalid/rest/1/{AUTH_VALUE}/")
    codec = ProtocolCodec(redactor=Redactor(known_secrets=webhook))
    request = Request("note.list", {"auth": AUTH_VALUE.upper(), "select": ["NOTE_FIELD_NAME"]}, route=RouteKind.API_V3)

    exact = _decode(AUTH_VALUE.upper(), codec=codec)
    containing = _decode(f"NOTE_{AUTH_VALUE.upper()}_REFUSED", codec=codec)
    request_secret = _decode(f"NOTE_{AUTH_VALUE}_REFUSED".lower(), diagnostics=diagnostic_context(request))
    field_name = _decode("NOTE_FIELD_NAME", diagnostics=diagnostic_context(request))

    for error in (exact, containing, request_secret):
        assert error.wire_code == REDACTED
        assert AUTH_VALUE not in _code_channels(error).casefold()
    assert field_name.wire_code == field_name.to_safe_dict()["normalized_code"] == "field#1"
    assert str(field_name) == "API error [field#1]: refused"
    # Control: the same codec and request still show an unrelated server code.
    assert _decode("NOTE_EMPTY_UPDATE", codec=codec).wire_code == "NOTE_EMPTY_UPDATE"
    assert _decode("NOTE_EMPTY_UPDATE", diagnostics=diagnostic_context(request)).wire_code == "NOTE_EMPTY_UPDATE"


def test_a_lower_cased_form_is_recognized_only_when_the_wire_code_was() -> None:
    mixed = _decode("Note_Rest_V3_Empty_Update")

    assert mixed.wire_code == REDACTED
    assert mixed.code == "note_rest_v3_empty_update"
    assert mixed.code not in _channels(mixed)


# ---- retry classification ----


@pytest.mark.parametrize(
    ("code", "status", "retryable", "refusal"),
    [
        (RATE_LIMIT_ERROR_CODE, 429, True, True),
        (QUERY_TIME_LIMIT_ERROR_CODE, 503, True, False),
        # A missing portal module and a moderation cooldown are not over within one retry budget.
        ("LANDING_AI_MODULE_UNAVAILABLE", 503, False, False),
        ("LANDING_AI_SITE_MODERATION_BLOCK_LIMIT", 429, False, False),
        # A 500 thrown after a side effect, and every deterministic refusal, stay as they were.
        ("CRM_FORM_SIDE_EFFECT_FAILED", 500, False, False),
        ("BITRIX_REST_V3_EXCEPTION_ACCESSDENIEDEXCEPTION", 403, False, False),
        ("BITRIX_REST_V3_EXCEPTION_ENTITYNOTFOUNDEXCEPTION", 404, False, False),
        (VALIDATION_ERROR_CODE, 400, False, False),
        (METHOD_NOT_FOUND_ERROR_CODE, 404, False, False),
    ],
)
def test_retry_follows_the_policy_not_recognition(code: str, status: int, *, retryable: bool, refusal: bool) -> None:
    error = _decode(code, status=status)

    assert error.wire_code == code
    assert error.retryable is retryable
    assert (code.casefold() in AmbiguityPolicy().refusal_api_codes) is refusal


def test_the_two_request_faults_stay_non_retryable_even_when_listed() -> None:
    listed = frozenset(code.casefold() for code in (VALIDATION_ERROR_CODE, METHOD_NOT_FOUND_ERROR_CODE, "NOTE_X_Y"))

    for code in (VALIDATION_ERROR_CODE, METHOD_NOT_FOUND_ERROR_CODE):
        error = ProtocolCodec().error_from_http(status_code=400, body=_v3_body(code), retry_codes=listed)
        assert isinstance(error, ApiResponseError)
        assert error.retryable is False
        assert error.wire_code == code
    # Recognition does not veto a caller's own choice for any other code.
    other = ProtocolCodec().error_from_http(status_code=409, body=_v3_body("NOTE_X_Y"), retry_codes=listed)
    assert isinstance(other, ApiResponseError)
    assert other.retryable is True


def _policy() -> ExecutionPolicy:
    return ExecutionPolicy(
        max_requests=3,
        max_attempts_per_request=3,
        max_elapsed=10,
        max_retry_elapsed_per_request=10,
        retry=RetryPolicy(initial_delay=0, maximum_delay=0, jitter=0),
    )


def _v3_wire(code: str, status: int) -> WireResponse:
    headers = (("content-type", "application/json"), ("retry-after", "0"))
    return WireResponse(status_code=status, headers=headers, body=json.dumps(_v3_body(code)).encode())


def _ok() -> WireResponse:
    return WireResponse(status_code=200, headers=(("content-type", "application/json"),), body=b'{"result":{"ok":1}}')


@pytest.mark.asyncio
@pytest.mark.parametrize("safety", [ReplaySafety.SAFE, ReplaySafety.UNKNOWN])
async def test_a_rate_limit_refusal_is_retried_whatever_the_safety(safety: ReplaySafety) -> None:
    transport = ResponderTransport(replies(_v3_wire(RATE_LIMIT_ERROR_CODE, 429), _ok()))
    request = Request("note.document.add", {"title": "x"}, safety, route=RouteKind.BARE)

    response = await Executor(transport).execute(request, policy=_policy())

    assert response.result == {"ok": 1}
    assert len(transport.requests) == 2  # noqa: PLR2004


@pytest.mark.asyncio
@pytest.mark.parametrize(("safety", "sends"), [(ReplaySafety.SAFE, 2), (ReplaySafety.UNKNOWN, 1)])
async def test_a_query_time_limit_is_retried_only_for_safe_work(safety: ReplaySafety, sends: int) -> None:
    transport = ResponderTransport(replies(_v3_wire(QUERY_TIME_LIMIT_ERROR_CODE, 503), _ok()))
    request = Request("note.document.list", {}, safety, route=RouteKind.BARE)

    if sends == 2:  # noqa: PLR2004
        assert (await Executor(transport).execute(request, policy=_policy())).result == {"ok": 1}
    else:
        with pytest.raises(ApiResponseError) as caught:
            await Executor(transport).execute(request, policy=_policy())
        assert caught.value.wire_code == QUERY_TIME_LIMIT_ERROR_CODE
    assert len(transport.requests) == sends


@pytest.mark.asyncio
async def test_a_recognized_deterministic_refusal_is_not_retried() -> None:
    transport = ResponderTransport(replies(_v3_wire("BITRIX_REST_V3_EXCEPTION_ACCESSDENIEDEXCEPTION", 403), _ok()))
    request = Request("note.document.list", {}, ReplaySafety.SAFE, route=RouteKind.BARE)

    with pytest.raises(ApiResponseError) as caught:
        await Executor(transport).execute(request, policy=_policy())

    assert str(caught.value) == "API error [BITRIX_REST_V3_EXCEPTION_ACCESSDENIEDEXCEPTION]: refused"
    assert len(transport.requests) == 1
