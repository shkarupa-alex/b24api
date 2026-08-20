"""Tests for the one canonical bounded redaction path."""

from dataclasses import FrozenInstanceError

import pytest

from b24api.models import BatchCommandEvidence, RequestSummary, ResponseEvidence, summarize_request
from b24api.redaction import REDACTED, TRUNCATED, Redactor

EXAMPLE_CREDENTIAL = "n1x2y3z4q5w6e7r8"
WEBHOOK = "https://portal.invalid/" + "rest/1/" + EXAMPLE_CREDENTIAL + "/"
MAX_TEST_STRING = 20


def test_redactor_covers_nested_and_textual_credential_forms() -> None:
    redactor = Redactor()
    value = {
        "nested": {
            "Authorization": f"Bearer {EXAMPLE_CREDENTIAL}",
            "token": EXAMPLE_CREDENTIAL,
            "body": '{"auth": "' + EXAMPLE_CREDENTIAL + '"}',
            "dump": f"AUTH_ID={EXAMPLE_CREDENTIAL} APPLICATION_TOKEN={EXAMPLE_CREDENTIAL}",
            "url": WEBHOOK + "profile?auth=" + EXAMPLE_CREDENTIAL,
        },
    }

    redacted = redactor.redact(value)
    rendered = repr(redacted)

    assert EXAMPLE_CREDENTIAL not in rendered
    assert "/rest/1/" not in rendered
    assert redacted["nested"]["Authorization"] == REDACTED
    assert redacted["nested"]["token"] == REDACTED


def test_redactor_supports_configured_paths_and_pii_fields() -> None:
    redactor = Redactor(
        secret_paths=(("payload", "custom"),),
        pii_fields=frozenset({"email", "phone"}),
    )

    assert redactor.redact(
        {"payload": {"custom": "private", "public": 1}, "EMAIL": "person@example.test", "phone": "+123"},
    ) == {
        "payload": {"custom": REDACTED, "public": 1},
        "EMAIL": REDACTED,
        "phone": REDACTED,
    }


def test_redactor_bounds_depth_items_strings_and_cycles() -> None:
    recursive: list[object] = []
    recursive.append(recursive)
    redactor = Redactor(max_depth=2, max_items=2, max_string=MAX_TEST_STRING)

    assert redactor.redact(recursive) == [TRUNCATED]
    assert redactor.redact([1, 2, 3]) == [1, 2, TRUNCATED]
    assert redactor.redact({"a": {"b": {"c": 1}}}) == {"a": {"b": TRUNCATED}}
    assert len(redactor.redact_text("x" * 100)) == MAX_TEST_STRING


def test_safe_preview_redacts_before_truncating() -> None:
    preview = Redactor().safe_preview('{"auth":"' + EXAMPLE_CREDENTIAL + '","value":"ok"}', max_chars=200)

    assert preview is not None
    assert EXAMPLE_CREDENTIAL not in preview
    assert REDACTED in preview


def test_safe_evidence_values_are_frozen_and_serializable() -> None:
    summary = RequestSummary(method="profile", parameter_keys=("select",))
    response = ResponseEvidence(http_status=200, headers=(("x-request-id", "safe"),))
    command = BatchCommandEvidence(command_index=0, stable_key="_0", original_code=0, normalized_code="0")

    assert summary.to_dict() == {"method": "profile", "parameter_keys": ["select"]}
    assert response.to_dict()["headers"] == {"x-request-id": "safe"}
    assert command.to_dict()["original_code"] == 0
    with pytest.raises(FrozenInstanceError):
        summary.method = "changed"  # type: ignore[misc]


def test_request_summary_excludes_parameter_values() -> None:
    summary = summarize_request("profile", {"auth": EXAMPLE_CREDENTIAL, "select": ["ID"]})

    assert summary.parameter_keys == ("auth", "select")
    assert EXAMPLE_CREDENTIAL not in repr(summary)


def test_lineage_hashes_are_not_mistaken_for_bare_credentials() -> None:
    sha = "a" * 40
    sha256 = "b" * 64

    assert Redactor().redact_text(f"{sha} {sha256}") == f"{sha} {sha256}"
