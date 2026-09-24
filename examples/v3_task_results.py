"""Scenario 19: REST 3.0 task result read and object-valued validation error.

Offline fixture: one readable task with expected result ID, one invalid task
with dotted `filter.taskId` validation, rendered as the request-local alias `filter.field#1`.
`RouteKind.API_V3` selects `/rest/api/`;
the denied parent keeps its checkpoint unchanged. Live semantic verification
requires a disposable task fixture. Run: `uv run python -m examples.v3_task_results`.
"""

from __future__ import annotations
import asyncio
import json

from b24api import ApiResponseError, Bitrix24, ReplaySafety, Request, RouteKind, Settings
from b24api.testing import ScriptedExchange, ScriptedTransport
from b24api.transport import WireResponse
from examples._support.evidence import RecipeEvidence

METHOD = "tasks.task.result.list"
ERROR_CODE = "BITRIX_REST_V3_EXCEPTION_VALIDATION_REQUESTVALIDATIONEXCEPTION"
EXPECTED_RESULT_IDS = (71,)


def _request(task_id: int) -> Request:
    return Request(
        METHOD,
        {"filter": {"taskId": task_id}},
        replay_safety=ReplaySafety.SAFE,
        route=RouteKind.API_V3,
    )


def _fixture() -> ScriptedTransport:
    error = {
        "error": {
            "code": ERROR_CODE,
            "message": "Invalid task",
            "validation": [{"field": "filter.taskId", "message": "Task is required"}],
        }
    }
    return ScriptedTransport(
        (
            ScriptedExchange.json(_request(11), {"result": {"items": [{"id": 71}]}}),
            ScriptedExchange(
                _request(-1),
                WireResponse(400, (("content-type", "application/json"),), json.dumps(error).encode()),
            ),
        )
    )


async def run() -> RecipeEvidence:
    """Verify independent IDs, typed validation, and unchanged failed checkpoint."""
    transport = _fixture()
    checkpoint = {11: 0, -1: 0}
    settings = Settings(webhook_url="https://fixture.invalid/rest/1/test/")
    async with Bitrix24(settings, transport=transport) as client:
        response = await client.call(_request(11))
        if not isinstance(response, dict) or not isinstance(response.get("items"), list):
            raise TypeError("scenario 19 valid V3 result shape differed from fixture")
        ids = tuple(int(row["id"]) for row in response["items"])
        if ids != EXPECTED_RESULT_IDS:
            raise AssertionError("scenario 19 valid V3 result IDs differed from oracle")
        checkpoint[11] = ids[-1]
        try:
            await client.call(_request(-1))
        except ApiResponseError as error:
            if error.normalized_code != ERROR_CODE or not error.validation:
                raise AssertionError("scenario 19 lost the typed object-valued error") from error
            # Caller field names render as request-local aliases: `taskId` is the first filter field.
            if error.validation[0].field != "filter.field#1":
                raise AssertionError("scenario 19 lost the dotted validation field") from error
        else:
            raise AssertionError("scenario 19 invalid task was treated as success")
    transport.assert_exhausted()
    if checkpoint != {11: EXPECTED_RESULT_IDS[-1], -1: 0}:
        raise AssertionError("scenario 19 advanced a failed parent checkpoint")
    return RecipeEvidence(len(ids))


if __name__ == "__main__":
    asyncio.run(run())
