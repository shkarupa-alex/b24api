"""Table R: counted contradiction rules, observed on the batched and the sequential counted paths.

Each row serves the same offset-keyed pages to ``iter_list_counted`` (direct head plus batched tail)
and to a counted reference binding (sequential ``_counted`` behind the batch queue). The expected
outcome per path pins today's behavior: only R2 (``continuation_after_total``) is shared, so the
sequential path newly rejects a page that reaches its total while a positive ``next`` remains, and
keeps accepting variable-width pages that only the parallel plan's prerequisites reject.
"""

from __future__ import annotations
import json
from dataclasses import dataclass
from urllib.parse import parse_qs, urlsplit

import pytest

from b24api import (
    BatchDispatch,
    Binding,
    Bitrix24,
    CountedTraversal,
    OffsetContinuation,
    OffsetSpec,
    ReplaySafety,
    Request,
    RouteKind,
    Settings,
    TotalTermination,
)
from b24api.contracts import PageStride, ReferenceFailure, ReferenceItem
from b24api.errors import IncompleteTraversalError
from b24api.transport import WireResponse
from b24api.traversal.counted_rules import (
    CountedContradiction,
    CountedPageFacts,
    CountedVerdict,
    expected_counted_next,
    judge_counted_page,
)

HOST = "fixture.invalid"
PAGE = 2
type Page = tuple[int, int | None, int | None]  # rows, total, next


_COUNTED = OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED)


@dataclass(frozen=True, slots=True)
class Row:
    """One table-R case and its pinned outcome on each path (``completed:N`` or ``Cause: message``)."""

    name: str
    pages: dict[int, Page]
    batched: str
    sequential: str
    offset: OffsetSpec = _COUNTED


_FIXED_STEP = OffsetSpec(
    continuation=OffsetContinuation.FIXED_STEP,
    step=PAGE,
    total_termination=TotalTermination.EXACT_QUALIFIED,
)
_FIXED_STRIDE = OffsetSpec(
    continuation=OffsetContinuation.FIXED_STEP,
    step=PAGE,
    total_termination=TotalTermination.EXACT_QUALIFIED,
    page_stride=PageStride(server_granularity=PAGE, wire_increment=PAGE, max_decoded_rows=PAGE),
)
TABLE_R = (
    # The sequential path requires a total on every page; only the batched head witnesses an empty source.
    Row("empty_head", {0: (0, None, None)}, "completed:0", "_PageRejectionError: ended before its exact total"),
    Row("zero_total_no_next", {0: (0, 0, None)}, "completed:0", "completed:0"),
    # Shared driver rule: an empty page may not keep a continuation unless the plan is FIXED_STEP.
    Row(
        "zero_total_next_zero",
        {0: (0, 0, 0)},
        "PaginationError: empty response retained a continuation",
        "PaginationError: empty response retained a continuation",
    ),
    Row("short_complete", {0: (1, 1, None)}, "completed:1", "completed:1"),
    Row(
        "short_incomplete",
        {0: (1, 3, None), 1: (0, 3, None), 2: (0, 3, None)},
        "CapabilityError: page length contradicts the planned exact range",
        "_PageRejectionError: ended before its exact total",
    ),
    Row("two_full_pages", {0: (2, 4, 2), 2: (2, 4, None)}, "completed:4", "completed:4"),
    Row("missing_next", {0: (2, 4, None), 2: (2, 4, None)}, "completed:4", "completed:4"),
    # H7 stays a parallel-plan check: without a qualified stride or identity the sequential path cannot
    # prove the skipped offset is a gap, so it follows the server continuation as it does today.
    Row(
        "next_out_of_range",
        {0: (2, 4, 3), 3: (2, 4, None)},
        "CapabilityError: head length contradicts the planned exact range",
        "completed:4",
    ),
    Row(
        "total_changed",
        {0: (2, 4, 2), 2: (2, 5, None)},
        "CapabilityError: page total contradicts the head total",
        "_PageRejectionError: exact total drifted",
    ),
    # R2, the only rule the sequential path takes over from the batched head (A8).
    Row(
        "continuation_after_total",
        {0: (2, 2, 2)},
        "CapabilityError: completed while continuation remained",
        "CapabilityError: completed while continuation remained",
    ),
    # H5/T1 is a fixed-width prerequisite of the parallel plan, never applied to sequential pages.
    Row(
        "variable_width",
        {0: (2, 5, 2), 2: (1, 5, 3), 3: (2, 5, None), 4: (1, 5, None)},
        "CapabilityError: page length contradicts the planned exact range",
        "completed:5",
    ),
    Row("fixed_step_ignores_next", {0: (2, 3, 2), 2: (1, 3, 1)}, "completed:3", "completed:3", _FIXED_STEP),
    Row("fixed_stride", {0: (2, 3, 2), 2: (1, 3, None)}, "completed:3", "completed:3", _FIXED_STRIDE),
)


class _Portal:
    """Serve offset-keyed pages to direct list requests and to physical batch commands."""

    host = HOST

    def __init__(self, pages: dict[int, Page]) -> None:
        self.pages = pages

    def _envelope(self, start: int) -> dict[str, object]:
        rows, total, next_offset = self.pages[start]
        envelope: dict[str, object] = {"result": [{"ID": start + index + 1} for index in range(rows)]}
        if total is not None:
            envelope["total"] = total
        if next_offset is not None:
            envelope["next"] = next_offset
        return envelope

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        del attempt_timeout, max_response_bytes
        parameters = request.copy_parameters()
        if request.method == "batch":
            commands = parameters["cmd"]
            assert isinstance(commands, dict)
            envelopes = {}
            for key, command in commands.items():
                assert isinstance(command, str)
                envelopes[key] = self._envelope(int(parse_qs(urlsplit(command).query).get("start", ["0"])[0]))
            payload: dict[str, object] = {
                "result": {
                    "result": {key: value["result"] for key, value in envelopes.items()},
                    "result_error": {},
                    "result_total": {key: value["total"] for key, value in envelopes.items() if "total" in value},
                    "result_next": {key: value["next"] for key, value in envelopes.items() if "next" in value},
                }
            }
        else:
            start = parameters.get("start", 0)
            assert isinstance(start, int)
            payload = self._envelope(start)
        return WireResponse(200, (("content-type", "application/json"),), json.dumps(payload).encode())

    async def aclose(self) -> None:
        return None


def _client(portal: _Portal) -> Bitrix24:
    return Bitrix24(Settings(webhook_url=f"https://{HOST}/rest/1/table/"), transport=portal)


def _request() -> Request:
    return Request("example.list", replay_safety=ReplaySafety.SAFE, route=RouteKind.BARE)


def _outcome(error: BaseException) -> str:
    cause = error.__cause__ if isinstance(error, IncompleteTraversalError) and error.__cause__ else error
    return f"{type(cause).__name__}: {cause}"


def _matches(observed: str, expected: str) -> bool:
    if expected.startswith("completed:"):
        return observed == expected
    expected_class, _, expected_text = expected.partition(": ")
    return observed.startswith(f"{expected_class}: ") and expected_text in observed


async def _batched(row: Row) -> str:
    async with _client(_Portal(row.pages)) as client:
        stream = client.iter_list_counted(_request(), page_size=PAGE, offset=row.offset)
        rows = []
        try:
            async with stream:
                rows = [item async for item in stream]
        except Exception as error:  # noqa: BLE001 - the table pins the terminal cause
            return _outcome(error)
        return f"completed:{len(rows)}"


async def _sequential(row: Row) -> str:
    async with _client(_Portal(row.pages)) as client:
        stream = client.iter_reference_outcomes(
            _request(),
            [Binding("one", (), None)],
            traversal=CountedTraversal(page_size=PAGE, offset=row.offset),
            dispatch=BatchDispatch(batch_size=1),
        )
        async with stream:
            outcomes = [outcome async for outcome in stream]
    failures = [outcome for outcome in outcomes if isinstance(outcome, ReferenceFailure)]
    if failures:
        return _outcome(failures[0].error)
    return f"completed:{sum(isinstance(outcome, ReferenceItem) for outcome in outcomes)}"


@pytest.mark.asyncio
@pytest.mark.parametrize("row", TABLE_R, ids=[row.name for row in TABLE_R])
async def test_table_r_batched_counted_path(row: Row) -> None:
    observed = await _batched(row)
    assert _matches(observed, row.batched), observed


@pytest.mark.asyncio
@pytest.mark.parametrize("row", TABLE_R, ids=[row.name for row in TABLE_R])
async def test_table_r_sequential_counted_path(row: Row) -> None:
    observed = await _sequential(row)
    assert _matches(observed, row.sequential), observed


@pytest.mark.parametrize(
    ("facts", "verdict"),
    [
        (CountedPageFacts(0, 2, 0, 2, 2, OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT), (True, True)),
        (CountedPageFacts(0, 2, 0, 2, None, OffsetContinuation.SERVER_NEXT_OR_OBSERVED_COUNT), (True, False)),
        (CountedPageFacts(0, 0, 0, 0, 0, OffsetContinuation.SERVER_NEXT), (True, False)),
        (CountedPageFacts(2, 1, 2, 3, 1, OffsetContinuation.FIXED_STEP), (True, False)),
        (CountedPageFacts(0, 2, 0, 4, 2, OffsetContinuation.SERVER_NEXT), (False, False)),
        (CountedPageFacts(0, 0, 0, None, None, OffsetContinuation.SERVER_NEXT), (False, False)),
    ],
)
def test_judge_counted_page_applies_only_the_width_independent_rule(
    facts: CountedPageFacts,
    verdict: tuple[bool, bool],
) -> None:
    terminal, contradictory = verdict
    assert judge_counted_page(facts) == CountedVerdict(
        terminal,
        CountedContradiction.CONTINUATION_AFTER_TOTAL if contradictory else None,
    )


def test_expected_counted_next_is_the_qualified_fixed_stride_expectation() -> None:
    assert expected_counted_next(0, PAGE, 5) == PAGE
    assert expected_counted_next(2, 2, 4) is None
    assert expected_counted_next(4, 2, 5) is None
