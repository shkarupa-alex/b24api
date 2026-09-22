"""Exact admitted keyset upper boundary through the public list API."""

# ruff: noqa: ANN202, D102, D107, PLR2004

from __future__ import annotations
import json

import pytest

from b24api import (
    AutoKeysetExecution,
    Bitrix24,
    BoundedIdentityRange,
    CapabilityError,
    IdentityCoercion,
    IdentitySpec,
    IncompleteTraversalError,
    KeysetSpec,
    ParameterPath,
    Request,
    ResultSelector,
    RouteKind,
    SequentialKeysetExecution,
    StableIntegerKeysetContract,
    TraversalAssurance,
)
from b24api.errors import PaginationError
from b24api.execution import Executor, WireResponse


class BoundaryTransport:
    """Independent finite rows with optional server fence defect."""

    def __init__(self, ids: tuple[int, ...], *, ignore_fence: bool = False) -> None:
        self.ids = ids
        self.ignore_fence = ignore_fence
        self.requests: list[dict[str, object]] = []

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        assert attempt_timeout > 0
        assert max_response_bytes > 0
        parameters = request.copy_parameters()
        self.requests.append(parameters)
        controls = parameters["filter"]
        assert isinstance(controls, dict)
        selected = (
            value
            for value in self.ids
            if value > controls.get(">ID", 0) and (self.ignore_fence or value <= controls["<=ID"])
        )
        rows = [{"id": value} for value in list(selected)[: parameters["limit"]]]
        return WireResponse(200, (), json.dumps({"result": rows}).encode())


def _setup(ids: tuple[int, ...], *, upper: int = 3, ignore_fence: bool = False):
    transport = BoundaryTransport(ids, ignore_fence=ignore_fence)
    client = Bitrix24._from_executor(Executor(transport))  # noqa: SLF001 - deterministic facade seam
    request = Request("item.list", {"filter": {"STATUS": "open"}}, route=RouteKind.BARE)
    boundary = BoundedIdentityRange.capture(
        request,
        filter_path=ParameterPath(("filter",)),
        upper_id=upper,
        lower_exclusive=0,
        fence_path=ParameterPath(("filter", "<=ID")),
        source_version="qualified-test-v1",
    )
    spec = KeysetSpec(limit_path=ParameterPath(("limit",)), boundary=boundary)
    identity = IdentitySpec(("id",), "ID", "id", IdentityCoercion.EXACT_INTEGER)
    return transport, client, request, spec, identity


@pytest.mark.asyncio
async def test_exact_admitted_upper_id_closes_without_empty_confirmation() -> None:
    transport, client, request, spec, identity = _setup((1, 2, 3, 4))
    stream = client.iter_list_keyset(
        request,
        selector=ResultSelector.root(),
        identity=identity,
        page_size=2,
        keyset=spec,
        execution=SequentialKeysetExecution(),
    )
    assert [row["id"] async for row in stream] == [1, 2, 3]
    assert len(transport.requests) == 2
    assert [call["filter"]["<=ID"] for call in transport.requests] == [3, 3]
    assert transport.requests[1]["filter"][">ID"] == 2
    assert stream.report is not None
    assert stream.report.assurance is TraversalAssurance.BOUNDED_RANGE_OBSERVED
    assert not stream.report.exhausted
    assert stream.report.partial


@pytest.mark.asyncio
async def test_missing_boundary_cannot_claim_completion() -> None:
    transport, client, request, spec, identity = _setup((1, 2, 4))
    stream = client.iter_list_keyset(
        request,
        selector=ResultSelector.root(),
        identity=identity,
        page_size=2,
        keyset=spec,
        execution=SequentialKeysetExecution(),
    )
    with pytest.raises(IncompleteTraversalError) as captured:
        _ = [row async for row in stream]
    assert isinstance(captured.value.__cause__, PaginationError)
    assert len(transport.requests) == 2
    assert stream.report is not None
    assert not stream.report.exhausted


@pytest.mark.asyncio
async def test_ignored_server_fence_is_rejected() -> None:
    transport, client, request, spec, identity = _setup((1, 2, 3, 4), upper=2, ignore_fence=True)
    stream = client.iter_list_keyset(
        request,
        selector=ResultSelector.root(),
        identity=identity,
        page_size=3,
        keyset=spec,
        execution=SequentialKeysetExecution(),
    )
    with pytest.raises(IncompleteTraversalError) as captured:
        _ = [row async for row in stream]
    assert isinstance(captured.value.__cause__, PaginationError)
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_mismatched_filter_and_auto_execution_reject_before_io() -> None:
    transport, client, request, spec, identity = _setup((1, 2, 3))
    changed = Request("item.list", {"filter": {"STATUS": "closed"}}, route=RouteKind.BARE)
    stream = client.iter_list_keyset(
        changed,
        selector=ResultSelector.root(),
        identity=identity,
        keyset=spec,
        execution=SequentialKeysetExecution(),
    )
    with pytest.raises(CapabilityError, match="fingerprint"):
        _ = [row async for row in stream]
    with pytest.raises(CapabilityError, match="not qualified"):
        client.iter_list_keyset(
            request,
            selector=ResultSelector.root(),
            identity=identity,
            keyset=spec,
            execution=AutoKeysetExecution(StableIntegerKeysetContract()),
        )
    assert transport.requests == []
