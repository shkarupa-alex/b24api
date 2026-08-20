"""Tests for frozen plan schemas and invalid pre-I/O combinations."""

from dataclasses import FrozenInstanceError

import pytest

from b24api.models import IdentityCoercion, IdentityRequirement, OrderSemantics, ParameterPath, TotalSemantics
from b24api.plans import (
    BatchDispatch,
    CountedOffsetMode,
    CountedOffsetPlan,
    DirectDispatch,
    ItemCursorPlan,
    KeysetPlan,
    KeysetTerminalRule,
    OffsetSequentialPlan,
    OffsetTerminalRule,
    PartitionedKeysetPlan,
    SingleResponsePlan,
)

PAGE_SIZE = 50
PARTITION_LANES = 4


def test_single_and_offset_plans_are_frozen() -> None:
    single = SingleResponsePlan()
    offset = OffsetSequentialPlan(limit_path=ParameterPath(("limit",)), requested_page_size=PAGE_SIZE)

    assert single.reject_continuation is True
    assert offset.requested_page_size == PAGE_SIZE
    with pytest.raises(FrozenInstanceError):
        offset.requested_page_size = PAGE_SIZE * 2  # type: ignore[misc]


def test_page_size_requires_a_limit_path() -> None:
    with pytest.raises(ValueError, match="limit_path"):
        OffsetSequentialPlan(requested_page_size=PAGE_SIZE)


def test_counted_offset_mode_and_stride_are_coherent() -> None:
    parallel = CountedOffsetPlan(
        mode=CountedOffsetMode.PARALLEL_FIXED_STRIDE,
        limit_path=ParameterPath(("limit",)),
        requested_page_size=PAGE_SIZE,
        fixed_stride=PAGE_SIZE,
    )

    assert parallel.fixed_stride == PAGE_SIZE
    with pytest.raises(ValueError, match="fixed_stride"):
        CountedOffsetPlan(mode=CountedOffsetMode.PARALLEL_FIXED_STRIDE)
    with pytest.raises(ValueError, match="equal"):
        CountedOffsetPlan(
            mode=CountedOffsetMode.PARALLEL_FIXED_STRIDE,
            limit_path=ParameterPath(("limit",)),
            requested_page_size=PAGE_SIZE,
            fixed_stride=PAGE_SIZE // 2,
        )
    with pytest.raises(ValueError, match="sequential"):
        CountedOffsetPlan(fixed_stride=PAGE_SIZE)
    with pytest.raises(TypeError, match="mode"):
        CountedOffsetPlan(mode="parallel_fixed_stride")  # type: ignore[arg-type]
    assert CountedOffsetPlan().total_semantics is TotalSemantics.FILTERED_EXACT
    with pytest.raises(ValueError, match="filtered exact"):
        CountedOffsetPlan(total_semantics=TotalSemantics.IGNORE)


def test_evidence_based_terminal_rules_require_their_declared_contract() -> None:
    with pytest.raises(ValueError, match="requested_page_size"):
        OffsetSequentialPlan(terminal=frozenset({OffsetTerminalRule.PROFILE_SHORT_PAGE}))
    with pytest.raises(ValueError, match="filtered exact"):
        OffsetSequentialPlan(terminal=frozenset({OffsetTerminalRule.QUALIFIED_TOTAL}))
    with pytest.raises(ValueError, match="requested_page_size"):
        KeysetPlan(
            terminal=KeysetTerminalRule.PROFILE_SHORT_PAGE,
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
        )


def test_keyset_requires_identity_order_and_distinct_paths() -> None:
    plan = KeysetPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
        limit_path=ParameterPath(("limit",)),
        requested_page_size=PAGE_SIZE,
    )

    assert plan.direction == "asc"
    with pytest.raises(ValueError, match="requires identity"):
        KeysetPlan(order_semantics=OrderSemantics.ASCENDING)
    with pytest.raises(ValueError, match="contradicts"):
        KeysetPlan(
            direction="desc",
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
        )
    with pytest.raises(ValueError, match="direction"):
        KeysetPlan(
            direction="sideways",  # type: ignore[arg-type]
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
        )
    with pytest.raises(ValueError, match="distinct"):
        KeysetPlan(
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
            filter_path=ParameterPath(("FILTER",)),
            order_path=ParameterPath(("filter",)),
        )


def test_item_cursor_and_partition_validate_identity_direction_and_bounds() -> None:
    cursor = ItemCursorPlan(
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
    )
    partition = PartitionedKeysetPlan(
        lane_count=PARTITION_LANES,
        identity_requirement=IdentityRequirement.REQUIRED,
        order_semantics=OrderSemantics.ASCENDING,
    )

    assert cursor.cursor_item_path == ("id",)
    assert cursor.cursor_coercion is IdentityCoercion.EXACT_INTEGER
    assert partition.lane_count == PARTITION_LANES
    with pytest.raises(ValueError, match="cursor_item_path"):
        ItemCursorPlan(
            cursor_item_path=(),
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
        )
    with pytest.raises(TypeError, match="cursor_coercion"):
        ItemCursorPlan(
            cursor_coercion="integer",  # type: ignore[arg-type]
            identity_requirement=IdentityRequirement.REQUIRED,
        )
    with pytest.raises(ValueError, match="lane_count"):
        PartitionedKeysetPlan(
            lane_count=1,
            identity_requirement=IdentityRequirement.REQUIRED,
            order_semantics=OrderSemantics.ASCENDING,
        )


def test_dispatch_limits_validate_before_execution() -> None:
    assert BatchDispatch(batch_size=PAGE_SIZE).batch_size == PAGE_SIZE
    assert DirectDispatch(concurrency=1).concurrency == 1
    with pytest.raises(ValueError, match="batch_size"):
        BatchDispatch(batch_size=51)
    with pytest.raises(ValueError, match="concurrency"):
        DirectDispatch(concurrency=0)
