"""Pure caller-rule continuation: the rule picks the next control vector, the SDK owns everything else."""

from __future__ import annotations
import hashlib
import json
from typing import TYPE_CHECKING

from b24api.completion.closure import CONTINUATION_RULE_DECLARED_END
from b24api.contracts.continuation import (
    ContinuationReason,
    ContinueWith,
    EndOfSource,
    _ContinuationContractError,
    _response_more,
)
from b24api.contracts.json import _freeze_json, thaw_json
from b24api.contracts.page import PageView
from b24api.contracts.reference import ParameterUpdate
from b24api.contracts.response import result_snapshot
from b24api.errors import CapabilityError, ContinuationError
from b24api.traversal.identity import _request_with_controls
from b24api.traversal.keyset_eligibility import _path_lookup
from b24api.traversal.strategy_context import PageStop, PageVerdict

if TYPE_CHECKING:
    from b24api.contracts.continuation import ResponseFlagTerminal
    from b24api.contracts.json import FrozenJson, JsonValue
    from b24api.contracts.request import ParameterPath, Request
    from b24api.contracts.response import Response
    from b24api.traversal.plans import ContinuationPlan
    from b24api.traversal.strategy_context import StrategyContext
    from b24api.traversal.values import IdentityValue

# One canonical control vector (every declared control, sorted by path) may not exceed this many bytes.
CONTROL_VECTOR_MAXIMUM_BYTES = 65536


def response_more(response: Response, terminal: ResponseFlagTerminal) -> bool:
    """Return a page's declared more-flag, failing closed when it is missing or not a boolean."""
    try:
        return _response_more(result_snapshot(response), terminal)
    except _ContinuationContractError as error:
        raise ContinuationError(error.reason, str(error)) from None


def _normalized(path: ParameterPath) -> tuple[str | int, ...]:
    return tuple(part.casefold() if isinstance(part, str) else part for part in path.path)


def _canonical(vector: list[tuple[tuple[str | int, ...], bool, JsonValue]]) -> bytes:
    """Encode a control vector independently of mapping key order and update order."""
    ordered = sorted(vector, key=lambda entry: json.dumps(entry[0]))
    return json.dumps(
        [[list(path), present, value] for path, present, value in ordered],
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _fingerprint(encoded: bytes) -> str:
    return hashlib.sha256(encoded).hexdigest()


def _bounded_fingerprint(vector: list[tuple[tuple[str | int, ...], bool, JsonValue]]) -> str:
    """Fingerprint one canonical control vector, the head's included, no larger than the bound."""
    encoded = _canonical(vector)
    if len(encoded) > CONTROL_VECTOR_MAXIMUM_BYTES:
        raise ContinuationError(ContinuationReason.INVALID_UPDATES, "continuation control vector is too large")
    return _fingerprint(encoded)


class ContinuationStrategy:
    """Apply a pure rule to each validated page; repeat no control vector, carry no token into diagnostics."""

    def __init__(self, plan: ContinuationPlan) -> None:
        """Hold the plan; the head's vector is read from the request on the first request."""
        self._plan = plan
        self._seen: set[str] = set()
        self._next: tuple[str, dict[ParameterPath, object]] | None = None
        self._terminal = False

    def first_request(self, ctx: StrategyContext) -> Request:
        """Record the head's control vector as written by the caller and send it unchanged."""
        plan = self._plan
        if ctx.request.positional is not None:
            raise CapabilityError("continuation traversal requires named request parameters")
        parameters = ctx.request.copy_parameters()
        vector: list[tuple[tuple[str | int, ...], bool, JsonValue]] = []
        for path in plan.controls:
            present, value = _path_lookup(parameters, path)
            vector.append((_normalized(path), present, thaw_json(_freeze_json(value)) if present else None))
        self._seen.add(_bounded_fingerprint(vector))
        return self._request(ctx, {})

    def _request(self, ctx: StrategyContext, controls: dict[ParameterPath, object]) -> Request:
        plan = self._plan
        updates = dict(controls)
        if plan.limit_path is not None and plan.requested_page_size is not None:
            updates[plan.limit_path] = plan.requested_page_size
        if not updates:
            return ctx.request
        return _request_with_controls(
            ctx.request,
            updates,
            allow_create=plan.allow_create_controls,
            replace=frozenset(plan.controls),
        )

    def judge(self, ctx: StrategyContext, response: Response, items: tuple[FrozenJson, ...]) -> PageVerdict:
        """Evaluate the rule on the immutable page and validate its decision before the page commits."""
        page = PageView(result_snapshot(response), ctx.source_page.current(items))
        try:
            decision = self._plan.rule.evaluate(page)
        except _ContinuationContractError as error:
            raise ContinuationError(error.reason, str(error)) from None
        except Exception as error:
            raise ContinuationError(ContinuationReason.RULE_FAILED, "continuation rule raised") from error
        if isinstance(decision, EndOfSource):
            self._terminal, self._next = True, None
            return PageVerdict(terminal=True, deliver_empty=True)
        if not isinstance(decision, ContinueWith):
            raise ContinuationError(ContinuationReason.RULE_FAILED, "continuation rule returned an invalid decision")
        self._next = self._validated(decision)
        self._terminal = False
        return PageVerdict(terminal=False, deliver_empty=True)

    def _validated(self, decision: ContinueWith) -> tuple[str, dict[ParameterPath, object]]:
        """Require exactly the declared controls once each, a bounded vector and no repeated vector."""
        declared = {_normalized(path): path for path in self._plan.controls}
        controls: dict[ParameterPath, object] = {}
        vector: list[tuple[tuple[str | int, ...], bool, JsonValue]] = []
        for update in decision.updates:
            if not isinstance(update, ParameterUpdate):
                raise ContinuationError(
                    ContinuationReason.INVALID_UPDATES,
                    "continuation update is not a ParameterUpdate",
                )
            key = _normalized(update.path)
            path = declared.get(key)
            if path is None or path in controls:
                raise ContinuationError(
                    ContinuationReason.INVALID_UPDATES,
                    "continuation updates must set each declared control exactly once",
                )
            value = update.value
            controls[path] = value
            vector.append((key, True, value))
        if len(controls) != len(declared):
            raise ContinuationError(
                ContinuationReason.INVALID_UPDATES,
                "continuation updates must set each declared control exactly once",
            )
        fingerprint = _bounded_fingerprint(vector)
        if fingerprint in self._seen:
            raise ContinuationError(ContinuationReason.CONTINUATION_CYCLE, "continuation control vector repeated")
        return fingerprint, controls

    def advance(self, ctx: StrategyContext, identities: list[IdentityValue]) -> Request | PageStop:
        """Stop where the rule declared the end, else request the validated next vector."""
        del identities
        if self._terminal:
            return PageStop(CONTINUATION_RULE_DECLARED_END)
        if self._next is None:
            raise RuntimeError("continuation advanced without a validated decision")
        fingerprint, controls = self._next
        self._next = None
        self._seen.add(fingerprint)
        return self._request(ctx, controls)


__all__ = ["CONTROL_VECTOR_MAXIMUM_BYTES", "ContinuationStrategy", "response_more"]
