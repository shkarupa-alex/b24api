"""One explicit, method-agnostic b24api public surface.

The root exports the names a typical application needs. Every other public name lives in one of the
public aggregators listed in ``b24api.migration.PUBLIC_NAMESPACES``. A name that left the root in 3.0
does not resolve here; ``b24api.Name`` raises an ``AttributeError`` naming its new import path.
"""

import importlib
from typing import TYPE_CHECKING

from b24api.client import (
    Bitrix24,
)
from b24api.contracts import (
    AmbiguityPolicy,
    AutoKeysetExecution,
    BatchDispatch,
    Binding,
    BoundedIdentityRange,
    CountedTraversal,
    CursorSpec,
    CursorTraversal,
    DirectDispatch,
    ExecutionPolicy,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    KeysetTraversal,
    OffsetContinuation,
    OffsetSpec,
    OperationReport,
    PageAdapter,
    PageStopPolicy,
    ParameterPath,
    ParameterUpdate,
    PartitionedKeysetExecution,
    RangeKeysetExecution,
    ReplaySafety,
    Request,
    ResultSelector,
    RetryPolicy,
    RouteKind,
    SequentialKeysetExecution,
    SequentialTraversal,
    StableIntegerKeysetContract,
    TerminalState,
    TotalTermination,
    TraversalAssurance,
)
from b24api.errors import (
    AmbiguousExecutionError,
    ApiResponseError,
    B24ApiError,
    BatchFailed,
    BudgetExceededError,
    CapabilityError,
    IncompleteTraversalError,
    KeysetCapabilityError,
    PaginationError,
    ProtocolError,
    ReferenceFailed,
    TransportError,
)
from b24api.settings import (
    Settings,
)
from b24api.transport import (
    HttpxTransport,
    Transport,
    WireTransport,
)

__all__ = [
    "AmbiguityPolicy",
    "AmbiguousExecutionError",
    "ApiResponseError",
    "AutoKeysetExecution",
    "B24ApiError",
    "BatchDispatch",
    "BatchFailed",
    "Binding",
    "Bitrix24",
    "BoundedIdentityRange",
    "BudgetExceededError",
    "CapabilityError",
    "CountedTraversal",
    "CursorSpec",
    "CursorTraversal",
    "DirectDispatch",
    "ExecutionPolicy",
    "HttpxTransport",
    "IdentityCoercion",
    "IdentitySpec",
    "IncompleteTraversalError",
    "KeysetCapabilityError",
    "KeysetSpec",
    "KeysetTraversal",
    "OffsetContinuation",
    "OffsetSpec",
    "OperationReport",
    "PageAdapter",
    "PageStopPolicy",
    "PaginationError",
    "ParameterPath",
    "ParameterUpdate",
    "PartitionedKeysetExecution",
    "ProtocolError",
    "RangeKeysetExecution",
    "ReferenceFailed",
    "ReplaySafety",
    "Request",
    "ResultSelector",
    "RetryPolicy",
    "RouteKind",
    "SequentialKeysetExecution",
    "SequentialTraversal",
    "Settings",
    "StableIntegerKeysetContract",
    "TerminalState",
    "TotalTermination",
    "Transport",
    "TransportError",
    "TraversalAssurance",
    "WireTransport",
]

if not TYPE_CHECKING:
    # Hidden from type checkers: a visible module ``__getattr__`` would let every moved root import type-check.
    def __getattr__(name: str) -> object:
        """Fail for any unknown name, naming the new import path of a name that moved out of the root."""
        # Loaded on first use, so ``python -m b24api.migration`` does not find it already imported.
        package = importlib.import_module("b24api.migration").ROOT_MOVES.get(name)
        message = f"module 'b24api' has no attribute {name!r}"
        raise AttributeError(message if package is None else f"{message}; it moved to {package}.{name} in 3.0")
