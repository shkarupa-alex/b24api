"""One explicit, method-agnostic b24api public surface.

The root exports the names a typical application needs. Every other public name lives in one of the
public aggregators listed in ``b24api.migration.PUBLIC_NAMESPACES``. A name that left the root in 3.0
still resolves here for this major, with a ``DeprecationWarning`` naming its new import path.
"""

import importlib
import warnings
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
    # Hidden from type checkers, so a moved root import stays a visible ``attr-defined`` error there.
    def __getattr__(name: str) -> object:
        """Resolve a name that moved out of the root, warning with its new import path."""
        # Loaded on first use, so ``python -m b24api.migration`` does not find it already imported.
        package = importlib.import_module("b24api.migration").ROOT_MOVES.get(name)
        if package is None:
            raise AttributeError(f"module 'b24api' has no attribute {name!r}")
        warnings.warn(f"b24api.{name} moved to {package}.{name}", DeprecationWarning, stacklevel=2)
        return getattr(importlib.import_module(package), name)
