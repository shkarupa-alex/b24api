"""Root import migration for the 3.0 public surface (spec §3.12, C7).

``ROOT_MOVES`` is the single source for every name that left the ``b24api`` root: it maps the name to
the public aggregator that keeps exporting the same object. The old root paths still work for this
major with a ``DeprecationWarning``; the migration guide's table is generated from this map.

Run ``python -m b24api.migration PATH...`` to list deprecated root imports in files or directories.
It prints ``file:line b24api.Name -> package.Name`` for each one, rewrites nothing, and exits with 1
when it finds any. The module itself imports nothing else from ``b24api``; running it with ``-m`` still
imports the package root first, so the package must be importable.
"""

from __future__ import annotations
import ast
import sys
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence

# The aggregators whose ``__all__`` is the public contract of this major; leaf modules are not.
PUBLIC_NAMESPACES: Final = (
    "b24api",
    "b24api.contracts",
    "b24api.errors",
    "b24api.transport",
    "b24api.completion",
    "b24api.testing",
)

ROOT_MOVES: Final[Mapping[str, str]] = MappingProxyType(
    {
        # error hierarchy, kept by b24api.errors
        "BatchCommandError": "b24api.errors",
        "EnvelopeContractError": "b24api.errors",
        "HTTPGatewayError": "b24api.errors",
        "IdentityContractError": "b24api.errors",
        "InputSourceError": "b24api.errors",
        "PageAdaptationError": "b24api.errors",
        "PageAdaptationViolation": "b24api.errors",
        "ResponseTooLargeError": "b24api.errors",
        "ResultShapeError": "b24api.errors",
        "ValidationIssue": "b24api.errors",
        # transport wire contract, kept by b24api.transport
        "TransportCapabilities": "b24api.transport",
        "WireRequest": "b24api.transport",
        "WireResponse": "b24api.transport",
        # completion gate, kept by b24api.completion
        "CompletionGate": "b24api.completion",
        # request and wire, kept by b24api.contracts
        "BodyEncoding": "b24api.contracts",
        "RequestHeaders": "b24api.contracts",
        "RequestSummary": "b24api.contracts",
        "ResultErrorSpec": "b24api.contracts",
        "ResultErrorShape": "b24api.contracts",
        "CompositeIdentitySpec": "b24api.contracts",
        "IdentityComponent": "b24api.contracts",
        "TraversalIdentity": "b24api.contracts",
        # positional arguments, kept by b24api.contracts
        "PositionalArguments": "b24api.contracts",
        "PositionalLayout": "b24api.contracts",
        "SlotContract": "b24api.contracts",
        "SlotShape": "b24api.contracts",
        "Present": "b24api.contracts",
        "Omitted": "b24api.contracts",
        "Null": "b24api.contracts",
        "EmptyArray": "b24api.contracts",
        "EmptyObject": "b24api.contracts",
        # frozen JSON, kept by b24api.contracts
        "FrozenJson": "b24api.contracts",
        "FrozenMapping": "b24api.contracts",
        # policies, kept by b24api.contracts
        "AmbiguityReason": "b24api.contracts",
        "ConsistencyPolicy": "b24api.contracts",
        "DuplicatePolicy": "b24api.contracts",
        "ReplayDisposition": "b24api.contracts",
        "UnknownRequestAudit": "b24api.contracts",
        "UnknownRequestCollector": "b24api.contracts",
        # response and page, kept by b24api.contracts
        "Response": "b24api.contracts",
        "BinaryResponse": "b24api.contracts",
        "BinaryEvidence": "b24api.contracts",
        "ResultCollectionShape": "b24api.contracts",
        "AdaptedPage": "b24api.contracts",
        "IdentityPageAdapter": "b24api.contracts",
        "PageView": "b24api.contracts",
        # traversal geometry, kept by b24api.contracts
        "CursorDomain": "b24api.contracts",
        "PageIndex": "b24api.contracts",
        "PageStride": "b24api.contracts",
        "RawTotalSource": "b24api.contracts",
        "SparseRawBound": "b24api.contracts",
        "SplitOrderSpec": "b24api.contracts",
        "traversal_control_paths": "b24api.contracts",
        # page stop, kept by b24api.contracts
        "CallerStop": "b24api.contracts",
        "ContinuePage": "b24api.contracts",
        "PageBoundary": "b24api.contracts",
        # dispatch, kept by b24api.contracts
        "DeliveryOrder": "b24api.contracts",
        # batch outcomes, kept by b24api.contracts
        "Command": "b24api.contracts",
        "CommandSuccess": "b24api.contracts",
        "CommandFailure": "b24api.contracts",
        "CommandNotExecuted": "b24api.contracts",
        "CommandOutcomeUnknown": "b24api.contracts",
        "CommandOutcome": "b24api.contracts",
        "NotExecutedReason": "b24api.contracts",
        "partition_command_outcomes": "b24api.contracts",
        # fan-out outcomes, kept by b24api.contracts
        "ReferenceItem": "b24api.contracts",
        "ReferenceComplete": "b24api.contracts",
        "ReferenceFailure": "b24api.contracts",
        "ReferenceNotExecuted": "b24api.contracts",
        "ReferenceOutcomeUnknown": "b24api.contracts",
        "ReferenceOutcome": "b24api.contracts",
        "ReferenceEvent": "b24api.contracts",
        "partition_reference_outcomes": "b24api.contracts",
        # streams, kept by b24api.contracts
        "OperationStream": "b24api.contracts",
        "PartialResult": "b24api.contracts",
        # identity store, kept by b24api.contracts
        "IdentityStore": "b24api.contracts",
        "identity_store_key": "b24api.contracts",
        # report, kept by b24api.contracts
        "PageDispatch": "b24api.contracts",
        "PageOutcome": "b24api.contracts",
        "PageRecord": "b24api.contracts",
        "PageRejectionCode": "b24api.contracts",
        "KeysetExecutionReport": "b24api.contracts",
        "Violation": "b24api.contracts",
        "ViolationSeverity": "b24api.contracts",
        "CleanupState": "b24api.contracts",
        "CleanupOutcome": "b24api.contracts",
        # keyset execution, kept by b24api.contracts
        "KeysetExecution": "b24api.contracts",
        "KeysetExecutionKind": "b24api.contracts",
        "KeysetPhase": "b24api.contracts",
        "KeysetAssuranceSource": "b24api.contracts",
        "KeysetPageCompletion": "b24api.contracts",
        "KeysetSelectionReason": "b24api.contracts",
        "ClosureWitness": "b24api.contracts",
        "TotalHintMode": "b24api.contracts",
        "TraceClass": "b24api.contracts",
        # keyset capability check, kept by b24api.contracts
        "KeysetCapabilityReport": "b24api.contracts",
        "KeysetCapabilityVerdict": "b24api.contracts",
        "KeysetCapabilityCheckName": "b24api.contracts",
        "KeysetCapabilityCheckOutcome": "b24api.contracts",
        "KeysetCapabilityCheckResult": "b24api.contracts",
        "KeysetInconclusiveReason": "b24api.contracts",
        "MembershipRecheck": "b24api.contracts",
        # completion events, kept by b24api.contracts
        "CompletionEvent": "b24api.contracts",
        "PageScheduled": "b24api.contracts",
        "PageDelivered": "b24api.contracts",
        "PageValidated": "b24api.contracts",
        "PageAcknowledged": "b24api.contracts",
        "PageRejected": "b24api.contracts",
        "PageCommandOutcome": "b24api.contracts",
        "CommandSettlement": "b24api.contracts",
        "BindingAdmitted": "b24api.contracts",
        "BindingClosure": "b24api.contracts",
        "BindingTerminal": "b24api.contracts",
        "StreamClosure": "b24api.contracts",
        "StreamTerminal": "b24api.contracts",
    },
)


@dataclass(frozen=True, slots=True)
class Finding:
    """One deprecated root import in a scanned file."""

    path: Path
    line: int
    name: str

    def __str__(self) -> str:
        """Render ``file:line b24api.Name -> package.Name``."""
        return f"{self.path}:{self.line} b24api.{self.name} -> {ROOT_MOVES[self.name]}.{self.name}"


def scan_source(source: str, path: Path) -> list[Finding]:
    """Find ``from b24api import Name`` and ``b24api.Name`` uses of moved names in one module."""
    tree = ast.parse(source, filename=str(path))
    aliases = {"b24api"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            aliases.update(alias.asname or alias.name for alias in node.names if alias.name == "b24api")
    findings: list[Finding] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "b24api" and node.level == 0:
            findings.extend(Finding(path, node.lineno, alias.name) for alias in node.names if alias.name in ROOT_MOVES)
        elif (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id in aliases
            and node.attr in ROOT_MOVES
        ):
            findings.append(Finding(path, node.lineno, node.attr))
    return sorted(findings, key=lambda finding: (finding.line, finding.name))


def _python_files(paths: Iterable[Path]) -> Iterator[Path]:
    for path in paths:
        if path.is_dir():
            yield from sorted(path.rglob("*.py"))
        else:
            yield path


def scan(paths: Iterable[Path]) -> list[Finding]:
    """Scan files and directories (recursively, ``*.py``) for deprecated root imports."""
    findings: list[Finding] = []
    for path in _python_files(paths):
        findings.extend(scan_source(path.read_text(encoding="utf-8"), path))
    return findings


def migration_table() -> str:
    """Render the migration guide's Markdown table of moved names, grouped by package."""
    rows = ["| Old import | New import |", "|---|---|"]
    rows.extend(
        f"| `from b24api import {name}` | `from {package} import {name}` |"
        for name, package in sorted(ROOT_MOVES.items(), key=lambda item: (item[1], item[0].casefold()))
    )
    return "\n".join(rows) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """Print every deprecated root import under the given paths; exit 1 when any is found."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        sys.stderr.write("usage: python -m b24api.migration PATH...\n")
        return 2
    findings = scan(Path(argument) for argument in arguments)
    for finding in findings:
        sys.stdout.write(f"{finding}\n")
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
