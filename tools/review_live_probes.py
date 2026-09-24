"""Read-only live probes L1-L5 of the astra-fable review (spec §2.5 and §5).

Run from a reviewed checkout: ``uv run python tools/review_live_probes.py --env-file ../.env``. The
webhook comes from ``BITRIX24_API_WEBHOOK_URL`` (``--env-file`` reads only that key). Only the
allowlisted read methods below are sent. The printed protocol never contains the webhook, the portal
host or row values: headers are reduced to names and numeric shapes, bodies to digests and counts,
and a failure to its exception class.
"""

from __future__ import annotations
import argparse
import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from b24api import (
    BatchDispatch,
    Binding,
    Bitrix24,
    CountedTraversal,
    HttpxTransport,
    IdentityCoercion,
    IdentitySpec,
    KeysetSpec,
    OffsetContinuation,
    OffsetSpec,
    ParameterPath,
    ParameterUpdate,
    ReplaySafety,
    Request,
    ResultSelector,
    Settings,
    TotalTermination,
)
from b24api.contracts import CompositeIdentitySpec, IdentityComponent, PageStride
from b24api.traversal import counted_batch, sequential

if TYPE_CHECKING:
    from collections.abc import Callable

    from b24api.transport import WireResponse
    from b24api.traversal.counted_rules import CountedPageFacts, CountedVerdict

READ_METHODS = frozenset(
    {
        "scope",
        "server.time",
        "crm.deal.list",
        "crm.requisitelink.list",
        "crm.item.list",
        "crm.timeline.comment.list",
        "tasks.task.list",
        "user.get",
        "batch",
    }
)
ENCODINGS = (None, "gzip, deflate", "identity", "br", "zstd")
EPOCH_FLOOR = 1_000_000_000
PAGE = 50


def _webhook(env_file: Path | None) -> str:
    if env_file is not None:
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, _, value = line.partition("=")
            if key.strip() == "BITRIX24_API_WEBHOOK_URL":
                return value.strip().strip("'\"")
        raise SystemExit("BITRIX24_API_WEBHOOK_URL is not set in the env file")
    configured = os.environ.get("BITRIX24_API_WEBHOOK_URL")
    if not configured:
        raise SystemExit("BITRIX24_API_WEBHOOK_URL is not set")
    return configured


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()[:16]


def _reset_shape(value: str | None, now: float) -> dict[str, object] | None:
    """Classify ``X-Bitrix-RateLimit-Reset`` as spec §3.4 reads it: epoch (>= 1e9), delta or other."""
    if value is None:
        return None
    try:
        number = float(value)
    except ValueError:
        return {"form": "non-numeric", "length": len(value)}
    if number >= EPOCH_FLOOR:
        return {"form": "epoch", "seconds_from_now": round(number - now, 1)}
    return {"form": "delta", "seconds": number}


def _header_facts(headers: httpx.Headers | dict[str, str]) -> dict[str, object]:
    names = sorted({name.lower() for name in headers})
    limit_names = [
        name for name in names if "ratelimit" in name or name == "retry-after" or name.startswith("x-bitrix")
    ]
    return {
        "content_encoding": headers.get("content-encoding"),
        "rate_headers": {
            name: headers.get(name) if name != "x-bitrix-ratelimit-reset" else None for name in limit_names
        },
        "reset": _reset_shape(headers.get("x-bitrix-ratelimit-reset"), time.time()),
        "retry_after_present": "retry-after" in names,
    }


async def l1_l2_raw(webhook: str) -> list[dict[str, object]]:
    """L1 and L2 over plain HTTPX: headers per ``Accept-Encoding`` and whether the JSON is unchanged."""
    rows: list[dict[str, object]] = []
    async with httpx.AsyncClient(http2=True, timeout=30) as client:
        for method in ("scope", "server.time"):
            for encoding in ENCODINGS:
                headers = {} if encoding is None else {"accept-encoding": encoding}
                sent = encoding if encoding is not None else client.headers.get("accept-encoding")
                row: dict[str, object] = {"probe": "L1/L2 raw", "method": method, "accept_encoding": sent}
                try:
                    response = await client.post(f"{webhook.rstrip('/')}/{method}.json", json={}, headers=headers)
                except httpx.HTTPError as error:
                    row["error"] = type(error).__name__
                    rows.append(row)
                    continue
                row |= {"status": response.status_code, "http_version": response.http_version}
                row |= _header_facts(response.headers)
                try:
                    body = response.json()
                except (ValueError, httpx.DecodingError) as error:
                    row["body"] = f"undecodable: {type(error).__name__}"
                else:
                    row["result_digest"] = _digest(body.get("result")) if method == "scope" else None
                    row["time_keys"] = sorted(body.get("time", {}))
                    reset_at = body.get("time", {}).get("operating_reset_at")
                    row["operating_reset_at"] = _reset_shape(None if reset_at is None else str(reset_at), time.time())
                rows.append(row)
    return rows


@dataclass
class RecordingTransport:
    """Delegates to the library's own ``HttpxTransport`` and keeps each response's header facts."""

    inner: HttpxTransport
    sends: list[dict[str, object]] = field(default_factory=list)

    @property
    def host(self) -> str:
        """Return the delegate's host."""
        return self.inner.host

    @property
    def capabilities(self) -> object:
        """Return the delegate's capabilities, so routes and encodings stay the library's own."""
        return self.inner.capabilities

    async def send(self, request: Request, *, attempt_timeout: float, max_response_bytes: int) -> WireResponse:
        """Send one allowlisted read through the delegate and record the response headers."""
        if request.method not in READ_METHODS:
            raise PermissionError(f"{request.method} is not an allowlisted read method")
        response = await self.inner.send(
            request, attempt_timeout=attempt_timeout, max_response_bytes=max_response_bytes
        )
        commands = request.copy_parameters().get("cmd")
        self.sends.append(
            {
                "method": request.method,
                "commands": len(commands) if isinstance(commands, dict) else None,
                "status": response.status_code,
                **_header_facts(response.header_map),
            }
        )
        return response

    async def aclose(self) -> None:
        """Close the delegate."""
        await self.inner.aclose()


@dataclass
class VerdictLog:
    """Records every ``judge_counted_page`` verdict (R2) by path while a probe runs."""

    verdicts: Counter[str] = field(default_factory=Counter)

    def install(self) -> None:
        """Wrap the validator where the sequential and the batched counted paths call it."""
        for module in (sequential, counted_batch):
            original = vars(module)["judge_counted_page"]

            def judged(
                facts: CountedPageFacts,
                *,
                _original: Callable[[CountedPageFacts], CountedVerdict] = original,
                _path: str = module.__name__,
            ) -> CountedVerdict:
                verdict = _original(facts)
                outcome = verdict.contradiction.value if verdict.contradiction else "none"
                self.verdicts[f"{_path.rsplit('.', 1)[-1]}:terminal={verdict.terminal}:contradiction={outcome}"] += 1
                return verdict

            vars(module)["judge_counted_page"] = judged


def _report_facts(stream: object, rows: list[object], transport: RecordingTransport, before: int) -> dict[str, object]:
    report = getattr(stream, "report", None)
    sends = transport.sends[before:]
    facts: dict[str, object] = {
        "rows": len(rows),
        "http": len(sends),
        "sends": [send["method"] if send["commands"] is None else f"batch x{send['commands']}" for send in sends],
    }
    if report is not None:
        facts |= {
            "state": report.state.value,
            "exhausted": report.exhausted,
            "assurance": getattr(report.assurance, "value", None),
            "violations": sorted(item.code for item in report.violations),
        }
        execution = getattr(report, "keyset_execution", None)
        selection = getattr(report, "keyset_selection", None)
        if execution is not None:
            facts["keyset_execution"] = {
                "requested": execution.requested_kind.value,
                "selected": execution.selected_kind.value,
                "final_reason": getattr(execution.final_selection_reason, "value", None),
                "planning_requests": execution.planning_requests,
                "boundary_requests": execution.boundary_requests,
                "range_windows": execution.range_window_count,
                "closure_witnesses": {witness.value: count for witness, count in execution.closure_witness_counts},
            }
        if selection is not None:
            facts["keyset_selection"] = selection.reason.value
    return facts


async def _traverse(
    label: str, stream: object, transport: RecordingTransport, log: VerdictLog | None = None
) -> dict[str, object]:
    before, verdicts = len(transport.sends), Counter(log.verdicts) if log else Counter()
    rows: list[object] = []
    row: dict[str, object] = {"probe": label}
    try:
        rows = [item async for item in stream]  # type: ignore[attr-defined]
    except Exception as error:  # noqa: BLE001 - the protocol records the class, never the message
        row["error"] = type(error).__name__
    row |= _report_facts(stream, rows, transport, before)
    kinds = Counter(type(item).__name__ for item in rows)
    if any(kind.startswith("Reference") for kind in kinds):
        row["outcomes"] = dict(kinds)
    if log is not None:
        row["counted_verdicts"] = dict(log.verdicts - verdicts)
    return row


_ID = IdentitySpec(("ID",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER)
_FIXED = OffsetSpec(
    continuation=OffsetContinuation.FIXED_STEP,
    step=PAGE,
    total_termination=TotalTermination.EXACT_QUALIFIED,
    page_stride=PageStride(PAGE, PAGE, PAGE),
)
_QUALIFIED = OffsetSpec(total_termination=TotalTermination.EXACT_QUALIFIED)


def _read(method: str, parameters: dict[str, object] | None = None) -> Request:
    return Request.bare(method, parameters or {}, replay_safety=ReplaySafety.SAFE)


_LINKS = _read(
    "crm.requisitelink.list",
    {"select": ["entityTypeId", "entityId"], "filter": {"entityTypeId": 2}, "order": {"entityId": "ASC"}},
)
_LINK_IDENTITY = CompositeIdentitySpec(
    (
        IdentityComponent(("entityTypeId",), IdentityCoercion.EXACT_INTEGER, "entityTypeId"),
        IdentityComponent(("entityId",), IdentityCoercion.EXACT_INTEGER, "entityId"),
    )
)
_LINK_SELECTOR = ResultSelector(("requisiteLinks",))


async def library_probes(webhook: str) -> list[dict[str, object]]:
    """L2 on the library path, L3 counted traversals with R2 verdicts, L4 auto keyset."""
    settings = Settings(webhook_url=webhook)
    transport = RecordingTransport(HttpxTransport(webhook))
    log = VerdictLog()
    log.install()
    rows: list[dict[str, object]] = []
    async with Bitrix24(settings, transport=transport) as client:
        await client.call(_read("scope"))
        rows.append({"probe": "L2 library", **transport.sends[-1]})

        deals = _read("crm.deal.list", {"select": ["ID"], "order": {"ID": "ASC"}})
        tasks = _read("tasks.task.list", {"select": ["ID"], "order": {"ID": "asc"}})
        task_identity = IdentitySpec(("id",), "ID", "ID", IdentityCoercion.DECIMAL_STRING_INTEGER)
        counted = (
            ("crm.deal.list", client.iter_list_counted(deals, identity=_ID)),
            (
                "tasks.task.list",
                client.iter_list_counted(tasks, selector=ResultSelector(("tasks",)), identity=task_identity),
            ),
            (
                "crm.requisitelink.list (scenario 14)",
                client.iter_list_counted(_LINKS, selector=_LINK_SELECTOR, identity=_LINK_IDENTITY, offset=_FIXED),
            ),
        )
        for label, stream in counted:
            rows.append(await _traverse(f"L3 counted {label}", stream, transport, log))
        sequential_paths = (
            ("crm.deal.list", client.iter_list(deals, identity=_ID, offset=_QUALIFIED)),
            (
                "crm.requisitelink.list (scenario 14)",
                client.iter_list(_LINKS, selector=_LINK_SELECTOR, identity=_LINK_IDENTITY, offset=_FIXED),
            ),
        )
        for label, stream in sequential_paths:
            rows.append(await _traverse(f"L3 sequential qualified {label}", stream, transport, log))
        first = await client.call(deals)
        deal_ids = [row["ID"] for row in first if isinstance(row, dict)] if isinstance(first, list) else []
        for deal_id in deal_ids[:3]:
            comments = _read(
                "crm.timeline.comment.list",
                {"filter": {"ENTITY_TYPE": "deal", "ENTITY_ID": deal_id}, "select": ["ID"], "order": {"ID": "ASC"}},
            )
            stream = client.iter_list_counted(comments, identity=_ID, offset=_FIXED)
            rows.append(await _traverse("L3 counted crm.timeline.comment.list (scenario 10)", stream, transport, log))

        # Counted reference traversals: R2 per binding, batched (BatchDispatch is required).
        references = (
            (
                "crm.timeline.comment.list by deal (scenario 10)",
                _read("crm.timeline.comment.list", {"filter": {"ENTITY_TYPE": "deal"}, "select": ["ID"]}),
                [(deal_id, ParameterPath(("filter", "ENTITY_ID"))) for deal_id in deal_ids[:5]],
                CountedTraversal(identity=_ID, offset=_FIXED),
            ),
            (
                "crm.requisitelink.list by entity type (scenario 14)",
                _LINKS,
                [(entity_type, ParameterPath(("filter", "entityTypeId"))) for entity_type in (2, 3, 4)],
                CountedTraversal(identity=_LINK_IDENTITY, selector=_LINK_SELECTOR, offset=_FIXED),
            ),
        )
        for label, base, keys, traversal in references:
            bindings = [Binding(str(key), (ParameterUpdate(path, key),), key) for key, path in keys]
            outcomes = client.iter_reference_outcomes(base, bindings, traversal=traversal, dispatch=BatchDispatch())
            rows.append(await _traverse(f"L3 reference counted {label}", outcomes, transport, log))

        keysets = (
            ("crm.deal.list", _read("crm.deal.list", {"select": ["ID"]}), ResultSelector.root(), _ID),
            (
                "crm.item.list (scenario 8)",
                _read("crm.item.list", {"entityTypeId": 2, "select": ["id"]}),
                ResultSelector(("items",)),
                IdentitySpec(("id",), "id", "id", IdentityCoercion.EXACT_INTEGER),
            ),
            (
                "tasks.task.list",
                _read("tasks.task.list", {"select": ["ID"]}),
                ResultSelector(("tasks",)),
                task_identity,
            ),
            # Negative control: user.get takes sort/order strings, not an order map, so it must fail closed.
            ("user.get (negative control)", _read("user.get"), ResultSelector.root(), _ID),
        )
        for label, request, selector, identity in keysets:
            stream = client.iter_list_keyset(request, selector=selector, identity=identity, keyset=KeysetSpec())
            rows.append(await _traverse(f"L4 auto keyset {label}", stream, transport))
    await transport.aclose()
    return rows


async def l1_throttle(webhook: str, *, budget: int, concurrency: int) -> dict[str, object]:
    """L1 under throttling: a capped burst of ``server.time`` that stops at the first non-200 answer."""
    throttled: list[dict[str, object]] = []
    sent = 0
    stop = asyncio.Event()

    async def worker(client: httpx.AsyncClient) -> None:
        nonlocal sent
        while not stop.is_set() and sent < budget:
            sent += 1
            try:
                response = await client.post(f"{webhook.rstrip('/')}/server.time.json", json={})
            except httpx.HTTPError as error:
                throttled.append({"error": type(error).__name__})
                stop.set()
                return
            if response.status_code != httpx.codes.OK:
                body = response.json() if "json" in response.headers.get("content-type", "") else {}
                throttled.append(
                    {"status": response.status_code, "error": body.get("error"), **_header_facts(response.headers)}
                )
                stop.set()

    async with httpx.AsyncClient(http2=True, timeout=30) as client:
        await asyncio.gather(*(worker(client) for _ in range(concurrency)))
    return {"probe": "L1 throttle", "budget": budget, "sent": sent, "first_throttled": throttled[:1]}


def _revision() -> str:
    """Return the checkout's short SHA, marked ``-dirty`` when tracked files differ from it."""
    git = shutil.which("git")
    if git is None:
        return "unknown"
    root = Path(__file__).resolve().parents[1]
    sha = subprocess.run(  # noqa: S603 - resolved executable and fixed arguments
        [git, "rev-parse", "--short", "HEAD"], cwd=root, capture_output=True, text=True, check=True
    )
    status = subprocess.run(  # noqa: S603 - resolved executable and fixed arguments
        [git, "status", "--porcelain", "--untracked-files=no"], cwd=root, capture_output=True, text=True, check=True
    )
    return sha.stdout.strip() + ("-dirty" if status.stdout.strip() else "")


async def main() -> None:
    """Print the protocol header, then one JSON line per probe."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument(
        "--throttle-probe",
        type=int,
        default=0,
        metavar="BUDGET",
        help="also send up to BUDGET server.time calls in a burst and record the first throttled answer",
    )
    arguments = parser.parse_args()
    webhook = _webhook(arguments.env_file)
    header = {
        "protocol": "astra-fable L1-L5",
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "client": metadata.version("b24api"),
        "revision": _revision(),
        "httpx": httpx.__version__,
    }
    sys.stdout.write(json.dumps(header) + "\n")
    rows = [*await l1_l2_raw(webhook), *await library_probes(webhook)]
    if arguments.throttle_probe:
        rows.append(await l1_throttle(webhook, budget=min(arguments.throttle_probe, 200), concurrency=20))
    for row in rows:
        sys.stdout.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


if __name__ == "__main__":
    asyncio.run(main())
