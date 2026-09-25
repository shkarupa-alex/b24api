"""Capture golden projections for every offline recipe and every core scripted scenario.

``uv run python -m tests.golden.capture --write`` rewrites ``baseline.json``. It is run once on the
unchanged base commit; later behavior changes are declared in ``DELTAS.md`` instead of recapturing.
"""

from __future__ import annotations
import argparse
import asyncio
import importlib
import json
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from b24api import Bitrix24
from b24api.testing import ScriptedExchange, ScriptedTransport
from examples.run import SCENARIOS as RECIPES
from tests.golden.projection import RecordedStream, canonical, report_projection, request_projection
from tests.golden.scenarios import SCENARIOS, GoldenPortal

if TYPE_CHECKING:
    from collections.abc import Iterator

BASELINE = Path(__file__).with_name("baseline.json")
DELTAS = Path(__file__).with_name("DELTAS.md")


@contextmanager
def _record_streams() -> Iterator[list[RecordedStream]]:
    """Wrap every public stream the client registers so its yielded items are observed."""
    streams: list[RecordedStream] = []
    original = Bitrix24._register_stream  # noqa: SLF001 - the one public-stream seam of the client

    def register(self: Bitrix24, stream: Any) -> Any:  # noqa: ANN401 - any public stream
        return RecordedStream(original(self, stream), streams)

    Bitrix24._register_stream = register  # type: ignore[method-assign] # noqa: SLF001
    try:
        yield streams
    finally:
        Bitrix24._register_stream = original  # type: ignore[method-assign] # noqa: SLF001


@contextmanager
def _record_scripted_transports() -> Iterator[list[ScriptedTransport]]:
    transports: list[ScriptedTransport] = []
    original_init = ScriptedTransport.__init__

    def observe_init(
        self: ScriptedTransport,
        exchanges: tuple[ScriptedExchange, ...],
        *,
        host: str = "fixture.invalid",
    ) -> None:
        original_init(self, exchanges, host=host)
        transports.append(self)

    ScriptedTransport.__init__ = observe_init  # type: ignore[method-assign]
    try:
        yield transports
    finally:
        ScriptedTransport.__init__ = original_init  # type: ignore[method-assign]


async def capture_recipe(module_name: str) -> dict[str, object]:
    """Run one offline recipe and project its requests, streams and evidence reports."""
    with _record_streams() as streams, _record_scripted_transports() as transports:
        module = importlib.import_module(f"examples.{module_name}")
        evidence = await module.run()
    return {
        "requests": [[request_projection(call) for call in transport.calls] for transport in transports],
        "streams": [stream.projection() for stream in streams],
        "evidence": {
            "observed_count": evidence.observed_count,
            "reports": [report_projection(report) for report in evidence.reports],
        },
    }


async def capture_scenario(name: str) -> dict[str, object]:
    """Run one core scripted scenario against a fresh deterministic portal."""
    portal = GoldenPortal()
    with _record_streams() as streams:
        await SCENARIOS[name](portal)
    return {
        "requests": [[request_projection(call) for call in portal.calls]],
        "streams": [stream.projection() for stream in streams],
    }


def fixture_names() -> list[str]:
    """Return every golden fixture name in a stable order."""
    return [f"recipe:{recipe.module}" for recipe in RECIPES] + [f"scenario:{name}" for name in SCENARIOS]


async def capture(name: str) -> dict[str, object]:
    """Capture one named fixture."""
    kind, _, target = name.partition(":")
    projection = await (capture_recipe(target) if kind == "recipe" else capture_scenario(target))
    return canonical(json.loads(json.dumps(projection)))  # type: ignore[return-value]


def main() -> None:
    """Write the baseline projection of every fixture."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="rewrite baseline.json")
    arguments = parser.parse_args()
    projections = {name: asyncio.run(capture(name)) for name in fixture_names()}
    text = json.dumps(projections, indent=1, sort_keys=True, ensure_ascii=False) + "\n"
    if arguments.write:
        BASELINE.write_text(text, encoding="utf-8")
    else:
        print(text)  # noqa: T201 - command-line output


if __name__ == "__main__":
    main()
