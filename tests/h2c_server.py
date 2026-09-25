"""A minimal cleartext HTTP/2 (prior knowledge) server, so hpack runs exactly as it does against a portal."""

from __future__ import annotations
import asyncio
import contextlib
from typing import TYPE_CHECKING, Self

import h2.config
import h2.connection
import h2.events
import h2.exceptions

if TYPE_CHECKING:
    from collections.abc import Callable

type Respond = Callable[[str], tuple[int, tuple[tuple[str, str], ...], bytes]]


def _text(value: str | bytes) -> str:
    return value.decode() if isinstance(value, bytes) else value


class H2cServer:
    """Answer each request path through ``respond``; every connection keeps its own HPACK tables."""

    def __init__(self, respond: Respond) -> None:
        """Bind the response function."""
        self.respond = respond
        self.paths: list[str] = []
        self._server: asyncio.Server | None = None
        self._connections: set[asyncio.Task[None]] = set()

    @property
    def origin(self) -> str:
        """Return the ``http://host:port`` origin the server listens on."""
        if self._server is None:
            raise RuntimeError("server is not started")
        host, port = self._server.sockets[0].getsockname()[:2]
        return f"http://{host}:{port}"

    async def __aenter__(self) -> Self:
        """Start listening on an ephemeral loopback port."""
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Stop listening and finish every connection, even one a failed test left open."""
        if self._server is not None:
            self._server.close()
        # Cancelled first: ``wait_closed`` waits for every open connection, so an unclosed client would hang it.
        for task in tuple(self._connections):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._server is not None:
            await self._server.wait_closed()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connections.add(task)
        try:
            await self._serve(reader, writer)
        finally:
            if task is not None:
                self._connections.discard(task)

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=False),
        )
        connection.initiate_connection()
        writer.write(connection.data_to_send())
        paths: dict[int, str] = {}
        try:
            while data := await reader.read(65_536):
                for event in connection.receive_data(data):
                    if isinstance(event, h2.events.RequestReceived):
                        paths[event.stream_id] = next(
                            _text(value) for name, value in event.headers or () if _text(name) == ":path"
                        )
                    elif isinstance(event, h2.events.DataReceived):
                        connection.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                    elif isinstance(event, h2.events.StreamEnded):
                        path = paths.pop(event.stream_id)
                        self.paths.append(path)
                        status, headers, body = self.respond(path)
                        connection.send_headers(
                            event.stream_id,
                            [(":status", str(status)), *headers, ("content-length", str(len(body)))],
                            end_stream=not body,
                        )
                        if body:
                            connection.send_data(event.stream_id, body, end_stream=True)
                writer.write(connection.data_to_send())
                await writer.drain()
        except (ConnectionError, h2.exceptions.ProtocolError):
            pass
        finally:
            writer.close()
