"""Tests for CanaryRequestHandler – Chicago Style.

Uses real socket pairs for I/O. No mocks. Focus on observable behavior.
"""

from __future__ import annotations

import asyncio
import socket

import pytest

from canaryserver.handler import CanaryRequestHandler, Connection


class SocketConnection:
    """Real socket-based connection implementing the Connection protocol."""

    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._loop = asyncio.get_running_loop()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def _ensure_streams(self) -> None:
        if self._reader is None:
            self._reader, self._writer = await asyncio.open_connection(
                sock=self._sock
            )

    async def read(self, n: int) -> bytes:
        await self._ensure_streams()
        assert self._reader is not None
        return await self._reader.read(n)

    async def write(self, data: bytes) -> None:
        await self._ensure_streams()
        assert self._writer is not None
        self._writer.write(data)
        await self._writer.drain()

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            await self._writer.wait_closed()
        self._sock.close()

    def get_peername(self) -> tuple[str, int] | None:
        return self._sock.getpeername()

    def get_sockname(self) -> tuple[str, int] | None:
        return self._sock.getsockname()

    def get_socket(self) -> socket.socket | None:
        return self._sock


@pytest.fixture
async def connected_pair():
    """Creates a connected TCP socket pair on localhost and yields two SocketConnection instances."""
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(1)

    client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_sock.connect(server_sock.getsockname())

    conn_sock, _ = server_sock.accept()

    server_conn = SocketConnection(conn_sock)
    client_conn = SocketConnection(client_sock)
    try:
        yield server_conn, client_conn
    finally:
        await server_conn.close()
        await client_conn.close()
        server_sock.close()


@pytest.mark.asyncio
async def test_handler_reads_first_payload(connected_pair):
    """Handler reads the first chunk of data from the connection."""
    server_conn, client_conn = connected_pair
    handler = CanaryRequestHandler(server_conn)

    await client_conn.write(b"GET /test HTTP/1.1\r\n")
    entry = await handler.handle()

    assert entry["proto"] == "RAW"
    assert entry["bytes"] > 0
    assert "GET /test" in entry["payload_text"]
    assert entry["client"] is not None


@pytest.mark.asyncio
async def test_handler_reads_extra_payload(connected_pair):
    """Handler reads additional data after the first chunk, if available."""
    server_conn, client_conn = connected_pair
    handler = CanaryRequestHandler(server_conn)

    await client_conn.write(b"HELLO ")
    await asyncio.sleep(0.05)  # ensure first read returns
    await client_conn.write(b"WORLD")

    entry = await handler.handle()

    assert "HELLO WORLD" in entry["payload_text"]


@pytest.mark.asyncio
async def test_handler_empty_connection_returns_empty_entry(connected_pair):
    """Handler returns an EMPTY entry when client closes without sending data."""
    server_conn, client_conn = connected_pair
    handler = CanaryRequestHandler(server_conn)

    client_conn._sock.close()  # close without sending
    entry = await handler.handle()

    assert entry["proto"] == "EMPTY"
    assert entry["note"] == "Connection without data (closed)"


@pytest.mark.asyncio
async def test_handler_connection_reset_is_silent(connected_pair):
    """Handler does not raise when client resets the connection."""
    server_conn, client_conn = connected_pair
    handler = CanaryRequestHandler(server_conn)

    # Simulate reset by closing abruptly
    client_conn._sock.close()

    # Should not raise
    entry = await handler.handle()
    assert entry is not None


@pytest.mark.asyncio
async def test_handler_includes_peer_info(connected_pair):
    """Handler entry includes client address and port."""
    server_conn, client_conn = connected_pair
    handler = CanaryRequestHandler(server_conn)

    await client_conn.write(b"test")
    entry = await handler.handle()

    assert "client" in entry
    assert "port" in entry
    assert ":" in entry["client"]
