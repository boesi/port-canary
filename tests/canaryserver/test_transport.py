"""Tests for AsyncioTransport – Chicago Style.

Uses real TCP sockets on localhost. No mocks. Focus on observable behavior:
connections are accepted, handler is invoked, shutdown is clean.
"""

from __future__ import annotations

import asyncio
import socket

import pytest

from canaryserver.transport import AsyncioTransport


class RecordingHandler:
    """Test handler that records connections and data for assertions."""

    def __init__(self):
        self.connections = []
        self.data_received = []
        self.should_stop = False

    async def __call__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        self.connections.append(addr)
        data = await reader.read(4096)
        self.data_received.append(data)
        if self.should_stop:
            writer.close()
            await writer.wait_closed()
            return
        writer.write(b"ACK\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()


@pytest.fixture
async def transport_with_handler():
    """Creates an AsyncioTransport with a RecordingHandler on port 0."""
    handler = RecordingHandler()
    transport = AsyncioTransport(host="127.0.0.1", port=0, handler=handler)
    await transport.start()
    try:
        yield transport, handler
    finally:
        await transport.stop()


@pytest.mark.asyncio
async def test_transport_accepts_connection(transport_with_handler):
    """Transport accepts a TCP connection and invokes the handler."""
    transport, handler = transport_with_handler
    port = transport.servers[0].sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"PING\n")
    await writer.drain()
    response = await reader.read(100)
    writer.close()
    await writer.wait_closed()

    await asyncio.sleep(0.1)

    assert len(handler.connections) == 1
    assert b"PING" in handler.data_received[0]
    assert response == b"ACK\n"


@pytest.mark.asyncio
async def test_transport_multiple_connections(transport_with_handler):
    """Transport handles multiple concurrent connections."""
    transport, handler = transport_with_handler
    port = transport.servers[0].sockets[0].getsockname()[1]

    for i in range(5):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(f"MSG{i}\n".encode())
        await writer.drain()
        await reader.read(100)
        writer.close()
        await writer.wait_closed()

    await asyncio.sleep(0.2)

    assert len(handler.connections) == 5
    assert len(handler.data_received) == 5


@pytest.mark.asyncio
async def test_transport_stop_closes_servers(transport_with_handler):
    """Transport.stop() closes all listening sockets."""
    transport, handler = transport_with_handler
    port = transport.servers[0].sockets[0].getsockname()[1]

    await transport.stop()

    # Connecting should fail after stop
    with pytest.raises((OSError, ConnectionRefusedError)):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_transport_reports_listening_port():
    """Transport exposes the actual bound port after start."""
    handler = RecordingHandler()
    transport = AsyncioTransport(host="127.0.0.1", port=0, handler=handler)
    await transport.start()
    try:
        assert transport.port is not None
        assert transport.port > 0
    finally:
        await transport.stop()
