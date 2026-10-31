"""CanaryServer – public API, composes transport delegate and request handler.

CanaryServer owns the request-processing logic. Network I/O is delegated
to AsyncioTransport, which encapsulates all asyncio specifics.
"""

from __future__ import annotations

import asyncio
import logging
import socket
from typing import Optional

from canaryserver.handler import CanaryRequestHandler, Connection
from canaryserver.transport import AsyncioTransport, ConnectionHandler


class CanaryServer:
    """Public API for the canary server.

    Usage:
        server = CanaryServer(host="0.0.0.0", ports=[8000])
        server.start()
    """

    def __init__(
        self,
        host: str,
        ports: list[int],
        config: dict | None = None,
        log_file: str | None = None,
        quiet: bool = False,
        workers: int = 128,          # not used with asyncio (API compatibility)
        ipv6: bool = False,
        enable_peer_pid: bool = True,
    ):
        self.host = host
        self.ports = ports
        self.config = config or {}
        self.log_file = log_file
        self.quiet = quiet
        self.ipv6 = ipv6
        self.enable_peer_pid = enable_peer_pid

        self._stop: Optional[asyncio.Event] = None
        self._transports: list[AsyncioTransport] = []
        self._count = 0
        self._count_lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        """Start the server and block until stopped."""
        try:
            asyncio.run(self._run())
        except KeyboardInterrupt:
            print("\n\033[1;33mShutting down…\033[0m")

    async def stop(self) -> None:
        """Stop all transports (close all listening sockets)."""
        for transport in self._transports:
            await transport.stop()

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #
    async def _run(self) -> None:
        self._stop = asyncio.Event()
        self._transports = []

        for port in self.ports:
            transport = await self._start_port(port)
            self._transports.append(transport)

        print(f"\033[1;32mCanaryServer listening on {self.host}:{self.ports}\033[0m", flush=True)

        # Wait for stop signal (set by signal handler in first transport)
        await self._stop.wait()

        await self.stop()
        self._print_summary()

    async def _start_port(self, port: int) -> AsyncioTransport:
        handler: ConnectionHandler = self._make_handler()
        transport = AsyncioTransport(
            host=self.host,
            port=port,
            handler=handler,
            ipv6=self.ipv6,
            stop_event=self._stop,
        )
        await transport.start()
        return transport

    def _make_handler(self) -> ConnectionHandler:
        async def handle_connection(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            # Adapt asyncio streams to our Connection protocol
            conn = AsyncioConnection(reader, writer)
            handler = CanaryRequestHandler(
                conn, enable_peer_pid=self.enable_peer_pid
            )
            entry = await handler.handle()
            await self._record(entry)

        return handle_connection

    async def _record(self, entry: dict) -> None:
        async with self._count_lock:
            self._count += 1
            await self._print(entry)

    async def _print(self, entry: dict) -> None:
        proto = entry.get("proto")
        client = entry.get("client", "-")
        port = entry.get("port", "?")
        head = f"\n{entry.get('ts')}"

        if proto == "RAW":
            head += (
                f"\n\033[1;35m◆ RAW {entry.get('bytes',0)} Bytes\033[0m "
                f"\033[2mvon {client} [Port {port}]\033[0m"
            )
        elif proto == "EMPTY":
            head += f"\n\033[2m· EMPTY von {client} [Port {port}]\033[0m"
        else:
            head += f"\n\033[2m· {proto} von {client} [Port {port}]\033[0m"

        if proc := entry.get("process"):
            name = proc.get("cmd", "?")
            if len(name) > 120:
                name = name[:120] + "…"
            head += f"\n\033[1;32m<PID {proc.get('pid')} {name}>\033[0m"

        if entry.get("payload_text"):
            lines = ["    \033[2m--- Payload ---\033[0m"]
            lines.append("    " + entry["payload_text"].replace("\n", "\n    "))
            head += "\n" + "\n".join(lines)

        print(head, flush=True)

    def _print_summary(self) -> None:
        print(f"\n\033[1;36mTotal {self._count} connections processed.\033[0m", flush=True)


class AsyncioConnection:
    """Adapts asyncio StreamReader/StreamWriter to the Connection protocol."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer

    async def read(self, n: int) -> bytes:
        return await self._reader.read(n)

    async def write(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()

    async def close(self) -> None:
        self._writer.close()
        await self._writer.wait_closed()

    def get_peername(self) -> tuple[str, int] | None:
        return self._writer.get_extra_info("peername")

    def get_sockname(self) -> tuple[str, int] | None:
        return self._writer.get_extra_info("sockname")

    def get_socket(self) -> socket.socket | None:
        return self._writer.get_extra_info("socket")
