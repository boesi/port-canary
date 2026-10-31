"""Request handler logic for CanaryServer.

Works against an abstract Connection protocol. No asyncio dependencies.
"""

from __future__ import annotations

import asyncio
import logging
import socket
from datetime import datetime, timezone
from typing import Optional, Protocol, runtime_checkable

from canarypid import peer_process


@runtime_checkable
class Connection(Protocol):
    """Abstract stream connection. Implemented by asyncio delegate."""

    async def read(self, n: int) -> bytes: ...
    async def write(self, data: bytes) -> None: ...
    async def close(self) -> None: ...
    def get_peername(self) -> tuple[str, int] | None: ...
    def get_sockname(self) -> tuple[str, int] | None: ...
    def get_socket(self) -> socket.socket | None: ...


class CanaryRequestHandler:
    """Handles a single client connection: read payload, build log entry.

    Dependencies: a Connection implementation (provided by the transport delegate).
    """

    def __init__(self, connection: Connection, enable_peer_pid: bool = True):
        self._connection = connection
        self._enable_peer_pid = enable_peer_pid

    async def handle(self) -> dict:
        """Read data from connection and return a log entry dict."""
        addr = self._connection.get_peername()
        peer = f"{addr[0]}:{addr[1]}" if addr else "?"
        port = self._connection.get_sockname()
        local_port = port[1] if port else 0

        proc: Optional[dict] = None
        if self._enable_peer_pid and addr and (
            addr[0].startswith("127.") or addr[0] == "::1"
        ):
            sock = self._connection.get_socket()
            if sock is not None:
                proc = await asyncio.to_thread(
                    peer_process, sock, addr, self._enable_peer_pid
                )

        try:
            first = await self._read_first()
        except (ConnectionResetError, BrokenPipeError, OSError):
            return self._empty_entry(peer, local_port, "Connection closed without data", proc)

        if not first:
            return self._empty_entry(peer, local_port, "Connection without data (closed)", proc)

        payload = await self._read_extra(first)

        entry = {
            "proto": "RAW",
            "client": peer,
            "port": local_port,
            "process": proc,
            "bytes": len(payload),
            "payload_text": self._printable(payload)[:2000],
            "payload_hex": payload[:256].hex(" "),
            "ts": datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds"),
        }
        return entry

    async def _read_first(self) -> bytes:
        return await asyncio.wait_for(self._connection.read(8192), timeout=5.0)

    async def _read_extra(self, first: bytes) -> bytes:
        payload = first
        try:
            extra = await asyncio.wait_for(self._connection.read(4096), timeout=1.5)
            if extra:
                payload += extra
        except (asyncio.TimeoutError, TimeoutError):
            pass
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass
        return payload

    def _empty_entry(
        self, peer: str, port: int, note: str, proc: Optional[dict] = None
    ) -> dict:
        return {
            "proto": "EMPTY",
            "client": peer,
            "port": port,
            "process": proc,
            "note": note,
        }

    @staticmethod
    def _printable(payload: bytes) -> str:
        return "".join(
            chr(b) if 32 <= b < 127 or b in (9, 10, 13) else "." for b in payload
        )
