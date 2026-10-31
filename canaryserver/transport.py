"""Asyncio transport delegate for CanaryServer.

Encapsulates all asyncio-specific I/O: event loop, sockets, signals,
accept loop, and graceful shutdown. No request logic here.
"""

from __future__ import annotations

import asyncio
import errno
import logging
import signal
import socket
import time
from typing import Callable, Optional

# Handler type: called with (reader, writer) for each accepted connection
ConnectionHandler = Callable[
    [asyncio.StreamReader, asyncio.StreamWriter], None
]


class AsyncioTransport:
    """Delegates asyncio network I/O.

    Responsibilities:
      - Bind listening sockets
      - Accept connections and dispatch to handler
      - Graceful shutdown on SIGTERM / SIGHUP / SIGINT
      - EMFILE / ENOBUFS warnings (rate-limited)
    """

    def __init__(
        self,
        host: str,
        port: int,
        handler: ConnectionHandler,
        ipv6: bool = False,
        stop_event: Optional[asyncio.Event] = None,
    ):
        self._host = host
        self._port = port
        self._handler = handler
        self._ipv6 = ipv6

        self._servers: list[asyncio.AbstractServer] = []
        self._stop = stop_event or asyncio.Event()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._last_resource_warning: float = 0.0
        self._started = False
        self._signals_installed = False

    @property
    def port(self) -> Optional[int]:
        """Actual bound port (None until started)."""
        if self._servers:
            return self._servers[0].sockets[0].getsockname()[1]
        return None

    @property
    def servers(self) -> list[asyncio.AbstractServer]:
        return self._servers

    async def start(self) -> None:
        """Bind sockets and begin accepting connections (non-blocking).

        asyncio.start_server already accepts connections in the background,
        so no extra accept loop is needed here.
        """
        loop = asyncio.get_running_loop()
        self._loop = loop
        self._started = True
        self._install_exception_handler(loop)
        self._install_signal_handlers(loop)

        await self._bind()

        logging.info(
            "Transport listening on %s:%s", self._host, self.port
        )

    async def serve_forever(self) -> None:
        """Block until a stop signal is received, then close all sockets."""
        try:
            await self._stop.wait()
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Close all listening sockets and wait for them to close."""
        for srv in self._servers:
            srv.close()
        for srv in self._servers:
            await srv.wait_closed()
        self._servers.clear()
        self._started = False

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #
    async def _bind(self) -> None:
        families = [socket.AF_INET]
        if self._ipv6:
            families.append(socket.AF_INET6)

        for family in families:
            bind_host = "::" if family == socket.AF_INET6 else self._host
            try:
                srv = await asyncio.start_server(
                    self._client_connected,
                    host=bind_host,
                    port=self._port,
                    family=family,
                    reuse_address=True,
                )
                self._servers.append(srv)
            except OSError as exc:
                logging.warning(
                    "Port %s (AF=%s) not bindable: %s", self._port, family, exc
                )

    async def _client_connected(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            # Schedule handler as a task so one slow connection
            # does not block the accept loop.
            asyncio.create_task(self._handler(reader, writer))
        except Exception:
            logging.exception("Failed to dispatch connection to handler")
            try:
                writer.close()
                await writer.wait_closed()
            except OSError:
                pass

    def _install_exception_handler(self, loop: asyncio.AbstractEventLoop) -> None:
        def handler(loop, context):
            exception = context.get("exception")
            if isinstance(exception, OSError) and exception.errno in (
                errno.EMFILE,
                errno.ENOBUFS,
            ):
                self._warn_resource_limit(exception.strerror)
                return
            loop.default_exception_handler(context)

        loop.set_exception_handler(handler)

    def _warn_resource_limit(self, message: str) -> None:
        now = time.monotonic()
        if now - self._last_resource_warning < 5.0:
            return
        self._last_resource_warning = now
        logging.warning(
            "System limit reached: too many open files (EMFILE). "
            "New connections cannot be accepted temporarily. "
            "Increase the limit with: ulimit -n 65536"
        )

    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._signals_installed:
            return
        for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except NotImplementedError:
                pass
        self._signals_installed = True
