"""CanaryServer (asyncio variant) – modern concurrency handling.

Instead of one thread per connection, this uses a single asyncio event loop
and one coroutine per client. This eliminates the classic threading issues:

* no manual thread pool / no future lists anymore
* no race condition on shutdown (event is created in __init__)
* real graceful shutdown via signals (SIGTERM, SIGHUP, SIGINT)
* blocking /proc access runs via asyncio.to_thread outside the loop

The public API remains synchronous; asyncio is encapsulated as an
implementation detail inside start().
"""

from __future__ import annotations

import asyncio
import errno
import logging
import os
import signal
import socket
import time
from datetime import datetime, timezone
from typing import Optional

ENABLE_PEER_PID = True   # can be disabled with --no-peer-pid


class CanaryServer:
    def __init__(
        self,
        host: str,
        ports: list[int],
        config: dict | None = None,
        log_file: str | None = None,
        quiet: bool = False,
        workers: int = 128,          # not used with asyncio (API compatibility)
        ipv6: bool = False,
    ):
        self.host = host
        self.ports = ports
        self.config = config or {}
        self.log_file = log_file
        self.quiet = quiet
        self.ipv6 = ipv6

        self._servers: list[asyncio.AbstractServer] = []
        self._stop = asyncio.Event()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._count = 0
        self._by_path: dict = {}
        self._by_client: dict = {}
        self._print_lock = asyncio.Lock()   # serializes terminal output

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        """Starts the server and blocks until it is stopped.

        asyncio is encapsulated internally; the caller does not need to
        manage the event loop themselves.
        """
        try:
            asyncio.run(self._run())
        except KeyboardInterrupt:
            print("\n\033[1;33mShutting down…\033[0m")

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        self._loop = loop

        # Timestamp of the last EMFILE/ENOBUFS warning, to avoid endless spam
        _last_resource_warning: float = 0.0

        # Custom exception handler for the event loop, so that
        # "Too many open files" (EMFILE) is not flooded as an ugly traceback
        # to the terminal, but logged as a single WARNING.
        def _loop_exception_handler(loop, context):
            nonlocal _last_resource_warning
            exception = context.get("exception")
            if isinstance(exception, OSError) and exception.errno in (
                errno.EMFILE,   # Too many open files
                errno.ENOBUFS,  # No buffer space available
            ):
                now = time.monotonic()
                if now - _last_resource_warning < 5.0:
                    return
                _last_resource_warning = now
                logging.warning(
                    "System limit reached: too many open files (EMFILE). "
                    "New connections cannot be accepted temporarily. "
                    "Increase the limit with: ulimit -n 65536"
                )
                return
            # Fallback: asyncio default handler for all other exceptions
            loop.default_exception_handler(context)

        loop.set_exception_handler(_loop_exception_handler)

        for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except NotImplementedError:
                # e.g. Windows: add_signal_handler not supported
                pass

        await self._bind_all()

        print(f"\033[1;32mCanaryServer listening on {self.host}:{self.ports}\033[0m", flush=True)

        try:
            await self._stop.wait()
        finally:
            await self.stop()
            self._print_summary()

    async def stop(self) -> None:
        for srv in self._servers:
            srv.close()
        for srv in self._servers:
            await srv.wait_closed()
        self._servers.clear()

    # ------------------------------------------------------------------ #
    # Network
    # ------------------------------------------------------------------ #
    async def _bind_all(self) -> None:
        for port in self.ports:
            await self._bind_port(port)

    async def _bind_port(self, port: int) -> None:
        families = [socket.AF_INET]
        if self.ipv6:
            families.append(socket.AF_INET6)

        for family in families:
            bind_host = "::" if family == socket.AF_INET6 else self.host
            try:
                srv = await asyncio.start_server(
                    self._client_handler,
                    host=bind_host,
                    port=port,
                    family=family,
                    reuse_address=True,
                )
                self._servers.append(srv)
                actual = srv.sockets[0].getsockname()[1]
                logging.info("Listening on %s:%s (AF=%s)", bind_host, actual, family)
            except OSError as exc:
                logging.warning("Port %s (AF=%s) not bindable: %s", port, family, exc)

    # ------------------------------------------------------------------ #
    # per connection (coroutine, no thread!)
    # ------------------------------------------------------------------ #
    async def _client_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        peer = f"{addr[0]}:{addr[1]}" if addr else "?"
        port = writer.get_extra_info("sockname")[1] if addr else 0
        sock = writer.get_extra_info("socket")

        proc: Optional[dict] = None
        if ENABLE_PEER_PID and addr and (
            addr[0].startswith("127.") or addr[0] == "::1"
        ):
            proc = await asyncio.to_thread(self._peer_process, sock, addr)

        try:
            try:
                first = await asyncio.wait_for(reader.read(8192), timeout=5.0)
            except (asyncio.TimeoutError, TimeoutError):
                first = b""

            if not first:
                await self._record(
                    {
                        "proto": "EMPTY",
                        "client": peer,
                        "port": port,
                        "process": proc,
                        "note": "Connection without data (closed)",
                    }
                )
                return

            await self._handle(reader, writer, peer, port, first, proc)

        except asyncio.CancelledError:
            raise
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            # Client terminated the connection early – normal operation
            pass
        except OSError as exc:
            if exc.errno in (errno.EMFILE, errno.ENOBUFS):
                logging.warning(
                    "Resource limit reached for client %s: %s", peer, exc.strerror
                )
            else:
                logging.exception("Client handler error for %s: %s", peer, exc)
        except Exception as exc:  # never kill the event loop
            logging.exception("Client handler error for %s: %s", peer, exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except OSError:
                pass

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer: str,
        port: int,
        first: bytes,
        proc: Optional[dict] = None,
    ) -> None:
        payload = first
        try:
            extra = await asyncio.wait_for(reader.read(4096), timeout=1.5)
            if extra:
                payload += extra
        except (asyncio.TimeoutError, TimeoutError):
            pass
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            # Client closed the connection while reading – normal
            pass

        entry = {
            "proto": "RAW",
            "client": peer,
            "port": port,
            "process": proc,
            "bytes": len(payload),
            "payload_text": self._printable(payload)[:2000],
            "payload_hex": payload[:256].hex(" "),
            "ts": datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds"),
        }
        await self._print(entry)

    # ------------------------------------------------------------------ #
    # peer PID (blocking -> separate thread via to_thread)
    # ------------------------------------------------------------------ #
    def _peer_process(self, sock: socket.socket, addr) -> Optional[dict]:
        """Determines the local process behind a loopback connection (blocking)."""
        try:
            local_ip, local_port = sock.getsockname()[:2]
        except OSError:
            local_ip, local_port = "127.0.0.1", 0
        peer_ip, peer_port = addr[0], addr[1]
        return self._peer_process_from(local_ip, local_port, peer_ip, peer_port)

    def _peer_process_from(self, local_ip, local_port, peer_ip, peer_port):
        if not ENABLE_PEER_PID:
            return None
        if not (peer_ip.startswith("127.") or peer_ip == "::1"):
            return None
        inode = self._socket_inode(local_ip, local_port, peer_ip, peer_port)
        if not inode:
            return None
        pid = self._pid_for_inode(inode)
        if pid is None:
            return None
        cmd = ""
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                cmd = (
                    fh.read().replace(b"\0", b" ").decode("utf-8", "replace").strip()
                )
        except OSError:
            pass
        if not cmd:
            try:
                with open(f"/proc/{pid}/comm") as fh:
                    cmd = fh.read().strip()
            except OSError:
                cmd = "?"
        return {"pid": pid, "cmd": cmd[:200]}

    def _proc_addr(self, ip: str, port: int) -> str:
        try:
            raw = socket.inet_aton(ip)[::-1].hex().upper()
        except (OSError, ValueError):
            return ""
        return f"{raw}:{port:04X}"

    def _socket_inode(self, local_ip, local_port, peer_ip, peer_port):
        my_hex = self._proc_addr(local_ip, local_port)
        peer_hex = self._proc_addr(peer_ip, peer_port)
        if not my_hex or not peer_hex:
            return None
        try:
            with open("/proc/net/tcp") as fh:
                next(fh)
                for line in fh:
                    p = line.split()
                    if len(p) < 10 or p[3] != "01":  # 01 = ESTABLISHED
                        continue
                    # p[1] = local, p[2] = remote
                    if p[1] == peer_hex and p[2] == my_hex:
                        return p[9]
        except (OSError, ValueError, IndexError):
            pass
        return None

    def _pid_for_inode(self, inode: str) -> Optional[int]:
        target = f"socket:[{inode}]"
        try:
            pids = [p for p in os.listdir("/proc") if p.isdigit()]
        except OSError:
            return None
        for pid in pids:
            fddir = f"/proc/{pid}/fd"
            try:
                fds = os.listdir(fddir)
            except OSError:
                continue
            for fd in fds:
                try:
                    if os.readlink(f"{fddir}/{fd}") == target:
                        return int(pid)
                except OSError:
                    continue
        return None

    # ------------------------------------------------------------------ #
    # Output
    # ------------------------------------------------------------------ #
    @staticmethod
    def _printable(payload: bytes) -> str:
        return "".join(
            chr(b) if 32 <= b < 127 or b in (9, 10, 13) else "." for b in payload
        )

    async def _record(self, entry: dict) -> None:
        async with self._print_lock:
            self._count += 1
            await self._print(entry)

    async def _print(self, entry: dict) -> None:
        proto = entry.get("proto")
        client = entry.get("client", "-")
        port = entry.get("port", "?")
        proc = entry.get("process")
        head = f"\n{entry.get('ts')}"
        if proto == "HTTP":
            head += (
                f"\n\033[1;33m▶ {entry.get('method','?')} {entry.get('path','?')}"
                f"\033[0m \033[2mHTTP/{entry.get('version','?')} von {client} "
                f"[Port {port}]\033[0m"
            )
        elif proto == "RAW":
            head += (
                f"\n\033[1;35m◆ RAW {entry.get('bytes',0)} Bytes\033[0m "
                f"\033[2mvon {client} [Port {port}]\033[0m"
            )
        else:
            head += f"\n\033[2m· {proto} von {client} [Port {port}]\033[0m"
        if proc:
            name = proc.get("cmd", "?")
            if len(name) > 120:
                name = name[:120] + "…"
            head += f"\n\033[1;32m<PID {proc.get('pid')} {name}>\033[0m"

        lines = [head]
        for k, v in entry.get("headers", []) or []:
            lines.append(f"    \033[2m{k}: {v}\033[0m")
        body = entry.get("body")
        if body:
            preview = body if len(body) <= 2000 else body[:2000] + " …(gekürzt)"
            lines.append("    \033[2m--- Body ---\033[0m")
            lines.extend("    " + ln for ln in preview.splitlines() or [""])
        if entry.get("payload_text"):
            lines.append("    \033[2m--- Payload ---\033[0m")
            lines.append("    " + entry["payload_text"].replace("\n", "\n    "))
        print("\n".join(lines), flush=True)

    def _print_summary(self) -> None:
        print(f"\n\033[1;36mTotal {self._count} connections processed.\033[0m", flush=True)
