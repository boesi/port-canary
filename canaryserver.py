import socket
import time
import signal
import threading
import selectors
import logging
from datetime import datetime, timezone
import os

from concurrent.futures import ThreadPoolExecutor, Future

ENABLE_PEER_PID = True   # kann per --no-peer-pid abgeschaltet werden

class CanaryServer:
    def __init__(self, host: str, ports: list[int], config: dict | None = None,
                 log_file: str | None = None, quiet: bool = False,
                 workers: int = 128, ipv6: bool = False):
        self.host = host
        self.config = config
        self.log_file = log_file
        self.quiet = quiet
        self.state = {"count": 0, "by_path": {}, "by_client": {}, "lock": threading.Lock()}
        self.selector = selectors.DefaultSelector()
        self.socks: list[socket.socket] = []
        self.failed: list[tuple[int, OSError]] = []
        # thread heandling
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="canary")
        self.running = True
        self._futures: list[Future] = []

        for port in ports:
            self._bind(host, port, ipv6)

    def _bind(self, host: str, port: int, ipv6: bool) -> None:
        families = [(socket.AF_INET, socket.SOCK_STREAM, host)]
        if ipv6:
            families.append((socket.AF_INET6, socket.SOCK_STREAM, "::"))
        for family, stype, addr in families:
            try:
                s = socket.socket(family, stype)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if family == socket.AF_INET6:
                    s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                s.bind((addr, port))
                s.listen(128)
                s.setblocking(False)
                self.selector.register(s, selectors.EVENT_READ, port)
                self.socks.append(s)
            except OSError as exc:
                self.failed.append((port, exc))
                try:
                    s.close()
                except OSError:
                    pass

    def _on_term(self, signum, frame):
        self.stop_event.set()

    def start(self):
        worker = threading.Thread(target=self._serve_forever, daemon=True)
        worker.start()

        self.stop_event = threading.Event()

        signal.signal(signal.SIGTERM, self._on_term)
        signal.signal(signal.SIGHUP, self._on_term)

        try:
            while worker.is_alive() and not self.stop_event.is_set():
                time.sleep(0.3)
        except KeyboardInterrupt:
            print("\n\033[1;33mBeende…\033[0m")
        finally:
            self.stop()
            print(server.summary())

    def stop(self) -> None:
        self.running = False
        for s in self.socks:
            try:
                self.selector.unregister(s)
            except (KeyError, ValueError, OSError):
                pass
            try:
                s.close()
            except OSError:
                pass
        self.pool.shutdown(wait=False, cancel_futures=True)

    def _serve_forever(self) -> None:
        while self.running:
            try:
                events = self.selector.select(timeout=0.5)
            except OSError:
                continue
            if not events:
                print(".", end='', flush=True)
            for key, _ in events:
                try:
                    conn, addr = key.fileobj.accept()
                except OSError:
                    continue
                port = key.data
                future = self.pool.submit(self._process, conn, addr, port)
                self._futures.append(future)
            self._reap_futures()

    def _reap_futures(self) -> None:
        remaining: list[Future] = []
        for future in self._futures:
            if future.done():
                try:
                    future.result()
                except Exception as exc:
                    logging.exception("Wprker exception")
            else:
                remaining.append(future)
        self._futures = remaining

    def _process(self, conn: socket.socket, addr, port: int) -> None:
        peer = f"{addr[0]}:{addr[1]}"
        # Wer sendet? (nur bei Loopback sinnvoll)
        try:
            local_ip, local_port = conn.getsockname()[:2]
        except OSError:
            local_ip, local_port = "127.0.0.1", port
        proc = self._peer_process(local_ip, local_port, addr[0], addr[1])
        try:
            conn.settimeout(5.0)
            try:
                first = conn.recv(8192)
            except (socket.timeout, TimeoutError):
                first = b""
            except OSError:
                first = b""

            if not first:
                self.record({"proto": "EMPTY", "client": peer, "port": port,
                             "process": proc,
                             "note": "Verbindung ohne Daten (geschlossen)"})
                return

            self._handle(conn, peer, port, first, proc)
        except Exception as exc:  # nie den Listener killen
            try:
                self.record({"proto": "ERROR", "client": peer, "port": port,
                             "process": proc, "note": repr(exc)})
            except Exception:
                pass
        finally:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()

    def _peer_process(self, local_ip: str, local_port: int,
                     peer_ip: str, peer_port: int) -> dict | None:
        """Ermittelt den lokalen Prozess hinter einer Loopback-Verbindung.

        Nur moeglich, wenn die Verbindung noch steht - bei localhost klappt das
        praktisch immer, weil der Absender auf die Antwort wartet.
        """
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
                cmd = fh.read().replace(b"\0", b" ").decode("utf-8", "replace").strip()
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
        """Formatiert eine Adresse so, wie sie in /proc/net/tcp steht.

        Achtung: /proc/net/tcp zeigt die IP in Little-Endian-Byte-Reihenfolge,
        also 127.0.0.1 -> 0100007F (und Grossbuchstaben).
        """
        try:
            raw = socket.inet_aton(ip)[::-1].hex().upper()
        except (OSError, ValueError):
            return ""
        return f"{raw}:{port:04X}"


    def _socket_inode(self, local_ip: str, local_port: int,
                      peer_ip: str, peer_port: int) -> str | None:
        """Inode des Sockets der GEGENSEITE, aus /proc/net/tcp (nur IPv4).

        Wichtig: Gesucht ist der Eintrag, bei dem die Gegenstelle der Client ist,
        also local=Client-Port und remote=mein Port - nicht die eigene Seite.
        """
        my_hex = self._proc_addr(local_ip, local_port)
        peer_hex = self._proc_addr(peer_ip, peer_port)
        if not my_hex or not peer_hex:
            return None
        try:
            with open("/proc/net/tcp") as fh:
                next(fh)
                for line in fh:
                    p = line.split()
                    if len(p) < 10 or p[3] != "01":   # 01 = ESTABLISHED
                        continue
                    # p[1] = local, p[2] = remote
                    if p[1] == peer_hex and p[2] == my_hex:
                        return p[9]
        except (OSError, ValueError, IndexError):
            pass
        return None


    def _pid_for_inode(self, inode: str) -> int | None:
        """Sucht den Prozess, der den Socket mit dieser Inode-Nummer haelt."""
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

    def _handle(self, conn: socket.socket, peer: str, port: int, first: bytes,
                    proc: dict | None = None) -> None:
        payload = first
        try:
            conn.settimeout(1.5)
            extra = conn.recv(4096)
            if extra:
                payload += extra
        except (socket.timeout, TimeoutError, OSError):
            pass

        printable = "".join(chr(b) if 32 <= b < 127 or b in (9, 10, 13) else "."
                            for b in payload)
        entry = {
            "proto": "RAW",
            "client": peer,
            "port": port,
            "process": proc,
            "bytes": len(payload),
            "payload_text": printable[:2000],
            "payload_hex": payload[:256].hex(" "),
            "ts": datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")
        }
        self._print(entry)

    def _print(self, entry: dict) -> None:
        proto = entry.get("proto")
        client = entry.get("client", "-")
        port = entry.get("port", "?")
        proc = entry.get("process")
        who = ""
        head = f"\n{entry.get("ts")}"
        if proto == "HTTP":
            head += (f"\n\033[1;33m▶ {entry.get('method','?')} {entry.get('path','?')}"
                    f"\033[0m \033[2mHTTP/{entry.get('version','?')} von {client} "
                    f"[Port {port}]\033[0m")
        elif proto == "RAW":
            head += (f"\n\033[1;35m◆ RAW {entry.get('bytes',0)} Bytes\033[0m "
                    f"\033[2mvon {client} [Port {port}]\033[0m")
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

