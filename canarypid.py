"""Peer-PID and command-line detection for loopback TCP connections.

This module has no dependency on CanaryServer. It provides standalone
functions to determine the local process behind a loopback connection
by inspecting /proc/net/tcp and /proc/*/fd.
"""

from __future__ import annotations

import os
import socket
from typing import Optional

ENABLE_PEER_PID = True   # can be disabled by setting to False


def peer_process(sock: socket.socket, addr, enabled: bool = True) -> Optional[dict]:
    """Determine the local process behind a loopback connection (blocking)."""
    try:
        local_ip, local_port = sock.getsockname()[:2]
    except OSError:
        local_ip, local_port = "127.0.0.1", 0
    peer_ip, peer_port = addr[0], addr[1]
    return peer_process_from(local_ip, local_port, peer_ip, peer_port, enabled)


def peer_process_from(
    local_ip: str,
    local_port: int,
    peer_ip: str,
    peer_port: int,
    enabled: bool = True,
) -> Optional[dict]:
    """Determine the local process behind a loopback connection (blocking)."""
    if not enabled:
        return None
    if not (peer_ip.startswith("127.") or peer_ip == "::1"):
        return None
    inode = socket_inode(local_ip, local_port, peer_ip, peer_port)
    if not inode:
        return None
    pid = pid_for_inode(inode)
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


def proc_addr(ip: str, port: int) -> str:
    """Format an address as shown in /proc/net/tcp.

    /proc/net/tcp shows the IP in little-endian byte order,
    so 127.0.0.1 -> 0100007F (uppercase).
    """
    try:
        raw = socket.inet_aton(ip)[::-1].hex().upper()
    except (OSError, ValueError):
        return ""
    return f"{raw}:{port:04X}"


def socket_inode(local_ip, local_port, peer_ip, peer_port):
    """Inode of the peer's socket from /proc/net/tcp (IPv4 only).

    The entry searched is the one where the peer is the local side:
    local=client port, remote=our port - not our own side.
    """
    my_hex = proc_addr(local_ip, local_port)
    peer_hex = proc_addr(peer_ip, peer_port)
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


def pid_for_inode(inode: str) -> Optional[int]:
    """Find the process that holds the socket with this inode number."""
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
