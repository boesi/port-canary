import logging
import resource

MIN_PORT = 1
MAX_PORT = 65535

# Puffer für FDs, die der Python-Interpreter selbst benötigt
# (stdin/stdout/stderr, Event-Loop, interne Sockets, etc.)
_FD_BUFFER = 100

def get_ephemeral_port_range() -> (int, int):
    with open("/proc/sys/net/ipv4/ip_local_port_range") as f:
        low, high = map(int, f.read().split())
    return low, high

EPHEMERAL_LOW, EPHEMERAL_HIGH = get_ephemeral_port_range()

def get_ports(arg_ports: str, include_ephemeral: bool) -> list[int]:
    """'8080' | '1025-32767' | '8000,8080,9000' -> list of ports."""
    ports: list[int] = []
    for part in arg_ports.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, _, end = part.partition("-")
            lo, hi = int(start), int(end)
            if lo > hi:
                lo, hi = hi, lo
            if lo < MIN_PORT:
                raise ValueError(f"arg_ports: {arg_ports}, only ports between {MIN_PORT} and {MAX_PORT} allowed.")
            if hi > MAX_PORT:
                raise ValueError(f"arg_ports: {arg_ports}, only ports between {MIN_PORT} and {MAX_PORT} allowed.")
            ports.extend(range(lo, hi + 1))
        else:
            port = int(part)
            if port < MIN_PORT or port > MAX_PORT:
                raise ValueError(f"arg_ports: {arg_ports}, only ports between {MIN_PORT} and {MAX_PORT} allowed.")
            ports.append(port)
    # Exclude the ephemeral range: otherwise, the kernel will run out of source ports
    # for outgoing connections. If a single port is explicitly selected, it remains available.
    if len(ports) > 1 and not include_ephemeral:
        skipped = [p for p in ports if EPHEMERAL_LOW <= p <= EPHEMERAL_HIGH]
        if skipped:
            ports = [p for p in ports if not (32768 <= p <= 60999)]
            logging.warning(f"ephemeral range 32768-60999 excluded ({len(skipped)} Ports) - "
                  f"otherwise outgoing connections would break. "
                  f"To use these ports: --include-ephemeral")
    if not ports:
        raise ValueError(f"arg_ports: {arg_ports}, the resulting port list is empty")

    # Warnung, wenn die Anzahl der Ports wahrscheinlich das FD-Limit überschreitet
    try:
        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        needed = len(ports) + _FD_BUFFER
        if needed > soft_limit:
            logging.warning(
                f"Estimated {needed} file descriptors needed for {len(ports)} ports "
                f"+ {_FD_BUFFER} buffer, but ulimit -n (soft limit) is {soft_limit}, "
                f"hard limit is {hard_limit}. "
                f"Consider raising it with: ulimit -n {min(needed + 100, hard_limit)}"
            )
    except (ImportError, OSError, AttributeError):
        # resource-Modul nicht verfügbar (z. B. Windows) oder Limit nicht lesbar
        pass

    return sorted(set(ports))


