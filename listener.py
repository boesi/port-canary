#!/usr/bin/env python3
import argparse
import sys

import logging
import logging.config
logging.config.fileConfig("logging.ini")

from canaryserver import CanaryServer
from canaryports import get_ports

def getArguments():
    ap = argparse.ArgumentParser(
        description="Netzwerk-Canary: lauscht, zeigt Requests, sendet konfigurierbare Responses."
    )
    ap.add_argument("-H", "--host", default="0.0.0.0",
                    help="Bind-Adresse (0.0.0.0 = im ganzen Netz, 127.0.0.1 = nur lokal)")
    ap.add_argument("-p", "--port", required=True,
                    help="port, port range (1025-32767) or list (8000,8080,9000)")
    ap.add_argument("-c", "--config", help="JSON-Datei mit Response-/Regel-Konfiguration")
    ap.add_argument("-l", "--log-file", help="JSONL-Logdatei (anhängend)")
    ap.add_argument("--status", type=int, help="HTTP-Status der Standardantwort (überschreibt Config)")
    ap.add_argument("--response-body", help="Body der Standardantwort (überschreibt Config)")
    ap.add_argument("--response-header", action="append", default=[],
                    metavar="K:V", help="zusätzlicher Header der Standardantwort")
    ap.add_argument("--workers", type=int, default=128,
                    help="Anzahl paralleler Verbindungs-Handler (Default 128)")
    ap.add_argument("--ipv6", action="store_true", help="zusätzlich auf IPv6 lauschen")
    ap.add_argument("-q", "--quiet", action="store_true", help="nur in Datei loggen")
    ap.add_argument("--no-peer-pid", action="store_true",
                    help="keine Ermittlung des absendenden Prozesses (spart ~30 ms "
                         "pro Verbindung; nur sinnvoll bei sehr viel Verkehr)")
    ap.add_argument("--include-ephemeral", action="store_true",
                    help="Auch den ephemeren Bereich 32768-60999 belegen. ACHTUNG: "
                         "danach kann der Rechner keine ausgehenden TCP-Verbindungen "
                         "mehr aufbauen (kein Internet/DNS). Bei Bereichen sonst automatisch ausgespart.")
    return ap.parse_args()


def main() -> int:
    args = getArguments()
    ports = get_ports(args.port, args.include_ephemeral)

    server = CanaryServer(
        host=args.host,
        ports=ports,
        config=args.config,
        log_file=args.log_file,
        quiet=args.quiet,
        workers=args.workers,
        ipv6=args.ipv6,
        enable_peer_pid=not args.no_peer_pid,
    )
    server.start()

    return 0


if __name__ == "__main__":
    sys.exit(main())
