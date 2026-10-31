# AGENTS.md – Instructions for AI Agents

## Project
port-canary is a Python server that listens for TCP and UDP connections (IPv4 and IPv6) and prints the requests colorized to the terminal. Network responses and log files will be added later.

## Tech Stack
- Language: Python 3.12+
- Concurrency: `asyncio` (event loop, coroutines per client – **no threads**)
- Networking: `asyncio.start_server` (TCP), `asyncio.create_datagram_endpoint` (UDP), `StreamReader`/`StreamWriter` for TCP, datagram-based connections for UDP
- Peer-PID detection via `/proc/net/tcp`, `/proc/net/tcp6`, `/proc/net/udp`, `/proc/net/udp6` and `/proc/*/fd` (Linux-specific)
- CLI entry point: `listener.py`

## Supported Protocols
- TCP over IPv4 (`AF_INET`) – always available
- TCP over IPv6 (`AF_INET6`) – via `--ipv6`
- UDP over IPv4 (`AF_INET`, `SOCK_DGRAM`) – via `--protocol udp` or `--protocol both`
- UDP over IPv6 (`AF_INET6`, `SOCK_DGRAM`) – via `--protocol udp` or `--protocol both` combined with `--ipv6`
- `"both"` starts one TCP and one UDP listener per port

## Code Conventions
- **Language**: Code, comments, commit messages, and log output always in **English**
- **Naming**: Classes, methods, variables in English (PascalCase / snake_case)
- **Types**: Use type hints (`list[int]`, `dict | None`, `Optional[...]`)
- **Error handling**: Never kill the event loop. Catch `ConnectionResetError` etc. silently. Log `OSError` with `errno.EMFILE`/`errno.ENOBUFS` as `WARNING`, not as traceback.
- **TDD (Test-First)**: Tests werden **vor** dem Produktionscode geschrieben. Ein Feature gilt erst als erledigt, wenn ein neuer, fehlschlagender Test existiert, der durch den Implementierungscode grün wird, und alle bestehenden Tests weiterhin bestehen.
- **Chicago Style**: Tests arbeiten mit echten Objekten und echten Sockets – keine Mocks. Fokus liegt auf beobachtbarem Verhalten (Daten ankommen, Handler wird aufgerufen, Sockets werden geschlossen, sauberes Shutdown). Test-Dateien beginnen mit einem Docstring, der "Chicago Style" und "No mocks" nennt.

## Key Architecture Decisions
1. **No threads per connection** – previously used `ThreadPoolExecutor`, replaced by `asyncio`. Do not re-introduce thread pools.
2. **`start()` is synchronous** – encapsulates `asyncio.run()` internally. The public API looks synchronous from the outside.
3. **No responses yet** – currently only listener and terminal display. Response handling comes later.
4. **Peer-PID only for loopback** – only for `127.x.x.x` and `::1`, via `/proc`. Not portable to other platforms.
5. **UDP is connectionless** – each handler invocation processes exactly one datagram. There is no persistent stream, no `_read_extra`, and no connection timeout on the first read. The `Connection` protocol is implemented by `DatagramConnection` for UDP.
6. **`--protocol both` opens two sockets per port** – one TCP listener and one UDP listener. This doubles the FD count per port; observe `ulimit -n`.

## Operational Notes
- When listening on many ports simultaneously: check `ulimit -n` (soft limit for open file descriptors). Each listening socket occupies one FD.
- `canaryports.get_ports()` automatically warns if the estimated FD count exceeds `ulimit -n`.
- As a normal user, the soft limit can only be raised up to the hard limit. Raising the hard limit requires root.

## What to Avoid
- No `threading.Thread` for client handlers
- No manual `selectors.DefaultSelector` management (asyncio handles this)
- No German comments or strings in the code
- No `asyncio.run()` outside of `CanaryServer.start()`

## Version Control & Commits
- **Never commit without explicit permission.** Before every `git commit` (all projects, always), show the commit message and the files to be included and ask whether it is OK. Only commit after explicit approval.
- Commit messages in **English**.
