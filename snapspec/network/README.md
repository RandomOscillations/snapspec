# Network Layer

Author: Adithya Srinivasan

`snapspec/network/` contains the TCP protocol used by coordinators, nodes, and
workload clients.

## Files

- `protocol.py`: message enum plus length-prefixed JSON encoder/decoder.
- `connection.py`: persistent node connection wrapper with reconnect and timeout
  handling.

## Protocol Shape

Every message is:

1. 4-byte big-endian payload length.
2. JSON payload containing `type`, `logical_timestamp`, and message-specific
   fields.

TCP_NODELAY is used to avoid Nagle delays during snapshot coordination.

Author: Niharika Maruvanahalli Suresh

### Runtime Fault Tolerance

Added network-level operational support for long-running runs and transient failure handling.

- Persistent node connections include retry and exponential backoff so short transport failures do not force a full experiment restart.
- Coordinator RPCs are wrapped with bounded timeouts so an unresponsive node cannot stall snapshot coordination indefinitely.
- Periodic `PING`/`PONG` health checks provide proactive liveness tracking,allowing the system to reason about participant health before starting a snapshot round.
