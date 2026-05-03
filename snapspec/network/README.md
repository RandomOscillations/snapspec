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
