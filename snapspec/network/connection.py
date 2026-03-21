"""
Persistent TCP connection management.

Connections are opened at startup and reused for the lifetime of the experiment.
TCP_NODELAY is set on all sockets for latency-sensitive messages.
"""

import asyncio
import socket
from dataclasses import dataclass
from typing import Any

from .protocol import encode_message, read_message, MessageType


@dataclass
class NodeConnection:
    """A persistent connection to a storage node."""
    node_id: int
    host: str
    port: int
    reader: asyncio.StreamReader | None = None
    writer: asyncio.StreamWriter | None = None

    async def connect(self):
        """Establish connection with TCP_NODELAY."""
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        # Disable Nagle's algorithm
        sock = self.writer.get_extra_info("socket")
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    async def send(self, msg_type: MessageType, logical_timestamp: int, **kwargs):
        """Send a message to this node."""
        data = encode_message(msg_type, logical_timestamp, **kwargs)
        self.writer.write(data)
        await self.writer.drain()

    async def receive(self) -> dict[str, Any] | None:
        """Receive a message from this node."""
        return await read_message(self.reader)

    async def send_and_receive(self, msg_type: MessageType, logical_timestamp: int, **kwargs) -> dict[str, Any] | None:
        """Send a message and wait for the response."""
        await self.send(msg_type, logical_timestamp, **kwargs)
        return await self.receive()

    async def close(self):
        """Close the connection."""
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.writer = None
            self.reader = None
