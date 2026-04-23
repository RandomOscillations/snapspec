"""
Persistent TCP connection management.

Connections are opened at startup and reused for the lifetime of the experiment.
TCP_NODELAY is set on all sockets for latency-sensitive messages.
"""

import asyncio
import logging
import socket
from dataclasses import dataclass, field
from typing import Any

from ..logging_utils import log_event
from .protocol import encode_message, read_message, MessageType

logger = logging.getLogger(__name__)


@dataclass
class NodeConnection:
    """A persistent connection to a storage node."""
    node_id: int
    host: str
    port: int
    reader: asyncio.StreamReader | None = None
    writer: asyncio.StreamWriter | None = None
    max_reconnect_attempts: int = 2
    reconnect_backoff_base_s: float = 0.1
    reconnect_backoff_max_s: float = 0.5
    state: str = field(default="disconnected", init=False)
    _io_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    async def connect(self, timeout_s: float = 5.0):
        """Establish connection with TCP_NODELAY."""
        await self._cleanup_closed_connection()
        self.state = "reconnecting"
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=timeout_s,
        )
        # Disable Nagle's algorithm
        sock = self.writer.get_extra_info("socket")
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.state = "connected"

    async def send(self, msg_type: MessageType, logical_timestamp: int, **kwargs):
        """Send a message to this node."""
        if self.writer is None:
            raise ConnectionError(
                f"NodeConnection {self.node_id} is not connected"
            )
        data = encode_message(msg_type, logical_timestamp, **kwargs)
        self.writer.write(data)
        await self.writer.drain()

    async def receive(self) -> dict[str, Any] | None:
        """Receive a message from this node."""
        return await read_message(self.reader)

    async def send_and_receive(self, msg_type: MessageType, logical_timestamp: int, **kwargs) -> dict[str, Any] | None:
        """Send a message and wait for the response."""
        async with self._io_lock:
            for attempt in range(2):
                try:
                    if not self._is_connected():
                        reconnected = await self._reconnect()
                        if not reconnected:
                            return None

                    await self.send(msg_type, logical_timestamp, **kwargs)
                    resp = await self.receive()
                    if resp is None:
                        log_event(
                            logger,
                            component=f"conn-{self.node_id}",
                            event="connection_closed",
                            level=logging.WARNING,
                            op=msg_type.value,
                        )
                        await self._mark_disconnected()
                        if attempt == 0 and await self._reconnect():
                            continue
                        return None
                    return resp
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError) as exc:
                    log_event(
                        logger,
                        component=f"conn-{self.node_id}",
                        event="connection_error",
                        level=logging.WARNING,
                        op=msg_type.value,
                        error=exc,
                    )
                    await self._mark_disconnected()
                    if attempt == 0 and await self._reconnect():
                        continue
                    return None

            return None

    async def close(self):
        """Close the connection."""
        await self._mark_disconnected()

    def _is_connected(self) -> bool:
        return self.reader is not None and self.writer is not None and self.state == "connected"

    async def _reconnect(self) -> bool:
        """Reconnect with exponential backoff and retry once at the caller."""
        await self._mark_disconnected()

        for attempt in range(self.max_reconnect_attempts):
            try:
                await self.connect()
                log_event(
                    logger,
                    component=f"conn-{self.node_id}",
                    event="reconnected",
                    node=self.node_id,
                    host=self.host,
                    port=self.port,
                    attempt=attempt + 1,
                )
                return True
            except (ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, OSError, asyncio.TimeoutError) as exc:
                delay = min(
                    self.reconnect_backoff_base_s * (2 ** attempt),
                    self.reconnect_backoff_max_s,
                )
                log_event(
                    logger,
                    component=f"conn-{self.node_id}",
                    event="reconnect_failed",
                    level=logging.WARNING,
                    attempt=attempt + 1,
                    max_attempts=self.max_reconnect_attempts,
                    node=self.node_id,
                    error=exc,
                )
                if attempt == self.max_reconnect_attempts - 1:
                    break
                await asyncio.sleep(delay)

        self.state = "disconnected"
        return False

    async def _mark_disconnected(self):
        self.state = "disconnected"
        await self._cleanup_closed_connection()

    async def _cleanup_closed_connection(self):
        writer = self.writer
        self.writer = None
        self.reader = None
        if writer is not None:
            try:
                writer.close()
            except Exception:
                return
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
            except (Exception, asyncio.TimeoutError):
                pass
