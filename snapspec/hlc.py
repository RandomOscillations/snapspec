"""
Hybrid Logical Clock (HLC).

Combines physical wall-clock time with a logical counter to provide
causally ordered timestamps that stay close to real time. Based on
Kulkarni & Demirbas 2014 ("Logical Physical Clocks and Consistent
Snapshots in Globally Distributed Databases").

Used by CockroachDB, MongoDB, YugabyteDB.

Packing: a single 64-bit integer = (physical_ms << 16) | counter.
  - 48 bits for milliseconds since epoch (~8900 years)
  - 16 bits for logical counter (65535 events per ms before overflow)

This is backward compatible with the existing logical_timestamp field
in the message protocol — it's still one integer.
"""

from __future__ import annotations

import time

# Bit layout
_COUNTER_BITS = 16
_COUNTER_MASK = (1 << _COUNTER_BITS) - 1  # 0xFFFF


def pack(physical_ms: int, counter: int) -> int:
    """Pack physical time + counter into a single 64-bit int."""
    return (physical_ms << _COUNTER_BITS) | (counter & _COUNTER_MASK)


def unpack(packed: int) -> tuple[int, int]:
    """Unpack a 64-bit HLC timestamp into (physical_ms, counter)."""
    return packed >> _COUNTER_BITS, packed & _COUNTER_MASK


def _now_ms() -> int:
    """Current wall-clock time in milliseconds."""
    return int(time.time() * 1000)


class HybridLogicalClock:
    """Per-process HLC state.

    Usage:
        hlc = HybridLogicalClock()

        # Local event or message send:
        ts = hlc.tick()

        # Message receive:
        ts = hlc.receive(remote_timestamp)

        # Unpack for display:
        physical_ms, counter = hlc.unpack(ts)
    """

    def __init__(self):
        self._l: int = _now_ms()  # physical component
        self._c: int = 0          # logical counter

    def tick(self) -> int:
        """Local event or message send. Returns packed HLC timestamp."""
        now = _now_ms()
        if now > self._l:
            self._l = now
            self._c = 0
        else:
            self._c += 1
        return pack(self._l, self._c)

    def receive(self, remote_packed: int) -> int:
        """Merge with a received HLC timestamp. Returns packed HLC timestamp."""
        remote_l, remote_c = unpack(remote_packed)
        now = _now_ms()

        old_l = self._l

        if now > old_l and now > remote_l:
            # Physical clock is ahead — reset counter
            self._l = now
            self._c = 0
        elif old_l == remote_l:
            # Same physical time — advance counter past both
            self._c = max(self._c, remote_c) + 1
        elif old_l > remote_l:
            # Our clock is ahead — just increment our counter
            self._c += 1
        else:
            # Remote clock is ahead — adopt it, advance counter
            self._l = remote_l
            self._c = remote_c + 1

        return pack(self._l, self._c)

    def now(self) -> int:
        """Return current HLC value without advancing (read-only)."""
        return pack(self._l, self._c)

    @staticmethod
    def unpack(packed: int) -> tuple[int, int]:
        """Convenience: unpack a timestamp."""
        return unpack(packed)

    @staticmethod
    def pack(physical_ms: int, counter: int) -> int:
        """Convenience: pack a timestamp."""
        return pack(physical_ms, counter)
