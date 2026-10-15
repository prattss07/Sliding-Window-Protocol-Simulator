"""
selective_repeat.py
===================
Selective Repeat (SR) ARQ protocol implementation.

Protocol Rules
--------------
Sender:
    - Maintains a sliding window of size `window_size`.
    - Sends packets continuously while the window is not full.
    - Keeps a per-packet retransmission timer.
    - On timeout for a specific packet: retransmit ONLY that packet.
    - Tracks individual ACK receipt for each packet in the window.
    - Advances the sender base to the next un-ACKed packet when the
      oldest unacknowledged packet is acknowledged.

Receiver:
    - Maintains a sliding window of size `window_size`.
    - Accepts and buffers any packet within its receive window (in-order OR
      out-of-order).
    - Sends a selective ACK for every correctly received (unique) packet.
    - When the packet at the receive base arrives, delivers all consecutively
      buffered packets to the application layer and advances the receive window.
    - Discards packets outside the receive window.
    - Re-ACKs duplicates; does not re-buffer them.

Sequence Number Space
---------------------
SR requires the sequence number space >= 2 * window_size.

Time Model
----------
All time values are abstract simulated time units. No real-time sleeps.
Events at the same simulated time are processed FIFO via a monotonic tiebreaker.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field
from typing import Any, Optional

from network import (
    AckPacket,
    AckState,
    EventType,
    NetworkChannel,
    Packet,
    PacketState,
    SimEvent,
)

_EVT_SEND    = "send"
_EVT_ARRIVE  = "arrive"
_EVT_ACK     = "ack_arrive"
_EVT_TIMEOUT = "timeout"


@dataclass(order=True)
class _PQEvent:
    """
    Priority queue entry.

    Ordered by (time, insert_order) for FIFO tie-breaking.
    """
    time: float
    insert_order: int           # monotonic counter; breaks ties deterministically
    kind: str = field(compare=False)
    seq_num: int = field(compare=False)
    payload: Any = field(compare=False, default=None)


class SelectiveRepeat:
    """
    Selective Repeat ARQ protocol engine.

    Parameters
    ----------
    total_packets : int
        Number of unique data packets the sender must deliver.
    window_size : int
        Sender and receiver window size (W).
    packet_channel : NetworkChannel
        Channel for sender → receiver data transmission.
    ack_channel : NetworkChannel
        Channel for receiver → sender ACK transmission.
    timeout_duration : float
        Per-packet timeout in simulated time units.
    max_retries : int
        Maximum retransmission attempts per packet before failure.
    seq_num_bits : Optional[int]
        Bit-width of sequence numbers. Defaults to ceil(log2(2*W)).
    """

    def __init__(
        self,
        total_packets: int,
        window_size: int,
        packet_channel: NetworkChannel,
        ack_channel: NetworkChannel,
        timeout_duration: float,
        max_retries: int = 20,
        seq_num_bits: Optional[int] = None,
    ) -> None:
        self.total_packets = total_packets
        self.window_size   = window_size
        self.pkt_channel   = packet_channel
        self.ack_channel   = ack_channel
        self.timeout_dur   = timeout_duration
        self.max_retries   = max_retries

        # Sequence number space: must be >= 2*W for SR
        bits = seq_num_bits if seq_num_bits else math.ceil(math.log2(max(2 * window_size, 4)))
        self.seq_space: int = 2 ** bits

        # ---- Sender state ----
        self.snd_base: int = 0   # Oldest unacknowledged packet (absolute index)
        self.snd_next: int = 0   # Next packet to send (absolute index)
        # Per-packet ACK flag within the sender window
        self._snd_acked: dict[int, bool] = {}  # absolute index → bool

        # ---- Receiver state ----
        self.rcv_base: int = 0   # Next expected in-order packet (absolute index)
        # Buffer: absolute index → bool (True = received, awaiting consecutive delivery)
        self._rcv_buffer: dict[int, bool] = {}

        # ---- Packet store ----
        self.packets: dict[int, Packet] = {
            i: Packet(seq_num=i % self.seq_space, data=f"PKT-{i}")
            for i in range(total_packets)
        }

        # Per-packet retry counter
        self._retries: dict[int, int] = {i: 0 for i in range(total_packets)}

        # Set of absolute indices delivered to the application layer
        self._delivered: set[int] = set()

        # Priority queue
        self._pq: list[_PQEvent] = []

        # Monotonic counter for FIFO tiebreaking
        self._pq_counter: int = 0

        # Event counter for unique IDs
        self._event_counter: int = 0

        # Event log
        self.events: list[SimEvent] = []

        # Simulation clock
        self.current_time: float = 0.0

        # Timeout registry: absolute index → scheduled timeout time
        self._timeout_scheduled: dict[int, float] = {}

        # Simulation flags
        self.done: bool   = False
        self.failed: bool = False

        # Seed initial sends
        self._schedule_sends()

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    def step(self) -> Optional[SimEvent]:
        """Process the next scheduled event. Returns the processed SimEvent."""
        if self.done or self.failed or not self._pq:
            return None

        pq_evt = heapq.heappop(self._pq)
        self.current_time = pq_evt.time

        if pq_evt.kind == _EVT_SEND:
            return self._handle_send(pq_evt)
        elif pq_evt.kind == _EVT_ARRIVE:
            return self._handle_arrive(pq_evt)
        elif pq_evt.kind == _EVT_ACK:
            return self._handle_ack(pq_evt)
        elif pq_evt.kind == _EVT_TIMEOUT:
            return self._handle_timeout(pq_evt)
        return None

    def run(self) -> list[SimEvent]:
        """Run the simulation to completion and return all events."""
        while not self.done and not self.failed and self._pq:
            self.step()
        return self.events

    # -----------------------------------------------------------------------
    # Internal event handlers
    # -----------------------------------------------------------------------

    def _handle_send(self, evt: _PQEvent) -> SimEvent:
        """Transmit a packet into the channel."""
        pkt_idx = evt.seq_num
        pkt     = self.packets[pkt_idx]

        pkt.state          = PacketState.IN_TRANSIT
        pkt.send_time      = self.current_time
        pkt.transmit_count += 1

        is_retransmit = pkt.transmit_count > 1
        etype = EventType.RETRANSMISSION if is_retransmit else EventType.PACKET_SENT

        sim_evt = self._log(
            etype,
            pkt_idx,
            f"{'Retransmit' if is_retransmit else 'Send'} PKT[{pkt_idx}] "
            f"seq={pkt.seq_num} (attempt {pkt.transmit_count})",
        )

        # Schedule timeout
        timeout_time = self.current_time + self.timeout_dur
        self._timeout_scheduled[pkt_idx] = timeout_time
        self._push(timeout_time, _EVT_TIMEOUT, pkt_idx)

        # Decide loss
        tx_res = self.pkt_channel.transmit(self.current_time)
        delivered, arrive_time = tx_res
        if delivered:
            self._push(arrive_time, _EVT_ARRIVE, pkt_idx, payload={"corrupted": getattr(tx_res, "corrupted", False)})
        else:
            pkt.state = PacketState.LOST
            self._log(EventType.PACKET_LOST, pkt_idx, f"PKT[{pkt_idx}] seq={pkt.seq_num} lost in transit")

        self._schedule_sends()
        return sim_evt

    def _handle_arrive(self, evt: _PQEvent) -> SimEvent:
        """Packet arrives at the receiver."""
        pkt_idx = evt.seq_num
        pkt     = self.packets[pkt_idx]
        pkt.receive_time = self.current_time

        # Check for bit error / checksum failure
        if evt.payload and evt.payload.get("corrupted"):
            pkt.state = PacketState.CORRUPTED
            return self._log(
                EventType.PACKET_CORRUPTED,
                pkt_idx,
                f"PKT[{pkt_idx}] seq={pkt.seq_num} corrupted (checksum failure — dropped)",
            )

        rcv_upper = self.rcv_base + self.window_size

        if pkt_idx < self.rcv_base or pkt_idx >= rcv_upper:
            # Outside receive window
            pkt.state = PacketState.LOST
            sim_evt   = self._log(
                EventType.PACKET_DISCARDED,
                pkt_idx,
                f"PKT[{pkt_idx}] seq={pkt.seq_num} outside recv window "
                f"[{self.rcv_base}, {rcv_upper}) — discarded",
            )
            # Re-ACK if this was already delivered (stale retransmission)
            if pkt_idx in self._delivered:
                self._send_ack(pkt.seq_num, self.current_time)
            return sim_evt

        if self._rcv_buffer.get(pkt_idx, False) or pkt_idx in self._delivered:
            # Duplicate within window
            sim_evt = self._log(
                EventType.PACKET_DUPLICATE,
                pkt_idx,
                f"PKT[{pkt_idx}] seq={pkt.seq_num} duplicate — re-ACKing",
            )
            self._send_ack(pkt.seq_num, self.current_time)
            return sim_evt

        # Valid new packet — buffer it
        self._rcv_buffer[pkt_idx] = True
        pkt.state = PacketState.BUFFERED

        if pkt_idx == self.rcv_base:
            sim_evt = self._log(
                EventType.PACKET_RECEIVED,
                pkt_idx,
                f"PKT[{pkt_idx}] seq={pkt.seq_num} received in order",
            )
        else:
            sim_evt = self._log(
                EventType.PACKET_BUFFERED,
                pkt_idx,
                f"PKT[{pkt_idx}] seq={pkt.seq_num} buffered out of order",
            )

        # Send selective ACK
        self._send_ack(pkt.seq_num, self.current_time)

        # Deliver consecutive buffered packets starting at rcv_base
        self._deliver_buffered()

        return sim_evt

    def _handle_ack(self, evt: _PQEvent) -> SimEvent:
        """
        Selective ACK arrives at the sender.

        Marks the specific packet as ACKed and advances the sender base
        over any consecutively ACKed packets.
        """
        ack_seq = evt.seq_num   # sequence number that was selectively ACKed

        sim_evt = self._log(
            EventType.ACK_RECEIVED,
            ack_seq,
            f"ACK received for seq={ack_seq}",
        )

        # Locate the absolute index of this ACK within the sender window
        for idx in range(self.snd_base, self.snd_next):
            if self.packets[idx].seq_num == ack_seq:
                if not self._snd_acked.get(idx, False):
                    self._snd_acked[idx] = True
                    self.packets[idx].state = PacketState.ACKNOWLEDGED
                    # Cancel any pending timeout for this packet
                    self._timeout_scheduled.pop(idx, None)
                break

        # Advance sender base over consecutively ACKed packets
        advanced_from = self.snd_base
        while self._snd_acked.get(self.snd_base, False):
            self.snd_base += 1

        if self.snd_base > advanced_from:
            self._log(
                EventType.WINDOW_ADVANCED,
                self.snd_base,
                f"Sender window advanced from {advanced_from} to base={self.snd_base}",
            )
            self._schedule_sends()

        self._check_done()
        return sim_evt

    def _handle_timeout(self, evt: _PQEvent) -> SimEvent:
        """Per-packet timeout: retransmit only this packet (SR rule)."""
        pkt_idx = evt.seq_num

        # Ignore stale timeout if already ACKed
        if self._snd_acked.get(pkt_idx, False):
            return self._log(
                EventType.TIMEOUT,
                pkt_idx,
                f"Stale timeout ignored for PKT[{pkt_idx}] (already ACKed)",
            )

        if pkt_idx < self.snd_base or pkt_idx >= self.snd_next:
            return self._log(
                EventType.TIMEOUT,
                pkt_idx,
                f"Stale timeout ignored for PKT[{pkt_idx}] (outside window)",
            )

        scheduled = self._timeout_scheduled.get(pkt_idx)
        if scheduled != evt.time:
            return self._log(
                EventType.TIMEOUT,
                pkt_idx,
                f"Stale timeout ignored for PKT[{pkt_idx}] (rescheduled)",
            )

        self._retries[pkt_idx] += 1
        if self._retries[pkt_idx] > self.max_retries:
            self.failed = True
            return self._log(
                EventType.SIMULATION_FAILED,
                pkt_idx,
                f"Max retries ({self.max_retries}) exceeded for PKT[{pkt_idx}]",
            )

        sim_evt = self._log(
            EventType.TIMEOUT,
            pkt_idx,
            f"Timeout for PKT[{pkt_idx}] seq={self.packets[pkt_idx].seq_num} "
            f"— retransmitting only this packet (SR)",
        )

        # SR: retransmit only the timed-out packet
        self._push(self.current_time, _EVT_SEND, pkt_idx)
        return sim_evt

    # -----------------------------------------------------------------------
    # Receiver helper
    # -----------------------------------------------------------------------

    def _deliver_buffered(self) -> None:
        """
        Deliver all consecutively buffered packets starting at rcv_base.
        Advance the receive window accordingly.
        """
        while self._rcv_buffer.get(self.rcv_base, False):
            pkt = self.packets[self.rcv_base]
            pkt.state = PacketState.DELIVERED
            self._delivered.add(self.rcv_base)
            self._rcv_buffer.pop(self.rcv_base)
            self._log(
                EventType.PACKET_DELIVERED,
                self.rcv_base,
                f"PKT[{self.rcv_base}] seq={pkt.seq_num} delivered to application layer",
            )
            self.rcv_base += 1

    # -----------------------------------------------------------------------
    # ACK helper
    # -----------------------------------------------------------------------

    def _send_ack(self, ack_seq: int, send_time: float) -> None:
        """Generate a selective ACK at the receiver."""
        ack = AckPacket(ack_num=ack_seq, send_time=send_time, is_cumulative=False)
        self._log(EventType.ACK_SENT, ack_seq, f"Selective ACK sent for seq={ack_seq}")

        delivered, arrive_time = self.ack_channel.transmit(send_time)
        if delivered:
            ack.state = AckState.IN_TRANSIT
            self._push(arrive_time, _EVT_ACK, ack_seq)
        else:
            ack.state = AckState.LOST
            self._log(EventType.ACK_LOST, ack_seq, f"ACK for seq={ack_seq} lost")

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _push(self, time: float, kind: str, seq_num: int, payload: Any = None) -> None:
        """Push an event onto the PQ with a monotonic FIFO tiebreaker."""
        heapq.heappush(self._pq, _PQEvent(time, self._pq_counter, kind, seq_num, payload))
        self._pq_counter += 1

    def _schedule_sends(self) -> None:
        """Schedule SEND events for packets in the sender window not yet sent."""
        while (
            self.snd_next < self.total_packets
            and self.snd_next < self.snd_base + self.window_size
        ):
            self._push(self.current_time, _EVT_SEND, self.snd_next)
            self.snd_next += 1

    def _check_done(self) -> None:
        """Mark simulation done when all packets are acknowledged."""
        if self.snd_base >= self.total_packets:
            self.done = True
            self._log(
                EventType.SIMULATION_DONE,
                None,
                f"All {self.total_packets} packets delivered successfully (SR)",
            )

    def _log(self, etype: EventType, seq: Optional[int], desc: str, **extra) -> SimEvent:
        """Record and return a simulation event."""
        evt = SimEvent(
            event_id    = self._event_counter,
            time        = self.current_time,
            event_type  = etype,
            seq_num     = seq,
            description = desc,
            extra       = extra,
        )
        self._event_counter += 1
        self.events.append(evt)
        return evt

    # -----------------------------------------------------------------------
    # State export
    # -----------------------------------------------------------------------

    def get_state(self) -> dict:
        """
        Return a snapshot of the current SR simulation state.

        Keys
        ----
        protocol          : "SR"
        current_time      : float
        sender_base       : int   — oldest unacknowledged packet (absolute index)
        sender_next_seq   : int   — next packet to send (absolute index)
        sender_window     : list[int]
        sender_acked      : dict[int, bool] — per-packet ACK flags
        receiver_base     : int   — next expected in-order packet (absolute index)
        receiver_window   : list[int]
        receiver_buffer   : list[int] — buffered out-of-order packet abs indices
        delivered         : list[int]
        packet_states     : dict[int, str]
        done              : bool
        failed            : bool
        total_packets     : int
        window_size       : int
        seq_space         : int
        """
        return {
            "protocol":         "SR",
            "current_time":     self.current_time,
            "sender_base":      self.snd_base,
            "sender_next_seq":  self.snd_next,
            "sender_window":    list(range(self.snd_base, self.snd_next)),
            "sender_acked":     dict(self._snd_acked),
            "receiver_base":    self.rcv_base,
            "receiver_window":  list(range(self.rcv_base, self.rcv_base + self.window_size)),
            "receiver_buffer":  [i for i, v in self._rcv_buffer.items() if v],
            "delivered":        sorted(self._delivered),
            "packet_states":    {i: p.state.name for i, p in self.packets.items()},
            "done":             self.done,
            "failed":           self.failed,
            "total_packets":    self.total_packets,
            "window_size":      self.window_size,
            "seq_space":        self.seq_space,
        }
