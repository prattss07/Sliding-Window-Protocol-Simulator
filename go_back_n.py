"""
go_back_n.py
============
Go-Back-N (GBN) ARQ protocol implementation.

Protocol Rules
--------------
Sender:
    - Maintains a sliding window of size `window_size`.
    - Sends packets continuously as long as the window is not full.
    - Keeps per-packet timeout timers.
    - On timeout for packet S: retransmit S, S+1, ..., next_seq-1 (all
      outstanding packets from the timed-out one onward).
    - Advances the window base when a cumulative ACK (ack_num) arrives,
      sliding the window forward to ack_num + 1.

Receiver:
    - Maintains a single expected sequence number (receiver window of size 1).
    - Accepts only the in-order packet. Sends ACK(seq) for each accepted packet.
    - Discards any out-of-order packet and resends ACK(last_good_seq) to nudge
      the sender.

Sequence Number Space
---------------------
With window size W the protocol requires at least 2W sequence numbers.
The sequence number space is set to 2^ceil(log2(2*W+1)).

Time Model
----------
All time values are abstract simulated time units. No real-time sleep calls.
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


# ---------------------------------------------------------------------------
# Internal event types
# ---------------------------------------------------------------------------

_EVT_SEND    = "send"
_EVT_ARRIVE  = "arrive"
_EVT_ACK     = "ack_arrive"
_EVT_TIMEOUT = "timeout"


@dataclass(order=True)
class _PQEvent:
    """
    Priority queue entry.

    Ordered by (time, insert_order) so that events at the same simulated
    time are processed FIFO — ensuring packets sent in the same tick
    arrive and are processed in transmission order.
    """
    time: float
    insert_order: int           # monotonic counter; breaks ties deterministically
    kind: str = field(compare=False)
    seq_num: int = field(compare=False)
    payload: Any = field(compare=False, default=None)


class GoBackN:
    """
    Go-Back-N ARQ protocol engine.

    Parameters
    ----------
    total_packets : int
        Number of unique data packets the sender must deliver.
    window_size : int
        Sender window size (W). Receiver window is always 1.
    packet_channel : NetworkChannel
        Channel used for sender → receiver data transmission.
    ack_channel : NetworkChannel
        Channel used for receiver → sender ACK transmission.
    timeout_duration : float
        Per-packet timeout in simulated time units.
    max_retries : int
        Maximum retransmission attempts per packet before failure.
    seq_num_bits : Optional[int]
        Bit-width of sequence numbers. Defaults to ceil(log2(2*W+1)).
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
        self.total_packets   = total_packets
        self.window_size     = window_size
        self.pkt_channel     = packet_channel
        self.ack_channel     = ack_channel
        self.timeout_dur     = timeout_duration
        self.max_retries     = max_retries

        # Sequence number space: must be >= 2*W for GBN
        bits = seq_num_bits if seq_num_bits else math.ceil(math.log2(max(2 * window_size + 1, 4)))
        self.seq_space: int = 2 ** bits

        # ---- Sender state ----
        self.base: int      = 0   # Oldest unacknowledged packet (absolute index)
        self.next_seq: int  = 0   # Next packet to send (absolute index)

        # ---- Receiver state ----
        self.rcv_expected: int = 0   # Absolute index of the next expected packet
        self.last_acked: int   = -1  # Absolute index of last successfully received packet

        # ---- Packet store ----
        # absolute index → Packet; seq_num = absolute_index % seq_space
        self.packets: dict[int, Packet] = {
            i: Packet(seq_num=i % self.seq_space, data=f"PKT-{i}")
            for i in range(total_packets)
        }

        # Per-packet retry counter
        self._retries: dict[int, int] = {i: 0 for i in range(total_packets)}

        # Priority queue
        self._pq: list[_PQEvent] = []

        # Monotonic counter for FIFO tiebreaking in the PQ
        self._pq_counter: int = 0

        # Event counter for unique IDs
        self._event_counter: int = 0

        # Event log
        self.events: list[SimEvent] = []

        # Simulation clock
        self.current_time: float = 0.0

        # Simulation state flags
        self.done: bool   = False
        self.failed: bool = False

        # Timeout registry: absolute index → scheduled timeout time
        # Used to discard stale timeouts after window advance or retransmission.
        self._timeout_scheduled: dict[int, float] = {}

        # Seed the initial send events
        self._schedule_sends()

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    def step(self) -> Optional[SimEvent]:
        """
        Process the next scheduled event.

        Returns the SimEvent processed, or None if the simulation is complete.
        """
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
        """Transmit a packet from the sender into the channel."""
        pkt_idx = evt.seq_num   # absolute index
        pkt     = self.packets[pkt_idx]

        pkt.state         = PacketState.IN_TRANSIT
        pkt.send_time     = self.current_time
        pkt.transmit_count += 1

        is_retransmit = pkt.transmit_count > 1
        etype = EventType.RETRANSMISSION if is_retransmit else EventType.PACKET_SENT

        sim_evt = self._log(
            etype,
            pkt_idx,
            f"{'Retransmit' if is_retransmit else 'Send'} PKT[{pkt_idx}] "
            f"seq={pkt.seq_num} (attempt {pkt.transmit_count})",
        )

        # Schedule a timeout for this packet
        timeout_time = self.current_time + self.timeout_dur
        self._timeout_scheduled[pkt_idx] = timeout_time
        self._push(timeout_time, _EVT_TIMEOUT, pkt_idx)

        # Decide if packet reaches receiver
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
            sim_evt = self._log(
                EventType.PACKET_CORRUPTED,
                pkt_idx,
                f"PKT[{pkt_idx}] seq={pkt.seq_num} corrupted (checksum failure)",
            )
            if self.last_acked >= 0:
                self._send_ack(self.packets[self.last_acked].seq_num, self.current_time)
            return sim_evt

        expected_abs = self.rcv_expected   # absolute index expected

        if pkt_idx == expected_abs:
            # In-order delivery
            pkt.state = PacketState.DELIVERED
            sim_evt   = self._log(
                EventType.PACKET_DELIVERED,
                pkt_idx,
                f"PKT[{pkt_idx}] seq={pkt.seq_num} delivered in order",
            )
            self.last_acked   = pkt_idx
            self.rcv_expected += 1

            # Send cumulative ACK for this packet's seq_num
            self._send_ack(pkt.seq_num, self.current_time)
        else:
            # Out-of-order: GBN discards
            pkt.state = PacketState.LOST  # discarded
            sim_evt   = self._log(
                EventType.PACKET_DISCARDED,
                pkt_idx,
                f"PKT[{pkt_idx}] seq={pkt.seq_num} discarded "
                f"(expected abs={expected_abs}, seq={self.packets[expected_abs % self.total_packets].seq_num if expected_abs < self.total_packets else '?'})",
            )
            # Re-ACK the last successfully received packet
            if self.last_acked >= 0:
                self._send_ack(self.packets[self.last_acked].seq_num, self.current_time)

        return sim_evt

    def _handle_ack(self, evt: _PQEvent) -> SimEvent:
        """
        Cumulative ACK arrives at the sender.

        GBN ACK(n) means: the receiver has correctly received all packets
        up to and including the one with seq_num n.  We advance the sender
        base to include all packets from base whose seq_num == n or whose
        absolute index precedes the ACKed packet.

        Because seq_nums cycle and absolute indices don't, we map the ACK's
        seq_num back to an absolute index by scanning [base, next_seq).
        """
        ack_seq = evt.seq_num   # seq_num value that was ACKed

        sim_evt = self._log(
            EventType.ACK_RECEIVED,
            ack_seq,
            f"ACK received for seq={ack_seq}",
        )

        # Find the absolute index of the packet whose seq_num == ack_seq
        # within the current window. Advance base to abs_idx + 1.
        acked_abs_idx: Optional[int] = None
        for idx in range(self.base, self.next_seq):
            if self.packets[idx].seq_num == ack_seq:
                acked_abs_idx = idx
                break

        if acked_abs_idx is None:
            # ACK for something already acknowledged or outside window — ignore
            return sim_evt

        # Advance base up to and including acked_abs_idx
        advanced_from = self.base
        while self.base <= acked_abs_idx:
            self.packets[self.base].state = PacketState.ACKNOWLEDGED
            self.base += 1

        if self.base > advanced_from:
            self._log(
                EventType.WINDOW_ADVANCED,
                self.base,
                f"Sender window advanced from {advanced_from} to base={self.base} (ack_seq={ack_seq})",
            )
            self._schedule_sends()

        self._check_done()
        return sim_evt

    def _handle_timeout(self, evt: _PQEvent) -> SimEvent:
        """
        Timeout for a packet.

        GBN rule: retransmit all outstanding packets from the timed-out one.
        """
        pkt_idx = evt.seq_num

        # Ignore stale timeouts
        if pkt_idx < self.base or pkt_idx >= self.next_seq:
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

        # Check retry limit
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
            f"— retransmitting window [{pkt_idx}, {self.next_seq})",
        )

        # GBN: retransmit from pkt_idx onward
        for idx in range(pkt_idx, self.next_seq):
            self._push(self.current_time, _EVT_SEND, idx)

        return sim_evt

    # -----------------------------------------------------------------------
    # ACK sending
    # -----------------------------------------------------------------------

    def _send_ack(self, ack_seq: int, send_time: float) -> None:
        """Generate an ACK at the receiver and schedule its arrival at sender."""
        ack = AckPacket(ack_num=ack_seq, send_time=send_time, is_cumulative=True)
        self._log(EventType.ACK_SENT, ack_seq, f"ACK sent for seq={ack_seq}")

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
        """Push an event onto the priority queue with a monotonic FIFO tiebreaker."""
        heapq.heappush(self._pq, _PQEvent(time, self._pq_counter, kind, seq_num, payload))
        self._pq_counter += 1

    def _schedule_sends(self) -> None:
        """
        Schedule SEND events for all packets in the current sender window
        that have not yet been sent.
        """
        while (
            self.next_seq < self.total_packets
            and self.next_seq < self.base + self.window_size
        ):
            self._push(self.current_time, _EVT_SEND, self.next_seq)
            self.next_seq += 1

    def _check_done(self) -> None:
        """Mark simulation done when all packets have been acknowledged."""
        if self.base >= self.total_packets:
            self.done = True
            self._log(
                EventType.SIMULATION_DONE,
                None,
                f"All {self.total_packets} packets delivered successfully (GBN)",
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
        Return a snapshot of the current GBN simulation state.

        Keys
        ----
        protocol          : "GBN"
        current_time      : float
        sender_base       : int  — absolute index of oldest unacknowledged packet
        sender_next_seq   : int  — absolute index of next packet to send
        sender_window     : list[int] — absolute indices in sender window
        receiver_expected : int  — absolute index of next expected packet
        packet_states     : dict[int, str] — abs index → PacketState name
        done              : bool
        failed            : bool
        total_packets     : int
        window_size       : int
        seq_space         : int
        """
        return {
            "protocol":           "GBN",
            "current_time":       self.current_time,
            "sender_base":        self.base,
            "sender_next_seq":    self.next_seq,
            "sender_window":      list(range(self.base, self.next_seq)),
            "receiver_expected":  self.rcv_expected,
            "packet_states":      {i: p.state.name for i, p in self.packets.items()},
            "done":               self.done,
            "failed":             self.failed,
            "total_packets":      self.total_packets,
            "window_size":        self.window_size,
            "seq_space":          self.seq_space,
        }
