"""
network.py
==========
Simulated network layer for the Sliding Window Protocol Simulator.

Responsibilities:
    - Model packet and ACK transmission with configurable propagation delays.
    - Apply independent packet-loss and ACK-loss probabilities.
    - Provide a deterministic random source (seeded) so experiments are reproducible.
    - Avoid any real-time sleep calls; all time values are simulated integers/floats.

All delay values are in abstract "time units". One time unit represents the
propagation delay for a single hop in one direction.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class PacketState(Enum):
    """Lifecycle states of a data packet on the sender side."""
    WAITING      = auto()   # Not yet sent
    IN_TRANSIT   = auto()   # Sent, travelling to receiver
    LOST         = auto()   # Dropped in transit
    RECEIVED     = auto()   # Arrived at receiver
    BUFFERED     = auto()   # Buffered out-of-order at receiver (SR only)
    DELIVERED    = auto()   # Passed to the application layer at receiver
    ACKNOWLEDGED = auto()   # ACK received by the sender
    CORRUPTED    = auto()   # Received with bit error / checksum failure


class AckState(Enum):
    """Lifecycle states of an acknowledgement."""
    PENDING  = auto()   # ACK generated, not yet delivered to sender
    IN_TRANSIT = auto() # ACK travelling back to sender
    LOST     = auto()   # ACK dropped
    RECEIVED = auto()   # ACK received by sender


class EventType(Enum):
    """Types of events that the simulation engine records."""
    PACKET_SENT        = "PACKET_SENT"
    PACKET_LOST        = "PACKET_LOST"
    PACKET_CORRUPTED   = "PACKET_CORRUPTED"      # Checksum failure
    PACKET_RECEIVED    = "PACKET_RECEIVED"
    PACKET_BUFFERED    = "PACKET_BUFFERED"       # SR: buffered out-of-order
    PACKET_DELIVERED   = "PACKET_DELIVERED"      # Passed to application layer
    PACKET_DISCARDED   = "PACKET_DISCARDED"      # GBN: out-of-order discard
    PACKET_DUPLICATE   = "PACKET_DUPLICATE"      # Duplicate received
    ACK_SENT           = "ACK_SENT"
    ACK_LOST           = "ACK_LOST"
    ACK_RECEIVED       = "ACK_RECEIVED"
    TIMEOUT            = "TIMEOUT"
    RETRANSMISSION     = "RETRANSMISSION"
    WINDOW_ADVANCED    = "WINDOW_ADVANCED"
    SIMULATION_STARTED = "SIMULATION_STARTED"
    SIMULATION_DONE    = "SIMULATION_DONE"
    SIMULATION_FAILED  = "SIMULATION_FAILED"     # Retry limit exceeded


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Packet:
    """
    Represents a single data packet.

    Attributes
    ----------
    seq_num : int
        Sequence number of the packet.
    data : str
        Payload (used for display / logging; not processed by the protocol).
    state : PacketState
        Current lifecycle state.
    send_time : Optional[float]
        Simulated time at which this packet was most recently transmitted.
    receive_time : Optional[float]
        Simulated time at which this packet arrived at the receiver.
    transmit_count : int
        Total number of transmission attempts (1 = first attempt, 2+ = retransmission).
    """
    seq_num: int
    data: str = ""
    state: PacketState = PacketState.WAITING
    send_time: Optional[float] = None
    receive_time: Optional[float] = None
    transmit_count: int = 0


@dataclass
class AckPacket:
    """
    Represents an acknowledgement frame.

    Attributes
    ----------
    ack_num : int
        For GBN  : cumulative ACK — acknowledges all packets up to (ack_num - 1).
        For SR   : selective ACK — acknowledges exactly packet ack_num.
    state : AckState
        Current lifecycle state.
    send_time : Optional[float]
        Simulated time at which this ACK was generated.
    receive_time : Optional[float]
        Simulated time at which this ACK reached the sender.
    is_cumulative : bool
        True for Go-Back-N style cumulative ACKs.
    """
    ack_num: int
    state: AckState = AckState.PENDING
    send_time: Optional[float] = None
    receive_time: Optional[float] = None
    is_cumulative: bool = True


@dataclass
class SimEvent:
    """
    A single recorded simulation event.

    Attributes
    ----------
    event_id : int
        Monotonically increasing event sequence number.
    time : float
        Simulated timestamp at which the event occurred.
    event_type : EventType
        Category of the event.
    seq_num : Optional[int]
        Packet or ACK sequence number, if applicable.
    description : str
        Human-readable summary of the event.
    extra : dict
        Optional additional key-value metadata.
    """
    event_id: int
    time: float
    event_type: EventType
    seq_num: Optional[int]
    description: str
    extra: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Network Channel
# ---------------------------------------------------------------------------

class TransmissionResult(tuple):
    """
    Result of a transmission attempt. Inherits from tuple so that:
        delivered, arrive_time = channel.transmit(t)
    unpacks as a standard 2-tuple, while .corrupted provides checksum status.
    """
    def __new__(cls, delivered: bool, arrival_time: Optional[float], corrupted: bool = False):
        return super().__new__(cls, (delivered, arrival_time))

    def __init__(self, delivered: bool, arrival_time: Optional[float], corrupted: bool = False) -> None:
        self.delivered = delivered
        self.arrival_time = arrival_time
        self.corrupted = corrupted


class NetworkChannel:
    """
    Simulates a unidirectional lossy and noisy channel with jitter.

    Parameters
    ----------
    loss_probability : float
        Probability in [0, 1) that a frame is dropped.
    propagation_delay : float
        Base number of simulated time units for a frame to traverse the channel.
    rng : random.Random
        Seeded random-number generator shared across the simulation.
    corrupt_probability : float
        Probability in [0, 1) that an arrived frame suffers bit corruption (checksum error).
    jitter_max : float
        Maximum additional random delay added to base propagation delay.
    """

    def __init__(
        self,
        loss_probability: float,
        propagation_delay: float,
        rng: random.Random,
        corrupt_probability: float = 0.0,
        jitter_max: float = 0.0,
    ) -> None:
        if not 0.0 <= loss_probability < 1.0:
            raise ValueError("loss_probability must be in [0, 1)")
        if propagation_delay <= 0:
            raise ValueError("propagation_delay must be positive")
        if not 0.0 <= corrupt_probability < 1.0:
            raise ValueError("corrupt_probability must be in [0, 1)")
        if jitter_max < 0.0:
            raise ValueError("jitter_max must be non-negative")

        self.loss_probability = loss_probability
        self.propagation_delay = propagation_delay
        self.corrupt_probability = corrupt_probability
        self.jitter_max = jitter_max
        self._rng = rng

    def transmit(self, send_time: float) -> TransmissionResult:
        """
        Decide whether a frame is delivered, when it arrives, and if corrupted.
        """
        if self._rng.random() < self.loss_probability:
            return TransmissionResult(False, None, False)

        jitter = self._rng.uniform(0.0, self.jitter_max) if self.jitter_max > 0.0 else 0.0
        arrival_time = send_time + self.propagation_delay + jitter

        corrupted = (self._rng.random() < self.corrupt_probability) if self.corrupt_probability > 0.0 else False
        return TransmissionResult(True, arrival_time, corrupted)
