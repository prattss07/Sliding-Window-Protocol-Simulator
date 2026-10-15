"""
simulation.py
=============
Simulation engine and public interface for the Sliding Window Protocol Simulator.

This module is the single entry point for Person 2's Streamlit frontend.
It wraps both Go-Back-N and Selective Repeat protocol engines behind a
clean, protocol-agnostic API.

Quick-Start Example
-------------------
>>> from simulation import create_simulation
>>> sim = create_simulation({
...     "protocol":        "GBN",       # or "SR"
...     "total_packets":   8,
...     "window_size":     3,
...     "pkt_loss_prob":   0.2,
...     "ack_loss_prob":   0.1,
...     "timeout":         5.0,
...     "prop_delay":      1.0,
...     "seed":            42,
... })
>>> sim.run()
>>> print(sim.get_metrics())
>>> print(sim.get_state())

Public API
----------
create_simulation(config) → SimulationEngine
    Factory function.  Preferred way to build a simulation.

SimulationEngine.step()        → dict | None
SimulationEngine.run()         → list[dict]
SimulationEngine.reset()       → None
SimulationEngine.get_state()   → dict
SimulationEngine.get_events()  → list[dict]
SimulationEngine.get_metrics() → dict
"""

from __future__ import annotations

import random
from typing import Optional

from go_back_n import GoBackN
from metrics import calculate_metrics, SimulationMetrics
from network import EventType, NetworkChannel, SimEvent
from selective_repeat import SelectiveRepeat


# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------

_DEFAULTS = {
    "protocol":       "GBN",   # "GBN" | "SR"
    "total_packets":  8,
    "window_size":    3,
    "pkt_loss_prob":  0.0,     # [0, 1)
    "ack_loss_prob":  0.0,     # [0, 1)
    "corrupt_prob":   0.0,     # [0, 1) packet corruption probability (checksum error)
    "jitter_max":     0.0,     # max random propagation delay jitter
    "timeout":        5.0,     # simulated time units
    "prop_delay":     1.0,     # simulated time units per direction
    "max_retries":    20,
    "seed":           None,    # int | None  (None → non-deterministic)
}


# ---------------------------------------------------------------------------
# SimulationEngine
# ---------------------------------------------------------------------------

class SimulationEngine:
    """
    Protocol-agnostic simulation engine.

    This class wraps either GoBackN or SelectiveRepeat and exposes a
    uniform interface to the Streamlit frontend.

    Attributes
    ----------
    config : dict
        A copy of the configuration used to construct this engine.
    protocol_name : str
        "GBN" or "SR".
    """

    def __init__(self, config: dict) -> None:
        """
        Initialise the engine with the given configuration.

        Parameters
        ----------
        config : dict
            Accepted keys (all optional; missing keys use defaults):

            protocol       : str   — "GBN" or "SR"
            total_packets  : int   — number of unique data packets to deliver
            window_size    : int   — sender (and SR receiver) window size
            pkt_loss_prob  : float — data-packet loss probability ∈ [0, 1)
            ack_loss_prob  : float — ACK loss probability ∈ [0, 1)
            timeout        : float — per-packet timeout in simulated time units
            prop_delay     : float — one-way propagation delay in time units
            max_retries    : int   — per-packet retry limit before failure
            seed           : int | None — random seed (None = non-deterministic)
        """
        cfg = {**_DEFAULTS, **config}

        protocol = cfg["protocol"].upper()
        if protocol not in ("GBN", "SR"):
            raise ValueError(f"Unknown protocol '{protocol}'. Choose 'GBN' or 'SR'.")

        self.config        = cfg
        self.protocol_name = protocol
        self._engine       = self._build_engine(cfg)

    # -----------------------------------------------------------------------
    # Core API
    # -----------------------------------------------------------------------

    def step(self) -> Optional[dict]:
        """
        Process the next simulation event.

        Returns
        -------
        dict | None
            The event that was just processed as a plain dictionary, or None
            if the simulation is already complete.

        Dictionary keys
        ---------------
        event_id    : int
        time        : float
        event_type  : str   — EventType enum name (e.g. "PACKET_SENT")
        seq_num     : int | None
        description : str
        extra       : dict
        """
        evt: Optional[SimEvent] = self._engine.step()
        return _evt_to_dict(evt) if evt else None

    def run(self) -> list[dict]:
        """
        Run the simulation to completion (or failure).

        Returns
        -------
        list[dict]
            All recorded events as plain dictionaries (same schema as step()).
        """
        self._engine.run()
        return self.get_events()

    def reset(self) -> None:
        """
        Reset the simulation to its initial state using the same configuration.

        This rebuilds the entire protocol engine from scratch so the engine
        is ready to be stepped or run again.
        """
        self._engine = self._build_engine(self.config)

    def get_state(self) -> dict:
        """
        Return the current simulation state as a plain dictionary.

        Common keys (present for both GBN and SR)
        ------------------------------------------
        protocol          : str   — "GBN" or "SR"
        current_time      : float
        sender_base       : int   — absolute index of the oldest unACKed packet
        sender_next_seq   : int   — absolute index of the next packet to send
        sender_window     : list[int]
        receiver_base     : int   — (SR) or receiver_expected (GBN mapped here)
        receiver_window   : list[int]  — SR: full window; GBN: [expected]
        packet_states     : dict[str, str]  — str(abs_index) → state name
        done              : bool
        failed            : bool
        total_packets     : int
        window_size       : int
        seq_space         : int

        GBN-only keys
        -------------
        receiver_expected : int

        SR-only keys
        ------------
        sender_acked      : dict[str, bool]
        receiver_buffer   : list[int]
        delivered         : list[int]
        """
        raw = self._engine.get_state()
        # Normalise keys so the frontend can use a single code path
        if self.protocol_name == "GBN":
            raw["receiver_base"]   = raw.get("receiver_expected", 0)
            raw["receiver_window"] = [raw["receiver_expected"]]
        # Stringify packet_states keys for JSON compatibility
        raw["packet_states"] = {str(k): v for k, v in raw["packet_states"].items()}
        return raw

    def get_events(self) -> list[dict]:
        """
        Return all recorded events as a list of plain dictionaries.

        Each dictionary contains:
            event_id    : int
            time        : float
            event_type  : str
            seq_num     : int | None
            description : str
            extra       : dict
        """
        return [_evt_to_dict(e) for e in self._engine.events]

    def get_metrics(self) -> dict:
        """
        Return computed performance metrics as a plain dictionary.

        Keys
        ----
        protocol               : str
        total_intended         : int
        total_tx_attempts      : int
        total_retransmissions  : int
        unique_delivered       : int
        delivery_rate          : float  — percent
        transmission_efficiency: float  — percent
        total_ack_sent         : int
        total_ack_lost         : int
        total_pkt_lost         : int
        simulation_time        : float
        completed              : bool
        """
        m: SimulationMetrics = calculate_metrics(
            events         = self._engine.events,
            total_intended = self.config["total_packets"],
            protocol       = self.protocol_name,
        )
        return m.as_dict()

    # -----------------------------------------------------------------------
    # Properties
    # -----------------------------------------------------------------------

    @property
    def done(self) -> bool:
        """True if the simulation finished successfully."""
        return self._engine.done

    @property
    def failed(self) -> bool:
        """True if the simulation hit the maximum retry limit."""
        return self._engine.failed

    # -----------------------------------------------------------------------
    # Private helpers
    # -----------------------------------------------------------------------

    def _build_engine(self, cfg: dict):
        """Construct (or reconstruct) the appropriate protocol engine."""
        rng = random.Random(cfg["seed"])

        pkt_channel = NetworkChannel(
            loss_probability    = cfg["pkt_loss_prob"],
            propagation_delay   = cfg["prop_delay"],
            rng                 = rng,
            corrupt_probability = cfg.get("corrupt_prob", 0.0),
            jitter_max          = cfg.get("jitter_max", 0.0),
        )
        ack_channel = NetworkChannel(
            loss_probability    = cfg["ack_loss_prob"],
            propagation_delay   = cfg["prop_delay"],
            rng                 = rng,
            jitter_max          = cfg.get("jitter_max", 0.0),
        )

        common_kwargs = dict(
            total_packets    = cfg["total_packets"],
            window_size      = cfg["window_size"],
            packet_channel   = pkt_channel,
            ack_channel      = ack_channel,
            timeout_duration = cfg["timeout"],
            max_retries      = cfg["max_retries"],
        )

        if cfg["protocol"].upper() == "GBN":
            return GoBackN(**common_kwargs)
        else:
            return SelectiveRepeat(**common_kwargs)


# ---------------------------------------------------------------------------
# Factory function
# ---------------------------------------------------------------------------

def create_simulation(config: dict) -> SimulationEngine:
    """
    Initialise and return a new SimulationEngine.

    Parameters
    ----------
    config : dict
        See SimulationEngine.__init__ for accepted keys.

    Returns
    -------
    SimulationEngine
        A ready-to-use simulation engine.

    Examples
    --------
    >>> sim = create_simulation({"protocol": "SR", "total_packets": 10, "window_size": 4, "seed": 7})
    >>> sim.run()
    >>> metrics = sim.get_metrics()
    """
    return SimulationEngine(config)


# ---------------------------------------------------------------------------
# Helper: SimEvent → plain dict
# ---------------------------------------------------------------------------

def _evt_to_dict(evt: Optional[SimEvent]) -> Optional[dict]:
    if evt is None:
        return None
    return {
        "event_id":   evt.event_id,
        "time":       evt.time,
        "event_type": evt.event_type.value,
        "seq_num":    evt.seq_num,
        "description":evt.description,
        "extra":      evt.extra,
    }
