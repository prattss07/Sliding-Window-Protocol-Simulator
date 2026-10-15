# Sliding Window Protocol Simulator — Backend

A fully event-driven Python simulation engine implementing **Go-Back-N (GBN)** and **Selective Repeat (SR)** ARQ protocols, complete with packet/ACK loss, timeouts, retransmissions, sliding windows, event logging, and performance metrics.

---

## Project Structure

```
sliding-window-simulator/
├── network.py           — Packet/ACK data classes, enums, NetworkChannel
├── go_back_n.py         — Go-Back-N ARQ engine
├── selective_repeat.py  — Selective Repeat ARQ engine
├── simulation.py        — Public API (SimulationEngine, create_simulation)
├── metrics.py           — Performance metric calculation
├── tests/
│   ├── __init__.py
│   └── test_protocols.py — 44 automated correctness tests
└── README.md
```

---

## Quick Start (for Person 2 — Streamlit Frontend)

```python
from simulation import create_simulation

# 1. Create a simulation
sim = create_simulation({
    "protocol":      "GBN",   # or "SR"
    "total_packets": 8,
    "window_size":   3,
    "pkt_loss_prob": 0.2,     # 20% packet loss
    "ack_loss_prob": 0.1,     # 10% ACK loss
    "timeout":       5.0,     # simulated time units
    "prop_delay":    1.0,     # one-way propagation delay
    "seed":          42,      # fixed seed for reproducibility
})

# 2a. Run to completion at once
sim.run()

# 2b. OR step through one event at a time
while not sim.done and not sim.failed:
    event = sim.step()        # returns a dict or None
    print(event)

# 3. Get current state
state   = sim.get_state()

# 4. Get all recorded events
events  = sim.get_events()

# 5. Get performance metrics
metrics = sim.get_metrics()

# 6. Reset and run again (same config, same seed → identical results)
sim.reset()
sim.run()
```

---

## API Reference

### `create_simulation(config)` → `SimulationEngine`

Factory function. Preferred entry point.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `protocol` | `str` | `"GBN"` | `"GBN"` or `"SR"` |
| `total_packets` | `int` | `8` | Number of unique packets to deliver |
| `window_size` | `int` | `3` | Sender (and SR receiver) window size |
| `pkt_loss_prob` | `float` | `0.0` | Data packet loss probability ∈ [0, 1) |
| `ack_loss_prob` | `float` | `0.0` | ACK loss probability ∈ [0, 1) |
| `corrupt_prob` | `float` | `0.0` | Packet bit corruption probability (checksum failure) ∈ [0, 1) |
| `jitter_max` | `float` | `0.0` | Maximum random propagation delay jitter in time units |
| `timeout` | `float` | `5.0` | Per-packet timeout in simulated time units |
| `prop_delay` | `float` | `1.0` | One-way propagation delay in time units |
| `max_retries` | `int` | `20` | Per-packet retry limit before simulation failure |
| `seed` | `int\|None` | `None` | Random seed (None = non-deterministic) |

---

### `SimulationEngine` methods

#### `.step()` → `dict | None`
Process the next simulation event. Returns the event dict or `None` when done.

#### `.run()` → `list[dict]`
Run to completion. Returns all recorded events.

#### `.reset()` → `None`
Reset the engine to its initial state using the same config and seed.

#### `.get_state()` → `dict`

Returns the current simulation state. **Common keys** (GBN and SR):

| Key | Type | Description |
|-----|------|-------------|
| `protocol` | `str` | `"GBN"` or `"SR"` |
| `current_time` | `float` | Current simulated time |
| `sender_base` | `int` | Absolute index of oldest unACKed packet |
| `sender_next_seq` | `int` | Absolute index of next packet to send |
| `sender_window` | `list[int]` | Absolute indices in sender window |
| `receiver_base` | `int` | Next expected in-order packet (abs index) |
| `receiver_window` | `list[int]` | Receiver window indices |
| `packet_states` | `dict[str, str]` | `str(abs_index)` → `PacketState` name |
| `done` | `bool` | True if all packets delivered |
| `failed` | `bool` | True if retry limit exceeded |
| `total_packets` | `int` | Total packets to deliver |
| `window_size` | `int` | Configured window size |
| `seq_space` | `int` | Sequence number space size |

**GBN-only key:** `receiver_expected` (int)

**SR-only keys:** `sender_acked` (dict), `receiver_buffer` (list), `delivered` (list)

#### `.get_events()` → `list[dict]`

Each event dict contains:

| Key | Type | Description |
|-----|------|-------------|
| `event_id` | `int` | Monotonically increasing sequence number |
| `time` | `float` | Simulated timestamp |
| `event_type` | `str` | One of the event type strings below |
| `seq_num` | `int\|None` | Packet or ACK sequence number |
| `description` | `str` | Human-readable summary |
| `extra` | `dict` | Optional additional metadata |

**Event types:**

| Event Type | When it occurs |
|-----------|----------------|
| `PACKET_SENT` | First transmission of a packet |
| `RETRANSMISSION` | Any subsequent transmission of the same packet |
| `PACKET_LOST` | Packet dropped in the channel |
| `PACKET_CORRUPTED` | Packet suffered bit error / failed checksum |
| `PACKET_RECEIVED` | Packet received in order at receiver |
| `PACKET_BUFFERED` | SR: packet received out of order and buffered |
| `PACKET_DELIVERED` | Packet passed to the application layer |
| `PACKET_DISCARDED` | GBN: out-of-order packet discarded; or packet outside window |
| `PACKET_DUPLICATE` | Duplicate packet received |
| `ACK_SENT` | Receiver sends an ACK |
| `ACK_LOST` | ACK dropped in the channel |
| `ACK_RECEIVED` | Sender receives an ACK |
| `TIMEOUT` | Per-packet timer fires |
| `WINDOW_ADVANCED` | Sender window base moves forward |
| `SIMULATION_DONE` | All packets delivered successfully |
| `SIMULATION_FAILED` | Max retries exceeded |

#### `.get_metrics()` → `dict`

| Key | Type | Description |
|-----|------|-------------|
| `protocol` | `str` | Protocol name |
| `total_intended` | `int` | Packets to deliver |
| `total_tx_attempts` | `int` | Total data-packet transmissions (1st + retransmissions) |
| `total_retransmissions` | `int` | Retransmission attempts only |
| `unique_delivered` | `int` | Unique packets delivered to application |
| `delivery_rate` | `float` | `unique_delivered / total_intended × 100` % |
| `transmission_efficiency` | `float` | `unique_delivered / total_tx_attempts × 100` % |
| `total_ack_sent` | `int` | ACK frames generated |
| `total_ack_lost` | `int` | ACK frames lost |
| `total_pkt_lost` | `int` | Data packets lost |
| `simulation_time` | `float` | Simulated timestamp of last event |
| `completed` | `bool` | True if `SIMULATION_DONE` was recorded |

---

## Protocol Assumptions

### Go-Back-N
- Receiver window size is always **1**.
- Out-of-order packets are **discarded**; no buffering at the receiver.
- Cumulative ACK(n): acknowledges all packets up to and including sequence number n.
- On timeout: **all** outstanding unACKed packets from the timed-out one onward are retransmitted.
- Sequence number space ≥ 2W (where W = window size).

### Selective Repeat
- Receiver window size equals the sender window size.
- Out-of-order packets within the receive window are **buffered**.
- Selective ACK(n): acknowledges exactly packet n.
- On timeout: **only** the timed-out packet is retransmitted.
- Buffered packets are delivered in sequence when the missing in-order packet arrives.
- Sequence number space ≥ 2W.

### Time Model
- All time values are abstract simulated units (integers/floats).
- No `time.sleep()` calls — this is a discrete-event simulation.
- Events at the same simulated time are processed FIFO (insertion order).
- Timeouts are per-packet; stale timeouts are safely ignored after window advance.

---

## Running Tests

```bash
# From the sliding-window-simulator/ directory:
python -m pytest tests/ -v

# Or without pytest:
python tests/test_protocols.py
```

Expected output: **44 passed**.

---

## Metric Definitions

- **Delivery rate:** `unique_delivered / total_intended × 100`
- **Transmission efficiency:** `unique_delivered / total_tx_attempts × 100`
- **Retransmissions:** counted from `RETRANSMISSION` events (not `PACKET_SENT`)
- ACK transmissions are **excluded** from `total_tx_attempts`
- Duplicate receptions (`PACKET_DUPLICATE`) do **not** increment `unique_delivered`
