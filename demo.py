"""
demo.py — Quick demonstration of the Sliding Window Simulator
Run: python demo.py
"""
from simulation import create_simulation

def run_demo(protocol, pkt_loss=0.3, seed=42):
    print("=" * 60)
    print(f"  Protocol : {protocol}")
    print(f"  Packets  : 6   Window: 3   Loss: {pkt_loss*100:.0f}%   Seed: {seed}")
    print("=" * 60)

    sim = create_simulation({
        "protocol":      protocol,
        "total_packets": 6,
        "window_size":   3,
        "pkt_loss_prob": pkt_loss,
        "ack_loss_prob": 0.0,
        "timeout":       5.0,
        "prop_delay":    1.0,
        "seed":          seed,
    })

    sim.run()

    for e in sim.get_events():
        print(f"  t={e['time']:4.0f}  [{e['event_type']:<22s}]  seq={str(e['seq_num']):<4}  {e['description']}")

    print()
    print("  --- Metrics ---")
    m = sim.get_metrics()
    print(f"  Delivered          : {m['unique_delivered']} / {m['total_intended']}")
    print(f"  Delivery Rate      : {m['delivery_rate']} %")
    print(f"  TX Attempts        : {m['total_tx_attempts']}")
    print(f"  Retransmissions    : {m['total_retransmissions']}")
    print(f"  Efficiency         : {m['transmission_efficiency']} %")
    print(f"  Packets Lost       : {m['total_pkt_lost']}")
    print(f"  ACKs Lost          : {m['total_ack_lost']}")
    print(f"  Simulation Time    : {m['simulation_time']}")
    print(f"  Completed          : {m['completed']}")
    print()

if __name__ == "__main__":
    run_demo("GBN", pkt_loss=0.3, seed=42)
    run_demo("SR",  pkt_loss=0.3, seed=42)
