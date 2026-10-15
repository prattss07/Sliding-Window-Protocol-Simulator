"""
compare.py — Comprehensive comparative benchmark: Go-Back-N vs Selective Repeat.
Run: python compare.py
"""

from simulation import create_simulation


def run_benchmark(scenario_name: str, config_override: dict):
    base_config = {
        "total_packets": 12,
        "window_size": 4,
        "pkt_loss_prob": 0.0,
        "ack_loss_prob": 0.0,
        "corrupt_prob": 0.0,
        "jitter_max": 0.0,
        "timeout": 5.0,
        "prop_delay": 1.0,
        "seed": 42,
    }
    cfg = {**base_config, **config_override}

    # Run GBN
    gbn_sim = create_simulation({**cfg, "protocol": "GBN"})
    gbn_sim.run()
    m_gbn = gbn_sim.get_metrics()

    # Run SR
    sr_sim = create_simulation({**cfg, "protocol": "SR"})
    sr_sim.run()
    m_sr = sr_sim.get_metrics()

    return scenario_name, m_gbn, m_sr


def print_comparison_table():
    scenarios = [
        ("1. Ideal Channel (0% Loss)", {"pkt_loss_prob": 0.0, "ack_loss_prob": 0.0}),
        ("2. 20% Packet Loss", {"pkt_loss_prob": 0.2}),
        ("3. 25% ACK Loss", {"ack_loss_prob": 0.25}),
        ("4. 20% Packet Corruption (Checksum Failure)", {"corrupt_prob": 0.2}),
        ("5. Heavy Jitter (Delay Variance)", {"jitter_max": 1.5, "pkt_loss_prob": 0.15}),
        ("6. Adverse Channel (Loss + Corruption + Jitter)", {
            "pkt_loss_prob": 0.25,
            "corrupt_prob": 0.15,
            "jitter_max": 1.0,
        }),
    ]

    header = (
        f"{'Scenario':<42} | {'Proto':<4} | {'Attempts':<8} | {'Retx':<5} | "
        f"{'Lost':<5} | {'Corrupt':<7} | {'Efficiency':<10} | {'SimTime':<7}"
    )
    separator = "-" * len(header)

    print("\n" + "=" * len(header))
    print("  SLIDING WINDOW PROTOCOL BENCHMARK: GO-BACK-N vs SELECTIVE REPEAT")
    print("=" * len(header))
    print(header)
    print(separator)

    for name, overrides in scenarios:
        _, m_gbn, m_sr = run_benchmark(name, overrides)

        for m in (m_gbn, m_sr):
            proto = m["protocol"]
            att = m["total_tx_attempts"]
            retx = m["total_retransmissions"]
            lost = m["total_pkt_lost"]
            corrupt = m.get("total_pkt_corrupted", 0)
            eff = f"{m['transmission_efficiency']:.1f}%"
            time_taken = f"{m['simulation_time']:.1f}"

            label = name if proto == "GBN" else ""
            print(
                f"{label:<42} | {proto:<4} | {att:<8} | {retx:<5} | "
                f"{lost:<5} | {corrupt:<7} | {eff:<10} | {time_taken:<7}"
            )
        print(separator)

    print("\nTakeaways:")
    print("  - GBN retransmits the whole window on loss/corruption -> lower efficiency.")
    print("  - SR retransmits strictly corrupted or missing packets -> higher efficiency.\n")


if __name__ == "__main__":
    print_comparison_table()
