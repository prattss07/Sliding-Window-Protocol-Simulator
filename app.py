"""
app.py
======
Streamlit GUI for the Sliding Window Protocol Simulator.

Run:
    pip install -r requirements.txt
    streamlit run app.py

The GUI is a thin layer over the existing engine (simulation.py); no protocol
logic lives here.
"""

from __future__ import annotations

import html
import time

import pandas as pd
import streamlit as st

from simulation import create_simulation

# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Sliding Window Protocol Simulator",
    page_icon="🪟",
    layout="wide",
)

STATE_COLORS = {
    "WAITING":      ("#e5e7eb", "#374151"),
    "IN_TRANSIT":   ("#bfdbfe", "#1e3a8a"),
    "LOST":         ("#fecaca", "#7f1d1d"),
    "CORRUPTED":    ("#fed7aa", "#7c2d12"),
    "RECEIVED":     ("#e9d5ff", "#581c87"),
    "BUFFERED":     ("#fde68a", "#78350f"),
    "DELIVERED":    ("#bbf7d0", "#14532d"),
    "ACKNOWLEDGED": ("#86efac", "#14532d"),
}

EVENT_ICONS = {
    "PACKET_SENT": "📤", "RETRANSMISSION": "🔁", "PACKET_LOST": "❌",
    "PACKET_CORRUPTED": "⚠️", "PACKET_RECEIVED": "📥", "PACKET_BUFFERED": "🗃️",
    "PACKET_DELIVERED": "✅", "PACKET_DISCARDED": "🗑️", "PACKET_DUPLICATE": "♊",
    "ACK_SENT": "📨", "ACK_LOST": "💥", "ACK_RECEIVED": "🎯", "TIMEOUT": "⏰",
    "WINDOW_ADVANCED": "➡️", "SIMULATION_STARTED": "🚦",
    "SIMULATION_DONE": "🏁", "SIMULATION_FAILED": "🛑",
}

# ---------------------------------------------------------------------------
# Sidebar: configuration
# ---------------------------------------------------------------------------


def sidebar_config() -> dict:
    st.sidebar.title("⚙️ Configuration")

    protocol_label = st.sidebar.radio(
        "Protocol",
        ["Go-Back-N", "Selective Repeat"],
        help="GBN: cumulative ACKs, receiver window 1. SR: selective ACKs, receiver buffers.",
    )
    total_packets = st.sidebar.slider("Total packets", 2, 30, 10)
    window_size = st.sidebar.slider("Window size", 1, 10, 4)

    st.sidebar.markdown("**Channel**")
    pkt_loss = st.sidebar.slider("Packet loss", 0.0, 0.9, 0.2, 0.05)
    ack_loss = st.sidebar.slider("ACK loss", 0.0, 0.9, 0.1, 0.05)
    corrupt = st.sidebar.slider("Packet corruption", 0.0, 0.9, 0.0, 0.05)
    jitter = st.sidebar.slider("Jitter (max extra delay)", 0.0, 3.0, 0.0, 0.25)

    st.sidebar.markdown("**Timing**")
    prop_delay = st.sidebar.slider("Propagation delay", 0.5, 5.0, 1.0, 0.5)
    timeout = st.sidebar.slider("Timeout", 1.0, 20.0, 5.0, 0.5)
    max_retries = st.sidebar.slider("Max retries / packet", 1, 50, 20)

    st.sidebar.markdown("**Randomness**")
    use_seed = st.sidebar.checkbox("Fixed seed (reproducible)", value=True)
    seed = st.sidebar.number_input("Seed", 0, 10_000, 42, disabled=not use_seed)

    if timeout <= 2 * prop_delay:
        st.sidebar.warning(
            "Timeout ≤ round-trip time (2 × propagation delay): "
            "expect premature timeouts and many retransmissions."
        )

    return {
        "protocol": "GBN" if protocol_label == "Go-Back-N" else "SR",
        "total_packets": int(total_packets),
        "window_size": int(window_size),
        "pkt_loss_prob": float(pkt_loss),
        "ack_loss_prob": float(ack_loss),
        "corrupt_prob": float(corrupt),
        "jitter_max": float(jitter),
        "timeout": float(timeout),
        "prop_delay": float(prop_delay),
        "max_retries": int(max_retries),
        "seed": int(seed) if use_seed else None,
    }


# ---------------------------------------------------------------------------
# Session handling
# ---------------------------------------------------------------------------


def config_key(cfg: dict) -> tuple:
    return tuple(sorted(cfg.items(), key=lambda kv: kv[0]))


def ensure_sim(cfg: dict) -> None:
    """Create / rebuild the simulation when the configuration changes."""
    key = config_key(cfg)
    if st.session_state.get("cfg_key") != key:
        st.session_state.cfg_key = key
        st.session_state.sim = create_simulation(cfg)
        st.session_state.steps = 0


def sim_finished() -> bool:
    sim = st.session_state.sim
    return sim.done or sim.failed


def do_steps(n: int) -> None:
    sim = st.session_state.sim
    for _ in range(n):
        if sim_finished():
            break
        if sim.step() is None:
            break
        st.session_state.steps += 1


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

CSS = """
<style>
.swp-row {display:flex; flex-wrap:wrap; gap:6px; margin:4px 0 14px 0; align-items:flex-end;}
.swp-cell {min-width:46px; padding:8px 6px; border-radius:8px; text-align:center;
           font-weight:700; font-size:15px; border:2px solid transparent; line-height:1.1;}
.swp-cell small {display:block; font-weight:500; font-size:9px; opacity:.8; margin-top:3px;}
.swp-inwin {border-color:#2563eb; box-shadow:0 0 0 2px rgba(37,99,235,.25);}
.swp-base  {border-color:#dc2626 !important;}
.swp-legend span {display:inline-block; padding:2px 8px; margin:2px 4px 2px 0;
                  border-radius:6px; font-size:12px; font-weight:600;}
.swp-note {font-size:12px; opacity:.7; margin-top:-8px; margin-bottom:6px;}
</style>
"""


def packet_row(state: dict, window: list[int], base: int, label_fn=None) -> str:
    total = state["total_packets"]
    pstates = state["packet_states"]
    cells = []
    for i in range(total):
        name = pstates.get(str(i), "WAITING")
        bg, fg = STATE_COLORS.get(name, STATE_COLORS["WAITING"])
        classes = "swp-cell"
        if i in window:
            classes += " swp-inwin"
        if i == base and i in window:
            classes += " swp-base"
        sub = label_fn(i, name) if label_fn else name.title().replace("_", " ")
        cells.append(
            f'<div class="{classes}" style="background:{bg};color:{fg}">'
            f"{i}<small>{html.escape(sub)}</small></div>"
        )
    return f'<div class="swp-row">{"".join(cells)}</div>'


def legend_html() -> str:
    spans = "".join(
        f'<span style="background:{bg};color:{fg}">{name.title().replace("_", " ")}</span>'
        for name, (bg, fg) in STATE_COLORS.items()
    )
    return (
        f'<div class="swp-legend">{spans}</div>'
        '<div class="swp-note">Blue outline = inside the window · red outline = window base.</div>'
    )


def render_windows(state: dict) -> None:
    st.markdown(CSS, unsafe_allow_html=True)
    st.markdown("**Sender**  —  window `[{} … {})`, next to send: `{}`".format(
        state["sender_base"], state["sender_next_seq"], state["sender_next_seq"]))
    st.markdown(
        packet_row(state, state["sender_window"], state["sender_base"]),
        unsafe_allow_html=True,
    )

    if state["protocol"] == "GBN":
        st.markdown(f"**Receiver**  —  expecting packet `{state['receiver_base']}` (window size 1)")
        st.markdown(
            packet_row(
                state, [state["receiver_base"]], state["receiver_base"],
                label_fn=lambda i, n: ("expected" if i == state["receiver_base"]
                                       else ("delivered" if n in ("DELIVERED", "ACKNOWLEDGED") else "—")),
            ),
            unsafe_allow_html=True,
        )
    else:
        buffered = state.get("receiver_buffer", [])
        st.markdown(
            f"**Receiver**  —  window `[{state['receiver_base']} … "
            f"{state['receiver_base'] + state['window_size']})`, buffered: `{buffered or 'none'}`"
        )
        delivered = set(state.get("delivered", []))
        st.markdown(
            packet_row(
                state, state["receiver_window"], state["receiver_base"],
                label_fn=lambda i, n: ("delivered" if i in delivered
                                       else ("buffered" if i in buffered else "—")),
            ),
            unsafe_allow_html=True,
        )
    st.markdown(legend_html(), unsafe_allow_html=True)


# --- Sequence diagram ------------------------------------------------------

_ARRIVAL = {
    "PACKET_RECEIVED", "PACKET_BUFFERED", "PACKET_DISCARDED",
    "PACKET_DUPLICATE", "PACKET_CORRUPTED", "PACKET_DELIVERED",
}


def sequence_diagram(events: list[dict], max_events: int = 400) -> str:
    """Build a sender/receiver time-sequence diagram (SVG) from the event log."""
    if not events:
        return "<div style='opacity:.6'>Press <b>Step</b> or <b>Run</b> to start.</div>"

    # Each send/ACK carries a small vertical offset so frames emitted at the
    # same instant (e.g. a whole window) are drawn side by side, not overlapped.
    OFF = 0.22
    sends: dict[int, list[tuple[float, float]]] = {}
    acks: dict[int, list[tuple[float, float]]] = {}
    slots: dict[tuple[str, float], int] = {}
    arrows = []  # (from_side, t0, to_side, t1, kind, label)
    markers = []  # (side, t, glyph, color, label)
    last_arrival_key = None

    def slot(side: str, t: float) -> float:
        k = slots.get((side, t), 0)
        slots[(side, t)] = k + 1
        return k * OFF

    for e in events[-max_events:]:
        et, t, seq = e["event_type"], e["time"], e["seq_num"]
        if et in ("PACKET_SENT", "RETRANSMISSION"):
            sends.setdefault(seq, []).append((t, slot("S", t)))
        elif et == "PACKET_LOST":
            q = sends.get(seq, [])
            if q:
                t0, off = q.pop(0)
                arrows.append(("S", t0 + off, "R", t0 + off + 0.6, "lost", f"{seq}"))
        elif et in _ARRIVAL:
            key = (t, seq)
            if key == last_arrival_key:
                continue
            last_arrival_key = key
            q = sends.get(seq, [])
            if q:
                t0, off = q.pop(0)
                kind = "corrupt" if et == "PACKET_CORRUPTED" else "data"
                arrows.append(("S", t0 + off, "R", t + off, kind, f"{seq}"))
        elif et == "ACK_SENT":
            acks.setdefault(seq, []).append((t, slot("R", t)))
        elif et == "ACK_LOST":
            q = acks.get(seq, [])
            if q:
                t0, off = q.pop()
                arrows.append(("R", t0 + off, "S", t0 + off + 0.6, "lost", f"A{seq}"))
        elif et == "ACK_RECEIVED":
            q = acks.get(seq, [])
            if q:
                t0, off = q.pop(0)
                arrows.append(("R", t0 + off, "S", t + off, "ack", f"A{seq}"))
        elif et == "TIMEOUT":
            markers.append(("S", t, "⏰", "#b45309", f"timeout {seq}"))

    tmax = max([e["time"] for e in events] + [a[3] for a in arrows]) + 1.5
    y_scale = 38.0
    top, left_x, right_x = 46, 120, 560
    height = int(top + tmax * y_scale + 30)
    width = 680
    colors = {"data": "#2563eb", "ack": "#16a34a", "lost": "#dc2626", "corrupt": "#ea580c"}

    def X(side: str) -> int:
        return left_x if side == "S" else right_x

    def Y(t: float) -> float:
        return top + t * y_scale

    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" '
        f'xmlns="http://www.w3.org/2000/svg" style="font-family:sans-serif;">',
        '<defs>'
        + "".join(
            f'<marker id="ah-{k}" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">'
            f'<path d="M0,0 L8,4 L0,8 z" fill="{c}"/></marker>'
            for k, c in colors.items()
        )
        + "</defs>",
        f'<text x="{left_x}" y="22" text-anchor="middle" font-weight="700" font-size="14" fill="currentColor">Sender</text>',
        f'<text x="{right_x}" y="22" text-anchor="middle" font-weight="700" font-size="14" fill="currentColor">Receiver</text>',
        f'<line x1="{left_x}" y1="{top - 8}" x2="{left_x}" y2="{height - 10}" stroke="#9ca3af" stroke-width="2"/>',
        f'<line x1="{right_x}" y1="{top - 8}" x2="{right_x}" y2="{height - 10}" stroke="#9ca3af" stroke-width="2"/>',
    ]

    step = max(1, int(tmax // 12))
    for tick in range(0, int(tmax) + 1, step):
        parts.append(
            f'<text x="{left_x - 14}" y="{Y(tick) + 4:.1f}" text-anchor="end" font-size="10" '
            f'fill="currentColor" opacity=".55">t={tick}</text>'
        )
        parts.append(
            f'<line x1="{left_x - 8}" y1="{Y(tick):.1f}" x2="{left_x}" y2="{Y(tick):.1f}" '
            f'stroke="#9ca3af"/>'
        )

    for s0, t0, s1, t1, kind, label in arrows:
        x0, y0, x1, y1 = X(s0), Y(t0), X(s1), Y(t1)
        c = colors[kind]
        dash = ' stroke-dasharray="5,4"' if kind == "lost" else ""
        if kind == "lost":
            # Stop halfway and mark the drop.
            x1 = x0 + (x1 - x0) * 0.55
            y1 = y0 + (y1 - y0) * 0.55
            parts.append(
                f'<line x1="{x0}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" stroke="{c}" '
                f'stroke-width="1.6"{dash}/>'
            )
            parts.append(
                f'<text x="{x1:.1f}" y="{y1 + 4:.1f}" text-anchor="middle" font-size="14" '
                f'fill="{c}" font-weight="700">✗</text>'
            )
        else:
            parts.append(
                f'<line x1="{x0}" y1="{y0:.1f}" x2="{x1}" y2="{y1:.1f}" stroke="{c}" '
                f'stroke-width="1.8" marker-end="url(#ah-{kind})"/>'
            )
        mx, my = (x0 + x1) / 2, (y0 + y1) / 2
        parts.append(
            f'<text x="{mx:.1f}" y="{my - 4:.1f}" text-anchor="middle" font-size="11" '
            f'font-weight="600" fill="{c}">{html.escape(label)}</text>'
        )

    for side, t, glyph, color, label in markers:
        x = X(side) + (14 if side == "S" else -14)
        parts.append(
            f'<text x="{x}" y="{Y(t) + 5:.1f}" text-anchor="middle" font-size="13" fill="{color}">'
            f"{glyph}<title>{html.escape(label)}</title></text>"
        )

    parts.append("</svg>")
    legend = (
        '<div class="swp-legend" style="margin-top:4px">'
        f'<span style="background:#dbeafe;color:{colors["data"]}">data</span>'
        f'<span style="background:#dcfce7;color:{colors["ack"]}">ACK</span>'
        f'<span style="background:#fee2e2;color:{colors["lost"]}">lost ✗</span>'
        f'<span style="background:#ffedd5;color:{colors["corrupt"]}">corrupted</span>'
        '<span style="background:#fef3c7;color:#b45309">⏰ timeout</span></div>'
    )
    return "".join(parts) + legend


def events_dataframe(events: list[dict]) -> pd.DataFrame:
    rows = [
        {
            "#": e["event_id"],
            "Time": e["time"],
            "Event": f"{EVENT_ICONS.get(e['event_type'], '•')} {e['event_type']}",
            "Seq": e["seq_num"],
            "Description": e["description"],
        }
        for e in events
    ]
    df = pd.DataFrame(rows, columns=["#", "Time", "Event", "Seq", "Description"])
    df["Seq"] = df["Seq"].astype("Int64")  # nullable int: no mixed types
    return df


def render_metrics(sim) -> None:
    m = sim.get_metrics()
    c = st.columns(6)
    c[0].metric("Delivered", f"{m['unique_delivered']} / {m['total_intended']}")
    c[1].metric("Delivery rate", f"{m['delivery_rate']:.1f}%")
    c[2].metric("Efficiency", f"{m['transmission_efficiency']:.1f}%")
    c[3].metric("TX attempts", m["total_tx_attempts"])
    c[4].metric("Retransmissions", m["total_retransmissions"])
    c[5].metric("Sim time", f"{m['simulation_time']:.1f}")
    c = st.columns(6)
    c[0].metric("Packets lost", m["total_pkt_lost"])
    c[1].metric("Corrupted", m["total_pkt_corrupted"])
    c[2].metric("ACKs sent", m["total_ack_sent"])
    c[3].metric("ACKs lost", m["total_ack_lost"])


def render_status(sim) -> None:
    if sim.failed:
        st.error("🛑 Simulation failed: a packet exceeded the retry limit.")
    elif sim.done:
        st.success("🏁 Simulation complete: all packets delivered.")
    else:
        st.info(f"Running · {st.session_state.steps} events processed.")


def render_view(container, sim) -> None:
    """Render the whole simulator view into a container (used for live play)."""
    with container.container():
        render_status(sim)
        left, right = st.columns([3, 2], gap="large")
        with left:
            st.subheader("Windows")
            render_windows(sim.get_state())
            st.subheader("Metrics")
            render_metrics(sim)
        with right:
            st.subheader("Sequence diagram")
            st.markdown(sequence_diagram(sim.get_events()), unsafe_allow_html=True)
        st.subheader("Event log")
        events = sim.get_events()
        df = events_dataframe(events)
        st.dataframe(df.iloc[::-1], width="stretch", height=300, hide_index=True)
        st.download_button(
            "⬇️ Download event log (CSV)",
            df.to_csv(index=False).encode(),
            file_name="events.csv",
            mime="text/csv",
            key=f"dl-{len(events)}-{time.time_ns()}",
        )


# ---------------------------------------------------------------------------
# Simulator tab
# ---------------------------------------------------------------------------


def simulator_tab(cfg: dict) -> None:
    proto_name = "Go-Back-N" if cfg["protocol"] == "GBN" else "Selective Repeat"
    st.markdown(f"### {proto_name} simulation")

    c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 2])
    step_1 = c1.button("⏭️ Step", width="stretch", disabled=sim_finished())
    step_5 = c2.button("⏩ Step ×5", width="stretch", disabled=sim_finished())
    run_all = c3.button("🏃 Run to end", width="stretch", disabled=sim_finished())
    reset = c4.button("🔄 Reset", width="stretch")
    speed = c5.select_slider(
        "Play speed", options=["Slow", "Medium", "Fast"], value="Medium",
        label_visibility="collapsed",
    )
    play = st.button("▶️ Play (animate)", disabled=sim_finished())

    if reset:
        st.session_state.sim = create_simulation(cfg)
        st.session_state.steps = 0
    if step_1:
        do_steps(1)
    if step_5:
        do_steps(5)
    if run_all:
        do_steps(10_000)

    view = st.empty()
    sim = st.session_state.sim

    if play:
        delay = {"Slow": 0.8, "Medium": 0.35, "Fast": 0.08}[speed]
        while not sim_finished():
            do_steps(1)
            render_view(view, sim)
            time.sleep(delay)

    render_view(view, sim)


# ---------------------------------------------------------------------------
# Compare tab
# ---------------------------------------------------------------------------


def compare_tab(cfg: dict) -> None:
    st.markdown("### Go-Back-N vs Selective Repeat")
    st.caption("Both protocols run with the current sidebar channel settings and seed.")

    runs = st.slider("Runs to average (different seeds)", 1, 50, 10)
    base_seed = cfg["seed"] if cfg["seed"] is not None else 0

    rows = []
    for proto in ("GBN", "SR"):
        agg = {"retx": 0, "att": 0, "eff": 0.0, "time": 0.0, "done": 0, "lost": 0}
        for r in range(runs):
            sim = create_simulation({**cfg, "protocol": proto, "seed": base_seed + r})
            sim.run()
            m = sim.get_metrics()
            agg["retx"] += m["total_retransmissions"]
            agg["att"] += m["total_tx_attempts"]
            agg["eff"] += m["transmission_efficiency"]
            agg["time"] += m["simulation_time"]
            agg["lost"] += m["total_pkt_lost"]
            agg["done"] += 1 if m["completed"] else 0
        rows.append({
            "Protocol": "Go-Back-N" if proto == "GBN" else "Selective Repeat",
            "Avg TX attempts": agg["att"] / runs,
            "Avg retransmissions": agg["retx"] / runs,
            "Avg efficiency (%)": agg["eff"] / runs,
            "Avg sim time": agg["time"] / runs,
            "Avg packets lost": agg["lost"] / runs,
            "Completed runs": f"{agg['done']}/{runs}",
        })

    df = pd.DataFrame(rows).set_index("Protocol")
    st.dataframe(df.style.format({
        "Avg TX attempts": "{:.1f}", "Avg retransmissions": "{:.1f}",
        "Avg efficiency (%)": "{:.1f}", "Avg sim time": "{:.1f}",
        "Avg packets lost": "{:.1f}",
    }), width="stretch")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Avg efficiency (%)**")
        st.bar_chart(df["Avg efficiency (%)"])
    with c2:
        st.markdown("**Avg retransmissions**")
        st.bar_chart(df["Avg retransmissions"])
    with c3:
        st.markdown("**Avg simulation time**")
        st.bar_chart(df["Avg sim time"])

    st.markdown("#### Efficiency vs packet loss")
    losses = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
    sweep = {"Packet loss (%)": [int(p * 100) for p in losses]}
    for proto, label in (("GBN", "Go-Back-N"), ("SR", "Selective Repeat")):
        vals = []
        for p in losses:
            effs = []
            for r in range(max(3, min(runs, 15))):
                s = create_simulation({**cfg, "protocol": proto, "pkt_loss_prob": p,
                                       "seed": base_seed + r})
                s.run()
                effs.append(s.get_metrics()["transmission_efficiency"])
            vals.append(sum(effs) / len(effs))
        sweep[label] = vals
    st.line_chart(pd.DataFrame(sweep).set_index("Packet loss (%)"))
    st.caption("Selective Repeat retransmits only the missing packets, so its efficiency "
               "degrades more slowly than Go-Back-N as loss increases.")


# ---------------------------------------------------------------------------
# About tab
# ---------------------------------------------------------------------------


def about_tab() -> None:
    st.markdown(
        """
### How it works
This is a discrete-event simulation: no real-time sleeping, every event has a simulated timestamp.

**Go-Back-N** — receiver window is 1, out-of-order packets are discarded, ACKs are cumulative,
and a timeout retransmits every outstanding packet from the timed-out one onward.

**Selective Repeat** — receiver window equals sender window, out-of-order packets are buffered,
each packet is ACKed individually, and a timeout retransmits only that packet.

### Using the GUI
* Tune the channel in the sidebar; changing any setting restarts the simulation.
* **Step / Step ×5 / Run to end** advance the simulation; **Play** animates it.
* A fixed seed makes runs reproducible, so you can compare protocols on identical conditions.
* The **Compare** tab averages both protocols over several seeds and sweeps packet loss.
        """
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    st.title("🪟 Sliding Window Protocol Simulator")
    st.caption("Go-Back-N and Selective Repeat ARQ, step by step.")

    cfg = sidebar_config()
    ensure_sim(cfg)

    tab_sim, tab_cmp, tab_about = st.tabs(["Simulator", "Compare GBN vs SR", "About"])
    with tab_sim:
        simulator_tab(cfg)
    with tab_cmp:
        compare_tab(cfg)
    with tab_about:
        about_tab()


main()
