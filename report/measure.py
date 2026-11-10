"""
measure.py - Phase 9: measurements and plots from the committed REAL-MODEL evidence.

    python report/measure.py            # writes report/plots/*.png and report/plots/measurements.json
    python report/measure.py --out DIR  # same, into another directory

Inputs are only the committed files under report/evidence/ whose first line is
"# REAL MODEL RESULT" (or the Phase 1 feasibility run, which is the real model by
construction). Nothing under report/evidence/simulated/ is read. No experiment is run.

Provenance, per source (the raw gateway logs were not kept by the demo scripts):
  phase3_scheduling_real.txt   raw [SCHED] log lines (monotonic mono= times)           -> direct from logs
  phase5_fault_real*.txt       timeline the demo script computed from raw log lines     -> derived from logs
  phase4_affinity_real.txt     per-turn table computed from logs + llama-server timings -> derived from logs
  phase6_autoscale_real.txt    5 s samples of [SCALE] signal lines + event timeline     -> derived from logs
  phase7_prewarm_real.txt      per-scale-up table + latency aggregates                  -> derived from logs
  phase1_feasibility_wsl.txt   output of the frozen check_feasibility.sh (wall clock)   -> script output
Every number in measurements.json carries the file it came from and that file's sha256.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
EVID = ROOT / "report" / "evidence"
SOURCES = {
    "phase1": "phase1_feasibility_wsl.txt",
    "phase3": "phase3_scheduling_real.txt",
    "phase4": "phase4_affinity_real.txt",
    "phase5_run1": "phase5_fault_real_run1_early_fault.txt",
    "phase5_run2": "phase5_fault_real.txt",
    "phase6": "phase6_autoscale_real.txt",
    "phase7": "phase7_prewarm_real.txt",
}
REAL_TAG = "REAL MODEL RESULT"
FLOAT = r"[-+]?\d+(?:\.\d+)?"


def read(key: str) -> str:
    path = EVID / SOURCES[key]
    text = path.read_text()
    if "simulated" in path.parts:
        raise ValueError(f"{path} is simulated evidence")
    if key != "phase1" and REAL_TAG not in text.splitlines()[0]:
        raise ValueError(f"{path} is not labelled {REAL_TAG!r}")
    return text


def provenance(key: str) -> dict:
    path = EVID / SOURCES[key]
    return {"file": f"report/evidence/{SOURCES[key]}", "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


# --------------------------------------------------------------------------- #
# Parsers (one per evidence format)
# --------------------------------------------------------------------------- #
def parse_phase1(text: str) -> dict:
    load = float(re.search(r"MODEL LOAD TIME .*?: (" + FLOAT + ") s", text).group(1))
    rows = []
    for m in re.finditer(r"concurrent=(\d+): ttft=\[([^\]]*)\]s\s+per-req tok/s=\[([^\]]*)\]\s+aggregate tok/s=(" + FLOAT + ")", text):
        nums = lambda s: [float(x) for x in s.split(",") if x.strip()]
        rows.append({"concurrent": int(m.group(1)), "ttft_s": nums(m.group(2)),
                     "per_request_chunks_per_s": nums(m.group(3)), "aggregate_chunks_per_s": float(m.group(4))})
    return {"model_load_s": load, "runs": rows}


def parse_phase3(text: str) -> dict:
    """From the raw [SCHED] lines: per request arrival (queued mono), selection (selected mono),
    wait_s as logged by the scheduler, prompt_tokens. Labels follow arrival order per run."""
    runs = {}
    for block in re.split(r"################ run ", text)[1:]:
        name = block.split()[0]
        policy = re.search(r"policy=(\w+) aging_alpha=(" + FLOAT + ")", block)
        labels = re.search(r"== service order.*?\n\s+(.*?)\n", block, re.S).group(1).split(" -> ")
        queued, selected = [], {}
        for m in re.finditer(r"mono=(" + FLOAT + r") \S+ gateway \[SCHED\] queued req=(\d+) prompt_tokens=(\d+)", block):
            queued.append((m.group(2), float(m.group(1)), int(m.group(3))))
        for m in re.finditer(r"mono=(" + FLOAT + r") \S+ gateway \[SCHED\] selected req=(\d+) policy=\w+ "
                             r"prompt_tokens=\d+ wait_s=(" + FLOAT + ")", block):
            selected[m.group(2)] = (float(m.group(1)), float(m.group(3)))
        # labels by arrival: blocker, L, S1, S2, ... (the demo sends them in this order)
        n_short = sum(1 for x in labels if x.startswith("S"))
        arrival_labels = ["blocker", "L"] + [f"S{i}" for i in range(1, n_short + 1)]
        t0 = queued[0][1]
        reqs = []
        for (rid, t_q, ptk), lab in zip(queued, arrival_labels):
            t_s, wait = selected[rid]
            reqs.append({"label": lab, "req": int(rid), "prompt_tokens": ptk, "arrived_s": round(t_q - t0, 3),
                         "selected_s": round(t_s - t0, 3), "wait_s": wait})
        runs[name] = {"policy": policy.group(1), "aging_alpha": float(policy.group(2)),
                      "service_order": labels, "requests": reqs}
    return runs


def parse_phase4(text: str) -> dict:
    out = {}
    for block in re.split(r"################ run ", text)[1:]:
        router = re.search(r"\(router: (\w+)\)", block).group(1)
        convs = []
        for cb in re.split(r"== conversation ", block)[1:]:
            name = cb.split()[0]
            turns = []
            for m in re.finditer(r"^\s+(\d)\s+(\d+)\s+(r\d+)\s+(\S+)\s+(" + FLOAT + r")\s+(\d+)\s+(\d+)\s", cb, re.M):
                turns.append({"turn": int(m.group(1)), "replica": m.group(3), "affinity": m.group(4),
                              "ttft_s": float(m.group(5)), "cache_n": int(m.group(6)), "prompt_n": int(m.group(7))})
            convs.append({"conversation": name, "turns": turns})
        out[router] = convs
    return out


def parse_phase5(text: str) -> dict:
    out = {}
    for block in re.split(r"################ run ", text)[1:]:
        fault = block.split()[0]
        dead = re.search(r"r1 declared DEAD:\s+\+(" + FLOAT + r") s\s+\(reason=(\w+)\)", block)
        ready = re.search(r"replacement r3 READY:\s+\+(" + FLOAT + r") s\s+\(its own load time (" + FLOAT + r") s\)", block)
        pref = re.search(r"prefix check: matched for (\d+) retried request\(s\), differed for (\d+)", block)
        same = re.search(r"final text equals the fault-free reference for (\d+)/(\d+) clients", block)
        sent = [int(x) for x in re.findall(r"retrying req=\d+ replica=r1 reason=\w+ attempt=\S+ tokens_sent=(\d+)", block)]
        out[fault] = {"dead_after_s": float(dead.group(1)), "detected_by": dead.group(2),
                      "replacement_ready_after_s": float(ready.group(1)), "replacement_load_s": float(ready.group(2)),
                      "retried_requests": len(sent), "tokens_already_sent": sorted(sent),
                      "prefix_matched": int(pref.group(1)), "prefix_differed": int(pref.group(2)),
                      "final_equal_reference": f"{same.group(1)}/{same.group(2)}"}
    return out


def parse_phase6(text: str) -> dict:
    burst = re.search(r"burst (" + FLOAT + r") -> (" + FLOAT + r")\)", text)
    samples = [dict(zip(("t", "signal", "queued", "in_flight", "ready", "spares", "starting", "total"),
                        (float(a), float(b), *map(int, rest))))
               for a, b, *rest in re.findall(r"^\s+(" + FLOAT + r")\s+(" + FLOAT + r")\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)$",
                                             text, re.M)]
    events = []
    for m in re.finditer(r"^\s+(" + FLOAT + r")\s+\[SCALE\] (scale up how=\w+|scale down|prewarm spare) replica=(r\d+)", text, re.M):
        events.append({"t": float(m.group(1)), "event": m.group(2).replace(" how=", ":"), "replica": m.group(3)})
    return {"burst_s": [float(burst.group(1)), float(burst.group(2))], "samples": samples, "events": events}


def parse_phase7(text: str) -> dict:
    out = {}
    for block in re.split(r"################ run prewarm: ", text)[1:]:
        mode = block.split()[0]
        ups = []
        for m in re.finditer(r"scale-up (\d+): how=(\w+) replica=(r\d+) trigger at \+?(" + FLOAT + r") s \| routable \+\s*(" + FLOAT +
                             r") \| first dispatch \+\s*(" + FLOAT + r") \| first token \+\s*(" + FLOAT + r") \| replica load_s=(" + FLOAT + ")",
                             block):
            ups.append({"scale_up": int(m.group(1)), "how": m.group(2), "replica": m.group(3),
                        "trigger_after_burst_s": float(m.group(4)),
                        "trigger_to_routable_s": abs(float(m.group(5))) if float(m.group(5)) == 0 else float(m.group(5)),
                        "trigger_to_first_dispatch_s": float(m.group(6)), "trigger_to_first_token_s": float(m.group(7)),
                        "new_replica_load_s": float(m.group(8))})
        ft = re.search(r"first token: mean (" + FLOAT + r")\s+median (" + FLOAT + r")\s+max (" + FLOAT + ")", block)
        cp = re.search(r"complete:\s+mean (" + FLOAT + r")\s+median (" + FLOAT + r")\s+max (" + FLOAT + ")", block)
        mk = re.search(r"makespan .*?: (" + FLOAT + ")", block)
        out[mode] = {"scale_ups": ups,
                     "client_first_token_s": dict(zip(("mean", "median", "max"), map(float, ft.groups()))),
                     "client_complete_s": dict(zip(("mean", "median", "max"), map(float, cp.groups()))),
                     "burst_makespan_s": float(mk.group(1))}
    return out


# --------------------------------------------------------------------------- #
# Plots
# --------------------------------------------------------------------------- #
C1, C2, C3 = "#1f77b4", "#d62728", "#7f7f7f"


def _foot(fig, text: str) -> None:
    fig.text(0.01, 0.01, text, fontsize=7, color="#444444", ha="left", va="bottom")


def _save(fig, out: Path, name: str) -> str:
    path = out / name
    fig.savefig(path, dpi=150, metadata={"Software": None})   # no version/time metadata: reproducible bytes
    plt.close(fig)
    return name


def plot_phase1(d: dict, out: Path) -> str:
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 4))
    n = [r["concurrent"] for r in d["runs"]]
    a.plot(n, [r["aggregate_chunks_per_s"] for r in d["runs"]], "o-", color=C1, label="aggregate")
    for r in d["runs"]:
        a.scatter([r["concurrent"]] * len(r["per_request_chunks_per_s"]), r["per_request_chunks_per_s"],
                  color=C2, marker="x", label="per request" if r["concurrent"] == 1 else None)
    a.set(xlabel="concurrent requests (1 replica: 1 CPU, 2 slots)", ylabel="streamed chunks / s",
          title="Throughput", xticks=n)
    a.legend()
    for r in d["runs"]:
        b.scatter([r["concurrent"]] * len(r["ttft_s"]), r["ttft_s"], color=C1)
    b.set(xlabel="concurrent requests", ylabel="time to first chunk (s)", xticks=n,
          title="TTFT (3rd request at n=3 waits for a slot)")
    fig.suptitle(f"Phase 1 feasibility - REAL MODEL, one run per point (model load {d['model_load_s']:.2f} s)")
    _foot(fig, "Source: report/evidence/phase1_feasibility_wsl.txt (frozen check_feasibility.sh; wall-clock timing; "
               "chunks/s includes the wait for the first chunk)")
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    return _save(fig, out, "phase1_feasibility.png")


def plot_phase3(d: dict, out: Path) -> str:
    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 5.0), gridspec_kw={"width_ratios": [2, 1]})
    for run, color, mk in (("C_aging", C1, "o"), ("D_no_aging", C2, "s")):
        reqs = [r for r in d[run]["requests"] if r["label"] != "blocker"]
        a.scatter([r["arrived_s"] for r in reqs if r["label"] != "L"], [r["wait_s"] for r in reqs if r["label"] != "L"],
                  color=color, marker=mk, alpha=0.6, label=f"{run}: short requests (alpha={d[run]['aging_alpha']:g})")
        L = next(r for r in reqs if r["label"] == "L")
        a.scatter([L["arrived_s"]], [L["wait_s"]], color=color, marker="*", s=220, edgecolor="k",
                  label=f"{run}: long request L ({L['wait_s']:.1f} s)")
    a.set(xlabel="arrival time (s after the first request)", ylabel="queue wait (s)",
          title="Overload: a short request every 1 s, 1 replica x 1 slot")
    a.legend(fontsize=7, loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=2)   # below the axes
    runs = ["A_fcfs", "B_spf", "C_aging", "D_no_aging"]
    waits = [next(r for r in d[x]["requests"] if r["label"] == "L")["wait_s"] for x in runs]
    b.bar(range(4), waits, color=[C3, C3, C1, C2])
    b.set_xticks(range(4), ["A fcfs", "B spf", "C spf\nalpha=5", "D spf\nalpha=0"], fontsize=8)
    b.set(ylabel="wait of the long request L (s)", title="L's wait, per run")
    for i, w in enumerate(waits):
        b.text(i, w, f"{w:.1f}", ha="center", va="bottom", fontsize=8)
    fig.suptitle("Phase 3 scheduling - REAL MODEL, one run per policy (n = 1 long request per run)")
    _foot(fig, "Source: [SCHED] queued/selected log lines (monotonic) in report/evidence/phase3_scheduling_real.txt")
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    return _save(fig, out, "phase3_scheduling.png")


def plot_phase4(d: dict, out: Path) -> str:
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 4))
    for router, color, name in (("session_affinity", C1, "session_affinity"), ("round_robin", C2, "round_robin")):
        for i, conv in enumerate(d[router]):
            t = [x["turn"] for x in conv["turns"]]
            a.plot(t, [x["ttft_s"] for x in conv["turns"]], "o-", color=color, alpha=0.7, label=name if i == 0 else None)
            b.plot(t, [x["cache_n"] for x in conv["turns"]], "o-", color=color, alpha=0.7, label=name if i == 0 else None)
    a.set(xlabel="turn", ylabel="TTFT measured by the gateway (s)", title="Time to first token per turn", xticks=range(1, 6))
    b.set(xlabel="turn", ylabel="prompt tokens reused from cache (cache_n)", title="llama-server prompt cache reuse",
          xticks=range(1, 6))
    a.legend()
    b.legend()
    fig.suptitle("Phase 4 session affinity - REAL MODEL, 2 replicas, 3 conversations per router (one line each)")
    _foot(fig, "Source: per-turn tables in report/evidence/phase4_affinity_real.txt (gateway ttft_s, monotonic; "
               "cache_n from llama-server timings). Turn 2 is the clean comparison.")
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    return _save(fig, out, "phase4_affinity.png")


def plot_phase5(d: dict, out: Path) -> str:
    fig, ax = plt.subplots(figsize=(8, 4))
    labels, dead, ready = [], [], []
    for run in ("run1", "run2"):
        for fault in ("kill", "pause"):
            x = d[run][fault]
            labels.append(f"{fault}\n{run}")
            dead.append(x["dead_after_s"])
            ready.append(x["replacement_ready_after_s"])
    idx = range(len(labels))
    ax.bar([i - 0.2 for i in idx], dead, width=0.4, color=C2, label="fault -> r1 declared DEAD")
    ax.bar([i + 0.2 for i in idx], ready, width=0.4, color=C1, label="fault -> replacement READY")
    for i in idx:
        ax.text(i - 0.2, dead[i], f"{dead[i]:.2f}", ha="center", va="bottom", fontsize=7)
        ax.text(i + 0.2, ready[i], f"{ready[i]:.2f}", ha="center", va="bottom", fontsize=7)
    ax.set_xticks(list(idx), labels)
    ax.set(ylabel="seconds after the fault", title="Phase 5 fault tolerance - REAL MODEL (2 replicas, 4 clients; one run each)")
    ax.legend(fontsize=8)
    _foot(fig, "Source: timelines (monotonic) in report/evidence/phase5_fault_real.txt (run2) and\n"
               "phase5_fault_real_run1_early_fault.txt (run1). kill: detected by the broken stream; "
               "pause: by 3 failed health probes.")
    fig.tight_layout(rect=(0, 0.09, 1, 1))
    return _save(fig, out, "phase5_fault.png")


def plot_phase6(d: dict, out: Path) -> str:
    fig, (a, b) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    t = [s["t"] for s in d["samples"]]
    for ax in (a, b):
        ax.axvspan(*d["burst_s"], color="#ffe9a8", label="burst (14 clients)" if ax is a else None)
        for e in d["events"]:
            if e["event"].startswith("scale"):
                ax.axvline(e["t"], color=C1 if "up" in e["event"] else C2, ls="--", lw=0.8)
    a.plot(t, [s["signal"] for s in d["samples"]], "o-", color="k", ms=3, label="signal (smoothed)")
    a.axhline(4.0, color=C1, lw=0.8, label="scale_up_threshold 4.0")
    a.axhline(1.0, color=C2, lw=0.8, label="scale_down_threshold 1.0")
    a.set(ylabel="(queued + in_flight) / READY", title="Load signal (dashed: scale up blue, scale down red)")
    a.legend(fontsize=7, loc="upper right")
    b.step(t, [s["ready"] for s in d["samples"]], where="post", color=C1, label="READY")
    b.step(t, [s["spares"] for s in d["samples"]], where="post", color=C3, label="warm spares")
    b.step(t, [s["total"] for s in d["samples"]], where="post", color="k", ls=":", label="containers (total)")
    b.set(xlabel="seconds since gateway start", ylabel="replicas", yticks=range(0, 4), title="Replicas")
    b.legend(fontsize=7, loc="upper right")
    fig.suptitle("Phase 6 autoscaler - REAL MODEL, config.yaml defaults, one 180 s run")
    _foot(fig, "Source: [SCALE] signal samples (every ~5 s) and event timeline in report/evidence/phase6_autoscale_real.txt")
    fig.tight_layout(rect=(0, 0.04, 1, 0.96))
    return _save(fig, out, "phase6_autoscale.png")


def plot_phase7(d: dict, out: Path) -> str:
    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharey=True)
    metrics = [("trigger_to_routable_s", "routable"), ("trigger_to_first_dispatch_s", "first request\ndispatched"),
               ("trigger_to_first_token_s", "first token")]
    for ax, k in zip(axes, (1, 2)):
        for j, (mode, color) in enumerate((("none", C2), ("warm_pool", C1))):
            up = next(u for u in d[mode]["scale_ups"] if u["scale_up"] == k)
            vals = [up[m] for m, _ in metrics]
            xs = [i + (j - 0.5) * 0.38 for i in range(3)]
            ax.bar(xs, vals, width=0.38, color=color, label=f"prewarm: {mode} ({up['how']})")
            for x, v in zip(xs, vals):
                ax.text(x, v, f"{v:.2f}", ha="center", va="bottom", fontsize=7)
        ax.set_xticks(range(3), [lab for _, lab in metrics])
        ax.set(title=f"scale-up {k}", ylabel="seconds after the scale-up trigger" if k == 1 else None)
        ax.legend(fontsize=7)
    fig.suptitle("Phase 7 pre-warming - REAL MODEL, same burst, one run per mode (new replica load ~2 s)")
    _foot(fig, "Source: per-scale-up table in report/evidence/phase7_prewarm_real.txt (gateway log, monotonic). "
               "Not a statistical comparison.")
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    return _save(fig, out, "phase7_prewarm.png")


def main() -> None:
    p = argparse.ArgumentParser(description="Phase 9 measurements and plots from committed real-model evidence")
    p.add_argument("--out", default=str(ROOT / "report" / "plots"))
    out = Path(p.parse_args().out)
    out.mkdir(parents=True, exist_ok=True)

    data = {
        "phase1_feasibility": parse_phase1(read("phase1")),
        "phase3_scheduling": parse_phase3(read("phase3")),
        "phase4_affinity": parse_phase4(read("phase4")),
        "phase5_fault": {"run1": parse_phase5(read("phase5_run1")), "run2": parse_phase5(read("phase5_run2"))},
        "phase6_autoscale": parse_phase6(read("phase6")),
        "phase7_prewarm": parse_phase7(read("phase7")),
    }
    a4 = data["phase4_affinity"]
    data["phase4_affinity_turn2"] = {
        r: {"ttft_s": [c["turns"][1]["ttft_s"] for c in a4[r]],
            "mean_ttft_s": round(sum(c["turns"][1]["ttft_s"] for c in a4[r]) / len(a4[r]), 3),
            "cache_n": [c["turns"][1]["cache_n"] for c in a4[r]], "n_conversations": len(a4[r])}
        for r in a4}
    plots = [plot_phase1(data["phase1_feasibility"], out), plot_phase3(data["phase3_scheduling"], out),
             plot_phase4(a4, out), plot_phase5(data["phase5_fault"], out),
             plot_phase6(data["phase6_autoscale"], out), plot_phase7(data["phase7_prewarm"], out)]
    doc = {"kind": "REAL MODEL measurements (no simulated evidence used)",
           "sources": {k: provenance(k) for k in SOURCES}, "plots": plots, "data": data}
    (out / "measurements.json").write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n")
    print(f"wrote {len(plots)} plots and measurements.json to {out}")


if __name__ == "__main__":
    main()
