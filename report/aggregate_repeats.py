"""
aggregate_repeats.py - per-run values and n / mean / range for the repeated real-model runs.

Reads only report/evidence/repeats/*_real_run<k>.txt (unedited demo-script output, first line
"# REAL MODEL RESULT"); nothing is estimated. Usage (project root):
    python report/aggregate_repeats.py > report/evidence/repeats/SUMMARY.txt
    python report/aggregate_repeats.py --plots     # also writes report/plots/repeats_*.png
The printed summary and the plots are drawn from the same parsed values.
Small n (3 runs): mean and range (min-max) are descriptive only; no significance is claimed.
"""
from __future__ import annotations

import argparse
import re
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "report" / "evidence" / "repeats"
PLOTS = ROOT / "report" / "plots"
POOLS = ("fixed1", "fixed3", "autoscale")
POOL_COLS = ["first token mean", "first token max", "complete mean", "makespan", "idle containers",
             "peak READY", "peak containers", "container-s in burst"]
SCHED = ("A_fcfs", "B_spf", "C_aging", "D_no_aging")


# --------------------------------------------------------------------------- #
# Parsing (one function per evidence format; each returns lists ordered by run)
# --------------------------------------------------------------------------- #
def runs(prefix: str) -> list[tuple[int, str]]:
    out = []
    for f in sorted(DIR.glob(f"{prefix}_run*.txt")):
        text = f.read_text()
        if not text.startswith("# REAL MODEL RESULT"):
            sys.exit(f"refusing {f.name}: not a REAL MODEL RESULT")
        out.append((int(re.search(r"_run(\d+)\.txt$", f.name).group(1)), text))
    return out


def section(text: str, start: str, end: str = "################") -> str:
    i = text.index(start)
    j = text.find(end, text.index("\n", i))       # skip the rest of the header line
    return text[i:j if j != -1 else None]


def checks(text: str) -> tuple[int, int]:
    return len(re.findall(r"^PASS", text, re.M)), len(re.findall(r"^FAIL", text, re.M))


def parse_demo3() -> dict:
    rs = runs("phase7_prewarm_lifecycle_real")
    d = {"runs": [k for k, _ in rs], "checks": [checks(t) for _, t in rs]}
    for mode in ("none", "warm_pool"):
        m = {k: [] for k in ("trig_rout", "trig_tok", "load", "ft", "ftmax", "cm", "mk", "down1", "back", "idle")}
        for _, t in rs:
            s = section(t, f"run prewarm: {mode} ")
            up = re.search(r"scale-up 1: how=\w+ replica=r\d+ trigger at [+-][\d.]+ s \| routable \+\s*(-?[\d.]+) \| "
                           r"first dispatch \+\s*(-?[\d.]+) \| first token \+\s*(-?[\d.]+) \| replica load_s=([\d.]+)", s)
            m["trig_rout"].append(abs(float(up.group(1)))); m["trig_tok"].append(float(up.group(3)))
            m["load"].append(float(up.group(4)))
            ft = re.search(r"first token: mean ([\d.]+)\s+median [\d.]+\s+max ([\d.]+)", s)
            m["ft"].append(float(ft.group(1))); m["ftmax"].append(float(ft.group(2)))
            m["cm"].append(float(re.search(r"complete:\s+mean ([\d.]+)", s).group(1)))
            m["mk"].append(float(re.search(r"makespan \(first arrival -> last completion\): ([\d.]+)", s).group(1)))
            last = float(re.search(r"last burst request complete at \+([\d.]+)", s).group(1))
            m["down1"].append(float(re.search(r"scale down r\d+ at \+([\d.]+)", s).group(1)) - last)
            m["back"].append(float(re.search(r"back at min_replicas=\d+ READY at \+([\d.]+)", s).group(1)) - last)
            m["idle"].append(float(re.search(r"final pool: .* total=(\d+)", s).group(1)))
        d[mode] = m
    return d


def parse_pools() -> dict:
    rs = runs("pool_comparison_real")
    d = {"runs": [k for k, _ in rs], "checks": [checks(t) for _, t in rs]}
    for pool in POOLS:
        vals = {c: [] for c in POOL_COLS}
        for _, t in rs:
            row = re.search(rf"^\s+{pool}\s+(.*)$", section(t, "comparison (one run per pool)"), re.M).group(1).split()
            for c, v in zip(POOL_COLS, row):
                vals[c].append(float(v))
        d[pool] = vals
    return d


def parse_demo1() -> dict:
    rs = runs("phase3_scheduling_real")
    d = {"runs": [k for k, _ in rs], "checks": [checks(t) for _, t in rs]}
    for name in SCHED:
        waits, orders, split, shorts, gaps = [], [], [], [], []
        for _, t in rs:
            s = section(t, f"run {name} ")
            gaps.append(selection_interval(s))
            orders.append(re.search(r"== service order.*\n\s+(.*)", s).group(1).strip())
            waits.append(float(re.search(r"^\s+\d+\s+L\s+\d+\s+[\d.]+\s+[\d.]+\s+([\d.]+)", s, re.M).group(1)))
            sw = [float(x) for x in re.findall(r"^\s+\d+\s+S\d+\s+\d+\s+[\d.]+\s+[\d.]+\s+([\d.]+)", s, re.M)]
            shorts.append(statistics.mean(sw))
            b = re.search(r"shorts served before L: (\d+), after L: (\d+)", s)
            if b:
                split.append((int(b.group(1)), int(b.group(2))))
        d[name] = {"waits": waits, "orders": orders, "split": split, "shorts": shorts, "gaps": gaps}
    # the committed single Phase 3 run has the same per-request table: same measure, for comparison
    committed = (ROOT / "report" / "evidence" / "phase3_scheduling_real.txt").read_text()
    d["committed_gaps"] = {name: selection_interval(section(committed, f"run {name} ")) for name in SCHED}
    return d


def selection_interval(s: str) -> float:
    """Mean time between consecutive scheduler selections after the blocker (selected_s column):
    with 1 replica x 1 slot and a non-empty queue this is the replica's time per request."""
    sel = sorted(float(x) for x in re.findall(r"^\s+\d+\s+\S+\s+\d+\s+[\d.]+\s+([\d.]+)\s+[\d.]+", s, re.M))
    return statistics.mean(b - a for a, b in zip(sel[1:], sel[2:]))


def parse_demo2() -> dict:
    rs = runs("phase4_affinity_real")
    d = {"runs": [k for k, _ in rs], "checks": [checks(t) for _, t in rs]}
    for key in ("aff", "rr", "diff", "aff_all", "rr_all", "aff_cache", "rr_cache"):
        d[key] = []
    for _, t in rs:
        s = section(t, "turn-2 TTFT: with vs without affinity")
        for key, label in (("aff", r"session_affinity"), ("rr", r"round_robin \(none\)")):
            m = re.search(label + r"\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+\[([^\]]*)\]", s)
            d[key].append(float(m.group(4)))
            d[key + "_all"] += [float(m.group(i)) for i in (1, 2, 3)]
            d[key + "_cache"].append([int(x) for x in m.group(5).split(",")])
        d["diff"].append(float(re.search(r"difference of means \(round_robin - affinity\): ([+-][\d.]+)", s).group(1)))
    return d


# --------------------------------------------------------------------------- #
# Text summary
# --------------------------------------------------------------------------- #
def fmt(xs: list[float], unit: str = " s") -> str:
    vals = ", ".join(f"{x:.2f}" for x in xs)
    return f"n={len(xs)}  mean {statistics.mean(xs):.2f}{unit}  range {min(xs):.2f}-{max(xs):.2f}{unit}  [{vals}]"


def head(title: str, d: dict) -> None:
    print("\n" + "=" * 100 + f"\n{title}\n" + "=" * 100)
    for k, (p, f) in zip(d["runs"], d["checks"]):
        print(f"  run {k}: PASS {p} / FAIL {f}")


def print_summary(d3: dict, dp: dict, d1: dict, d2: dict) -> None:
    print("# REAL MODEL RESULT - aggregate of repeated runs (report/aggregate_repeats.py)")
    print("# Source: report/evidence/repeats/*_real_run<k>.txt; run order and host load: run_log.txt")
    print("# n = number of runs; mean and range (min-max) are descriptive only, no statistical significance is claimed.")
    print("# Values in [...] are per run, in run order 1, 2, 3.")

    head("Demo 3 / Phase 7 lifecycle: prewarm none vs warm_pool (phase7_prewarm_lifecycle_real_run*.txt)", d3)
    for mode in ("none", "warm_pool"):
        m = d3[mode]
        print(f"\n  [{mode}]  first scale-up of each run")
        print(f"    trigger -> new capacity routable        {fmt(m['trig_rout'])}")
        print(f"    trigger -> first token on new replica   {fmt(m['trig_tok'])}")
        print(f"    load_s of that replica                  {fmt(m['load'])}")
        print(f"  [{mode}]  burst clients (14 x max_tokens 40; arrival -> first token / complete)")
        print(f"    first token mean                        {fmt(m['ft'])}")
        print(f"    first token max                         {fmt(m['ftmax'])}")
        print(f"    complete mean                           {fmt(m['cm'])}")
        print(f"    makespan                                {fmt(m['mk'])}")
        print(f"  [{mode}]  idle phase (s after the last burst request completed)")
        print(f"    first scale-down                        {fmt(m['down1'])}")
        print(f"    back at min_replicas READY              {fmt(m['back'])}")
        print(f"    replica containers at the end (idle)    {fmt(m['idle'], '')}")

    head("Baseline comparison: fixed1 / fixed3 / autoscale, same burst (pool_comparison_real_run*.txt)", dp)
    for pool in POOLS:
        print(f"\n  [{pool}]")
        for c in POOL_COLS:
            print(f"    {c:<22} {fmt(dp[pool][c], ' s' if c in POOL_COLS[:4] else '')}")

    head("Demo 1 / Phase 3 scheduling, 1 replica x 1 slot (phase3_scheduling_real_run*.txt)", d1)
    for name in SCHED:
        r = d1[name]
        same = "identical in all runs" if len(set(r["orders"])) == 1 else "DIFFERS between runs"
        print(f"\n  [{name}]  wait of L {fmt(r['waits'])}")
        print(f"    mean wait of the short requests (per-run mean)   {fmt(r['shorts'])}")
        print(f"    mean interval between selections (s/request)     {fmt(r['gaps'])}"
              f"  | committed single Phase 3 run: {d1['committed_gaps'][name]:.2f} s")
        print(f"    service order {same}: {r['orders'][0]}")
        if r["split"]:
            print(f"    short requests served before / after L, per run: {r['split']}")

    head("Demo 2 / Phase 4 session affinity vs round robin, 2 replicas (phase4_affinity_real_run*.txt)", d2)
    print("\n  turn-2 TTFT (gateway-measured, dispatch -> first token), per-run mean of 3 conversations")
    print(f"    session_affinity        {fmt(d2['aff'])}")
    print(f"    round_robin             {fmt(d2['rr'])}")
    print(f"    difference (rr - aff)   {fmt(d2['diff'])}")
    overlap = "do not overlap" if max(d2["aff_all"]) < min(d2["rr_all"]) else "OVERLAP"
    print(f"  all turn-2 TTFTs (3 runs x 3 conversations): affinity {min(d2['aff_all']):.2f}-{max(d2['aff_all']):.2f} s, "
          f"round_robin {min(d2['rr_all']):.2f}-{max(d2['rr_all']):.2f} s (ranges {overlap})")
    print(f"  turn-2 cache_n per run: affinity {d2['aff_cache']}, round_robin {d2['rr_cache']}")


# --------------------------------------------------------------------------- #
# Plots (every value comes from the parsed evidence above)
# --------------------------------------------------------------------------- #
C_NONE, C_WARM, C_MEAN = "#d62728", "#1f77b4", "#222222"
C_POOL = {"fixed1": "#7f7f7f", "fixed3": "#9467bd", "autoscale": "#1f77b4"}


def _runs_and_mean(ax, x: float, vals: list[float], color: str, runs_: list[int], label_runs: bool = False) -> None:
    """Every run as a numbered dot, the mean as a short black bar, the range as a thin line."""
    offs = [-0.12, 0.0, 0.12][:len(vals)]
    ax.vlines(x, min(vals), max(vals), color=color, lw=1, alpha=0.6)
    ax.scatter([x + o for o in offs], vals, color=color, s=36, zorder=3, edgecolor="white", lw=0.5)
    for o, v, k in zip(offs, vals, runs_):
        ax.annotate(str(k), (x + o, v), textcoords="offset points", xytext=(5, -3), fontsize=6, color="#555555")
    m = statistics.mean(vals)
    ax.hlines(m, x - 0.22, x + 0.22, color=C_MEAN, lw=2, zorder=4, label="mean" if label_runs else None)


def _foot(fig, text: str) -> None:
    fig.text(0.01, 0.01, text, fontsize=7, color="#444444", ha="left", va="bottom")


def _save(fig, plt, name: str) -> str:
    fig.savefig(PLOTS / name, dpi=150, metadata={"Software": None})
    plt.close(fig)
    return name


def plot_pools(dp: dict, plt) -> str:
    panels = [("first token mean", "first-token latency, mean of 14 clients (s)", "First token (arrival -> first token)"),
              ("makespan", "makespan: first arrival -> last completion (s)", "Burst makespan"),
              ("container-s in burst", "container-seconds in the burst window", "Reserved capacity during the burst")]
    fig, axes = plt.subplots(1, 3, figsize=(12, 4.3))
    for ax, (col, ylabel, title) in zip(axes, panels):
        for i, pool in enumerate(POOLS):
            _runs_and_mean(ax, i, dp[pool][col], C_POOL[pool], dp["runs"], label_runs=(i == 0))
        idle = [int(statistics.mean(dp[p]["idle containers"])) for p in POOLS]
        peak = [int(statistics.mean(dp[p]["peak containers"])) for p in POOLS]
        ax.set_xticks(range(3), [f"{p}\nidle {a} / peak {b}" for p, a, b in zip(POOLS, idle, peak)], fontsize=8)
        ax.set(ylabel=ylabel, title=title, xlim=(-0.6, 2.6))
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", alpha=0.3)
    axes[0].legend(fontsize=7, loc="upper right")
    r1 = dp["fixed3"]["makespan"][0]
    axes[1].annotate(f"fixed3 run 1: {r1:.1f} s\n(outlier, kept)", (1 - 0.12, r1), textcoords="offset points",
                     xytext=(18, -4), fontsize=7, arrowprops=dict(arrowstyle="-", color="#666666", lw=0.6))
    fig.suptitle(f"Fixed pool of 1 vs fixed pool of 3 vs autoscaling - REAL MODEL, same 14-client burst, "
                 f"{len(dp['runs'])} runs each (dots = runs, bar = mean)", fontsize=10)
    _foot(fig, "Source: report/evidence/repeats/pool_comparison_real_run*.txt (gateway log, monotonic). Labels: replica "
               "containers while idle / at peak. Container-seconds = reserved CPUs x time, not CPU used.")
    fig.tight_layout(rect=(0, 0.05, 1, 0.94))
    return _save(fig, plt, "repeats_pool_comparison.png")


def plot_prewarm(d3: dict, plt) -> str:
    panels = [("trig_rout", "trigger -> new capacity routable", "Scale-up 1: capacity routable"),
              ("trig_tok", "trigger -> first token on new replica", "Scale-up 1: first token on new replica"),
              ("ft", "first-token latency, mean of 14 clients", "Burst: first token (arrival -> first token)"),
              ("mk", "makespan: first arrival -> last completion", "Burst makespan")]
    fig, axes = plt.subplots(1, 4, figsize=(13, 4.0))
    for ax, (key, ylabel, title) in zip(axes, panels):
        for i, (mode, color) in enumerate((("none", C_NONE), ("warm_pool", C_WARM))):
            _runs_and_mean(ax, i, d3[mode][key], color, d3["runs"], label_runs=(i == 0))
        ax.set_xticks(range(2), [f"none\n(idle {int(statistics.mean(d3['none']['idle']))} container)",
                                 f"warm_pool\n(idle {int(statistics.mean(d3['warm_pool']['idle']))} containers)"],
                      fontsize=8)
        ax.set(ylabel=f"{ylabel} (s)", title=title, xlim=(-0.6, 1.6))
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", alpha=0.3)
        ax.title.set_fontsize(9)
    axes[0].legend(fontsize=7, loc="upper right")
    fig.suptitle(f"Pre-warming: prewarm none vs warm_pool - REAL MODEL, same 14-client burst, full 1 -> 3 -> 1 "
                 f"lifecycle, {len(d3['runs'])} runs each (dots = runs, bar = mean)", fontsize=10)
    loads = d3["none"]["load"] + d3["warm_pool"]["load"]
    _foot(fig, "Source: report/evidence/repeats/phase7_prewarm_lifecycle_real_run*.txt (gateway log, monotonic). "
               f"Replica load_s was {min(loads):.1f}-{max(loads):.1f} s (model file most likely cached); "
               "the scale-up persistence window is 5 s.")
    fig.tight_layout(rect=(0, 0.05, 1, 0.93))
    return _save(fig, plt, "repeats_prewarm.png")


def plot_sched_affinity(d1: dict, d2: dict, plt) -> str:
    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 4.2), gridspec_kw={"width_ratios": [3, 2]})
    names = ["A fcfs", "B spf_aging\nalpha=5", "C spf_aging alpha=5\nshort every 1 s", "D spf_aging alpha=0\nshort every 1 s"]
    for i, run in enumerate(SCHED):
        _runs_and_mean(a, i, d1[run]["waits"], "#2ca02c", d1["runs"], label_runs=(i == 0))
    a.set_xticks(range(4), names, fontsize=7.5)
    a.set(ylabel="queue wait of the long request L (s)", title="Scheduling: wait of L (1 replica x 1 slot)", ylim=(0, None))
    a.grid(axis="y", alpha=0.3)
    a.legend(fontsize=7, loc="upper left")
    for i, (key, label, color) in enumerate((("aff_all", "session_affinity", C_WARM), ("rr_all", "round_robin", C_NONE))):
        vals = d2[key]                                   # 3 runs x 3 conversations, run order
        xs = [i + (j % 3 - 1) * 0.1 for j in range(len(vals))]
        b.scatter(xs, vals, color=color, s=24, alpha=0.8, edgecolor="white", lw=0.5)
        b.hlines(statistics.mean(d2[key[:-4]]), i - 0.25, i + 0.25, color=C_MEAN, lw=2,
                 label="mean of the 3 per-run means" if i == 0 else None)
    b.set_xticks(range(2), ["session_affinity", "round_robin"])
    b.set(ylabel="turn-2 TTFT, dispatch -> first token (s)", title="Session affinity: turn-2 TTFT (sequential turns)",
          xlim=(-0.6, 1.6), ylim=(0, None))
    b.grid(axis="y", alpha=0.3)
    b.legend(fontsize=7, loc="upper left")
    fig.suptitle(f"Scheduling and session affinity - REAL MODEL, {len(d1['runs'])} runs each", fontsize=10)
    _foot(fig, "Sources: report/evidence/repeats/phase3_scheduling_real_run*.txt (left: dots = runs, bar = mean) and "
               "phase4_affinity_real_run*.txt (right: 3 conversations per run, 9 dots per router).")
    fig.tight_layout(rect=(0, 0.05, 1, 0.94))
    return _save(fig, plt, "repeats_scheduling_affinity.png")


def main() -> None:
    p = argparse.ArgumentParser(description="Aggregate the repeated real-model runs")
    p.add_argument("--plots", action="store_true", help="also write report/plots/repeats_*.png")
    args = p.parse_args()
    d3, dp, d1, d2 = parse_demo3(), parse_pools(), parse_demo1(), parse_demo2()
    if args.plots:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        names = [plot_prewarm(d3, plt), plot_pools(dp, plt), plot_sched_affinity(d1, d2, plt)]
        print(f"wrote {', '.join(names)} to {PLOTS}", file=sys.stderr)
    print_summary(d3, dp, d1, d2)


if __name__ == "__main__":
    main()
