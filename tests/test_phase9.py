"""
Phase 9 tests: report/measure.py reads only real-model evidence, parses it correctly, and
regenerates byte-identical outputs. No experiment is run.
"""
from __future__ import annotations

import filecmp
import importlib.util
import json
import re

import pytest

from helpers import ROOT

pytest.importorskip("matplotlib")
spec = importlib.util.spec_from_file_location("measure", ROOT / "report" / "measure.py")
measure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measure)


def test_only_real_model_evidence_is_read():
    for key, name in measure.SOURCES.items():
        path = measure.EVID / name
        assert path.exists() and "simulated" not in path.parts and "SIMULATED" not in name
        if key != "phase1":
            assert path.read_text().splitlines()[0].startswith("# REAL MODEL RESULT")
    with pytest.raises(ValueError):                      # a simulated file is refused
        old = measure.SOURCES["phase3"]
        measure.SOURCES["phase3"] = "simulated/phase3_scheduling_standin_SIMULATED.txt"
        try:
            measure.read("phase3")
        finally:
            measure.SOURCES["phase3"] = old


def test_parsed_values_match_the_evidence():
    p1 = measure.parse_phase1(measure.read("phase1"))
    assert p1["model_load_s"] == pytest.approx(2.968856780)
    assert [r["aggregate_chunks_per_s"] for r in p1["runs"]] == [3.4, 4.2, 4.5]
    assert p1["runs"][2]["ttft_s"] == [0.4, 0.41, 58.11]

    p4 = measure.parse_phase4(measure.read("phase4"))
    assert [len(p4[r]) for r in ("session_affinity", "round_robin")] == [3, 3]
    assert all(len(c["turns"]) == 5 for r in p4 for c in p4[r])
    assert [c["turns"][1]["ttft_s"] for c in p4["session_affinity"]] == [1.279, 1.697, 1.682]
    assert [c["turns"][1]["ttft_s"] for c in p4["round_robin"]] == [3.372, 3.401, 2.967]

    p5 = measure.parse_phase5(measure.read("phase5_run2"))
    assert (p5["kill"]["dead_after_s"], p5["kill"]["detected_by"]) == (0.66, "stream_error")
    assert (p5["pause"]["replacement_ready_after_s"], p5["pause"]["tokens_already_sent"]) == (9.84, [26, 27])

    p7 = measure.parse_phase7(measure.read("phase7"))
    first = {m: p7[m]["scale_ups"][0] for m in ("none", "warm_pool")}
    assert (first["none"]["how"], first["none"]["trigger_to_first_dispatch_s"]) == ("start_new", 1.59)
    assert (first["warm_pool"]["how"], first["warm_pool"]["trigger_to_first_dispatch_s"]) == ("promote_spare", 0.0)
    assert p7["none"]["burst_makespan_s"] == 81.09 and p7["warm_pool"]["burst_makespan_s"] == 66.23


def test_phase3_log_parse_agrees_with_the_scripts_own_table():
    """The raw [SCHED] lines give the same label, request id and wait as the demo's table."""
    text = measure.read("phase3")
    runs = measure.parse_phase3(text)
    assert set(runs) == {"A_fcfs", "B_spf", "C_aging", "D_no_aging"}
    for name, block in zip(runs, re.split(r"################ run ", text)[1:]):
        table = {int(m.group(1)): (m.group(2), float(m.group(3)))
                 for m in re.finditer(r"^\s+(\d+)\s+(\S+)\s+\d+\s+[\d.]+\s+[\d.]+\s+([\d.]+)", block, re.M)}
        for r in runs[name]["requests"]:
            assert table[r["req"]] == (r["label"], pytest.approx(r["wait_s"], abs=0.006))
    L = {n: next(r for r in runs[n]["requests"] if r["label"] == "L")["wait_s"] for n in runs}
    assert L == pytest.approx({"A_fcfs": 5.25, "B_spf": 10.22, "C_aging": 24.50, "D_no_aging": 27.28}, abs=0.006)


def test_phase6_keeps_every_sample_row():
    text = measure.read("phase6")
    section = text.split("== signal samples")[1].split("== checks ==")[0]
    rows = [l for l in section.splitlines() if re.match(r"^\s+\d", l)]
    p6 = measure.parse_phase6(text)
    assert len(p6["samples"]) == len(rows) and p6["burst_s"] == [9.6, 59.2]
    assert max(s["total"] for s in p6["samples"]) == 3
    assert [e["event"] for e in p6["events"] if e["event"].startswith("scale")] == \
           ["scale up:promote_spare", "scale up:promote_spare", "scale down", "scale down"]


def test_outputs_are_reproducible_from_the_committed_evidence(tmp_path):
    import sys
    for d in ("a", "b"):
        argv, sys.argv = sys.argv, ["measure.py", "--out", str(tmp_path / d)]
        try:
            measure.main()
        finally:
            sys.argv = argv
    names = sorted(p.name for p in (tmp_path / "a").iterdir())
    assert names == sorted(json.loads((tmp_path / "a" / "measurements.json").read_text())["plots"] + ["measurements.json"])
    assert all(filecmp.cmp(tmp_path / "a" / n, tmp_path / "b" / n, shallow=False) for n in names)   # byte-identical
    committed = ROOT / "report" / "plots"
    if committed.exists():                                # the committed outputs match a fresh regeneration
        assert all(filecmp.cmp(tmp_path / "a" / n, committed / n, shallow=False) for n in names)
