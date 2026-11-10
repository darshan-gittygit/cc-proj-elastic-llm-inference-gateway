"""
Phase 7 test: prewarm none vs warm_pool through the real gateway (stand-in containers,
2 s simulated model load; SIMULATED timings).

The same burst runs once per mode; only `prewarm` differs. It checks what demo 3 measures:
time from the scale-up trigger to the first request dispatched to the new replica.
"""
from __future__ import annotations

import re
import threading

import pytest

from helpers import GatewayProc, content_of, sim_text, stream_request, wait_for

SCALE = dict(max_replicas=2, scale_up_threshold=2.0, scale_up_after_s=1.0, scale_down_threshold=1.0,
             scale_down_cooldown_s=30.0)


def trigger_to_first_dispatch(log: str) -> tuple[str, float, float]:
    m = re.search(r"mono=([\d.]+) [^\n]*\[SCALE\] scale up how=(\w+) replica=(r\d+)", log)
    assert m, log
    t_trig, how, rid = float(m.group(1)), m.group(2), m.group(3)
    d = re.search(r"mono=([\d.]+) [^\n]*\[ROUTE\] dispatch req=\d+ replica=" + rid + " ", log)
    assert d, log
    routable = re.search(r"mono=([\d.]+) [^\n]*(\[HEALTH\] replica ready replica=" + rid +
                         r" state=ready|\[SCALE\] spare promoted replica=" + rid + ")", log)
    assert routable and float(routable.group(1)) <= float(d.group(1))   # never before READY / promotion
    return how, float(d.group(1)) - t_trig, t_trig


def run_burst(tmp_path, prewarm: str) -> tuple[str, float]:
    gw = GatewayProc(tmp_path, n_ready=1, prewarm=prewarm, **SCALE)
    try:
        if prewarm == "warm_pool":
            wait_for(lambda: "state=warm" in gw.log(), 30, "spare loaded")
        results = [None] * 8

        def one(i):
            results[i] = stream_request(gw.url, max_tokens=80, content=f"burst {i}")[0]
        ts = [threading.Thread(target=one, args=(i,)) for i in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(60)
        assert all(r and content_of(r) == sim_text(80) for r in results)
        how, gap, _ = trigger_to_first_dispatch(gw.log())
        return how, gap
    finally:
        gw.stop()


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_warm_pool_serves_on_the_new_replica_sooner_than_none(tmp_path):
    (tmp_path / "none").mkdir()
    (tmp_path / "warm").mkdir()
    how_none, gap_none = run_burst(tmp_path / "none", "none")
    how_warm, gap_warm = run_burst(tmp_path / "warm", "warm_pool")
    assert how_none == "start_new" and how_warm == "promote_spare"
    assert gap_none >= 1.5            # waits for the stand-in's 2 s "model load"
    assert gap_warm < 0.5             # the spare was already loaded
    assert gap_warm < gap_none
