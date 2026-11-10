"""
Phase 6 tests: autoscaler and warm pool (ARCHITECTURE.md decisions 7, 8, 12).

- Unit tests drive LoadAutoscaler.tick() with an explicit clock, a fake manager and a fake
  queue length. A started replica stays STARTING until the test "loads" it.
- End to end: stand-in containers, small thresholds so a short burst is enough.
  SIMULATED timings.
"""
from __future__ import annotations

import asyncio
import re
import threading
import time

import pytest

from gateway.autoscaler import LoadAutoscaler
from gateway.interfaces import Config, EventLog, Replica, ReplicaPool, ReplicaState
from helpers import GatewayProc, content_of, sim_text, stream_request, wait_for

R, S, W, D = ReplicaState.READY, ReplicaState.STARTING, ReplicaState.WARM_SPARE, ReplicaState.DRAINING


class FakeQueue:
    def __init__(self):
        self.n = 0

    def __len__(self):
        return self.n


class FakeManager:
    def __init__(self, pool: ReplicaPool):
        self.pool, self.events, self._n, self._spare = pool, [], 0, set()

    async def start_replica(self, *, as_spare: bool = False) -> Replica:
        self._n += 1
        r = Replica(id=f"n{self._n}", container_id="x", host="h", port=1, state=S)
        self.pool.add(r)
        if as_spare:
            self._spare.add(r.id)
        self.events.append(("start_spare" if as_spare else "start", r.id))
        return r

    async def promote_spare(self):
        spares = self.pool.by_state(W)
        if not spares:
            return None
        spares[0].state = R
        self.events.append(("promote", spares[0].id))
        return spares[0]

    async def stop_replica(self, r: Replica, drain_timeout_s: float = 30.0) -> None:
        r.state = D
        self.events.append(("drain", r.id))
        self.pool.remove(r.id)

    def load_all(self):
        """Every STARTING replica finishes loading."""
        for r in self.pool.by_state(S):
            r.state = W if r.id in self._spare else R


def setup(replicas, **cfg):
    pool = ReplicaPool()
    for rid, state, in_flight in replicas:
        pool.add(Replica(id=rid, container_id=rid, host="h", port=1, state=state, in_flight=in_flight))
    q, mgr = FakeQueue(), FakeManager(pool)
    base = dict(min_replicas=1, max_replicas=3, scale_up_threshold=4.0, scale_down_threshold=1.0,
                scale_up_after_s=5.0, scale_down_cooldown_s=30.0, prewarm="none", warm_pool_size=1,
                prewarm_trigger_ratio=0.7)
    base.update(cfg)
    a = LoadAutoscaler(Config(**base), pool, q, mgr, EventLog("test-autoscaler"))
    return a, pool, q, mgr


def run(a, start: float, end: float, step: float = 1.0):
    async def go():
        t = start
        while t <= end + 1e-9:
            await a.tick(t)
            await asyncio.sleep(0)                  # let a drain task started by tick() run
            t += step
    asyncio.run(go())


def test_scale_up_only_after_signal_stays_high_for_scale_up_after_s():
    a, pool, q, mgr = setup([("r1", R, 2)])
    q.n = 10                                        # (10 + 2) / 1 ready = 12
    run(a, 0, 4)
    assert mgr.events == []                         # above the threshold for 4 s only
    run(a, 5, 5)
    assert mgr.events == [("start", "n1")]
    assert pool.get("n1").state == S and not pool.get("n1").routable   # not routed before READY


def test_one_tick_spike_is_smoothed_away():
    a, pool, q, mgr = setup([("r1", R, 0)], scale_up_after_s=0)
    q.n = 7
    run(a, 0, 0)                                    # raw 7 -> smoothed 3.5 < 4
    q.n = 0
    run(a, 1, 20)
    assert a.signal < 4 and mgr.events == []


def test_no_second_start_while_one_is_starting_and_never_above_max():
    a, pool, q, mgr = setup([("r1", R, 2)], scale_up_after_s=1)
    q.n = 20
    run(a, 0, 10)
    assert mgr.events == [("start", "n1")]          # n1 still loading: nothing else starts
    mgr.load_all()
    run(a, 11, 20)
    assert mgr.events == [("start", "n1"), ("start", "n2")]
    mgr.load_all()
    run(a, 21, 60)
    assert len(pool.all()) == 3                     # max_replicas = 3
    assert mgr.events[-1] == ("start", "n2")


def test_warm_pool_promotes_spare_instantly_then_starts_a_new_spare():
    a, pool, q, mgr = setup([("r1", R, 2), ("s1", W, 0)], prewarm="warm_pool", scale_up_after_s=1)
    q.n = 10
    run(a, 0, 1)
    assert mgr.events[0] == ("promote", "s1") and pool.get("s1").routable
    assert mgr.events[1] == ("start_spare", "n1")   # replenished in the background (load trigger)
    assert pool.get("n1").state == S and not pool.get("n1").routable
    mgr.load_all()
    assert pool.get("n1").state == W and not pool.get("n1").routable    # a spare is never routed to


def test_spare_started_at_idle_or_under_load_but_not_in_between():
    a, pool, q, mgr = setup([("r1", R, 0)], prewarm="warm_pool", min_replicas=1)
    q.n = 2                                         # signal ~2: between 1 (idle) and 0.7 * 4 = 2.8
    run(a, 0, 10)
    assert mgr.events == []
    q.n = 0                                         # idle
    run(a, 11, 20)
    assert mgr.events == [("start_spare", "n1")]


def test_prewarm_none_never_starts_spares():
    a, pool, q, mgr = setup([("r1", R, 0)], prewarm="none")
    run(a, 0, 100)
    assert mgr.events == []


def test_scale_down_after_cooldown_drains_newest_idle_and_keeps_min():
    a, pool, q, mgr = setup([("r1", R, 0), ("r2", R, 0), ("r3", R, 0)])
    run(a, 0, 29)
    assert mgr.events == []
    run(a, 30, 30)
    assert mgr.events == [("drain", "r3")]
    run(a, 31, 59)
    assert mgr.events == [("drain", "r3")]          # the cooldown restarts after each step
    run(a, 60, 200)
    assert mgr.events == [("drain", "r3"), ("drain", "r2")] and [r.id for r in pool.all()] == ["r1"]


def test_scale_down_prefers_the_idle_replica_and_never_drains_a_spare():
    a, pool, q, mgr = setup([("r1", R, 0), ("r2", R, 1), ("s1", W, 0)], prewarm="warm_pool",
                            scale_down_threshold=1.0)
    run(a, 0, 30)                                    # signal 1/2 = 0.5 < 1
    assert ("drain", "r1") in mgr.events and all(e[1] != "s1" for e in mgr.events)


def test_load_between_thresholds_changes_nothing():
    a, pool, q, mgr = setup([("r1", R, 1), ("r2", R, 1)])
    q.n = 3                                          # (3 + 2) / 2 = 2.5, between 1 and 4
    run(a, 0, 300)
    assert mgr.events == []


def test_burst_then_idle_scales_up_then_down_without_flapping():
    """60 s burst, then 200 s idle; replicas load 2 s after being started."""
    a, pool, q, mgr = setup([("r1", R, 0)], prewarm="warm_pool")

    async def go():
        for t in range(0, 261):
            q.n = 12 if t < 60 else 0
            for r in pool.ready():
                r.in_flight = 2 if t < 60 else 0
            if t % 2 == 0:
                mgr.load_all()
            await a.tick(float(t))
            await asyncio.sleep(0)
    asyncio.run(go())
    kinds = [k for k, _ in mgr.events if k in ("start", "promote", "drain")]
    ups = [i for i, k in enumerate(kinds) if k in ("start", "promote")]
    downs = [i for i, k in enumerate(kinds) if k == "drain"]
    assert ups and downs and max(ups) < min(downs), mgr.events      # all ups, then all downs
    assert len(pool.ready()) == 1                                   # back to min_replicas


# --------------------------------------------------------------------------- #
# End to end (stand-in containers, SIMULATED timings)
# --------------------------------------------------------------------------- #
def mono_of(log: str, pattern: str) -> float:
    m = re.search(r"mono=([\d.]+) [^\n]*" + pattern, log)
    assert m, pattern
    return float(m.group(1))


def burst(url: str, n: int, max_tokens: int):
    results = [None] * n

    def one(i):
        results[i] = stream_request(url, max_tokens=max_tokens, content=f"burst {i}")[0]
    ts = [threading.Thread(target=one, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    return results, ts


SCALE = dict(max_replicas=2, scale_up_threshold=2.0, scale_up_after_s=1.0, scale_down_threshold=1.0,
             scale_down_cooldown_s=3.0)


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_burst_starts_a_replica_that_gets_requests_only_once_ready(tmp_path):
    gw = GatewayProc(tmp_path, n_ready=1, prewarm="none", **SCALE)
    try:
        results, ts = burst(gw.url, 8, 80)            # 8 x 4 s on 1 replica x 2 slots
        for t in ts:
            t.join(60)
        assert all(r and content_of(r) == sim_text(80) for r in results)
        log = gw.log()
        t_start = mono_of(log, r"\[SCALE\] scale up how=start_new replica=r2")
        t_ready = mono_of(log, r"\[HEALTH\] replica ready replica=r2 state=ready")
        t_first = mono_of(log, r"\[ROUTE\] dispatch req=\d+ replica=r2")
        assert t_start < t_ready <= t_first           # never routed before READY
    finally:
        gw.stop()


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_warm_spare_is_promoted_on_burst_and_idle_drains_back(tmp_path):
    gw = GatewayProc(tmp_path, n_ready=1, prewarm="warm_pool", **SCALE)
    try:
        wait_for(lambda: "[HEALTH] replica ready replica=r2 state=warm" in gw.log(), 30, "spare r2 loaded")
        assert "trigger=idle" in gw.log()
        results, ts = burst(gw.url, 8, 80)
        for t in ts:
            t.join(60)
        assert all(r and content_of(r) == sim_text(80) for r in results)
        wait_for(lambda: "[SCALE] replica stopped replica=" in gw.log(), 30, "idle scale-down")
        log = gw.log()
        t_promote = mono_of(log, r"\[SCALE\] scale up how=promote_spare replica=r2")
        t_first = mono_of(log, r"\[ROUTE\] dispatch req=\d+ replica=r2")
        assert t_promote <= t_first                   # the spare got no request before promotion
        assert re.search(r"\[SCALE\] scale down replica=r[12] ", log)
        assert re.search(r"\[SCALE\] replica stopped replica=r[12] drained=True", log)
    finally:
        gw.stop()
