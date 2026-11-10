"""
Phase 3 tests: request scheduling (fcfs, spf_aging).

- Scheduler tests are pure: times are passed in explicitly (monotonic seconds).
- Dispatch-loop tests drive the real Gateway dispatch loop with a fake stream
  (no Docker), to check slot accounting and scheduler/router composition.
- Gateway tests run end to end with stand-in replica containers (SIMULATED timings).
"""
from __future__ import annotations

import asyncio
import logging
import re
import threading
import time

import pytest

from gateway.interfaces import (SCHEDULERS, Config, EventLog, Replica, ReplicaState, Request,
                                build_scheduler)
from gateway.scheduler import FCFSScheduler, SPFAgingScheduler
from helpers import GatewayProc, content_of, stream_request

LOG = EventLog("test-sched")


def mk(prompt_tokens: int, t: float) -> Request:
    return Request(messages=[{"role": "user", "content": "x"}], prompt_tokens=prompt_tokens, arrival_ts=t)


def drain(s, now: float) -> list[Request]:
    out = []
    while (r := s.pop(now=now)) is not None:
        out.append(r)
    return out


# --------------------------------------------------------------------------- #
# Schedulers (pure)
# --------------------------------------------------------------------------- #
def test_fcfs_serves_in_arrival_order_regardless_of_length():
    s = FCFSScheduler(LOG)
    reqs = [mk(p, t) for p, t in [(300, 0.0), (5, 1.0), (50, 2.0)]]
    for r in reqs:
        s.push(r)
    assert len(s) == 3
    assert drain(s, now=3.0) == reqs
    assert s.pop(now=3.0) is None and len(s) == 0


def test_spf_without_aging_serves_shortest_first_ties_by_arrival():
    s = SPFAgingScheduler(LOG, aging_alpha=0.0)
    a, b, c, d = mk(30, 0.0), mk(10, 0.1), mk(20, 0.2), mk(10, 0.3)
    for r in (a, b, c, d):
        s.push(r)
    assert drain(s, now=1.0) == [b, d, c, a]


def test_aging_changes_priority_of_waiting_request():
    s = SPFAgingScheduler(LOG, aging_alpha=5.0)
    long = mk(100, 0.0)
    assert s.priority(long, now=0.0) == 100
    assert s.priority(long, now=10.0) == 50            # 100 - 5 * 10 s
    # just after arrival a short request beats it ...
    s.push(long)
    s.push(mk(5, 0.0))
    assert s.pop(now=0.0).prompt_tokens == 5
    # ... but a short request arriving more than (100 - 5) / 5 = 19 s later does not
    late_short = mk(5, 19.5)
    s.push(late_short)
    assert s.priority(long, now=19.5) < s.priority(late_short, now=19.5)
    assert s.pop(now=19.5) is long


def simulate_overload(alpha: float, horizon_s: float = 300.0):
    """One long request (100 tokens) at t=0, then a short one (5 tokens) every 1 s for
    the whole horizon; the server finishes one request every 1.5 s, so the queue never
    empties. Returns (time the long request was served or None, shorts served before it)."""
    s = SPFAgingScheduler(LOG, aging_alpha=alpha)
    long = mk(100, 0.0)
    s.push(long)
    served_before = []
    for k in range(int(horizon_s * 2) + 1):
        t = k * 0.5
        if t == int(t):
            s.push(mk(5, t))
        if k % 3 == 0:                                  # every 1.5 s
            r = s.pop(now=t)
            if r is long:
                return t, served_before
            served_before.append(r)
    return None, served_before


def test_aging_prevents_starvation_under_overload():
    served_at, before = simulate_overload(alpha=5.0)
    bound = (100 - 5) / 5.0                             # 19 s
    assert served_at is not None, "long request starved"
    # every short request served ahead of it arrived within the bound
    assert all(r.arrival_ts <= bound for r in before)
    assert served_at < 60


def test_without_aging_the_long_request_starves():
    served_at, before = simulate_overload(alpha=0.0)
    assert served_at is None and len(before) > 150     # the same workload starves it


def test_requeue_front_beats_everything():
    for s in (FCFSScheduler(LOG), SPFAgingScheduler(LOG, aging_alpha=5.0)):
        first, retried = mk(1, 0.0), mk(500, 5.0)
        s.push(first)
        s.requeue_front(retried)
        assert s.pop(now=6.0) is retried and s.pop(now=6.0) is first


def test_selection_log_explains_the_choice(caplog):
    caplog.set_level(logging.INFO)
    s = SPFAgingScheduler(EventLog("test-sched-log"), aging_alpha=5.0)
    s.push(mk(40, 0.0))
    s.push(mk(8, 1.0))
    s.pop(now=2.0)
    line = [r.getMessage() for r in caplog.records if "selected" in r.getMessage()][0]
    assert re.search(r"\[SCHED\] selected req=\d+ policy=spf_aging prompt_tokens=8 wait_s=1\.000 "
                     r"priority=3\.0 next_best=req\d+:30\.0 still_waiting=1", line), line


def test_registry_and_config_switch():
    assert set(SCHEDULERS) == {"fcfs", "spf_aging"}
    assert isinstance(build_scheduler(Config(scheduler="fcfs"), LOG), FCFSScheduler)
    s = build_scheduler(Config(scheduler="spf_aging", aging_alpha=2.5), LOG)
    assert isinstance(s, SPFAgingScheduler) and s.alpha == 2.5
    with pytest.raises(ValueError, match="unknown scheduler"):
        build_scheduler(Config(scheduler="priority"), LOG)
    with pytest.raises(ValueError):
        SPFAgingScheduler(LOG, aging_alpha=-1)


# --------------------------------------------------------------------------- #
# Dispatch loop + scheduler + router, without Docker
# --------------------------------------------------------------------------- #
class Harness:
    """Real Gateway dispatch loop; the replica stream is replaced by a fake that
    holds each request until finish() is called."""

    def __init__(self, cfg: Config, replicas: list[tuple[str, ReplicaState, int]]):
        from gateway.main import Gateway
        self.gw = Gateway(cfg, EventLog("test-dispatch"))
        for rid, state, in_flight in replicas:
            self.gw.pool.add(Replica(id=rid, container_id=rid, host="h", port=1, state=state,
                                     max_slots=cfg.slots_per_replica, in_flight=in_flight))
        self.dispatched: list[tuple[int, str]] = []
        self._done: dict[int, asyncio.Event] = {}
        self.gw._stream = self._fake_stream

    async def _fake_stream(self, req: Request, replica: Replica) -> None:
        self.dispatched.append((req.id, replica.id))
        assert replica.in_flight <= replica.max_slots
        ev = self._done.setdefault(req.id, asyncio.Event())
        try:
            await ev.wait()
        finally:
            replica.in_flight -= 1
            self.gw._streams.pop(req.id, None)
            self.gw._wake.set()

    def finish(self, req_id: int) -> None:
        self._done.setdefault(req_id, asyncio.Event()).set()

    def submit(self, prompt_tokens: int) -> Request:
        r = Request(messages=[{"role": "user", "content": "x"}], prompt_tokens=prompt_tokens)
        self.gw.submit(r)
        return r

    async def run(self, scenario):
        loop_task = asyncio.create_task(self.gw._dispatch_loop())
        try:
            await scenario(self)
        finally:
            loop_task.cancel()
            for t in list(self.gw._streams.values()):
                t.cancel()


async def settle():
    await asyncio.sleep(0.05)


R = ReplicaState.READY


def test_dispatch_fills_every_free_slot_in_scheduler_order():
    cfg = Config(scheduler="spf_aging", aging_alpha=0.0, router="least_busy", slots_per_replica=2)
    h = Harness(cfg, [("r1", R, 0), ("r2", R, 0)])

    async def scenario(h):
        reqs = {p: h.submit(p) for p in (50, 10, 40, 20, 30, 5)}
        await settle()
        assert [rid for rid, _ in h.dispatched] == [reqs[p].id for p in (5, 10, 20, 30)]
        assert [rep for _, rep in h.dispatched] == ["r1", "r2", "r1", "r2"]   # least_busy spreads
        assert len(h.gw.scheduler) == 2                                       # 40 and 50 wait
        h.finish(reqs[10].id)                                                 # r2 frees a slot
        await settle()
        assert h.dispatched[-1] == (reqs[40].id, "r2")
        assert len(h.gw.scheduler) == 1
    asyncio.run(h.run(scenario))


def test_no_dispatch_while_replicas_full_or_unavailable():
    cfg = Config(scheduler="fcfs", router="round_robin", slots_per_replica=2)
    h = Harness(cfg, [("r1", ReplicaState.STARTING, 0), ("r2", R, 2), ("r3", ReplicaState.DRAINING, 0),
                      ("r4", ReplicaState.WARM_SPARE, 0)])

    async def scenario(h):
        a, b = h.submit(10), h.submit(20)
        await settle()
        assert h.dispatched == [] and len(h.gw.scheduler) == 2
        h.gw.pool.get("r1").state = R                   # replica becomes ready
        h.gw._wake.set()                                # (the manager does this via on_change)
        await settle()
        assert h.dispatched == [(a.id, "r1"), (b.id, "r1")] and len(h.gw.scheduler) == 0
    asyncio.run(h.run(scenario))


@pytest.mark.parametrize("scheduler", ["fcfs", "spf_aging"])
@pytest.mark.parametrize("router", ["round_robin", "least_busy"])
def test_scheduler_picks_request_router_picks_replica(scheduler, router):
    """3 replicas x 1 slot, 5 queued requests: the scheduler alone decides the order of
    requests, the router alone decides the replica, and nothing exceeds a slot."""
    cfg = Config(scheduler=scheduler, aging_alpha=0.0, router=router, slots_per_replica=1)
    h = Harness(cfg, [("r1", R, 0), ("r2", R, 0), ("r3", R, 0)])

    async def scenario(h):
        reqs = [h.submit(p) for p in (40, 30, 20, 10, 5)]
        await settle()
        expected = reqs if scheduler == "fcfs" else sorted(reqs, key=lambda r: r.prompt_tokens)
        assert [rid for rid, _ in h.dispatched] == [r.id for r in expected[:3]]
        assert [rep for _, rep in h.dispatched] == ["r1", "r2", "r3"]
        h.finish(h.dispatched[1][0])                    # r2's request finishes -> only r2 is free
        await settle()
        assert h.dispatched[3] == (expected[3].id, "r2")
    asyncio.run(h.run(scenario))


def test_request_cancelled_while_queued_is_dropped_not_dispatched():
    """A client that disconnects while its request is still queued must not cost a slot later."""
    cfg = Config(scheduler="fcfs", router="least_busy", slots_per_replica=1)
    h = Harness(cfg, [("r1", R, 0)])

    async def scenario(h):
        a, b, c = h.submit(10), h.submit(10), h.submit(10)
        await settle()
        assert h.dispatched == [(a.id, "r1")]           # b and c wait
        h.gw.cancel(b)                                  # b's client goes away while queued
        h.finish(a.id)
        await settle()
        assert h.dispatched == [(a.id, "r1"), (c.id, "r1")]
        assert len(h.gw.scheduler) == 0
    asyncio.run(h.run(scenario))


# --------------------------------------------------------------------------- #
# Through the real gateway (stand-in containers, 1 replica x 1 slot)
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_gateway_drops_request_whose_client_disconnected_while_queued(tmp_path):
    import httpx
    gw = GatewayProc(tmp_path, scheduler="fcfs", slots_per_replica=1)
    try:
        blocker = threading.Thread(target=lambda: stream_request(gw.url, max_tokens=40, content="block"))
        blocker.start()
        time.sleep(0.3)
        body = {"messages": [{"role": "user", "content": "gone"}], "max_tokens": 5, "stream": True}
        with httpx.stream("POST", gw.url, json=body, timeout=10) as r:   # queued behind the blocker
            assert r.status_code == 200
        # connection closed here, while the request is still queued
        events, _ = stream_request(gw.url, max_tokens=3, content="after")
        assert content_of(events) == "sim0 sim1 sim2 "
        blocker.join(30)
        log = gw.log()
        gone_id = re.findall(r"\[SCHED\] queued req=(\d+)", log)[1]
        assert f"[SCHED] dropped (client disconnected while queued) req={gone_id}" in log, log
        assert f"[ROUTE] dispatch req={gone_id} " not in log
        assert len(re.findall(r"\[ROUTE\] dispatch", log)) == 2           # blocker and "after" only
    finally:
        gw.stop()
LONG_PROMPT = "Please summarise the following text in detail. " * 8     # ~100 estimated tokens


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
@pytest.mark.parametrize("scheduler,alpha,expected", [
    ("fcfs", 5.0, ["L", "S1", "S2", "S3"]),
    ("spf_aging", 5.0, ["S1", "S2", "S3", "L"]),       # waits ~2 s: aging is too small to matter
    ("spf_aging", 1000.0, ["L", "S1", "S2", "S3"]),    # strong aging: waiting time dominates
])
def test_gateway_service_order(tmp_path, scheduler, alpha, expected):
    gw = GatewayProc(tmp_path, scheduler=scheduler, aging_alpha=alpha, slots_per_replica=1)
    try:
        labels = ["blocker", "L", "S1", "S2", "S3"]
        prompts = {"blocker": "block", "L": LONG_PROMPT, "S1": "hi", "S2": "hi", "S3": "hi"}
        results: dict[str, list[str]] = {}

        def run(label):
            results[label] = stream_request(gw.url, max_tokens=40 if label == "blocker" else 2,
                                            content=prompts[label])[0]
        threads = []
        for label in labels:
            t = threading.Thread(target=run, args=(label,))
            t.start()
            threads.append(t)
            time.sleep(0.15)                            # fixes the arrival order
        for t in threads:
            t.join(30)
        assert all(results[l] and results[l][-1] == "[DONE]" and content_of(results[l]) for l in labels)

        log = gw.log()
        queued = re.findall(r"\[SCHED\] queued req=(\d+) prompt_tokens=(\d+)", log)
        id_to_label = {rid: labels[i] for i, (rid, _) in enumerate(queued)}
        assert int(queued[1][1]) > 90 and int(queued[2][1]) == 1      # L is long, S* are short
        selected = re.findall(r"\[SCHED\] selected req=(\d+) policy=(\w+)", log)
        assert all(p == scheduler for _, p in selected)
        order = [id_to_label[rid] for rid, _ in selected][1:]          # drop the blocker
        assert order == expected, log
    finally:
        gw.stop()
