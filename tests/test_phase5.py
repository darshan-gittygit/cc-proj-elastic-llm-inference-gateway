"""
Phase 5 tests: fault tolerance (ARCHITECTURE.md decisions 6 and 9).

- HealthMonitor unit tests: fake manager, probe results set by the test.
- Retry tests: the real Gateway dispatch/stream path against in-process mock replicas
  (httpx.MockTransport) that break mid-stream, hang, or regenerate different text. No Docker.
- End to end: stand-in containers, one is killed (docker kill) or frozen (docker pause)
  while busy. SIMULATED timings.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time

import httpx
import pytest

from gateway.health import ProbeHealthMonitor
from gateway.interfaces import Config, EventLog, Replica, ReplicaPool, ReplicaState, Request
from helpers import TEST_PREFIX, GatewayProc, content_of, sim_text, stream_request, wait_for

R = ReplicaState.READY


class FakeManager:
    def __init__(self, pool: ReplicaPool):
        self.pool, self.removed, self.started, self._n = pool, [], [], 10

    async def kill_and_remove(self, r: Replica, reason: str = "dead") -> None:
        self.pool.remove(r.id)
        r.state = ReplicaState.STOPPED
        self.removed.append(r.id)

    async def start_replica(self, *, as_spare: bool = False) -> Replica:
        self._n += 1
        r = Replica(id=f"r{self._n}", container_id="x", host="new", port=1, state=ReplicaState.STARTING)
        self.pool.add(r)
        self.started.append((r.id, as_spare))
        return r


def monitor(pool: ReplicaPool, fail_limit: int = 3):
    dead: list[str] = []

    async def on_dead(r: Replica) -> None:
        dead.append(r.id)
    cfg = Config(health_fail_limit=fail_limit, health_interval_s=0.01)
    mgr = FakeManager(pool)
    return ProbeHealthMonitor(cfg, pool, mgr, EventLog("test-health"), on_dead=on_dead), mgr, dead


def make_pool(*specs) -> ReplicaPool:
    pool = ReplicaPool()
    for rid, state in specs:
        pool.add(Replica(id=rid, container_id=rid, host=rid, port=1, state=state))
    return pool


# --------------------------------------------------------------------------- #
# HealthMonitor
# --------------------------------------------------------------------------- #
def test_dead_after_fail_limit_consecutive_failures_then_replaced():
    pool = make_pool(("r1", R), ("r2", R))
    mon, mgr, dead = monitor(pool)
    healthy = {"r1": False, "r2": True}

    async def probe(r):
        return healthy[r.id]
    mon.probe = probe

    async def scenario():
        await mon.probe_all()
        await mon.probe_all()
        assert pool.get("r1").state == R and pool.get("r1").consecutive_failures == 2 and dead == []
        await mon.probe_all()                                   # 3rd consecutive failure
        assert dead == ["r1"] and mgr.removed == ["r1"] and mgr.started == [("r11", False)]
        assert pool.get("r2").state == R and pool.get("r2").consecutive_failures == 0
    asyncio.run(scenario())


def test_a_successful_probe_resets_the_failure_count():
    pool = make_pool(("r1", R))
    mon, mgr, dead = monitor(pool)
    results = iter([False, False, True, False, False])

    async def probe(r):
        return next(results)
    mon.probe = probe

    async def scenario():
        for _ in range(5):
            await mon.probe_all()
        assert dead == [] and pool.get("r1").consecutive_failures == 2
    asyncio.run(scenario())


def test_spare_is_replaced_by_a_spare_and_draining_is_not_replaced_starting_not_probed():
    pool = make_pool(("r1", ReplicaState.WARM_SPARE), ("r2", ReplicaState.DRAINING), ("r3", ReplicaState.STARTING))
    mon, mgr, dead = monitor(pool, fail_limit=1)
    probed = []

    async def probe(r):
        probed.append(r.id)
        return False
    mon.probe = probe
    asyncio.run(mon.probe_all())
    assert sorted(probed) == ["r1", "r2"]                       # STARTING is not probed
    assert sorted(dead) == ["r1", "r2"] and mgr.started == [("r11", True)]


def test_mark_dead_is_idempotent():
    pool = make_pool(("r1", R))
    mon, mgr, dead = monitor(pool)

    async def scenario():
        r = pool.get("r1")
        await asyncio.gather(mon.mark_dead(r, "stream_error"), mon.mark_dead(r, "health_checks"))
    asyncio.run(scenario())
    assert dead == ["r1"] and mgr.removed == ["r1"] and len(mgr.started) == 1


# --------------------------------------------------------------------------- #
# Retry / resume through the real stream path (mock replicas, no Docker)
# --------------------------------------------------------------------------- #
def sse(objs) -> list[bytes]:
    return [f"data: {json.dumps(o)}\n\n".encode() for o in objs]


def role():
    return {"choices": [{"delta": {"role": "assistant", "content": None}}]}


def tok(t):
    return {"choices": [{"delta": {"content": t}}]}


class Replicas:
    """host -> behaviour. 'tokens' are streamed; 'fail_after' raises a connection error
    after that many tokens; 'hang_after' stops sending after that many tokens."""

    def __init__(self, **behaviour):
        self.b = behaviour
        self.calls: list[str] = []
        self.release = asyncio.Event()

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        self.calls.append(host)
        b = self.b[host]

        async def body():
            for chunk in sse([role()]):
                yield chunk
            for i, t in enumerate(b["tokens"]):
                if i == b.get("fail_after"):
                    raise httpx.ReadError("connection lost")
                if i == b.get("hang_after"):
                    await self.release.wait()
                yield sse([tok(t)])[0]
            yield sse([{"choices": [{"delta": {}, "finish_reason": "stop"}]}])[0]
            yield b"data: [DONE]\n\n"
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=body())


def run_one(replicas: Replicas, max_retries: int = 2, kill_r1_after_s: float | None = None,
            session_id: str | None = None):
    """One request through Gateway (least_busy picks r1 first). Returns (client events, gateway)."""
    from gateway.main import Gateway
    cfg = Config(router="least_busy", scheduler="fcfs", slots_per_replica=1, max_retries=max_retries)
    gw = Gateway(cfg, EventLog("test-retry"))
    for rid in ("r1", "r2"):
        gw.pool.add(Replica(id=rid, container_id=rid, host=rid, port=1, state=R, max_slots=1))
    mgr = FakeManager(gw.pool)
    gw.manager = mgr

    async def scenario():
        gw._client = httpx.AsyncClient(transport=httpx.MockTransport(replicas))
        gw.health = ProbeHealthMonitor(cfg, gw.pool, mgr, gw.log, on_dead=gw._on_replica_dead)
        loop = asyncio.create_task(gw._dispatch_loop())
        req = gw.new_request([{"role": "user", "content": "q"}], 16, session_id)
        gw.submit(req)
        if kill_r1_after_s is not None:                 # the health monitor declares r1 dead
            await asyncio.sleep(kill_r1_after_s)
            await gw.health.mark_dead(gw.pool.get("r1"), reason="health_checks")
        events = []
        while (c := await asyncio.wait_for(req.out.get(), 5)) is not None:
            events.append(json.loads(c[6:]))
        await asyncio.sleep(0.05)                         # let spawned mark_dead tasks finish
        loop.cancel()
        await gw._client.aclose()
        return events
    return asyncio.run(scenario()), gw, mgr


def text(events) -> str:
    return "".join((e.get("choices") or [{}])[0].get("delta", {}).get("content") or ""
                   for e in events if "choices" in e)


TOKS = [f"t{i} " for i in range(8)]


def test_broken_stream_is_retried_and_continues_seamlessly(caplog):
    caplog.set_level("INFO")
    reps = Replicas(r1={"tokens": TOKS, "fail_after": 3}, r2={"tokens": TOKS})
    events, gw, mgr = run_one(reps)
    assert reps.calls == ["r1", "r2"]
    assert text(events) == "".join(TOKS)                  # every token exactly once, in order
    assert sum(1 for e in events if "role" in (e.get("choices") or [{}])[0].get("delta", {})) == 1
    assert not any("llmgw_retry" in e or "error" in e for e in events)
    log = "\n".join(r.getMessage() for r in caplog.records)
    assert "stream failed, retrying" in log and "reason=ReadError" in log
    assert "retry resumed" in log and "skipped_tokens=3 prefix_matched=yes" in log
    assert "replica DEAD replica=r1 reason=stream_error" in log    # decision 9
    assert mgr.removed == ["r1"] and len(mgr.started) == 1


def test_different_regenerated_output_makes_client_restart(caplog):
    caplog.set_level("INFO")
    other = [f"u{i} " for i in range(8)]
    reps = Replicas(r1={"tokens": TOKS, "fail_after": 3}, r2={"tokens": other})
    events, _, _ = run_one(reps)
    i = next(n for n, e in enumerate(events) if "llmgw_retry" in e)
    assert text(events[:i]) == "".join(TOKS[:3])           # what the client had before
    assert events[i]["llmgw_retry"]["discard_previous_output"] is True
    assert text(events[i + 1:]) == "".join(other)          # complete new output after the event
    assert "retry output differs, client told to restart" in "\n".join(r.getMessage() for r in caplog.records)


def test_hung_replica_declared_dead_by_health_monitor_is_retried():
    reps = Replicas(r1={"tokens": TOKS, "hang_after": 4}, r2={"tokens": TOKS})
    events, gw, mgr = run_one(reps, kill_r1_after_s=0.2)
    assert reps.calls == ["r1", "r2"] and text(events) == "".join(TOKS)
    assert mgr.removed == ["r1"]


def test_retries_are_bounded_then_client_gets_an_error():
    reps = Replicas(r1={"tokens": TOKS, "fail_after": 2}, r2={"tokens": TOKS, "fail_after": 2})
    events, _, _ = run_one(reps, max_retries=1)
    assert reps.calls == ["r1", "r2"]                       # 1 retry, then give up
    assert events[-1]["error"]["type"] == "replica_error" and "2 attempt" in events[-1]["error"]["message"]


def test_session_history_gets_the_full_reply_once_after_a_retry():
    reps = Replicas(r1={"tokens": TOKS, "fail_after": 5}, r2={"tokens": TOKS})
    _, gw, _ = run_one(reps, session_id="s")
    assert gw.sessions["s"][-1] == {"role": "assistant", "content": "".join(TOKS)}


# --------------------------------------------------------------------------- #
# End to end with stand-in containers (SIMULATED timings)
# --------------------------------------------------------------------------- #
def run_clients(url: str, n: int, max_tokens: int) -> list:
    results: list = [None] * n

    def one(i):
        results[i] = stream_request(url, max_tokens=max_tokens, content=f"client {i}")[0]
    ts = [threading.Thread(target=one, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
        time.sleep(0.05)
    return results, ts


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
@pytest.mark.parametrize("fault", ["kill", "pause"])
def test_busy_replica_fails_all_clients_still_get_full_output(tmp_path, docker_client, fault):
    gw = GatewayProc(tmp_path, n_ready=2, router="least_busy", scheduler="fcfs")
    try:
        results, threads = run_clients(gw.url, n=4, max_tokens=60)     # 2 on r1, 2 on r2, ~3 s each
        time.sleep(1.0)
        c = docker_client.containers.get(f"{TEST_PREFIX}r1")
        c.kill() if fault == "kill" else c.pause()
        for t in threads:
            t.join(60)
        for ev in results:                                            # nothing lost, nothing repeated
            assert ev is not None and ev[-1] == "[DONE]"
            assert content_of(ev) == sim_text(60)
            assert not any("llmgw_retry" in e or '"error"' in e for e in ev)
        reason = "stream_error" if fault == "kill" else "health_checks"
        wait_for(lambda: "[HEALTH] replica ready replica=r3" in gw.log(), 30, "replacement r3 ready")
        log = gw.log()
        assert f"[HEALTH] replica DEAD replica=r1 reason={reason}" in log, log
        assert log.count("[ROUTE] retry resumed") == 2                # both r1 requests resumed
        assert "[HEALTH] replacement started replica=r3 replacing=r1" in log
        if fault == "pause":
            assert "[HEALTH] probe failed replica=r1 failures=3/3" in log
    finally:
        gw.stop()
