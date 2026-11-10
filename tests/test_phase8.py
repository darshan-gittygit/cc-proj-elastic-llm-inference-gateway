"""
Phase 8 tests: configuration reload (ARCHITECTURE.md decision 10) and the load script.

- Reload unit tests use a Gateway object that is not started (no Docker): they check what a
  reload changes, that queued requests move to the new scheduler, and that an invalid file
  changes nothing at all.
- End to end: stand-in containers (SIMULATED timings); POST /admin/reload on a running gateway,
  and loadgen/burst.py against it.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import re
import subprocess
import sys

import httpx
import pytest
import yaml

from gateway.autoscaler import LoadAutoscaler
from gateway.interfaces import Config, EventLog, Replica, ReplicaPool, ReplicaState, Request
from gateway.router import LeastBusyRouter, RoundRobinRouter, SessionAffinityRouter
from gateway.scheduler import FCFSScheduler, SPFAgingScheduler
from helpers import ROOT, GatewayProc, stream_request


def gateway(**cfg):
    from gateway.main import Gateway
    return Gateway(Config(**cfg), EventLog("test-reload"))


def req(prompt_tokens: int, t: float) -> Request:
    return Request(messages=[{"role": "user", "content": "x"}], prompt_tokens=prompt_tokens, arrival_ts=t)


def test_reload_switches_scheduler_and_carries_queued_requests_over(caplog):
    gw = gateway(scheduler="fcfs", router="round_robin")
    reqs = [req(p, t) for p, t in [(30, 1.0), (5, 2.0), (20, 3.0)]]
    for r in reqs:
        gw.submit(r)
    caplog.set_level(logging.INFO)
    caplog.clear()
    changed = gw.reload(Config(scheduler="spf_aging", aging_alpha=0.0, router="round_robin"))
    assert set(changed) == {"scheduler", "aging_alpha"}
    assert isinstance(gw.scheduler, SPFAgingScheduler) and len(gw.scheduler) == 3
    msgs = [r.getMessage() for r in caplog.records]
    assert not any("[SCHED] selected" in m or "[SCHED] queued" in m for m in msgs)   # a move, not decisions
    assert any("[CONFIG] reload applied" in m and "scheduler:fcfs->spf_aging" in m and "queued_moved=3" in m
               for m in msgs)
    assert [gw.scheduler.pop(now=4.0).prompt_tokens for _ in range(3)] == [5, 20, 30]   # new policy


def test_reload_back_to_fcfs_restores_arrival_order():
    gw = gateway(scheduler="spf_aging", aging_alpha=0.0)
    for p, t in [(30, 1.0), (5, 2.0), (20, 3.0)]:
        gw.submit(req(p, t))
    gw.reload(Config(scheduler="fcfs", aging_alpha=0.0))
    assert isinstance(gw.scheduler, FCFSScheduler)
    assert [gw.scheduler.pop(now=4.0).prompt_tokens for _ in range(3)] == [30, 5, 20]


def test_requests_whose_client_left_are_dropped_on_the_move():
    gw = gateway(scheduler="fcfs")
    a, b = req(10, 1.0), req(10, 2.0)
    gw.submit(a)
    gw.submit(b)
    gw.cancel(a)                                   # client gone while queued
    gw.reload(Config(scheduler="spf_aging"))
    assert len(gw.scheduler) == 1 and gw.scheduler.pop() is b


@pytest.mark.parametrize("start,target", [("fcfs", "spf_aging"), ("spf_aging", "fcfs")])
def test_reload_keeps_retried_requests_at_the_front(start, target):
    """Decision 6: a retried request waits at the FRONT of the queue; a scheduler reload must
    not move it behind newer ordinary requests, and retries keep their relative order."""
    gw = gateway(scheduler=start, aging_alpha=0.0)
    a = req(10, 1.0)
    gw.submit(a)
    r1, r2 = req(50, 0.5), req(60, 0.2)
    r1.attempts = r2.attempts = 1                    # both were dispatched once and failed
    gw.scheduler.requeue_front(r1)
    gw.scheduler.requeue_front(r2)
    front = [r2, r1] if start == "fcfs" else [r1, r2]   # FCFS prepends, SPF appends to its front
    b, c = req(1, 5.0), req(5, 6.0)                  # newer ordinary requests, shorter prompts
    gw.submit(b)
    gw.submit(c)
    gw.reload(Config(scheduler=target, aging_alpha=0.0))
    order = [gw.scheduler.pop(now=10.0) for _ in range(5)]
    assert order[:2] == front                        # retries first, in the same order
    rest = [a, b, c] if target == "fcfs" else [b, c, a]
    assert order[2:] == rest and gw.scheduler.pop(now=10.0) is None   # nothing lost or duplicated


def test_rejected_reload_with_multiline_yaml_error_logs_one_line(tmp_path, monkeypatch, caplog):
    import gateway.main as main
    bad = tmp_path / "config.yaml"
    bad.write_text("router: [unclosed\nscheduler: fcfs\n")
    monkeypatch.setattr(main, "CONFIG_PATH", str(bad))
    gw = gateway(scheduler="fcfs")
    snapshot = copy.deepcopy(gw.cfg)

    class HTTPReq:                                   # the endpoint only needs app.state.gw
        class app:
            class state:
                pass
    HTTPReq.app.state.gw = gw
    caplog.set_level(logging.INFO)
    resp = asyncio.run(main.admin_reload(HTTPReq()))
    assert resp.status_code == 400 and json.loads(resp.body)["applied"] is False
    lines = [r.getMessage() for r in caplog.records if "[CONFIG] reload rejected" in r.getMessage()]
    assert len(lines) == 1
    assert "\n" not in lines[0] and "\r" not in lines[0] and "\t" not in lines[0]
    assert lines[0].startswith("[CONFIG] reload rejected, nothing changed error=while_parsing_a_flow_sequence")
    assert gw.cfg == snapshot


def test_router_is_rebuilt_only_when_the_router_setting_changes():
    gw = gateway(router="session_affinity")
    before = gw.router
    assert isinstance(before, SessionAffinityRouter)
    assert set(gw.reload(Config(router="session_affinity", prewarm="none"))) == {"prewarm"}
    assert gw.router is before                     # affinity map kept on unrelated reloads
    gw.reload(Config(router="least_busy", prewarm="none"))
    assert isinstance(gw.router, LeastBusyRouter)


def test_new_values_reach_the_shared_config_object():
    gw = gateway()
    shared = gw.cfg                                # the manager / health / autoscaler hold this object
    gw.reload(Config(prewarm="none", max_replicas=2, scale_up_threshold=3.0, health_fail_limit=5))
    assert shared is gw.cfg
    assert (shared.prewarm, shared.max_replicas, shared.scale_up_threshold, shared.health_fail_limit) == \
           ("none", 2, 3.0, 5)


@pytest.mark.parametrize("bad,why", [
    (dict(scheduler="lottery"), "unknown scheduler"),
    (dict(router="random"), "unknown router"),
    (dict(scheduler="fcfs", router="random"), "unknown router"),     # one bad value: nothing applies
    (dict(prewarm="sometimes"), "prewarm"),
    (dict(aging_alpha=-1.0), "aging_alpha"),
    (dict(min_replicas=0), "min_replicas"),
    (dict(min_replicas=4, max_replicas=3), "min_replicas"),
    (dict(max_replicas="3"), "max_replicas: expected int"),
    (dict(scale_up_threshold="high"), "scale_up_threshold: expected float"),
    (dict(container_prefix="other-"), "container_prefix cannot change"),
    (dict(docker_network="othernet"), "docker_network cannot change"),
])
def test_invalid_config_is_rejected_and_changes_nothing(bad, why):
    gw = gateway(scheduler="spf_aging", router="session_affinity")
    gw.submit(req(10, 1.0))
    snapshot = copy.deepcopy(gw.cfg)
    sched, router = gw.scheduler, gw.router
    with pytest.raises(ValueError, match=why):
        gw.reload(Config(**{**{"scheduler": "spf_aging", "router": "session_affinity"}, **bad}))
    assert gw.cfg == snapshot and gw.scheduler is sched and gw.router is router and len(sched) == 1


def test_switching_prewarm_to_none_stops_existing_spares():
    pool = ReplicaPool()
    pool.add(Replica(id="r1", container_id="r1", host="h", port=1, state=ReplicaState.READY))
    pool.add(Replica(id="r2", container_id="r2", host="h", port=1, state=ReplicaState.WARM_SPARE))
    stopped = []

    class Mgr:
        async def stop_replica(self, r, drain_timeout_s=30.0):
            r.state = ReplicaState.DRAINING
            stopped.append(r.id)

    class Q:
        def __len__(self):
            return 0
    cfg = Config(prewarm="none")
    a = LoadAutoscaler(cfg, pool, Q(), Mgr(), EventLog("test-prewarm-switch"))

    async def go():
        await a.tick(100.0)
        await asyncio.sleep(0)
        await a.tick(101.0)                         # the spare is DRAINING now: not stopped twice
        await asyncio.sleep(0)
    asyncio.run(go())
    assert stopped == ["r2"]


# --------------------------------------------------------------------------- #
# End to end (stand-in containers, SIMULATED timings)
# --------------------------------------------------------------------------- #
def rewrite_config(tmp_path, **changes):
    path = tmp_path / "config.yaml"
    cfg = yaml.safe_load(path.read_text())
    cfg.update(changes)
    path.write_text(yaml.safe_dump(cfg))


def session_replicas(gw: GatewayProc, session: str) -> list[str]:
    return re.findall(r"\[ROUTE\] dispatch req=\d+ replica=(r\d+) [^\n]*session=" + session + r"\b", gw.log())


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_reload_endpoint_changes_behaviour_without_restart(tmp_path):
    gw = GatewayProc(tmp_path, n_ready=2, router="round_robin", scheduler="fcfs")
    url = gw.url.replace("/v1/chat/completions", "/admin/reload")
    try:
        for t in range(4):
            stream_request(gw.url, 2, f"turn {t}", session_id="before")
        assert session_replicas(gw, "before") == ["r1", "r2", "r1", "r2"]      # round robin

        rewrite_config(tmp_path, router="session_affinity", scheduler="spf_aging")
        r = httpx.post(url)
        assert r.status_code == 200 and r.json()["applied"] is True
        assert r.json()["changed"]["router"] == ["round_robin", "session_affinity"]
        for t in range(4):
            stream_request(gw.url, 2, f"turn {t}", session_id="after")
        assert len(set(session_replicas(gw, "after"))) == 1                     # affinity now

        rewrite_config(tmp_path, router="nonexistent")                          # invalid
        r = httpx.post(url)
        assert r.status_code == 400 and r.json()["applied"] is False and "unknown router" in r.json()["error"]
        (tmp_path / "config.yaml").write_text("router: [unclosed")                # not even YAML
        assert httpx.post(url).status_code == 400
        stream_request(gw.url, 2, "still", session_id="after")
        assert "policy=session_affinity" in gw.route_lines("dispatch")[-1]      # old values still in force

        assert gw.proc.poll() is None                                           # same process throughout
        assert gw.log().count("[SCALE] replica started") == 2                   # no restart, no new replicas
        assert "[CONFIG] reload rejected" in gw.log()
    finally:
        gw.stop()


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_load_script_drives_the_gateway(tmp_path):
    gw = GatewayProc(tmp_path, n_ready=1, slots_per_replica=1, scheduler="fcfs")
    base = gw.url.replace("/v1/chat/completions", "")
    try:
        out = tmp_path / "burst.jsonl"
        p = subprocess.run([sys.executable, "loadgen/burst.py", "--url", base, "--pattern", "B,L,S,S",
                            "--interval", "0.2", "--max-tokens", "3", "--busy-max-tokens", "30",
                            "--out", str(out)], cwd=ROOT, capture_output=True, text=True, timeout=60)
        assert p.returncode == 0, p.stderr
        lines = [json.loads(l) for l in out.read_text().splitlines()]
        recs, summ = lines[:-1], lines[-1]["summary"]
        assert [r["kind"] for r in recs] == ["B", "L", "S", "S"]
        assert all(r["ok"] and r["gateway_req"] and r["first_token_s"] >= r["start_s"] for r in recs)
        assert [r["tokens"] for r in recs] == [30, 3, 3, 3]
        assert summ["ok"] == 4 and summ["failed"] == 0
        # fcfs on 1 slot: the gateway served them in the order the script sent them
        selected = re.findall(r"\[SCHED\] selected req=(\d+)", gw.log())
        assert selected == [r["gateway_req"] for r in recs]

        p = subprocess.run([sys.executable, "loadgen/burst.py", "--url", base, "--session", "conv",
                            "--pattern", "S,S,S", "--max-tokens", "2"], cwd=ROOT, capture_output=True,
                           text=True, timeout=60)
        assert p.returncode == 0, p.stderr
        assert len(re.findall(r"\[ROUTE\] dispatch [^\n]*session=conv\b", gw.log())) == 3
    finally:
        gw.stop()
