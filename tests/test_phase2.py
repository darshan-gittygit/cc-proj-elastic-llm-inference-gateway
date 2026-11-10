"""
Phase 2 tests: ReplicaManager (Docker Engine API) and routing.

- Router tests are pure unit tests (no I/O).
- Manager and gateway tests start stand-in replica containers (llmgw-standin:test).
  Container limits and lifecycle are real Docker behaviour; token timings are SIMULATED.
"""
from __future__ import annotations

import asyncio
import threading
import time

import pytest

from gateway.interfaces import (ROUTERS, Config, EventLog, Replica, ReplicaPool, ReplicaState, Request,
                                build_router)
from gateway.router import LeastBusyRouter, RoundRobinRouter
from helpers import GatewayProc, content_of, make_test_config, sim_text, stream_request


# --------------------------------------------------------------------------- #
# Routers (pure)
# --------------------------------------------------------------------------- #
def make_pool(*specs: tuple[str, ReplicaState, int]) -> ReplicaPool:
    pool = ReplicaPool()
    for rid, state, in_flight in specs:
        pool.add(Replica(id=rid, container_id=rid, host="h", port=1, state=state, max_slots=2,
                         in_flight=in_flight))
    return pool


def req() -> Request:
    return Request(messages=[{"role": "user", "content": "x"}], prompt_tokens=1)


R = ReplicaState.READY


def test_round_robin_cycles_in_start_order():
    pool = make_pool(("r1", R, 0), ("r2", R, 0), ("r3", R, 0))
    rr = RoundRobinRouter()
    assert [rr.choose(req(), pool).id for _ in range(7)] == ["r1", "r2", "r3", "r1", "r2", "r3", "r1"]


def test_round_robin_skips_unroutable():
    pool = make_pool(("r1", R, 0), ("r2", R, 2), ("r3", ReplicaState.DRAINING, 0),
                     ("r4", ReplicaState.STARTING, 0), ("r5", R, 1))
    rr = RoundRobinRouter()
    assert [rr.choose(req(), pool).id for _ in range(4)] == ["r1", "r5", "r1", "r5"]


def test_round_robin_continues_after_removed_replica():
    pool = make_pool(("r1", R, 0), ("r2", R, 0), ("r3", R, 0))
    rr = RoundRobinRouter()
    assert rr.choose(req(), pool).id == "r1"
    assert rr.choose(req(), pool).id == "r2"
    pool.remove("r2")
    assert rr.choose(req(), pool).id in {"r1", "r3"}


def test_least_busy_picks_lowest_load_and_breaks_ties_by_start_order():
    pool = make_pool(("r1", R, 1), ("r2", R, 0), ("r3", R, 0))
    lb = LeastBusyRouter()
    assert lb.choose(req(), pool).id == "r2"
    pool.get("r2").in_flight = 1
    assert lb.choose(req(), pool).id == "r3"
    pool.get("r3").in_flight = 1
    assert lb.choose(req(), pool).id == "r1"          # all 1/2 -> first started


def test_least_busy_ignores_full_and_not_ready():
    pool = make_pool(("r1", R, 2), ("r2", ReplicaState.WARM_SPARE, 0), ("r3", R, 1))
    assert LeastBusyRouter().choose(req(), pool).id == "r3"


def test_routers_return_none_when_nothing_routable():
    pool = make_pool(("r1", R, 2), ("r2", ReplicaState.STARTING, 0))
    assert RoundRobinRouter().choose(req(), pool) is None
    assert LeastBusyRouter().choose(req(), pool) is None
    assert RoundRobinRouter().choose(req(), ReplicaPool()) is None


def test_registry_has_only_phase2_routers():
    assert set(ROUTERS) == {"round_robin", "least_busy"}
    log = EventLog("test")
    assert isinstance(build_router(Config(router="round_robin"), log), RoundRobinRouter)
    assert isinstance(build_router(Config(router="least_busy"), log), LeastBusyRouter)
    with pytest.raises(ValueError, match="unknown router 'session_affinity'"):   # Phase 4
        build_router(Config(router="session_affinity"), log)


# --------------------------------------------------------------------------- #
# DockerReplicaManager (real Docker, stand-in containers)
# --------------------------------------------------------------------------- #
def make_manager(tmp_path, **overrides):
    from gateway.replica_manager import DockerReplicaManager
    raw = make_test_config(tmp_path, **overrides)
    cfg = Config(**{k: v for k, v in raw.items() if k in Config.__dataclass_fields__})
    pool = ReplicaPool()
    return DockerReplicaManager(cfg, pool, EventLog("test-manager")), pool, cfg


async def wait_state(r: Replica, state: ReplicaState, timeout: float = 30) -> None:
    end = time.monotonic() + timeout
    while r.state != state:
        assert time.monotonic() < end, f"{r.id} stuck in {r.state}"
        await asyncio.sleep(0.1)


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_manager_starts_replicas_with_limits_and_stops_them(tmp_path, docker_client):
    mgr, pool, cfg = make_manager(tmp_path, replica_cpus=0.5, replica_memory="256m")

    async def scenario():
        await mgr.setup()
        r1 = await mgr.start_replica()
        r2 = await mgr.start_replica()
        assert {r1.state, r2.state} == {ReplicaState.STARTING}
        await wait_state(r1, ReplicaState.READY)
        await wait_state(r2, ReplicaState.READY)
        assert r1.ready_ts and r1.ready_ts > r1.created_ts
        for r in (r1, r2):
            hc = docker_client.containers.get(r.container_id).attrs["HostConfig"]
            assert hc["NanoCpus"] == 500_000_000                 # --cpus 0.5
            assert hc["Memory"] == 256 * 1024 * 1024              # --memory 256m
            assert hc["NetworkMode"] == cfg.docker_network
            assert r.max_slots == cfg.slots_per_replica
        await mgr.stop_replica(r1)
        await mgr.kill_and_remove(r2, reason="test")
        assert pool.all() == [] and r1.state == r2.state == ReplicaState.STOPPED
        for r in (r1, r2):
            assert not docker_client.containers.list(all=True, filters={"id": r.container_id})
        await mgr.shutdown()
    asyncio.run(scenario())


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_stop_replica_drains_in_flight_work_first(tmp_path, docker_client):
    mgr, pool, _ = make_manager(tmp_path)

    async def scenario():
        await mgr.setup()
        r = await mgr.start_replica()
        await wait_state(r, ReplicaState.READY)
        r.in_flight = 1
        stop = asyncio.create_task(mgr.stop_replica(r, drain_timeout_s=10))
        await asyncio.sleep(1.0)
        assert r.state == ReplicaState.DRAINING and not r.routable
        assert docker_client.containers.get(r.container_id).status == "running"
        r.in_flight = 0                                      # last request finishes
        await asyncio.wait_for(stop, 15)
        assert r.state == ReplicaState.STOPPED and pool.all() == []
        await mgr.shutdown()
    asyncio.run(scenario())


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_spare_is_not_routable_until_promoted(tmp_path):
    mgr, pool, _ = make_manager(tmp_path)

    async def scenario():
        await mgr.setup()
        r = await mgr.start_replica(as_spare=True)
        await wait_state(r, ReplicaState.WARM_SPARE)
        assert pool.routable() == []
        assert await mgr.promote_spare() is r
        assert r.state == ReplicaState.READY and pool.routable() == [r]
        assert await mgr.promote_spare() is None
        await mgr.shutdown()
    asyncio.run(scenario())


# --------------------------------------------------------------------------- #
# Gateway routing end to end (stand-in containers)
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_gateway_round_robin_over_three_replicas(tmp_path):
    gw = GatewayProc(tmp_path, n_ready=3, router="round_robin")
    try:
        for i in range(6):
            events, _ = stream_request(gw.url, max_tokens=3, content=f"q{i}")
            assert content_of(events) == sim_text(3)
        chosen = [l.split("replica=")[1].split()[0] for l in gw.route_lines("dispatch")]
        assert chosen == ["r1", "r2", "r3", "r1", "r2", "r3"]
        assert all("policy=round_robin" in l for l in gw.route_lines("dispatch"))
    finally:
        gw.stop()


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
@pytest.mark.parametrize("router,expected_third", [("least_busy", "r2"), ("round_robin", "r1")])
def test_least_busy_differs_from_round_robin(tmp_path, router, expected_third):
    """A long request occupies r1; a short one goes to r2 and finishes. The next short
    request goes to r2 again under least_busy (r1 still busy) but to r1 under round_robin."""
    gw = GatewayProc(tmp_path, n_ready=2, router=router)
    try:
        long_result = {}
        t = threading.Thread(target=lambda: long_result.setdefault("ev", stream_request(gw.url, 80)[0]))
        t.start()
        time.sleep(0.5)                                       # long request is now on r1
        for _ in range(2):
            assert content_of(stream_request(gw.url, 3)[0]) == sim_text(3)
        t.join(30)
        assert content_of(long_result["ev"]) == sim_text(80)
        chosen = [l.split("replica=")[1].split()[0] for l in gw.route_lines("dispatch")]
        assert chosen == ["r1", "r2", expected_third], gw.route_lines("dispatch")
    finally:
        gw.stop()


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_gateway_exit_removes_its_containers(tmp_path, docker_client):
    gw = GatewayProc(tmp_path, n_ready=2)
    gw.stop()
    assert gw.log().count("[SCALE] replica removed") == 2
    # no_leftover_containers checks Docker itself
