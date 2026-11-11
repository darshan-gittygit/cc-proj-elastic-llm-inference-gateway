"""
Failure-path tests added on review (independent evaluation findings F2 and F3).

F2: a replica whose container exits (or never answers /health) while it is still
    STARTING must not stay in the pool as DEAD with its exited container left behind.
F3: a background task of the gateway (dispatch loop, health monitor, autoscaler,
    mark_dead) that crashes must be logged instead of disappearing silently.
"""
from __future__ import annotations

import asyncio
import logging
import time

import pytest
import yaml

from gateway.interfaces import Config, EventLog, ReplicaPool
from helpers import ROOT, TEST_PREFIX, make_test_config

# The real llama.cpp server image pointed at a model file that does not exist: llama-server
# starts, fails to load the model and exits while the replica is still STARTING. No model runs.
REAL_IMAGE = yaml.safe_load((ROOT / "gateway" / "config.yaml").read_text())["replica_image"]


@pytest.mark.usefixtures("no_leftover_containers")
def test_replica_that_dies_while_loading_is_removed_and_replacement_is_bounded(tmp_path, docker_client, caplog):
    from docker.errors import ImageNotFound
    from gateway.replica_manager import MAX_LOAD_FAILURES, DockerReplicaManager
    try:
        docker_client.images.get(REAL_IMAGE)
    except ImageNotFound:
        pytest.skip(f"{REAL_IMAGE} not pulled")
    caplog.set_level(logging.INFO)
    raw = make_test_config(tmp_path, replica_image=REAL_IMAGE, model_file="missing-model.gguf", min_replicas=1)
    cfg = Config(**{k: v for k, v in raw.items() if k in Config.__dataclass_fields__})
    pool = ReplicaPool()
    mgr = DockerReplicaManager(cfg, pool, EventLog("test-f2"))

    async def scenario():
        await mgr.setup()
        await mgr.start_replica()
        end = time.monotonic() + 60
        while time.monotonic() < end:                 # wait until the manager has given up
            if "not replacing" in caplog.text:
                break
            await asyncio.sleep(0.5)
        await asyncio.sleep(1.0)
        left_in_pool = [(r.id, r.state) for r in pool.all()]
        containers = [c.name for c in docker_client.containers.list(all=True) if c.name.startswith(TEST_PREFIX)]
        await mgr.shutdown()
        return left_in_pool, containers
    left_in_pool, containers = asyncio.run(scenario())
    assert left_in_pool == [], left_in_pool                       # no DEAD entry lingers
    assert containers == [], containers                           # no exited container lingers
    assert caplog.text.count("replica dead while loading") == MAX_LOAD_FAILURES
    assert caplog.text.count("reason=dead_while_loading") == MAX_LOAD_FAILURES
    assert "not replacing" in caplog.text                         # bounded: no crash loop


def test_crashed_background_task_is_logged(caplog):
    from gateway.main import Gateway
    caplog.set_level(logging.INFO)
    gw = Gateway(Config(), EventLog("test-f3"))

    async def boom():
        raise RuntimeError("probe loop exploded")

    async def scenario():
        gw._spawn(boom())
        await asyncio.sleep(0.05)
    asyncio.run(scenario())
    crash = [r for r in caplog.records if "background task crashed" in r.getMessage()]
    assert len(crash) == 1 and crash[0].levelno == logging.ERROR
    assert "probe loop exploded" in crash[0].getMessage()


def test_cancelled_background_task_is_not_reported_as_crash(caplog):
    from gateway.main import Gateway
    caplog.set_level(logging.INFO)
    gw = Gateway(Config(), EventLog("test-f3"))

    async def forever():
        await asyncio.sleep(3600)

    async def scenario():
        t = gw._spawn(forever())
        await asyncio.sleep(0)
        t.cancel()
        await asyncio.sleep(0.05)
    asyncio.run(scenario())
    assert "background task crashed" not in caplog.text
