"""
Phase 1 tests: single-request streaming path.

Since Phase 2 the gateway starts its own replicas through the ReplicaManager, so
these tests run it with stand-in replica containers (image llmgw-standin:test,
20 tok/s, 2 s startup). Every timing seen here is SIMULATED; the tests check
gateway behaviour only, never model speed.

Run from the project root:  python -m pytest tests -v
"""
from __future__ import annotations

import json
import re
import threading
import time

import httpx
import pytest

from helpers import (TEST_PREFIX, GatewayProc, content_of, free_port, sim_text, start_standin_process,
                     stream_request, wait_for)

pytestmark = pytest.mark.usefixtures("standin_image", "no_leftover_containers")


def test_standin_reports_loading_then_ready():
    """Fixture sanity: /health is 503 during the startup delay, then 200, like llama-server."""
    port = free_port()
    p = start_standin_process(port, delay=2.0)
    try:
        r = httpx.get(f"http://127.0.0.1:{port}/health")
        assert r.status_code == 503 and r.headers["x-replica-kind"] == "standin-simulated"
        wait_for(lambda: httpx.get(f"http://127.0.0.1:{port}/health").status_code == 200, 10, "stand-in ready")
    finally:
        p.terminate()
        p.wait(10)


def test_single_request_streams_through_gateway(tmp_path):
    gw = GatewayProc(tmp_path)
    try:
        events, times = stream_request(gw.url, max_tokens=20)
        # complete, ordered output and a terminating [DONE]
        assert content_of(events) == sim_text(20)
        assert events[-1] == "[DONE]"
        assert json.loads(events[0])["model"] == "standin-simulated"
        # streamed, not buffered: 20 tokens at 20 tok/s take ~1 s to arrive
        assert times[-2] - times[1] > 0.6
        log = gw.log()
        assert re.search(r"\[SCHED\] queued req=\d+", log)
        assert re.search(r"\[ROUTE\] dispatch req=\d+ replica=r1", log)
        assert re.search(r"\[ROUTE\] complete req=\d+ replica=r1 tokens=20", log)
    finally:
        gw.stop()


def test_requests_wait_in_gateway_until_slot_free(tmp_path):
    """Decision 1: with 1 slot, the 2nd request stays in the gateway queue and is
    dispatched only after the 1st completes; both finish."""
    gw = GatewayProc(tmp_path, slots_per_replica=1)
    try:
        results = [None, None]

        def run(i):
            results[i] = stream_request(gw.url, max_tokens=10, content=f"req {i}")[0]
        ts = [threading.Thread(target=run, args=(i,)) for i in range(2)]
        ts[0].start()
        time.sleep(0.2)
        ts[1].start()
        for t in ts:
            t.join(30)
        for r in results:
            assert r is not None and content_of(r) == sim_text(10)
        lines = [l for l in gw.log().splitlines() if "[ROUTE]" in l]
        kinds = [("dispatch" if "] dispatch" in l else "complete") for l in lines]
        assert kinds == ["dispatch", "complete", "dispatch", "complete"], lines
    finally:
        gw.stop()


def test_replica_failure_ends_stream_with_error(tmp_path, docker_client):
    """When no retry is left (max_retries: 0; retries are Phase 5), the client must get an
    error and a closed stream, not a hang."""
    gw = GatewayProc(tmp_path, max_retries=0)
    try:
        docker_client.containers.get(f"{TEST_PREFIX}r1").remove(force=True)
        events, _ = stream_request(gw.url, max_tokens=5)
        assert events[-1] == "[DONE]"
        assert json.loads(events[0])["error"]["type"] == "replica_error"
        assert "stream failed, retries exhausted" in gw.log()
    finally:
        gw.stop()


def test_rejects_bad_requests(tmp_path):
    gw = GatewayProc(tmp_path)
    try:
        assert httpx.post(gw.url, json={"messages": []}).status_code == 400
        assert httpx.post(gw.url, json={"messages": [{"role": "user", "content": "x"}],
                                        "stream": False}).status_code == 400
        assert httpx.post(gw.url, content=b"not json").status_code == 400
    finally:
        gw.stop()
