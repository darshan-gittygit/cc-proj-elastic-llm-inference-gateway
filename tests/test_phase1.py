"""
Phase 1 tests: gateway skeleton, single-request streaming path.

All tests here run against tests/standin_replica.py, so every timing they see is
SIMULATED. They check gateway behaviour only (the streaming path, the central
queue holding work until a slot is free, error handling), never model speed.

Run from the project root:  python -m pytest tests -v
"""
from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
STANDIN = ROOT / "tests" / "standin_replica.py"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(cond, timeout: float, what: str) -> None:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {what}")


def start_standin(port: int, delay: float = 0.5, rate: float = 50.0, slots: int = 2) -> subprocess.Popen:
    p = subprocess.Popen([sys.executable, str(STANDIN), "--port", str(port), "--startup-delay", str(delay),
                          "--token-rate", str(rate), "--slots", str(slots)], cwd=ROOT)

    def up() -> bool:
        try:
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=0.5)
            return True
        except httpx.HTTPError:
            return False
    wait_for(up, 15, "stand-in to listen")
    return p


class GatewayProc:
    def __init__(self, replica_port: int, tmp: Path, overrides: dict | None = None):
        self.port = free_port()
        self.logfile = tmp / "gateway.log"
        cfg = yaml.safe_load((ROOT / "gateway" / "config.yaml").read_text())
        cfg.update(overrides or {})
        cfgfile = tmp / "config.yaml"
        cfgfile.write_text(yaml.safe_dump(cfg))
        env = dict(os.environ, LLMGW_STATIC_REPLICA=f"http://127.0.0.1:{replica_port}",
                   LLMGW_CONFIG=str(cfgfile), LLMGW_LOG_FILE=str(self.logfile))
        self.proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "gateway.main:app", "--port", str(self.port),
                                      "--log-level", "warning"], cwd=ROOT, env=env)
        wait_for(lambda: "[HEALTH] replica ready" in self.log(), 20, "gateway to see replica ready")

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v1/chat/completions"

    def log(self) -> str:
        return self.logfile.read_text() if self.logfile.exists() else ""

    def stop(self) -> None:
        self.proc.terminate()
        self.proc.wait(10)


def stream_request(url: str, max_tokens: int, content: str = "hello") -> tuple[list[str], list[float]]:
    """Returns the SSE data payloads and the arrival time of each one."""
    events, times = [], []
    body = {"messages": [{"role": "user", "content": content}], "max_tokens": max_tokens, "stream": True}
    with httpx.stream("POST", url, json=body, timeout=60) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        for line in r.iter_lines():
            if line.startswith("data: "):
                events.append(line[6:])
                times.append(time.monotonic())
    return events, times


def content_of(events: list[str]) -> str:
    out = ""
    for e in events:
        if e == "[DONE]":
            continue
        d = json.loads(e)
        for c in d.get("choices", []):
            out += (c.get("delta") or {}).get("content") or ""
    return out


@pytest.fixture
def standin():
    port = free_port()
    p = start_standin(port)
    yield port, p
    p.terminate()
    p.wait(10)


# --------------------------------------------------------------------------- #
def test_standin_reports_loading_then_ready(tmp_path):
    """Fixture sanity: /health is 503 during the startup delay, then 200, like llama-server."""
    port = free_port()
    p = start_standin(port, delay=2.0)
    try:
        r = httpx.get(f"http://127.0.0.1:{port}/health")
        assert r.status_code == 503 and r.headers["x-replica-kind"] == "standin-simulated"
        wait_for(lambda: httpx.get(f"http://127.0.0.1:{port}/health").status_code == 200, 10, "stand-in ready")
    finally:
        p.terminate()
        p.wait(10)


def test_single_request_streams_through_gateway(standin, tmp_path):
    port, _ = standin
    gw = GatewayProc(port, tmp_path)
    try:
        events, times = stream_request(gw.url, max_tokens=20)
        # complete, ordered output and a terminating [DONE]
        assert content_of(events) == "".join(f"sim{i} " for i in range(20))
        assert events[-1] == "[DONE]"
        assert json.loads(events[0])["model"] == "standin-simulated"
        # streamed, not buffered: 20 tokens at 50 tok/s take ~0.4 s to arrive
        assert times[-2] - times[1] > 0.25
        log = gw.log()
        assert re.search(r"\[SCHED\] queued req=\d+", log)
        assert re.search(r"\[ROUTE\] dispatch req=\d+ replica=r1", log)
        assert re.search(r"\[ROUTE\] complete req=\d+ replica=r1 tokens=20", log)
    finally:
        gw.stop()


def test_requests_wait_in_gateway_until_slot_free(tmp_path):
    """Decision 1: with 1 slot, the 2nd request stays in the gateway queue and is
    dispatched only after the 1st completes; both finish."""
    port = free_port()
    p = start_standin(port, slots=1, rate=20)
    gw = GatewayProc(port, tmp_path, overrides={"slots_per_replica": 1})
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
            assert r is not None and content_of(r) == "".join(f"sim{i} " for i in range(10))
        lines = [l for l in gw.log().splitlines() if "[ROUTE]" in l]
        kinds = [("dispatch" if "] dispatch" in l else "complete") for l in lines]
        assert kinds == ["dispatch", "complete", "dispatch", "complete"], lines
    finally:
        gw.stop()
        p.terminate()
        p.wait(10)


def test_replica_failure_ends_stream_with_error(standin, tmp_path):
    """No retry before Phase 5, but the client must get an error and a closed stream, not a hang."""
    port, proc = standin
    gw = GatewayProc(port, tmp_path)
    try:
        proc.terminate()
        proc.wait(10)
        events, _ = stream_request(gw.url, max_tokens=5)
        assert events[-1] == "[DONE]"
        assert json.loads(events[0])["error"]["type"] == "replica_error"
        assert "stream failed, no retry in phase 1" in gw.log()
    finally:
        gw.stop()


def test_rejects_bad_requests(standin, tmp_path):
    port, _ = standin
    gw = GatewayProc(port, tmp_path)
    try:
        assert httpx.post(gw.url, json={"messages": []}).status_code == 400
        assert httpx.post(gw.url, json={"messages": [{"role": "user", "content": "x"}],
                                        "stream": False}).status_code == 400
        assert httpx.post(gw.url, content=b"not json").status_code == 400
    finally:
        gw.stop()
