"""Shared helpers for the gateway tests. Everything here uses the stand-in replica,
so all timings seen by the tests are SIMULATED."""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import yaml

ROOT = Path(__file__).resolve().parents[1]
STANDIN = ROOT / "tests" / "standin_replica.py"
STANDIN_IMAGE = "llmgw-standin:test"
TEST_PREFIX = "llmgw-test-replica-"      # never collides with real gateway containers


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


def start_standin_process(port: int, delay: float = 0.5, rate: float = 50.0, slots: int = 2) -> subprocess.Popen:
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


def make_test_config(tmp: Path, **overrides) -> dict:
    """config.yaml values, switched to stand-in containers with a test name prefix."""
    cfg = yaml.safe_load((ROOT / "gateway" / "config.yaml").read_text())
    models = tmp / "models"
    models.mkdir(exist_ok=True)
    cfg.update(replica_image=STANDIN_IMAGE, container_prefix=TEST_PREFIX, models_dir=str(models))
    cfg.update(overrides)
    return cfg


class GatewayProc:
    """Runs `uvicorn gateway.main:app` with a test config; waits until `n_ready` replicas are READY."""

    def __init__(self, tmp: Path, n_ready: int = 1, **overrides):
        self.port = free_port()
        self.logfile = tmp / "gateway.log"
        cfgfile = tmp / "config.yaml"
        cfgfile.write_text(yaml.safe_dump(make_test_config(tmp, min_replicas=n_ready, **overrides)))
        env = dict(os.environ, LLMGW_CONFIG=str(cfgfile), LLMGW_LOG_FILE=str(self.logfile))
        self.proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "gateway.main:app", "--port", str(self.port),
                                      "--log-level", "warning"], cwd=ROOT, env=env)
        try:
            wait_for(lambda: self.log().count("[HEALTH] replica ready") >= n_ready, 60,
                     f"{n_ready} replica(s) ready")
        except Exception:
            self.stop()
            raise

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v1/chat/completions"

    def log(self) -> str:
        return self.logfile.read_text() if self.logfile.exists() else ""

    def route_lines(self, kind: str) -> list[str]:
        return [l for l in self.log().splitlines() if f"[ROUTE] {kind}" in l]

    def stop(self) -> None:
        self.proc.terminate()
        self.proc.wait(30)


def stream_request(url: str, max_tokens: int, content: str = "hello",
                   session_id: str | None = None) -> tuple[list[str], list[float]]:
    """Returns the SSE data payloads and the arrival time of each one."""
    events, times = [], []
    body = {"messages": [{"role": "user", "content": content}], "max_tokens": max_tokens, "stream": True}
    if session_id is not None:
        body["session_id"] = session_id
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
        for c in json.loads(e).get("choices", []):
            out += (c.get("delta") or {}).get("content") or ""
    return out


def sim_text(n: int) -> str:
    return "".join(f"sim{i} " for i in range(n))
