"""
main.py - FastAPI entry point of the Elastic LLM Inference Gateway.

PHASE 1 SKELETON. Implements only the single-request streaming path of
ARCHITECTURE.md section 2:

    curl --(SSE)--> POST /v1/chat/completions
        -> Request pushed to the central queue
        -> dispatch loop pops it only when a replica has a free slot (decision 1)
        -> stream task posts to the replica with stream=true and pushes each
           chunk into Request.out; the HTTP handler yields from it (decision 2)
        -> None in Request.out marks the end; the handler then sends [DONE]

Deliberately NOT here yet (each belongs to its own later phase):
  - scheduling policies FCFS / SPF+aging      -> Phase 3 (build_scheduler)
  - routers, ReplicaManager / Docker API      -> Phase 2
  - session affinity                          -> Phase 4
  - HealthMonitor, retry / requeue on failure -> Phase 5
  - autoscaler, warm pool                     -> Phase 6 / 7
  - POST /admin/reload                        -> Phase 8

Phase 1 bootstrap: the one replica is started outside the gateway (by
scripts/phase1_stream_check.sh, or by hand) and passed in with
LLMGW_STATIC_REPLICA=http://host:port. Phase 2 replaces this with ReplicaManager.

Run (from the project root):
    LLMGW_STATIC_REPLICA=http://127.0.0.1:18080 uvicorn gateway.main:app --port 8000
"""
from __future__ import annotations

import asyncio
import collections
import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI
from fastapi import Request as HTTPRequest
from fastapi.responses import JSONResponse, StreamingResponse

from gateway.interfaces import (Config, EventLog, Replica, ReplicaPool, ReplicaState,
                                Request, RequestState, Scheduler)
from gateway.logging_conf import setup_logging

CONFIG_PATH = os.environ.get("LLMGW_CONFIG", str(Path(__file__).with_name("config.yaml")))
STATIC_REPLICA_ENV = "LLMGW_STATIC_REPLICA"   # Phase 1 only
READINESS_POLL_S = 0.5                        # same poll step as check_feasibility.sh


def estimate_prompt_tokens(messages: list[dict]) -> int:
    """Decision 3: prompt length estimated as len(text)/4 at ingress."""
    chars = sum(len(str(m.get("content") or "")) for m in messages)
    return max(1, chars // 4)


class Phase1FifoQueue(Scheduler):
    """PHASE 1 PLACEHOLDER, not a scheduling policy.

    Holds requests in arrival order so the dispatch loop already talks to the
    Scheduler interface. It is not registered in SCHEDULERS; Phase 3 replaces it
    with build_scheduler(cfg) (FCFS / SPF+aging).
    """

    def __init__(self, log: EventLog) -> None:
        self._q: collections.deque[Request] = collections.deque()
        self.log = log

    def push(self, req: Request) -> None:
        self._q.append(req)
        self.log.sched("queued", req=req.id, prompt_tokens=req.prompt_tokens, queue_len=len(self._q))

    def pop(self, now: Optional[float] = None) -> Optional[Request]:
        if not self._q:
            return None
        req = self._q.popleft()
        self.log.sched("dequeued", req=req.id, wait_s=f"{req.wait_time(now):.3f}", queue_len=len(self._q))
        return req

    def requeue_front(self, req: Request) -> None:
        self._q.appendleft(req)

    def __len__(self) -> int:
        return len(self._q)


class ReplicaHTTPError(Exception):
    pass


class Gateway:
    """Owns the queue, the replica pool and the dispatch loop."""

    def __init__(self, cfg: Config, log: EventLog) -> None:
        self.cfg, self.log = cfg, log
        self.pool = ReplicaPool()
        self.scheduler: Scheduler = Phase1FifoQueue(log)
        self._wake = asyncio.Event()
        self._client: Optional[httpx.AsyncClient] = None
        self._background: set[asyncio.Task] = set()
        self._streams: dict[int, asyncio.Task] = {}

    # ---- lifecycle -------------------------------------------------------
    async def start(self) -> None:
        # read=None: tokens can be seconds apart on a CPU-limited replica
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(connect=5.0, read=None, write=10.0, pool=None))
        self._spawn(self._dispatch_loop())

    async def stop(self) -> None:
        tasks = list(self._background) + list(self._streams.values())
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._client:
            await self._client.aclose()

    def _spawn(self, coro) -> asyncio.Task:
        t = asyncio.create_task(coro)
        self._background.add(t)
        t.add_done_callback(self._background.discard)
        return t

    # ---- Phase 1 bootstrap: one externally started replica ----------------
    def add_static_replica(self, url: str) -> Replica:
        u = httpx.URL(url)
        r = Replica(id="r1", container_id="external", host=u.host, port=u.port or 80,
                    state=ReplicaState.STARTING, max_slots=self.cfg.slots_per_replica)
        self.pool.add(r)
        self.log.health("replica registered", replica=r.id, url=r.base_url, max_slots=r.max_slots)
        self._spawn(self._wait_until_ready(r))
        return r

    async def _wait_until_ready(self, r: Replica) -> None:
        """One-time readiness wait (STARTING -> READY). Continuous probing is Phase 5."""
        assert self._client
        while True:
            try:
                resp = await self._client.get(f"{r.base_url}/health", timeout=2.0)
                if resp.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(READINESS_POLL_S)
        r.state, r.ready_ts = ReplicaState.READY, time.monotonic()
        self.log.health("replica ready", replica=r.id, waited_s=f"{r.ready_ts - r.created_ts:.2f}")
        self._wake.set()

    # ---- request path ----------------------------------------------------
    def submit(self, req: Request) -> None:
        self.scheduler.push(req)
        self._wake.set()

    def cancel(self, req: Request) -> None:
        """Client went away: stop its replica stream so the slot is freed."""
        t = self._streams.get(req.id)
        if t and not t.done():
            t.cancel()

    async def _dispatch_loop(self) -> None:
        while True:
            await self._wake.wait()
            self._wake.clear()
            while len(self.scheduler) and self.pool.routable():
                req = self.scheduler.pop()
                if req is None:
                    break
                replica = self.pool.routable()[0]   # placeholder: routers arrive in Phase 2
                self._dispatch(req, replica)

    def _dispatch(self, req: Request, replica: Replica) -> None:
        replica.in_flight += 1
        req.attempts += 1
        req.replica_id = replica.id
        req.state = RequestState.RUNNING
        self.log.route("dispatch", req=req.id, replica=replica.id,
                       in_flight=replica.in_flight, free_slots=replica.free_slots)
        t = asyncio.create_task(self._stream(req, replica))
        self._streams[req.id] = t

    async def _stream(self, req: Request, replica: Replica) -> None:
        assert self._client
        payload = {"messages": req.messages, "max_tokens": req.max_tokens,
                   "temperature": self.cfg.default_temperature, "stream": True}
        t0 = time.monotonic()
        ttft: Optional[float] = None
        try:
            async with self._client.stream("POST", f"{replica.base_url}/v1/chat/completions",
                                           json=payload) as resp:
                if resp.status_code != 200:
                    body = (await resp.aread())[:300]
                    raise ReplicaHTTPError(f"HTTP {resp.status_code}: {body!r}")
                async for line in resp.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        break
                    if _has_content(data):
                        req.tokens_emitted += 1
                        if ttft is None:
                            ttft = time.monotonic() - t0
                    await req.out.put(f"data: {data}\n\n")
            req.state = RequestState.DONE
            self.log.route("complete", req=req.id, replica=replica.id, tokens=req.tokens_emitted,
                           ttft_s=f"{ttft:.3f}" if ttft is not None else "na",
                           total_s=f"{time.monotonic() - t0:.3f}")
        except asyncio.CancelledError:
            req.state = RequestState.FAILED
            self.log.route("cancelled (client disconnected)", req=req.id, replica=replica.id,
                           tokens=req.tokens_emitted)
            raise
        except (httpx.HTTPError, ReplicaHTTPError) as e:
            # No retry before Phase 5: report the error to the client and end the stream.
            req.state = RequestState.FAILED
            self.log.route("stream failed, no retry in phase 1", req=req.id, replica=replica.id,
                           tokens=req.tokens_emitted, error=type(e).__name__)
            err = {"error": {"message": f"replica {replica.id} failed: {e}", "type": "replica_error"}}
            await req.out.put(f"data: {json.dumps(err)}\n\n")
        finally:
            replica.in_flight -= 1
            self._streams.pop(req.id, None)
            req.out.put_nowait(None)
            self._wake.set()


def _has_content(data: str) -> bool:
    try:
        choices = json.loads(data).get("choices") or []
        return bool(choices and (choices[0].get("delta") or {}).get("content"))
    except (ValueError, AttributeError):
        return False


# --------------------------------------------------------------------------- #
# FastAPI app
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    cfg = Config.load(CONFIG_PATH)
    log = EventLog()
    url = os.environ.get(STATIC_REPLICA_ENV)
    if not url:
        raise RuntimeError(f"Phase 1 needs {STATIC_REPLICA_ENV}=http://host:port of a running replica")
    gw = Gateway(cfg, log)
    await gw.start()
    gw.add_static_replica(url)
    app.state.gw = gw
    try:
        yield
    finally:
        await gw.stop()


app = FastAPI(title="Elastic LLM Inference Gateway", lifespan=lifespan)


@app.post("/v1/chat/completions")
async def chat_completions(http_req: HTTPRequest):
    try:
        body = await http_req.json()
    except ValueError:
        return JSONResponse({"error": "body must be JSON"}, status_code=400)
    messages = body.get("messages") if isinstance(body, dict) else None
    if not isinstance(messages, list) or not messages:
        return JSONResponse({"error": "'messages' must be a non-empty list"}, status_code=400)
    if body.get("stream") is False:
        return JSONResponse({"error": "this gateway only streams; send stream=true"}, status_code=400)

    gw: Gateway = http_req.app.state.gw
    req = Request(messages=messages, prompt_tokens=estimate_prompt_tokens(messages),
                  max_tokens=int(body.get("max_tokens", 256)))
    gw.submit(req)

    async def sse():
        try:
            while True:
                chunk = await req.out.get()
                if chunk is None:
                    break
                yield chunk
            yield "data: [DONE]\n\n"
        finally:
            gw.cancel(req)

    return StreamingResponse(sse(), media_type="text/event-stream",
                             headers={"X-Request-Id": str(req.id), "Cache-Control": "no-cache"})
