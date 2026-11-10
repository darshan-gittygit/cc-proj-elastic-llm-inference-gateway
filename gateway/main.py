"""
main.py - FastAPI entry point of the Elastic LLM Inference Gateway.

Streaming path of ARCHITECTURE.md section 2:

    curl --(SSE)--> POST /v1/chat/completions
        -> Request pushed to the central queue
        -> dispatch loop pops it only when a replica has a free slot (decision 1)
           and asks the Router which replica gets it
        -> stream task posts to the replica with stream=true and pushes each
           chunk into Request.out; the HTTP handler yields from it (decision 2)
        -> None in Request.out marks the end; the handler then sends [DONE]

Implemented so far:
  - Phase 1: streaming path, central queue, dispatch loop
  - Phase 2: DockerReplicaManager starts `min_replicas` containers at startup and
             removes them at exit; Router chosen by `router:` in config.yaml
  - Phase 3: Scheduler chosen by `scheduler:` in config.yaml (fcfs | spf_aging)
  - Phase 4: optional "session_id" in the request body. The gateway keeps each session's
             full history (clients send only the new message) and `router: session_affinity`
             keeps a session on one replica (decision 5)
Deliberately NOT here yet (each belongs to its own later phase):
  - HealthMonitor, retry / requeue on failure -> Phase 5
  - autoscaler, warm pool                     -> Phase 6 / 7
  - POST /admin/reload                        -> Phase 8

Run (from the project root, on the Linux host that runs Docker):
    uvicorn gateway.main:app --port 8000
    LLMGW_CONFIG=/path/to/other.yaml uvicorn gateway.main:app --port 8000
"""
from __future__ import annotations

import asyncio
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

import gateway.router     # noqa: F401  (registers round_robin / least_busy)
import gateway.scheduler  # noqa: F401  (registers fcfs / spf_aging)
from gateway.interfaces import (Config, EventLog, Replica, ReplicaPool, Request, RequestState,
                                Scheduler, build_router, build_scheduler)
from gateway.logging_conf import setup_logging
from gateway.replica_manager import DockerReplicaManager

CONFIG_PATH = os.environ.get("LLMGW_CONFIG", str(Path(__file__).with_name("config.yaml")))


def estimate_prompt_tokens(messages: list[dict]) -> int:
    """Decision 3: prompt length estimated as len(text)/4 at ingress."""
    chars = sum(len(str(m.get("content") or "")) for m in messages)
    return max(1, chars // 4)


class ReplicaHTTPError(Exception):
    pass


class Gateway:
    """Owns the queue, the router, the replica pool and the dispatch loop."""

    def __init__(self, cfg: Config, log: EventLog) -> None:
        self.cfg, self.log = cfg, log
        self.pool = ReplicaPool()
        self.scheduler: Scheduler = build_scheduler(cfg, log)
        self.router = build_router(cfg, log)
        self.manager: Optional[DockerReplicaManager] = None
        self._wake = asyncio.Event()
        self._client: Optional[httpx.AsyncClient] = None
        self._background: set[asyncio.Task] = set()
        self._streams: dict[int, asyncio.Task] = {}
        # Decision 5: full conversation per session; clients send only the new message(s).
        self.sessions: dict[str, list[dict]] = {}

    # ---- lifecycle -------------------------------------------------------
    async def start(self) -> None:
        # read=None: tokens can be seconds apart on a CPU-limited replica
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(connect=5.0, read=None, write=10.0, pool=None))
        self.manager = DockerReplicaManager(self.cfg, self.pool, self.log, on_change=self._wake.set,
                                            on_removed=lambda r: self.router.on_replica_removed(r))
        await self.manager.setup()
        self._spawn(self._dispatch_loop())
        for _ in range(self.cfg.min_replicas):
            await self.manager.start_replica()

    async def stop(self) -> None:
        tasks = list(self._background) + list(self._streams.values())
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.manager:
            await self.manager.shutdown()
        if self._client:
            await self._client.aclose()

    def _spawn(self, coro) -> asyncio.Task:
        t = asyncio.create_task(coro)
        self._background.add(t)
        t.add_done_callback(self._background.discard)
        return t

    # ---- request path ----------------------------------------------------
    def new_request(self, messages: list[dict], max_tokens: int, session_id: Optional[str]) -> Request:
        """With a session_id, the stored history is prepended, so every replica receives the
        whole conversation (correctness never depends on one replica's cache)."""
        full = self.sessions.get(session_id, []) + messages if session_id else messages
        return Request(messages=full, prompt_tokens=estimate_prompt_tokens(full),
                       max_tokens=max_tokens, session_id=session_id)

    def submit(self, req: Request) -> None:
        self.scheduler.push(req)
        self._wake.set()

    def cancel(self, req: Request) -> None:
        """Client went away: stop its replica stream so the slot is freed. If the request
        is still queued, mark it so the dispatch loop drops it instead of dispatching it."""
        t = self._streams.get(req.id)
        if t and not t.done():
            t.cancel()
        elif req.state == RequestState.QUEUED:
            req.state = RequestState.FAILED

    async def _dispatch_loop(self) -> None:
        while True:
            await self._wake.wait()
            self._wake.clear()
            while len(self.scheduler) and self.pool.routable():
                req = self.scheduler.pop()
                if req is None:
                    break
                if req.state == RequestState.FAILED:      # client disconnected while queued
                    self.log.sched("dropped (client disconnected while queued)", req=req.id)
                    continue
                # load snapshot BEFORE the decision, so the log shows why it was made
                loads = ",".join(f"{r.id}:{r.in_flight}/{r.max_slots}" for r in self.pool.ready())
                replica = self.router.choose(req, self.pool)
                if replica is None:
                    self.scheduler.requeue_front(req)
                    break
                self._dispatch(req, replica, loads)

    def _dispatch(self, req: Request, replica: Replica, loads: str) -> None:
        replica.in_flight += 1
        req.attempts += 1
        req.replica_id = replica.id
        req.state = RequestState.RUNNING
        extra = {}
        if req.session_id is not None:
            extra["session"] = req.session_id
        if getattr(self.router, "last_decision", None):
            extra["affinity"] = self.router.last_decision
        self.log.route("dispatch", req=req.id, replica=replica.id, policy=self.cfg.router,
                       loads_before=loads, in_flight=replica.in_flight, **extra)
        t = asyncio.create_task(self._stream(req, replica))
        self._streams[req.id] = t

    async def _stream(self, req: Request, replica: Replica) -> None:
        assert self._client
        payload = {"messages": req.messages, "max_tokens": req.max_tokens,
                   "temperature": self.cfg.default_temperature, "stream": True}
        t0 = time.monotonic()
        ttft: Optional[float] = None
        reply: list[str] = []
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
                    content = _content_of(data)
                    if content:
                        req.tokens_emitted += 1
                        reply.append(content)
                        if ttft is None:
                            ttft = time.monotonic() - t0
                    await req.out.put(f"data: {data}\n\n")
            req.state = RequestState.DONE
            if req.session_id is not None:     # only a completed turn enters the history
                self.sessions[req.session_id] = req.messages + [{"role": "assistant", "content": "".join(reply)}]
            self.router.on_complete(req, replica)
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
            self.log.route("stream failed, no retry before phase 5", req=req.id, replica=replica.id,
                           tokens=req.tokens_emitted, error=type(e).__name__)
            err = {"error": {"message": f"replica {replica.id} failed: {e}", "type": "replica_error"}}
            await req.out.put(f"data: {json.dumps(err)}\n\n")
        finally:
            replica.in_flight -= 1
            self._streams.pop(req.id, None)
            req.out.put_nowait(None)
            self._wake.set()


def _content_of(data: str) -> str:
    try:
        choices = json.loads(data).get("choices") or []
        if not choices:
            return ""
        return (choices[0].get("delta") or {}).get("content") or ""
    except (ValueError, AttributeError):
        return ""


# --------------------------------------------------------------------------- #
# FastAPI app
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    cfg = Config.load(CONFIG_PATH)
    log = EventLog()
    gw = Gateway(cfg, log)
    try:
        await gw.start()
    except BaseException:
        await gw.stop()          # never leave containers behind on a failed start
        raise
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
    session_id = body.get("session_id")
    if session_id is not None and (not isinstance(session_id, str) or not session_id):
        return JSONResponse({"error": "'session_id' must be a non-empty string"}, status_code=400)

    gw: Gateway = http_req.app.state.gw
    req = gw.new_request(messages, int(body.get("max_tokens", 256)), session_id)
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
