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
  - Phase 5: HealthMonitor (gateway/health.py) marks replicas DEAD and replaces them.
             A stream that breaks goes back to the FRONT of the queue (up to max_retries)
             and restarts on another replica. The client's stream continues where it
             stopped: the regenerated tokens it already has are compared with what it got
             (assumption #6, greedy decoding) and skipped. If they differ, the client gets
             a {"llmgw_retry": ...} event and the output restarts from the beginning.
  - Phase 6: LoadAutoscaler (gateway/autoscaler.py) scales between min_replicas and
             max_replicas and keeps the warm pool (decisions 7, 8)
  - Phase 8: POST /admin/reload re-reads the config file (decision 10). The whole file is
             validated first; if anything is invalid nothing changes (HTTP 400). Otherwise all
             values are applied at once, the scheduler / router are rebuilt from the registries
             when their setting changed, and queued requests move to the new scheduler.
             Container settings apply to replicas started afterwards. container_prefix and
             docker_network need a restart and are rejected.

Run (from the project root, on the Linux host that runs Docker):
    uvicorn gateway.main:app --port 8000
    LLMGW_CONFIG=/path/to/other.yaml uvicorn gateway.main:app --port 8000
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Optional

import httpx
import yaml
from fastapi import FastAPI
from fastapi import Request as HTTPRequest
from fastapi.responses import JSONResponse, StreamingResponse

import gateway.router     # noqa: F401  (registers round_robin / least_busy / session_affinity)
import gateway.scheduler  # noqa: F401  (registers fcfs / spf_aging)
from gateway.autoscaler import LoadAutoscaler
from gateway.health import ProbeHealthMonitor
from gateway.interfaces import (Config, EventLog, HealthMonitor, Replica, ReplicaPool, Request,
                                RequestState, Scheduler, build_router, build_scheduler)
from gateway.logging_conf import setup_logging
from gateway.replica_manager import DockerReplicaManager


def _log_crash(t: asyncio.Task) -> None:
    """Done-callback for background tasks (dispatch loop, health monitor, autoscaler,
    mark_dead): a crash is logged at ERROR with its traceback instead of passing silently.
    Logging only; the gateway does not restart the task."""
    if t.cancelled() or t.exception() is None:
        return
    logging.getLogger("gateway").error("background task crashed task=%s error=%r",
                                       t.get_coro().__qualname__, t.exception(),
                                       exc_info=t.exception())


CONFIG_PATH = os.environ.get("LLMGW_CONFIG", str(Path(__file__).with_name("config.yaml")))


def estimate_prompt_tokens(messages: list[dict]) -> int:
    """Decision 3: prompt length estimated as len(text)/4 at ingress."""
    chars = sum(len(str(m.get("content") or "")) for m in messages)
    return max(1, chars // 4)


class ReplicaHTTPError(Exception):
    pass


RESTART_ONLY = ("container_prefix", "docker_network")   # running containers depend on them
PREWARM_MODES = ("none", "warm_pool")


def validate_config(new: Config, current: Config) -> dict[str, tuple[Any, Any]]:
    """Checks a freshly loaded Config; returns {key: (old, new)} for the values that differ.
    Raises ValueError and touches nothing if any value is unusable."""
    defaults = Config()
    for f in fields(Config):
        if f.name == "extra":
            continue
        want, got = type(getattr(defaults, f.name)), getattr(new, f.name)
        ok = (isinstance(got, (int, float)) and not isinstance(got, bool)) if want is float else \
             (isinstance(got, want) and not isinstance(got, bool))
        if not ok:
            raise ValueError(f"{f.name}: expected {want.__name__}, got {got!r}")
    if new.prewarm not in PREWARM_MODES:
        raise ValueError(f"prewarm: expected one of {PREWARM_MODES}, got {new.prewarm!r}")
    if not 1 <= new.min_replicas <= new.max_replicas:
        raise ValueError(f"need 1 <= min_replicas <= max_replicas, got {new.min_replicas} / {new.max_replicas}")
    for key in RESTART_ONLY:
        if getattr(new, key) != getattr(current, key):
            raise ValueError(f"{key} cannot change while running (restart the gateway)")
    return {f.name: (getattr(current, f.name), getattr(new, f.name)) for f in fields(Config)
            if f.name != "extra" and getattr(current, f.name) != getattr(new, f.name)}


@contextmanager
def _quiet(scheduler: Scheduler):
    """Moving requests between schedulers is not a scheduling decision: mute their log meanwhile."""
    log = getattr(scheduler, "log", None)
    if log is None:
        yield
        return
    silent = EventLog("gateway.reload-move")
    logging.getLogger("gateway.reload-move").disabled = True
    scheduler.log = silent
    try:
        yield
    finally:
        scheduler.log = log


@dataclass
class _Progress:
    """What the client of one request has received so far, kept across retries."""
    texts: list[str] = field(default_factory=list)    # content chunks sent to the client, in order
    forwarded_any: bool = False                        # the opening (role) chunk has been sent


class Gateway:
    """Owns the queue, the router, the replica pool and the dispatch loop."""

    def __init__(self, cfg: Config, log: EventLog) -> None:
        self.cfg, self.log = cfg, log
        self.pool = ReplicaPool()
        self.scheduler: Scheduler = build_scheduler(cfg, log)
        self.router = build_router(cfg, log)
        self.manager: Optional[DockerReplicaManager] = None
        self.health: Optional[HealthMonitor] = None
        self.autoscaler: Optional[LoadAutoscaler] = None
        self._wake = asyncio.Event()
        self._client: Optional[httpx.AsyncClient] = None
        self._background: set[asyncio.Task] = set()
        self._streams: dict[int, asyncio.Task] = {}
        self._inflight: dict[int, Request] = {}         # dispatched, stream task still running
        self._progress: dict[int, _Progress] = {}
        self._retry_ids: set[int] = set()               # stream tasks cancelled by us to requeue them
        # Decision 5: full conversation per session; clients send only the new message(s).
        self.sessions: dict[str, list[dict]] = {}

    # ---- lifecycle -------------------------------------------------------
    async def start(self) -> None:
        # read=None: tokens can be seconds apart on a CPU-limited replica
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(connect=5.0, read=None, write=10.0, pool=None))
        self.manager = DockerReplicaManager(self.cfg, self.pool, self.log, on_change=self._wake.set,
                                            on_removed=lambda r: self.router.on_replica_removed(r))
        await self.manager.setup()
        self.health = ProbeHealthMonitor(self.cfg, self.pool, self.manager, self.log,
                                         on_dead=self._on_replica_dead)
        self._spawn(self.health.run())
        self._spawn(self._dispatch_loop())
        for _ in range(self.cfg.min_replicas):
            await self.manager.start_replica()
        self.autoscaler = LoadAutoscaler(self.cfg, self.pool, self.scheduler, self.manager, self.log)
        self._spawn(self.autoscaler.run())

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
        t.add_done_callback(_log_crash)
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
        self._retry_ids.discard(req.id)          # a client that left is never retried
        t = self._streams.get(req.id)
        if t and not t.done():
            t.cancel()
        elif req.state == RequestState.QUEUED:
            req.state = RequestState.FAILED

    def reload(self, new: Config) -> dict[str, tuple[Any, Any]]:
        """Decision 10. Validate everything first (raises ValueError, nothing changed), then apply
        everything in one step with no await in between, so the dispatch loop never sees a
        half-applied config. Returns the changed keys."""
        changed = validate_config(new, self.cfg)
        new_sched = build_scheduler(new, self.log) if {"scheduler", "aging_alpha"} & changed.keys() else None
        new_router = build_router(new, self.log) if "router" in changed else None
        # ---- apply (nothing below can fail on bad input) ----
        for key, (_, value) in changed.items():
            setattr(self.cfg, key, value)               # manager / health / autoscaler share this object
        moved = 0
        if new_sched is not None:
            old = self.scheduler
            with _quiet(old):
                waiting = []
                while (r := old.pop()) is not None:       # front (retried) requests come out first
                    if r.state != RequestState.FAILED:  # client already gone: drop
                        waiting.append(r)
            # Decision 6: a retried request (dispatched before, attempts > 0) stays at the FRONT.
            retries = [r for r in waiting if r.attempts > 0]          # keep their current order
            fresh = sorted((r for r in waiting if r.attempts == 0), key=lambda r: (r.arrival_ts, r.id))
            with _quiet(new_sched):
                for r in fresh:
                    new_sched.push(r)
                # FCFS.requeue_front prepends (last call ends up first); SPF's appends to its
                # front deque (first call ends up first). Feed them so the order is unchanged.
                is_fcfs = isinstance(new_sched, gateway.scheduler.FCFSScheduler)
                for r in (reversed(retries) if is_fcfs else retries):
                    new_sched.requeue_front(r)
            moved = len(waiting)
            self.scheduler = new_sched
            if self.autoscaler is not None:
                self.autoscaler.scheduler = new_sched
        if new_router is not None:
            self.router = new_router
        self.log.emit("CONFIG", "reload applied",
                      changed=",".join(f"{k}:{o}->{n}" for k, (o, n) in changed.items()) or "nothing",
                      queued_moved=moved)
        self._wake.set()
        return changed

    async def _on_replica_dead(self, replica: Replica) -> None:
        """HealthMonitor callback: stop the streams still running on a dead replica (they may
        hang, e.g. a frozen container) so they are requeued at the front and retried."""
        victims = [r for r in self._inflight.values() if r.replica_id == replica.id]
        for req in victims:
            t = self._streams.get(req.id)
            if t and not t.done():
                self._retry_ids.add(req.id)
                t.cancel()
        self.log.health("requeueing in-flight requests of dead replica", replica=replica.id,
                        requests=",".join(str(r.id) for r in victims) or "none")

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
                    self._progress.pop(req.id, None)
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
        if req.attempts > 1:
            extra["attempt"] = req.attempts
        self.log.route("dispatch", req=req.id, replica=replica.id, policy=self.cfg.router,
                       loads_before=loads, in_flight=replica.in_flight, **extra)
        self._inflight[req.id] = req
        self._retry_ids.discard(req.id)          # a stale mark from an earlier attempt must not linger
        self._progress.setdefault(req.id, _Progress())
        t = asyncio.create_task(self._stream(req, replica))
        self._streams[req.id] = t

    async def _stream(self, req: Request, replica: Replica) -> None:
        """One attempt of one request on one replica. On a retry (attempts > 1) the first
        len(prog.texts) content chunks are what the client already has: they are held back
        and compared with the regenerated ones (assumption #6) instead of being sent again."""
        assert self._client
        prog = self._progress.setdefault(req.id, _Progress())
        payload = {"messages": req.messages, "max_tokens": req.max_tokens,
                   "temperature": self.cfg.default_temperature, "stream": True}
        t0 = time.monotonic()
        ttft: Optional[float] = None
        resume_at = len(prog.texts) if req.attempts > 1 else 0
        verifying = resume_at > 0
        held: list[str] = []          # this attempt's chunks while checking the prefix
        matched = 0
        requeued = False

        async def forward(data: str) -> None:
            nonlocal ttft
            c = _content_of(data)
            if not c and req.attempts > 1 and prog.forwarded_any and _is_role_chunk(data):
                return                # the client already got the opening chunk
            if c:
                prog.texts.append(c)
                req.tokens_emitted = len(prog.texts)
                if ttft is None:
                    ttft = time.monotonic() - t0
            prog.forwarded_any = True
            await req.out.put(f"data: {data}\n\n")

        async def restart_output(at_token: int) -> None:
            nonlocal verifying
            verifying = False
            self.log.route("retry output differs, client told to restart", req=req.id, replica=replica.id,
                           at_token=at_token, already_sent=len(prog.texts))
            await req.out.put("data: " + json.dumps({"llmgw_retry": {
                "reason": "regenerated output differs from what was sent",
                "discard_previous_output": True}}) + "\n\n")
            prog.texts.clear()
            req.tokens_emitted = 0
            for h in held:
                await forward(h)

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
                    if verifying:
                        held.append(data)
                        c = _content_of(data)
                        if c:
                            if c != prog.texts[matched]:
                                await restart_output(matched)
                                continue
                            matched += 1
                            if matched == resume_at:
                                verifying = False
                                self.log.route("retry resumed", req=req.id, replica=replica.id,
                                               skipped_tokens=resume_at, prefix_matched="yes")
                        continue
                    await forward(data)
            if verifying:             # regenerated output ended before the resume point
                await restart_output(matched)
            req.state = RequestState.DONE
            if req.session_id is not None:     # only a completed turn enters the history
                self.sessions[req.session_id] = req.messages + [
                    {"role": "assistant", "content": "".join(prog.texts)}]
            self.router.on_complete(req, replica)
            self.log.route("complete", req=req.id, replica=replica.id, tokens=req.tokens_emitted,
                           attempts=req.attempts, ttft_s=f"{ttft:.3f}" if ttft is not None else "na",
                           total_s=f"{time.monotonic() - t0:.3f}")
        except asyncio.CancelledError:
            if req.id not in self._retry_ids:
                req.state = RequestState.FAILED
                self.log.route("cancelled (client disconnected)", req=req.id, replica=replica.id,
                               tokens=req.tokens_emitted)
                raise
            # cancelled by _on_replica_dead: requeue instead of failing
            self._retry_ids.discard(req.id)
            requeued = await self._retry_or_fail(req, replica, "replica_dead")
        except (httpx.HTTPError, ReplicaHTTPError) as e:
            if isinstance(e, httpx.TransportError) and self.health is not None:
                # decision 9: a stream connection error marks the replica DEAD
                self._inflight.pop(req.id, None)       # this task handles its own retry
                self._spawn(self.health.mark_dead(replica, reason="stream_error"))
            requeued = await self._retry_or_fail(req, replica, type(e).__name__)
        finally:
            replica.in_flight -= 1
            self._streams.pop(req.id, None)
            self._inflight.pop(req.id, None)
            if not requeued:
                self._progress.pop(req.id, None)
                req.out.put_nowait(None)
            self._wake.set()

    async def _retry_or_fail(self, req: Request, replica: Replica, reason: str) -> bool:
        """Decision 6: back to the FRONT of the queue, at most cfg.max_retries times."""
        sent = len(self._progress.get(req.id, _Progress()).texts)
        if req.attempts <= self.cfg.max_retries:
            req.state = RequestState.QUEUED
            self.log.route("stream failed, retrying", req=req.id, replica=replica.id, reason=reason,
                           attempt=f"{req.attempts}/{self.cfg.max_retries + 1}", tokens_sent=sent)
            self.scheduler.requeue_front(req)
            return True
        req.state = RequestState.FAILED
        self.log.route("stream failed, retries exhausted", req=req.id, replica=replica.id, reason=reason,
                       attempts=req.attempts, tokens_sent=sent)
        err = {"error": {"message": f"replica {replica.id} failed ({reason}) after {req.attempts} attempt(s)",
                         "type": "replica_error"}}
        await req.out.put(f"data: {json.dumps(err)}\n\n")
        return False


def _content_of(data: str) -> str:
    try:
        choices = json.loads(data).get("choices") or []
        if not choices:
            return ""
        return (choices[0].get("delta") or {}).get("content") or ""
    except (ValueError, AttributeError):
        return ""


def _is_role_chunk(data: str) -> bool:
    try:
        choices = json.loads(data).get("choices") or []
        return bool(choices) and "role" in (choices[0].get("delta") or {})
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


@app.post("/admin/reload")
async def admin_reload(http_req: HTTPRequest):
    """Re-read the config file (LLMGW_CONFIG or gateway/config.yaml) and apply it without a restart."""
    gw: Gateway = http_req.app.state.gw
    try:
        new = Config.load(CONFIG_PATH)
        changed = gw.reload(new)
    except (OSError, yaml.YAMLError, AttributeError, TypeError, ValueError) as e:
        one_line = "_".join(str(e).split())      # collapse spaces, tabs, CR/LF: one event per log line
        gw.log.emit("CONFIG", "reload rejected, nothing changed", error=one_line[:200])
        return JSONResponse({"applied": False, "error": str(e)}, status_code=400)
    return {"applied": True, "changed": {k: [o, n] for k, (o, n) in changed.items()},
            "ignored_unknown_keys": sorted(new.extra)}
