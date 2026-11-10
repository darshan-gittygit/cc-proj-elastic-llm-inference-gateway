"""
scheduler.py - Scheduler implementations (owner A). The single central queue.

The scheduler only decides WHICH request goes next; the router decides WHERE
(ARCHITECTURE.md section 2). The dispatch loop calls pop() only when some replica
has a free slot (decision 1), so nothing queues anywhere else in the gateway.

fcfs       arrival order.
spf_aging  shortest prompt first with aging (decision 4):
               priority = prompt_tokens - aging_alpha * wait_seconds   (lowest served first)
           Every waiting request ages at the same rate, so
               priority = (prompt_tokens + alpha * arrival_ts) - alpha * now
           and the order depends only on the constant key  prompt_tokens + alpha * arrival_ts.
           A heap on that key gives O(log n) push/pop with no re-sorting as time passes.
           Starvation bound: a request with p_long tokens outranks every request with
           p_short tokens that arrives more than (p_long - p_short) / alpha seconds after it.
           It still waits for requests that arrived before that point to be served.

All times are time.monotonic() (Request.arrival_ts / wait_time()).
Requests put back by fault handling (requeue_front) are served before everything else.
"""
from __future__ import annotations

import collections
import heapq
import time
from typing import Optional

from gateway.interfaces import Config, EventLog, Request, Scheduler, register_scheduler


class FCFSScheduler(Scheduler):

    def __init__(self, log: EventLog) -> None:
        self.log = log
        self._q: collections.deque[Request] = collections.deque()

    def push(self, req: Request) -> None:
        self._q.append(req)
        self.log.sched("queued", req=req.id, prompt_tokens=req.prompt_tokens, queue_len=len(self._q))

    def pop(self, now: Optional[float] = None) -> Optional[Request]:
        if not self._q:
            return None
        now = time.monotonic() if now is None else now
        req = self._q.popleft()
        self.log.sched("selected", req=req.id, policy="fcfs", prompt_tokens=req.prompt_tokens,
                       wait_s=f"{req.wait_time(now):.3f}", reason="oldest", still_waiting=len(self._q))
        return req

    def requeue_front(self, req: Request) -> None:
        self._q.appendleft(req)
        self.log.sched("requeued at front", req=req.id, queue_len=len(self._q))

    def __len__(self) -> int:
        return len(self._q)


class SPFAgingScheduler(Scheduler):

    def __init__(self, log: EventLog, aging_alpha: float) -> None:
        if aging_alpha < 0:
            raise ValueError("aging_alpha must be >= 0")
        self.log = log
        self.alpha = aging_alpha
        self._heap: list[tuple[float, int, Request]] = []    # (static key, req id for ties, req)
        self._front: collections.deque[Request] = collections.deque()

    def priority(self, req: Request, now: float) -> float:
        return req.prompt_tokens - self.alpha * req.wait_time(now)

    def push(self, req: Request) -> None:
        heapq.heappush(self._heap, (req.prompt_tokens + self.alpha * req.arrival_ts, req.id, req))
        self.log.sched("queued", req=req.id, prompt_tokens=req.prompt_tokens, queue_len=len(self))

    def pop(self, now: Optional[float] = None) -> Optional[Request]:
        now = time.monotonic() if now is None else now
        if self._front:
            req = self._front.popleft()
            self.log.sched("selected", req=req.id, policy="spf_aging", prompt_tokens=req.prompt_tokens,
                           wait_s=f"{req.wait_time(now):.3f}", reason="requeued", still_waiting=len(self))
            return req
        if not self._heap:
            return None
        _, _, req = heapq.heappop(self._heap)
        nxt = f"req{self._heap[0][2].id}:{self.priority(self._heap[0][2], now):.1f}" if self._heap else "none"
        self.log.sched("selected", req=req.id, policy="spf_aging", prompt_tokens=req.prompt_tokens,
                       wait_s=f"{req.wait_time(now):.3f}", priority=f"{self.priority(req, now):.1f}",
                       next_best=nxt, still_waiting=len(self))
        return req

    def requeue_front(self, req: Request) -> None:
        self._front.append(req)
        self.log.sched("requeued at front", req=req.id, queue_len=len(self))

    def __len__(self) -> int:
        return len(self._heap) + len(self._front)


@register_scheduler("fcfs")
def _make_fcfs(cfg: Config, log: EventLog) -> Scheduler:
    return FCFSScheduler(log)


@register_scheduler("spf_aging")
def _make_spf_aging(cfg: Config, log: EventLog) -> Scheduler:
    return SPFAgingScheduler(log, cfg.aging_alpha)
