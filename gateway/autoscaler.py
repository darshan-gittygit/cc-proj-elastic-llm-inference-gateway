"""
autoscaler.py - Autoscaler implementation (owner B). ARCHITECTURE.md decisions 7, 8, 12.

Every TICK_S seconds:
  signal = (queued + in_flight) / ready_replicas, smoothed (EWMA, weight SMOOTHING on the new value).
  Up:   signal > scale_up_threshold continuously for scale_up_after_s
          -> warm_pool with a WARM_SPARE: promote it (instant, no model load), else
          -> start a new replica (it becomes routable only when /health is ok).
        The persistence timer restarts after each step, so steps are at least scale_up_after_s apart.
  Down: signal < scale_down_threshold continuously for scale_down_cooldown_s, and more than
        min_replicas READY -> drain one READY replica (DRAINING: no new requests, wait for
        in_flight == 0, then stop). The timer restarts after each step.
  Different up/down thresholds plus the two timers are the hysteresis that prevents flapping.

Warm pool (prewarm: warm_pool): keep warm_pool_size spares (loaded, not routable). A spare is
started when signal >= prewarm_trigger_ratio * scale_up_threshold, or at idle
(signal < scale_down_threshold), if capacity allows.

Capacity: every replica container counts toward max_replicas, spares included (decision 12 sizes
max_replicas by CPU; a loaded spare holds memory and a CPU limit too). Nothing new is started while
another replica is still STARTING, so one burst cannot start several containers at once.
Replacing DEAD replicas is the HealthMonitor's job; the replacement counts toward min_replicas.

Log tag: SCALE.
"""
from __future__ import annotations

import asyncio
import math
import time
from typing import Optional

from gateway.interfaces import Autoscaler, Replica, ReplicaState

TICK_S = 1.0
SMOOTHING = 0.5


class LoadAutoscaler(Autoscaler):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.signal: float = 0.0
        self._above_since: Optional[float] = None
        self._below_since: Optional[float] = None
        self._blocked: Optional[str] = None          # last "cannot scale up" reason, logged once
        self._tasks: set[asyncio.Task] = set()

    async def run(self) -> None:
        try:
            while True:
                await asyncio.sleep(TICK_S)
                await self.tick(time.monotonic())
        finally:
            for t in list(self._tasks):
                t.cancel()

    # ---- one control step (tests call this directly with their own clock) -------------
    async def tick(self, now: float) -> None:
        c = self.cfg
        ready = self.pool.ready()
        queued, in_flight = len(self.scheduler), self.pool.total_in_flight()
        load = queued + in_flight
        raw = load / len(ready) if ready else (math.inf if load else 0.0)
        if math.isinf(raw) or math.isinf(self.signal):
            self.signal = raw
        else:
            self.signal = SMOOTHING * raw + (1 - SMOOTHING) * self.signal

        starting = self.pool.by_state(ReplicaState.STARTING)
        spares = self.pool.by_state(ReplicaState.WARM_SPARE)
        total = len([r for r in self.pool.all() if r.state != ReplicaState.DEAD])
        self.log.scale("signal", value=_fmt(self.signal), raw=_fmt(raw), queued=queued, in_flight=in_flight,
                       ready=len(ready), spares=len(spares), starting=len(starting), total=total)

        if self.signal > c.scale_up_threshold:
            self._above_since = now if self._above_since is None else self._above_since
        else:
            self._above_since = None
        if self.signal < c.scale_down_threshold:
            self._below_since = now if self._below_since is None else self._below_since
        else:
            self._below_since = None

        if self._above_since is not None and now - self._above_since >= c.scale_up_after_s:
            if await self._scale_up(spares, starting, total):
                self._above_since = now
        elif (self._below_since is not None and now - self._below_since >= c.scale_down_cooldown_s
              and len(ready) > c.min_replicas):
            self._scale_down(ready)
            self._below_since = now

        await self._maintain_warm_pool()

    async def _scale_up(self, spares: list[Replica], starting: list[Replica], total: int) -> bool:
        c = self.cfg
        if c.prewarm == "warm_pool" and spares:
            r = await self.manager.promote_spare()
            if r is not None:
                self.log.scale("scale up", how="promote_spare", replica=r.id, signal=_fmt(self.signal))
                self._blocked = None
                return True
        if starting:
            return self._block("a replica is still starting")
        if total >= c.max_replicas:
            return self._block(f"at max_replicas={c.max_replicas}")
        r = await self.manager.start_replica()
        self.log.scale("scale up", how="start_new", replica=r.id, signal=_fmt(self.signal))
        self._blocked = None
        return True

    def _block(self, why: str) -> bool:
        if why != self._blocked:
            self.log.scale("scale up wanted but blocked", reason=why.replace(" ", "_"), signal=_fmt(self.signal))
            self._blocked = why
        return False

    def _scale_down(self, ready: list[Replica]) -> None:
        # least loaded first; among equals the newest (last started) goes first
        victim = min(reversed(ready), key=lambda r: r.in_flight)
        self.log.scale("scale down", replica=victim.id, in_flight=victim.in_flight, signal=_fmt(self.signal))
        t = asyncio.create_task(self.manager.stop_replica(victim))   # drains, then stops
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _maintain_warm_pool(self) -> None:
        c = self.cfg
        if c.prewarm != "warm_pool":
            # prewarm switched to none by a config reload (Phase 8): spares are no longer wanted
            for r in self.pool.by_state(ReplicaState.WARM_SPARE):
                self.log.scale("spare stopped", replica=r.id, reason="prewarm_none")
                t = asyncio.create_task(self.manager.stop_replica(r))
                self._tasks.add(t)
                t.add_done_callback(self._tasks.discard)
            return
        spares = self.pool.by_state(ReplicaState.WARM_SPARE)
        if len(spares) >= c.warm_pool_size or self.pool.by_state(ReplicaState.STARTING):
            return
        total = len([r for r in self.pool.all() if r.state != ReplicaState.DEAD])
        if total >= c.max_replicas:
            return
        busy = self.signal >= c.prewarm_trigger_ratio * c.scale_up_threshold
        idle = self.signal < c.scale_down_threshold
        if busy or idle:
            r = await self.manager.start_replica(as_spare=True)
            self.log.scale("prewarm spare", replica=r.id, trigger="load" if busy else "idle",
                           signal=_fmt(self.signal))


def _fmt(x: float) -> str:
    return "inf" if math.isinf(x) else f"{x:.2f}"
