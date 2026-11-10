"""
health.py - HealthMonitor implementation (owner C). ARCHITECTURE.md decision 9.

Every `health_interval_s` it probes GET /health on each READY / WARM_SPARE / DRAINING
replica (all probes in parallel). `health_fail_limit` consecutive failures mark the
replica DEAD. The gateway also calls mark_dead() directly when a stream to a replica
breaks with a connection error (decision 9: "... or a stream connection error").

mark_dead():  state -> DEAD, await on_dead(replica) (the gateway requeues that replica's
in-flight requests at the front of the queue), remove the container, start a replacement
(a spare is replaced by a spare; a DRAINING replica is not replaced). STARTING replicas are
not probed here; the ReplicaManager's readiness check covers them.

Log tag: HEALTH.
"""
from __future__ import annotations

import asyncio
from typing import Optional

import httpx

from gateway.interfaces import HealthMonitor, Replica, ReplicaState

PROBE_TIMEOUT_S = 1.0
PROBED = (ReplicaState.READY, ReplicaState.WARM_SPARE, ReplicaState.DRAINING)


class ProbeHealthMonitor(HealthMonitor):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._http: Optional[httpx.AsyncClient] = None

    async def run(self) -> None:
        self._http = httpx.AsyncClient(timeout=PROBE_TIMEOUT_S)
        try:
            while True:
                await asyncio.sleep(self.cfg.health_interval_s)
                await self.probe_all()
        finally:
            await self._http.aclose()

    async def probe_all(self) -> None:
        replicas = self.pool.by_state(*PROBED)
        results = await asyncio.gather(*(self.probe(r) for r in replicas))
        for r, ok in zip(replicas, results):
            if r.state not in PROBED:              # changed while we were probing
                continue
            if ok:
                if r.consecutive_failures:
                    self.log.health("probe ok again", replica=r.id, after_failures=r.consecutive_failures)
                r.consecutive_failures = 0
                continue
            r.consecutive_failures += 1
            self.log.health("probe failed", replica=r.id,
                            failures=f"{r.consecutive_failures}/{self.cfg.health_fail_limit}")
            if r.consecutive_failures >= self.cfg.health_fail_limit:
                await self.mark_dead(r, reason="health_checks")

    async def probe(self, r: Replica) -> bool:
        try:
            assert self._http is not None
            return (await self._http.get(f"{r.base_url}/health")).status_code == 200
        except (httpx.HTTPError, AssertionError):
            return False

    async def mark_dead(self, r: Replica, reason: str) -> None:
        if r.state in (ReplicaState.DEAD, ReplicaState.STOPPED):
            return                                  # already handled (probe and stream error can race)
        was = r.state
        r.state = ReplicaState.DEAD
        self.log.health("replica DEAD", replica=r.id, reason=reason, was=was.value, in_flight=r.in_flight)
        await self.on_dead(r)
        await self.manager.kill_and_remove(r, reason="dead")
        if was == ReplicaState.DRAINING:
            return                                  # it was being removed anyway
        new = await self.manager.start_replica(as_spare=(was == ReplicaState.WARM_SPARE))
        self.log.health("replacement started", replica=new.id, replacing=r.id)
