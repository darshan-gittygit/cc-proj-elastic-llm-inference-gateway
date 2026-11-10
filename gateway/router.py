"""
router.py - Router implementations (owner C). Pure decision logic, no I/O.

Phase 2: round_robin and least_busy.
session_affinity is Phase 4 and is not registered yet, so `router: session_affinity`
in config.yaml fails at startup with a clear "unknown router" error.

The dispatch loop logs every decision ([ROUTE] dispatch ... policy=... loads=...),
so routers themselves stay free of side effects.
"""
from __future__ import annotations

from typing import Optional

from gateway.interfaces import (Config, EventLog, Replica, ReplicaPool, Request, Router,
                                register_router)


class RoundRobinRouter(Router):
    """Takes replicas in turn (pool order = start order), skipping any that are not
    routable right now (not READY, or no free slot)."""

    def __init__(self) -> None:
        self._last: Optional[str] = None

    def choose(self, req: Request, pool: ReplicaPool) -> Optional[Replica]:
        order = pool.all()
        if not order:
            return None
        ids = [r.id for r in order]
        start = ids.index(self._last) + 1 if self._last in ids else 0
        for i in range(len(order)):
            r = order[(start + i) % len(order)]
            if r.routable:
                self._last = r.id
                return r
        return None


class LeastBusyRouter(Router):
    """Picks the routable replica with the lowest in_flight / max_slots.
    Ties go to the replica that started first (stable, easy to read in logs)."""

    def choose(self, req: Request, pool: ReplicaPool) -> Optional[Replica]:
        best: Optional[Replica] = None
        for r in pool.all():                       # pool order breaks ties
            if r.routable and (best is None or r.in_flight / r.max_slots < best.in_flight / best.max_slots):
                best = r
        return best


@register_router("round_robin")
def _make_round_robin(cfg: Config, log: EventLog) -> Router:
    return RoundRobinRouter()


@register_router("least_busy")
def _make_least_busy(cfg: Config, log: EventLog) -> Router:
    return LeastBusyRouter()
