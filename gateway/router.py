"""
router.py - Router implementations (owner C). Pure decision logic, no I/O.

Phase 2: round_robin and least_busy.
Phase 4: session_affinity (ARCHITECTURE.md decision 5).

The dispatch loop logs every decision ([ROUTE] dispatch ... policy=... loads=...),
so routers themselves stay free of side effects. session_affinity exposes the kind
of its last decision in `last_decision`, which the dispatch loop adds to that line.
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


class SessionAffinityRouter(Router):
    """Keeps session_id -> replica_id. A session's requests go to its replica while that
    replica is routable (READY with a free slot). Otherwise (dead, draining, full, removed)
    the request falls back to least_busy and the session is re-mapped; the gateway replays
    the full history, so correctness never depends on the old replica's cache.
    Requests without a session_id are routed least_busy and not mapped.

    last_decision: "no_session" | "new" | "hit" | "remap:<old>-<why>"  (why = full | draining |
    dead | starting | warm | removed).
    A mapping to a removed replica is kept until the session's next request, so that request
    is logged as "remap:<old>-removed" (replica ids are never reused)."""

    def __init__(self) -> None:
        self._map: dict[str, str] = {}
        self._fallback = LeastBusyRouter()
        self.last_decision: Optional[str] = None

    def choose(self, req: Request, pool: ReplicaPool) -> Optional[Replica]:
        sid = req.session_id
        if sid is None:
            self.last_decision = "no_session"
            return self._fallback.choose(req, pool)
        old_id = self._map.get(sid)
        old = pool.get(old_id) if old_id else None
        if old is not None and old.routable:
            self.last_decision = "hit"
            return old
        r = self._fallback.choose(req, pool)
        if r is None:
            return None
        if old_id is None:
            self.last_decision = "new"
        else:
            why = "removed" if old is None else ("full" if old.free_slots == 0 else old.state.value)
            self.last_decision = f"remap:{old_id}-{why}"
        self._map[sid] = r.id
        return r

    def mapping(self) -> dict[str, str]:
        return dict(self._map)


@register_router("round_robin")
def _make_round_robin(cfg: Config, log: EventLog) -> Router:
    return RoundRobinRouter()


@register_router("least_busy")
def _make_least_busy(cfg: Config, log: EventLog) -> Router:
    return LeastBusyRouter()


@register_router("session_affinity")
def _make_session_affinity(cfg: Config, log: EventLog) -> Router:
    return SessionAffinityRouter()
