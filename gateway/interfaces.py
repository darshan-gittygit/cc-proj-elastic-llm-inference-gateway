"""
interfaces.py - the shared contract for the Elastic LLM Inference Gateway.

Agree on this file in the first 2-3 days, then freeze it. Changes need all 4
members to approve (small PRs, tag everyone). Everything else imports FROM here;
nothing here imports from your modules.

Ownership (suggested):
  A  Gateway core + scheduling      -> Scheduler impls, config loader, dispatch loop, EventLog
  B  Replica manager + autoscaling  -> ReplicaManager impl, Autoscaler impl
  C  Routing + fault tolerance      -> Router impls, HealthMonitor impl, retry logic
  D  Model infra + load + eval      -> replica image/flags, loadgen, metrics, report

Requires: Python 3.10+, pyyaml.
"""
from __future__ import annotations

import asyncio
import itertools
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Optional

import yaml


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class ReplicaState(str, Enum):
    STARTING = "starting"    # container up, model still loading
    READY = "ready"          # passes /health, routable
    WARM_SPARE = "warm"      # loaded and healthy but NOT routable (pre-warm pool)
    DRAINING = "draining"    # finishing in-flight work, accepts nothing new
    DEAD = "dead"            # failed health checks / container gone
    STOPPED = "stopped"      # removed


class RequestState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


# --------------------------------------------------------------------------- #
# Core data objects
# --------------------------------------------------------------------------- #
_req_counter = itertools.count(1)


@dataclass
class Request:
    """One client request travelling through the gateway."""
    messages: list[dict[str, str]]               # OpenAI-style chat messages
    prompt_tokens: int                           # estimated at ingress (A)
    session_id: Optional[str] = None             # for session affinity
    max_tokens: int = 256
    id: int = field(default_factory=lambda: next(_req_counter))
    arrival_ts: float = field(default_factory=time.monotonic)
    state: RequestState = RequestState.QUEUED
    attempts: int = 0                            # incremented on every dispatch
    replica_id: Optional[str] = None             # set by router
    tokens_emitted: int = 0                      # tokens already sent to client
    # Token sink: the dispatch loop pushes str chunks, None marks end of stream.
    out: asyncio.Queue = field(default_factory=asyncio.Queue)

    def wait_time(self, now: Optional[float] = None) -> float:
        return (now if now is not None else time.monotonic()) - self.arrival_ts


@dataclass
class Replica:
    """One containerised llama.cpp server."""
    id: str                                      # e.g. "r1"
    container_id: str
    host: str                                    # reachable from gateway
    port: int
    state: ReplicaState = ReplicaState.STARTING
    max_slots: int = 2                           # concurrent requests (llama.cpp -np)
    in_flight: int = 0
    created_ts: float = field(default_factory=time.monotonic)
    ready_ts: Optional[float] = None             # lets D measure model-load time
    consecutive_failures: int = 0

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def free_slots(self) -> int:
        return max(0, self.max_slots - self.in_flight)

    @property
    def routable(self) -> bool:
        return self.state == ReplicaState.READY and self.free_slots > 0


# --------------------------------------------------------------------------- #
# Config (everything switchable without code changes lives here)
# --------------------------------------------------------------------------- #
@dataclass
class Config:
    scheduler: str = "spf_aging"      # "fcfs" | "spf_aging"
    router: str = "least_busy"        # "round_robin" | "least_busy" | "session_affinity"
    aging_alpha: float = 5.0          # SPF aging: priority = prompt_tokens - alpha * wait_s
    min_replicas: int = 1
    max_replicas: int = 4
    scale_up_threshold: float = 4.0   # queue_len + in_flight per ready replica
    scale_down_threshold: float = 1.0
    scale_up_after_s: float = 5.0     # signal must persist this long
    scale_down_cooldown_s: float = 30.0
    prewarm: str = "warm_pool"        # "none" | "warm_pool"
    warm_pool_size: int = 1
    prewarm_trigger_ratio: float = 0.7  # spare is started when signal >= ratio * scale_up_threshold
    health_interval_s: float = 1.5
    health_fail_limit: int = 3
    max_retries: int = 2
    # --- replica container settings (all passed to the Docker Engine API) ---
    replica_image: str = "ghcr.io/ggml-org/llama.cpp:server"
    replica_cpus: float = 1.0
    replica_memory: str = "1g"
    slots_per_replica: int = 2        # llama-server -np  (= max concurrent requests per replica)
    ctx_per_slot: int = 2048          # llama-server -c = slots_per_replica * ctx_per_slot
    models_dir: str = "./models"      # host dir mounted read-only at /models
    model_file: str = "qwen2.5-0.5b-instruct-q4_k_m.gguf"
    docker_network: str = "llmgw-net"
    container_prefix: str = "llmgw-replica-"
    default_temperature: float = 0.0  # greedy => retry-after-crash reproduces the same tokens
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, path: str) -> "Config":
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        known = {k: v for k, v in raw.items() if k in cls.__dataclass_fields__}
        cfg = cls(**known)
        cfg.extra = {k: v for k, v in raw.items() if k not in known}
        return cfg


# --------------------------------------------------------------------------- #
# Event log - every scheduling / routing / scaling / health decision goes here
# so the demos can be followed step by step. Tags: SCHED ROUTE SCALE HEALTH
# --------------------------------------------------------------------------- #
class EventLog:
    def __init__(self, name: str = "gateway"):
        self._log = logging.getLogger(name)

    def emit(self, tag: str, msg: str, **fields: Any) -> None:
        kv = " ".join(f"{k}={v}" for k, v in fields.items())
        self._log.info("[%s] %s %s", tag.upper(), msg, kv)

    # convenience wrappers
    def sched(self, msg: str, **kw: Any) -> None: self.emit("SCHED", msg, **kw)
    def route(self, msg: str, **kw: Any) -> None: self.emit("ROUTE", msg, **kw)
    def scale(self, msg: str, **kw: Any) -> None: self.emit("SCALE", msg, **kw)
    def health(self, msg: str, **kw: Any) -> None: self.emit("HEALTH", msg, **kw)


# --------------------------------------------------------------------------- #
# Shared registry of replicas (single source of truth; concrete, not abstract)
# --------------------------------------------------------------------------- #
class ReplicaPool:
    def __init__(self) -> None:
        self._replicas: dict[str, Replica] = {}

    def add(self, r: Replica) -> None: self._replicas[r.id] = r
    def remove(self, rid: str) -> None: self._replicas.pop(rid, None)
    def get(self, rid: str) -> Optional[Replica]: return self._replicas.get(rid)
    def all(self) -> list[Replica]: return list(self._replicas.values())

    def by_state(self, *states: ReplicaState) -> list[Replica]:
        return [r for r in self._replicas.values() if r.state in states]

    def ready(self) -> list[Replica]:
        return self.by_state(ReplicaState.READY)

    def routable(self) -> list[Replica]:
        return [r for r in self._replicas.values() if r.routable]

    def total_in_flight(self) -> int:
        return sum(r.in_flight for r in self._replicas.values())


# --------------------------------------------------------------------------- #
# Component contracts
# --------------------------------------------------------------------------- #
class Scheduler(ABC):
    """Owner: A. Central queue; the dispatch loop pops only when a slot is free."""

    @abstractmethod
    def push(self, req: Request) -> None: ...

    @abstractmethod
    def pop(self, now: Optional[float] = None) -> Optional[Request]:
        """Next request to serve, or None if empty. Must log via EventLog."""

    @abstractmethod
    def requeue_front(self, req: Request) -> None:
        """Used by fault tolerance (C): retried requests jump the line."""

    @abstractmethod
    def __len__(self) -> int: ...


class Router(ABC):
    """Owner: C. Pure decision logic; does not perform I/O."""

    @abstractmethod
    def choose(self, req: Request, pool: ReplicaPool) -> Optional[Replica]:
        """Pick a routable replica, or None if none is available."""

    def on_complete(self, req: Request, replica: Replica) -> None:
        """Optional hook (e.g. update affinity map)."""

    def on_replica_removed(self, replica: Replica) -> None:
        """Optional hook (e.g. drop affinity entries pointing at it)."""


class ReplicaManager(ABC):
    """Owner: B. Wraps the Docker Engine API. Always applies --cpus/--memory."""

    @abstractmethod
    async def start_replica(self, *, as_spare: bool = False) -> Replica:
        """Create + start a container and add it to the pool as STARTING.
        Returns immediately; readiness is detected by the HealthMonitor (or the
        manager polling /health), which flips state to READY / WARM_SPARE."""

    @abstractmethod
    async def stop_replica(self, replica: Replica, drain_timeout_s: float = 30.0) -> None:
        """Mark DRAINING, wait for in_flight==0 (or timeout), then stop + remove."""

    @abstractmethod
    async def kill_and_remove(self, replica: Replica) -> None:
        """Hard remove a dead replica's container."""

    @abstractmethod
    async def promote_spare(self) -> Optional[Replica]:
        """WARM_SPARE -> READY instantly (the pre-warming payoff). None if no spare."""


class Autoscaler(ABC):
    """Owner: B. Control loop; reads pool + scheduler depth, calls ReplicaManager."""

    def __init__(self, cfg: Config, pool: ReplicaPool, scheduler: Scheduler,
                 manager: ReplicaManager, log: EventLog):
        self.cfg, self.pool, self.scheduler = cfg, pool, scheduler
        self.manager, self.log = manager, log

    @abstractmethod
    async def run(self) -> None:
        """Loop forever: evaluate load, scale up/down, maintain warm pool."""


class HealthMonitor(ABC):
    """Owner: C. Probes replicas; on failure triggers retry + replacement."""

    def __init__(self, cfg: Config, pool: ReplicaPool, manager: ReplicaManager,
                 log: EventLog,
                 on_dead: Callable[[Replica], Awaitable[None]]):
        # on_dead is provided by the dispatch loop (A): it requeues the dead
        # replica's unfinished requests via Scheduler.requeue_front().
        self.cfg, self.pool, self.manager = cfg, pool, manager
        self.log, self.on_dead = log, on_dead

    @abstractmethod
    async def run(self) -> None:
        """Loop: probe /health, mark DEAD after cfg.health_fail_limit failures,
        await on_dead(replica), then ask the manager for a replacement."""


# --------------------------------------------------------------------------- #
# Plug-in registries: config strings -> implementations (no code change to swap)
# --------------------------------------------------------------------------- #
SCHEDULERS: dict[str, Callable[[Config, EventLog], Scheduler]] = {}
ROUTERS: dict[str, Callable[[Config, EventLog], Router]] = {}


def register_scheduler(name: str):
    def deco(factory):
        SCHEDULERS[name] = factory
        return factory
    return deco


def register_router(name: str):
    def deco(factory):
        ROUTERS[name] = factory
        return factory
    return deco


def build_scheduler(cfg: Config, log: EventLog) -> Scheduler:
    if cfg.scheduler not in SCHEDULERS:
        raise ValueError(f"unknown scheduler {cfg.scheduler!r}; have {list(SCHEDULERS)}")
    return SCHEDULERS[cfg.scheduler](cfg, log)


def build_router(cfg: Config, log: EventLog) -> Router:
    if cfg.router not in ROUTERS:
        raise ValueError(f"unknown router {cfg.router!r}; have {list(ROUTERS)}")
    return ROUTERS[cfg.router](cfg, log)


# Usage example (in A's / C's modules):
#
#   @register_scheduler("fcfs")
#   def _make_fcfs(cfg, log): return FCFSScheduler(log)
#
#   @register_router("round_robin")
#   def _make_rr(cfg, log): return RoundRobinRouter(log)
