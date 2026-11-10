"""
replica_manager.py - ReplicaManager implementation (owner B) on the Docker Engine API.

Every replica container gets the limits from config.yaml (--cpus -> nano_cpus,
--memory -> mem_limit) and the llama-server flags used by check_feasibility.sh:
    -m /models/<model_file> -c <slots*ctx_per_slot> -np <slots> --cont-batching --host 0.0.0.0 --port 8080
Containers join the `docker_network` bridge; the gateway (on the same Linux host)
reaches each one at <container IP>:8080, so no host ports are published.

Readiness: start_replica() returns at once with the replica STARTING; a background
task polls /health and flips it to READY (or WARM_SPARE when as_spare=True).
Continuous health probing and replacement of dead replicas are Phase 5
(HealthMonitor); here a container that exits or never loads is only marked DEAD.

Log tags: SCALE for start / drain / stop / remove, HEALTH for readiness.
"""
from __future__ import annotations

import asyncio
import itertools
import os
import time
from typing import Callable, Optional

import docker
import httpx
from docker.errors import NotFound

from gateway.interfaces import Config, EventLog, Replica, ReplicaManager, ReplicaPool, ReplicaState

LABEL_KEY = "llmgw.role"
REPLICA_PORT = 8080
READINESS_POLL_S = 0.5      # same poll step as check_feasibility.sh
READY_TIMEOUT_S = 180.0     # give up on a replica that never answers /health


class DockerReplicaManager(ReplicaManager):

    def __init__(self, cfg: Config, pool: ReplicaPool, log: EventLog,
                 on_change: Optional[Callable[[], None]] = None,
                 on_removed: Optional[Callable[[Replica], None]] = None) -> None:
        self.cfg, self.pool, self.log = cfg, pool, log
        self._on_change = on_change or (lambda: None)
        self._on_removed = on_removed or (lambda r: None)
        self._docker = docker.from_env()
        self._http = httpx.AsyncClient(timeout=2.0)
        self._ids = itertools.count(1)
        self._tasks: set[asyncio.Task] = set()
        self._creating: set[asyncio.Future] = set()     # docker run calls still in their thread

    # ---- setup / teardown ------------------------------------------------
    async def setup(self) -> None:
        """Create the network if needed and remove containers left by a previous run."""
        await asyncio.to_thread(self._ensure_network)
        for name in await asyncio.to_thread(self._remove_leftovers):
            self.log.scale("removed leftover container", container=name)

    async def shutdown(self) -> None:
        """Gateway exit: remove every replica container (no draining). A start_replica() that
        was cancelled mid-way (e.g. a replacement being started at exit) still has its
        `docker run` in a thread; wait for it, then also remove anything with our label and
        prefix that never made it into the pool."""
        for t in list(self._tasks):
            t.cancel()
        if self._creating:
            await asyncio.gather(*self._creating, return_exceptions=True)
        for r in self.pool.all():
            await self.kill_and_remove(r, reason="gateway shutdown")
        for name in await asyncio.to_thread(self._remove_leftovers):
            self.log.scale("removed container that never joined the pool", container=name)
        await self._http.aclose()

    def _ensure_network(self) -> None:
        try:
            self._docker.networks.get(self.cfg.docker_network)
        except NotFound:
            self._docker.networks.create(self.cfg.docker_network, driver="bridge",
                                         labels={LABEL_KEY: "network"})

    def _remove_leftovers(self) -> list[str]:
        """Only containers carrying our label AND our name prefix are touched."""
        names = []
        for c in self._docker.containers.list(all=True, filters={"label": f"{LABEL_KEY}=replica"}):
            if c.name.startswith(self.cfg.container_prefix):
                names.append(c.name)
                c.remove(force=True)
        return names

    # ---- ReplicaManager contract ----------------------------------------
    async def start_replica(self, *, as_spare: bool = False) -> Replica:
        rid = f"r{next(self._ids)}"
        name = f"{self.cfg.container_prefix}{rid}"
        t0 = time.monotonic()
        create = asyncio.ensure_future(asyncio.to_thread(self._run_container, name))
        self._creating.add(create)
        create.add_done_callback(self._creating.discard)
        container = await asyncio.shield(create)     # cancelling us must not lose track of it
        ip = await asyncio.to_thread(self._container_ip, container)
        r = Replica(id=rid, container_id=container.id, host=ip, port=REPLICA_PORT,
                    state=ReplicaState.STARTING, max_slots=self.cfg.slots_per_replica, created_ts=t0)
        self.pool.add(r)
        self.log.scale("replica started", replica=rid, container=name, ip=ip, image=self.cfg.replica_image,
                       cpus=self.cfg.replica_cpus, memory=self.cfg.replica_memory,
                       slots=self.cfg.slots_per_replica, spare=as_spare)
        t = asyncio.create_task(self._await_ready(r, as_spare))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)
        return r

    async def stop_replica(self, replica: Replica, drain_timeout_s: float = 30.0) -> None:
        replica.state = ReplicaState.DRAINING
        self.log.scale("replica draining", replica=replica.id, in_flight=replica.in_flight)
        self._on_change()
        deadline = time.monotonic() + drain_timeout_s
        while replica.in_flight > 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.2)
        drained = replica.in_flight == 0
        await asyncio.to_thread(self._remove_container, replica.container_id, False)
        self._forget(replica)
        self.log.scale("replica stopped", replica=replica.id, drained=drained, in_flight_left=replica.in_flight)

    async def kill_and_remove(self, replica: Replica, reason: str = "dead") -> None:
        await asyncio.to_thread(self._remove_container, replica.container_id, True)
        self._forget(replica)
        self.log.scale("replica removed", replica=replica.id, reason=reason.replace(" ", "_"))

    async def promote_spare(self) -> Optional[Replica]:
        spares = self.pool.by_state(ReplicaState.WARM_SPARE)
        if not spares:
            return None
        r = spares[0]
        r.state = ReplicaState.READY
        self.log.scale("spare promoted", replica=r.id)
        self._on_change()
        return r

    # ---- internals -------------------------------------------------------
    def _run_container(self, name: str):
        c = self.cfg
        slots = c.slots_per_replica
        command = ["-m", f"/models/{c.model_file}", "-c", str(slots * c.ctx_per_slot), "-np", str(slots),
                   "--cont-batching", "--host", "0.0.0.0", "--port", str(REPLICA_PORT)]
        return self._docker.containers.run(
            c.replica_image, command=command, name=name, detach=True,
            nano_cpus=int(c.replica_cpus * 1e9), mem_limit=c.replica_memory,
            network=c.docker_network, labels={LABEL_KEY: "replica"},
            volumes={os.path.abspath(c.models_dir): {"bind": "/models", "mode": "ro"}})

    def _container_ip(self, container) -> str:
        container.reload()
        return container.attrs["NetworkSettings"]["Networks"][self.cfg.docker_network]["IPAddress"]

    def _container_running(self, container_id: str) -> bool:
        try:
            return self._docker.containers.get(container_id).status == "running"
        except NotFound:
            return False

    def _remove_container(self, container_id: str, force: bool) -> None:
        try:
            c = self._docker.containers.get(container_id)
            if not force:
                c.stop(timeout=5)
            c.remove(force=True)
        except NotFound:
            pass

    def _forget(self, replica: Replica) -> None:
        replica.state = ReplicaState.STOPPED
        self.pool.remove(replica.id)
        self._on_removed(replica)
        self._on_change()

    async def _await_ready(self, r: Replica, as_spare: bool) -> None:
        while r.state == ReplicaState.STARTING:
            try:
                if (await self._http.get(f"{r.base_url}/health")).status_code == 200:
                    r.ready_ts = time.monotonic()
                    r.state = ReplicaState.WARM_SPARE if as_spare else ReplicaState.READY
                    self.log.health("replica ready", replica=r.id, state=r.state.value,
                                    load_s=f"{r.ready_ts - r.created_ts:.2f}")
                    self._on_change()
                    return
            except httpx.HTTPError:
                pass
            if not await asyncio.to_thread(self._container_running, r.container_id):
                r.state = ReplicaState.DEAD
                self.log.health("replica dead while loading (container exited)", replica=r.id)
                return
            if time.monotonic() - r.created_ts > READY_TIMEOUT_S:
                r.state = ReplicaState.DEAD
                self.log.health("replica dead while loading (timeout)", replica=r.id,
                                timeout_s=READY_TIMEOUT_S)
                return
            await asyncio.sleep(READINESS_POLL_S)
