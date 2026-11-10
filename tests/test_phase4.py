"""
Phase 4 tests: session affinity and per-session history (ARCHITECTURE.md decision 5).

- Router tests are pure unit tests.
- History tests drive the real Gateway dispatch/stream path against an in-process
  mock replica (httpx.MockTransport), no Docker.
- Gateway tests run end to end with stand-in containers (SIMULATED timings). The stand-in
  reports how many messages it received ("standin_prompt_messages"), which shows that the
  gateway replays the whole conversation.
"""
from __future__ import annotations

import asyncio
import json
import re
import threading
import time

import httpx
import pytest

from gateway.interfaces import ROUTERS, Config, EventLog, Replica, ReplicaPool, ReplicaState, Request, build_router
from gateway.router import SessionAffinityRouter
from helpers import GatewayProc, content_of, sim_text, stream_request

R = ReplicaState.READY


def make_pool(*specs: tuple[str, ReplicaState, int]) -> ReplicaPool:
    pool = ReplicaPool()
    for rid, state, in_flight in specs:
        pool.add(Replica(id=rid, container_id=rid, host="h", port=1, state=state, max_slots=2,
                         in_flight=in_flight))
    return pool


def req(session: str | None) -> Request:
    return Request(messages=[{"role": "user", "content": "x"}], prompt_tokens=1, session_id=session)


# --------------------------------------------------------------------------- #
# Router (pure)
# --------------------------------------------------------------------------- #
def test_new_session_goes_least_busy_then_sticks_even_when_another_is_idler():
    pool = make_pool(("r1", R, 1), ("r2", R, 0))
    rt = SessionAffinityRouter()
    assert rt.choose(req("s1"), pool).id == "r2" and rt.last_decision == "new"
    pool.get("r2").in_flight = 1
    pool.get("r1").in_flight = 0                       # r1 is now less busy ...
    assert rt.choose(req("s1"), pool).id == "r2" and rt.last_decision == "hit"   # ... but s1 stays
    assert rt.mapping() == {"s1": "r2"}


@pytest.mark.parametrize("state,in_flight,why", [
    (R, 2, "full"), (ReplicaState.DRAINING, 0, "draining"), (ReplicaState.DEAD, 0, "dead"),
])
def test_unusable_mapped_replica_falls_back_and_remaps(state, in_flight, why):
    pool = make_pool(("r1", R, 0), ("r2", R, 0))
    rt = SessionAffinityRouter()
    assert rt.choose(req("s"), pool).id == "r1"
    pool.get("r1").state, pool.get("r1").in_flight = state, in_flight
    assert rt.choose(req("s"), pool).id == "r2"
    assert rt.last_decision == f"remap:r1-{why}" and rt.mapping() == {"s": "r2"}
    assert rt.choose(req("s"), pool).id == "r2" and rt.last_decision == "hit"


def test_removed_replica_is_remapped():
    pool = make_pool(("r1", R, 0), ("r2", R, 0))
    rt = SessionAffinityRouter()
    rt.choose(req("s"), pool)
    pool.remove("r1")
    assert rt.choose(req("s"), pool).id == "r2" and rt.last_decision == "remap:r1-removed"


def test_requests_without_session_are_least_busy_and_unmapped():
    pool = make_pool(("r1", R, 1), ("r2", R, 0))
    rt = SessionAffinityRouter()
    assert rt.choose(req(None), pool).id == "r2" and rt.last_decision == "no_session"
    assert rt.mapping() == {}


def test_none_when_nothing_routable():
    pool = make_pool(("r1", R, 2), ("r2", ReplicaState.STARTING, 0))
    assert SessionAffinityRouter().choose(req("s"), pool) is None


def test_registered_by_config_name():
    assert set(ROUTERS) == {"round_robin", "least_busy", "session_affinity"}
    assert isinstance(build_router(Config(router="session_affinity"), EventLog("t")), SessionAffinityRouter)


# --------------------------------------------------------------------------- #
# Per-session history through the real stream path (mock replica, no Docker)
# --------------------------------------------------------------------------- #
class MockReplica:
    """Answers like llama-server; records the messages of every request it gets."""

    def __init__(self):
        self.received: list[list[dict]] = []
        self.fail_next = False

    def __call__(self, request: httpx.Request) -> httpx.Response:
        msgs = json.loads(request.content)["messages"]
        self.received.append(msgs)
        if self.fail_next:
            self.fail_next = False
            return httpx.Response(500, content=b"boom")
        n = len(self.received)
        chunks = [{"choices": [{"delta": {"role": "assistant", "content": None}}]},
                  {"choices": [{"delta": {"content": f"answer{n}"}}]},
                  {"choices": [{"delta": {}, "finish_reason": "stop"}]}]
        body = "".join(f"data: {json.dumps(c)}\n\n" for c in chunks) + "data: [DONE]\n\n"
        return httpx.Response(200, content=body.encode(), headers={"content-type": "text/event-stream"})


def run_turns(turns: list[tuple[str | None, str]], fail_turn: int | None = None):
    """Sends each (session_id, text) turn through Gateway.new_request -> dispatch -> stream."""
    from gateway.main import Gateway
    cfg = Config(router="session_affinity", scheduler="fcfs", slots_per_replica=2)
    gw = Gateway(cfg, EventLog("test-history"))
    gw.pool.add(Replica(id="r1", container_id="r1", host="mock", port=1, state=R, max_slots=2))
    mock = MockReplica()

    async def scenario():
        gw._client = httpx.AsyncClient(transport=httpx.MockTransport(mock))
        loop = asyncio.create_task(gw._dispatch_loop())
        outputs = []
        for i, (sid, text) in enumerate(turns):
            mock.fail_next = (i == fail_turn)
            r = gw.new_request([{"role": "user", "content": text}], 16, sid)
            gw.submit(r)
            chunks = []
            while (c := await r.out.get()) is not None:
                chunks.append(c)
            outputs.append((r, chunks))
        loop.cancel()
        await gw._client.aclose()
        return outputs
    return gw, mock, asyncio.run(scenario())


def test_history_is_replayed_and_only_new_message_is_sent_by_client():
    gw, mock, outs = run_turns([("s", "u1"), ("s", "u2"), ("s", "u3")])
    assert [len(m) for m in mock.received] == [1, 3, 5]
    assert mock.received[2] == [{"role": "user", "content": "u1"}, {"role": "assistant", "content": "answer1"},
                                {"role": "user", "content": "u2"}, {"role": "assistant", "content": "answer2"},
                                {"role": "user", "content": "u3"}]
    assert len(gw.sessions["s"]) == 6
    # the prompt-length estimate covers the whole conversation
    assert outs[2][0].prompt_tokens > outs[0][0].prompt_tokens


def test_failed_turn_is_not_added_to_history():
    gw, mock, outs = run_turns([("s", "u1"), ("s", "u2"), ("s", "u3")], fail_turn=1)
    assert '"replica_error"' in outs[1][1][0]
    assert mock.received[2] == [{"role": "user", "content": "u1"}, {"role": "assistant", "content": "answer1"},
                                {"role": "user", "content": "u3"}]


def test_sessions_are_separate_and_no_session_means_no_history():
    gw, mock, _ = run_turns([("a", "a1"), ("b", "b1"), (None, "x"), ("a", "a2"), (None, "y")])
    assert [len(m) for m in mock.received] == [1, 1, 1, 3, 1]
    assert mock.received[3][0]["content"] == "a1"
    assert set(gw.sessions) == {"a", "b"}


# --------------------------------------------------------------------------- #
# End to end (stand-in containers, SIMULATED timings)
# --------------------------------------------------------------------------- #
def dispatch_lines(gw: GatewayProc, session: str) -> list[tuple[str, str]]:
    out = []
    for line in gw.route_lines("dispatch"):
        if f"session={session}" in line:
            out.append((re.search(r"replica=(r\d+)", line).group(1), re.search(r"affinity=(\S+)", line).group(1)
                        if "affinity=" in line else "-"))
    return out


def prompt_messages(events: list[str]) -> int:
    return json.loads(events[0])["standin_prompt_messages"]


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_five_turn_conversation_stays_on_one_replica(tmp_path):
    gw = GatewayProc(tmp_path, n_ready=2, router="session_affinity")
    try:
        seen = []
        for turn in range(1, 6):
            if turn == 3:   # make r1 busier than r2: affinity must still keep the session on r1
                bg = threading.Thread(target=lambda: stream_request(gw.url, max_tokens=60, content="other"))
                bg.start()
                time.sleep(0.3)
            events, _ = stream_request(gw.url, max_tokens=3, content=f"turn {turn}", session_id="conv1")
            assert content_of(events) == sim_text(3)
            seen.append(prompt_messages(events))
        bg.join(30)
        assert seen == [1, 3, 5, 7, 9]                                  # full history every turn
        lines = dispatch_lines(gw, "conv1")
        assert [rep for rep, _ in lines] == ["r1"] * 5, lines
        assert [dec for _, dec in lines] == ["new", "hit", "hit", "hit", "hit"]
        other = [l for l in gw.route_lines("dispatch") if "session=" not in l][0]
        assert "replica=r1" in other and "loads_before=r1:0/2,r2:0/2" in other   # r1 was the busier one
    finally:
        gw.stop()


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_without_affinity_turns_spread_but_history_is_still_replayed(tmp_path):
    gw = GatewayProc(tmp_path, n_ready=2, router="round_robin")
    try:
        seen = [prompt_messages(stream_request(gw.url, 2, f"turn {t}", session_id="conv2")[0]) for t in range(4)]
        assert seen == [1, 3, 5, 7]
        assert [rep for rep, _ in dispatch_lines(gw, "conv2")] == ["r1", "r2", "r1", "r2"]
    finally:
        gw.stop()


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_full_replica_causes_remap_and_history_moves_with_the_session(tmp_path):
    gw = GatewayProc(tmp_path, n_ready=2, router="session_affinity", slots_per_replica=1)
    try:
        assert prompt_messages(stream_request(gw.url, 2, "turn 1", session_id="conv3")[0]) == 1
        bg = threading.Thread(target=lambda: stream_request(gw.url, max_tokens=60, content="occupy r1"))
        bg.start()
        time.sleep(0.3)                                                  # r1's only slot is now taken
        assert prompt_messages(stream_request(gw.url, 2, "turn 2", session_id="conv3")[0]) == 3
        assert prompt_messages(stream_request(gw.url, 2, "turn 3", session_id="conv3")[0]) == 5
        bg.join(30)
        assert dispatch_lines(gw, "conv3") == [("r1", "new"), ("r2", "remap:r1-full"), ("r2", "hit")]
    finally:
        gw.stop()


@pytest.mark.usefixtures("standin_image", "no_leftover_containers")
def test_bad_session_id_rejected(tmp_path):
    gw = GatewayProc(tmp_path)
    try:
        for bad in (123, ""):
            r = httpx.post(gw.url, json={"messages": [{"role": "user", "content": "x"}], "session_id": bad})
            assert r.status_code == 400
    finally:
        gw.stop()
