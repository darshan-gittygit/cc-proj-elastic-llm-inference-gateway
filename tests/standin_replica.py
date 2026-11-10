"""
standin_replica.py - TEST FIXTURE ONLY. A stand-in for one llama.cpp replica.

ARCHITECTURE.md section 5: gateway logic is tested against a small stand-in that
speaks the same HTTP endpoints as llama-server, with a configurable startup delay
and token rate. It runs no model. Anything measured against it is SIMULATED and
must never be reported as performance of the real model.

To make that impossible to miss, every response is marked:
  - streamed chunks carry  "model": "standin-simulated"
  - generated text is      "sim0 sim1 sim2 ..."
  - every response has the header  X-Replica-Kind: standin-simulated

Endpoints (same shape as llama-server):
  GET  /health                -> 503 {"error": {...Loading model}} during the startup delay, then 200 {"status": "ok"}
  POST /v1/chat/completions   -> SSE stream: role chunk, max_tokens content chunks, final chunk, data: [DONE]
                                 At most --slots requests generate at once; extra ones wait (like llama-server -np).

Usage:
  python tests/standin_replica.py --port 18081 --startup-delay 3 --token-rate 10 --slots 2
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

MODEL_NAME = "standin-simulated"
KIND_HEADER = {"X-Replica-Kind": "standin-simulated"}


def create_app(startup_delay: float, token_rate: float, slots: int) -> FastAPI:
    app = FastAPI(title="stand-in replica (SIMULATED)")
    started = time.monotonic()
    sem = asyncio.Semaphore(slots)

    def loaded() -> bool:
        return time.monotonic() - started >= startup_delay

    @app.get("/health")
    async def health():
        if not loaded():
            return JSONResponse({"error": {"code": 503, "message": "Loading model", "type": "unavailable_error"}},
                                status_code=503, headers=KIND_HEADER)
        return JSONResponse({"status": "ok"}, headers=KIND_HEADER)

    @app.post("/v1/chat/completions")
    async def chat(req: Request):
        if not loaded():
            return JSONResponse({"error": {"code": 503, "message": "Loading model"}}, status_code=503,
                                headers=KIND_HEADER)
        body = await req.json()
        max_tokens = int(body.get("max_tokens", 16))
        cid = f"chatcmpl-sim-{uuid.uuid4().hex[:12]}"

        def chunk(delta: dict, finish: str | None = None) -> str:
            return "data: " + json.dumps({
                "choices": [{"finish_reason": finish, "index": 0, "delta": delta}],
                "created": int(time.time()), "id": cid, "model": MODEL_NAME,
                "object": "chat.completion.chunk"}) + "\n\n"

        async def stream():
            async with sem:                      # extra requests wait here, like llama-server slots
                yield chunk({"role": "assistant", "content": None})
                for i in range(max_tokens):
                    await asyncio.sleep(1.0 / token_rate)
                    yield chunk({"content": f"sim{i} "})
                yield chunk({}, finish="length")
                yield "data: [DONE]\n\n"

        return StreamingResponse(stream(), media_type="text/event-stream", headers=KIND_HEADER)

    return app


def main() -> None:
    p = argparse.ArgumentParser(description="Stand-in llama.cpp replica (SIMULATED, test fixture only)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=18081)
    p.add_argument("--startup-delay", type=float, default=3.0, help="seconds /health returns 503 (model 'loading')")
    p.add_argument("--token-rate", type=float, default=10.0, help="tokens per second per request")
    p.add_argument("--slots", type=int, default=2, help="concurrent generations (llama-server -np)")
    a = p.parse_args()
    uvicorn.run(create_app(a.startup_delay, a.token_rate, a.slots), host=a.host, port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
