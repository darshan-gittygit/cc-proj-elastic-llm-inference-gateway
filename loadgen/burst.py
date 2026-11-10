"""
burst.py - load generator for the gateway (plain asyncio + httpx, ARCHITECTURE.md section 3).

Sends streamed chat requests to POST /v1/chat/completions and measures each one on the client
side with the monotonic clock. Deterministic: the workload is fully given by the arguments
(no randomness), so the same command always sends the same requests in the same order.

Workload, either
  --pattern "B,L,S,S"  one request per item:  S = short prompt, L = long prompt,
                       B = busy (short prompt with --busy-max-tokens, to occupy a slot)
or
  --n 20 --long-every 4   20 requests, every 4th one long, the rest short
Arrivals: request k starts at k * --interval seconds (0 = all at once).
--session ID sends the requests as consecutive turns of one conversation instead (one after
another, each waits for the previous answer), using the gateway's "session_id" field.

Output: one JSON line per request (to --out, default stdout) with gateway request id,
start / first-token / end times (seconds after the script started), tokens, ok, error,
and whether an llmgw_retry event arrived; then a summary line ("summary": ...).

Examples (gateway on :8000):
  python loadgen/burst.py --pattern "B,B,L,S,S,S,S" --interval 0.3 --max-tokens 4
  python loadgen/burst.py --n 14 --long-every 0 --max-tokens 40 --interval 0
  python loadgen/burst.py --session conv1 --pattern "S,S,S,S" --max-tokens 8
"""
from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time

import httpx

SHORT = "Short question {i}: say hi."
LONG = ("Request {i}. Read this paragraph and then answer with one word only. The gateway keeps one central "
        "queue and sends a request to a replica only when that replica has a free slot, so the scheduler "
        "alone decides who goes next, and the router alone decides where it goes.")


def plan(args) -> list[str]:
    if args.pattern:
        kinds = [k.strip().upper() for k in args.pattern.split(",") if k.strip()]
        bad = [k for k in kinds if k not in ("S", "L", "B")]
        if bad:
            raise SystemExit(f"--pattern items must be S, L or B, got {bad}")
        return kinds
    return ["L" if args.long_every and (i + 1) % args.long_every == 0 else "S" for i in range(args.n)]


async def one(client: httpx.AsyncClient, args, i: int, kind: str, t0: float) -> dict:
    prompt = (LONG if kind == "L" else SHORT).format(i=i)
    body = {"messages": [{"role": "user", "content": prompt}], "stream": True,
            "max_tokens": args.busy_max_tokens if kind == "B" else args.max_tokens}
    if args.session:
        body["session_id"] = args.session
    rec = {"i": i, "kind": kind, "prompt_chars": len(prompt), "session": args.session,
           "start_s": round(time.monotonic() - t0, 3), "gateway_req": None, "first_token_s": None,
           "end_s": None, "tokens": 0, "ok": False, "error": None, "retry_event": False}
    try:
        async with client.stream("POST", f"{args.url}/v1/chat/completions", json=body) as r:
            rec["gateway_req"] = r.headers.get("x-request-id")
            if r.status_code != 200:
                rec["error"] = f"HTTP {r.status_code}"
                return rec
            done = False
            async for line in r.aiter_lines():
                if not line.startswith("data: "):
                    continue
                data = line[6:]
                if data == "[DONE]":
                    done = True
                    break
                obj = json.loads(data)
                if "error" in obj:
                    rec["error"] = obj["error"].get("message", "error")
                if "llmgw_retry" in obj:
                    rec["retry_event"] = True
                for c in obj.get("choices") or []:
                    if (c.get("delta") or {}).get("content"):
                        rec["tokens"] += 1
                        if rec["first_token_s"] is None:
                            rec["first_token_s"] = round(time.monotonic() - t0, 3)
            rec["ok"] = done and rec["error"] is None
    except httpx.HTTPError as e:
        rec["error"] = type(e).__name__
    rec["end_s"] = round(time.monotonic() - t0, 3)
    return rec


async def run(args) -> list[dict]:
    kinds = plan(args)
    t0 = time.monotonic()
    timeout = httpx.Timeout(connect=10.0, read=None, write=10.0, pool=None)
    async with httpx.AsyncClient(timeout=timeout, limits=httpx.Limits(max_connections=None)) as client:
        if args.session:                     # turns of one conversation: strictly one after another
            out = []
            for i, k in enumerate(kinds):
                out.append(await one(client, args, i, k, t0))
            return out

        async def delayed(i, k):
            await asyncio.sleep(i * args.interval)
            return await one(client, args, i, k, t0)
        return list(await asyncio.gather(*(delayed(i, k) for i, k in enumerate(kinds))))


def summary(recs: list[dict]) -> dict:
    ok = [r for r in recs if r["ok"]]
    ttft = [r["first_token_s"] - r["start_s"] for r in ok if r["first_token_s"] is not None]
    lat = [r["end_s"] - r["start_s"] for r in ok]

    def stats(xs):
        if not xs:
            return None
        xs = sorted(xs)
        return {"mean": round(statistics.mean(xs), 3), "median": round(statistics.median(xs), 3),
                "p95": round(xs[min(len(xs) - 1, int(0.95 * len(xs)))], 3), "max": round(xs[-1], 3)}
    return {"requests": len(recs), "ok": len(ok), "failed": len(recs) - len(ok),
            "retry_events": sum(r["retry_event"] for r in recs), "ttft_s": stats(ttft), "latency_s": stats(lat)}


def main() -> None:
    p = argparse.ArgumentParser(description="Burst load for the Elastic LLM Inference Gateway")
    p.add_argument("--url", default="http://127.0.0.1:8000")
    p.add_argument("--pattern", help='comma list of S / L / B, e.g. "B,L,S,S"')
    p.add_argument("--n", type=int, default=10, help="number of requests (without --pattern)")
    p.add_argument("--long-every", type=int, default=0, help="every k-th request is long (0 = none)")
    p.add_argument("--interval", type=float, default=0.0, help="seconds between request starts")
    p.add_argument("--max-tokens", type=int, default=16)
    p.add_argument("--busy-max-tokens", type=int, default=64, help="max_tokens for B requests")
    p.add_argument("--session", help="send the requests as turns of this session, one after another")
    p.add_argument("--out", help="write JSON lines here (default: stdout)")
    args = p.parse_args()
    recs = asyncio.run(run(args))
    lines = [json.dumps(r) for r in recs] + [json.dumps({"summary": summary(recs)})]
    if args.out:
        with open(args.out, "w") as f:
            f.write("\n".join(lines) + "\n")
        print(lines[-1])
    else:
        print("\n".join(lines))
    sys.exit(0 if all(r["ok"] for r in recs) else 1)


if __name__ == "__main__":
    main()
