# Phase 4 evidence notes

Written after the runs; the evidence files themselves are unedited output of `scripts/demo2_affinity.sh`.

| File | Kind |
|---|---|
| `phase4_affinity_real.txt` | **REAL**: 2 llama.cpp replicas, real model |
| `simulated/phase4_affinity_standin_SIMULATED.txt` | **SIMULATED**: same script with the stand-in image (no model, no prompt cache). It shows routing and history replay only; its TTFTs are equal by construction. |

The two runs differ only in config (`router: session_affinity` vs `router: round_robin`).
Each run: 2 replicas, a warm-up request on each, then 3 conversations x 5 turns, one after another.

## Real-model results

- Affinity: all 5 turns of each conversation went to the same replica (decisions `new, hit, hit, hit, hit`).
- History: the client sent only the new message; llama-server's total prompt tokens per turn
  (`cache_n + prompt_n`) grew 43 -> 65 -> 88 -> 111 -> 133, identical under both routers, so the full
  conversation reached whichever replica served the turn. The model answered "What is my favourite
  colour?" correctly in all 6 conversations, including round robin, where turn 2 ran on the other replica.

Turn-2 TTFT (gateway-measured, monotonic):

| Router | c1 | c2 | c3 | mean | turn-2 cache_n / prompt_n |
|---|---|---|---|---|---|
| session_affinity | 1.279 s | 1.697 s | 1.682 s | **1.553 s** | 44 / 21 |
| round_robin | 3.372 s | 3.401 s | 2.967 s | **3.247 s** | 24-28 / 37-41 |

With affinity, llama-server reused the previous turn (44 cached tokens) and processed only the 21
new ones. Under round robin, turn 2 ran on the other replica, which could reuse only the shared chat
template prefix (24-28 tokens).

## Limitations

- Small sample: 3 conversations per router in one run; no statistical test. The difference (about
  1.7 s) is consistent across all three, and the cache counters explain it.
- New sessions are placed by least_busy at their first turn. In this sequential demo every
  conversation started on an idle pool, so all three landed on r1 (ties go to the first replica)
  and r2 served only the warm-up. Affinity keeps a session where it started; it does not spread
  sessions on its own.
- Under round robin, turns 3-5 can partly reuse the cache of the turn two steps earlier (same
  replica), so only turn 2 is a clean "no affinity" comparison, as the Phase 4 criterion specifies.
- The gateway stores each session's history in memory with no expiry; concurrent turns of the same
  session are not ordered (the last completed turn's history wins). Sessions are lost on restart.
- Replies like "OK" at turn 5 or "Violet Tomato" are the 0.5B model's answer quality, not gateway behaviour.
