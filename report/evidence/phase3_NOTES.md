# Phase 3 evidence notes

Written after the runs; the evidence files themselves are unedited output of `scripts/demo1_scheduling.sh`.

| File | Kind |
|---|---|
| `phase3_scheduling_real.txt` | **REAL**: llama.cpp container, real model, 1 replica x 1 slot |
| `simulated/phase3_scheduling_standin_SIMULATED.txt` | **SIMULATED**: same script with the stand-in image (no model). Gateway behaviour only, not model performance. |

Both runs were recorded before the fix for requests whose client disconnects while still queued
(`Gateway.cancel` / dispatch loop in `gateway/main.py`). No client disconnected in these runs, so the
results are unaffected by that fix.

All four runs differ only in config (`scheduler`, `aging_alpha`); `aging_alpha: 0` is a config value of
`spf_aging`, not a third algorithm. One replica with one slot is used so that a queue forms and the
scheduler alone decides the order. Prompt sizes are the gateway's estimate (`len(text)/4`, decision 3):
L = 52, short = 6.

## Real-model results

| Run | Service order | L wait |
|---|---|---|
| A fcfs | blocker, L, S1..S4 (arrival order) | 5.25 s |
| B spf_aging | blocker, S1..S4, L | 10.22 s |
| C spf_aging, alpha 5, a short request every 1 s | blocker, S1..S9, **L**, S10..S16 | 24.50 s |
| D spf_aging, alpha 0, same overload | blocker, S1..S16, L | 27.28 s |

## How to read the aging result (limitation)

ARCHITECTURE.md decision 4 states the bound as `wait <= (long_len - short_len) / alpha` (here 9.2 s).
What the formula actually bounds is the **crossover**: after waiting that long, L outranks every short
request that arrives later. L still waits for the short requests that arrived before the crossover.
That is exactly what run C shows: S1..S9 (all arrived within 9.2 s of L) went first, then L, then
S10..S16 (arrived later). So L's total wait (24.5 s) is longer than 9.2 s but finite. The frozen
document was not changed; this note records the precise meaning.

In run D the short requests stop after S16, so L is eventually served once the backlog drains.
With a never-ending stream of short requests and no aging, L would never be served; the unit test
`tests/test_phase3.py::test_without_aging_the_long_request_starves` checks that case.
