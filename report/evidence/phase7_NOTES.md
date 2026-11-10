# Phase 7 evidence notes

Written after the runs; the evidence files themselves are unedited output of `scripts/demo3_prewarm.sh`.

| File | Kind |
|---|---|
| `phase7_prewarm_real.txt` | **REAL**: llama.cpp containers, real model |
| `simulated/phase7_prewarm_standin_SIMULATED.txt` | **SIMULATED**: stand-in image (fixed 2 s "load", no model). Gateway behaviour only. |

## Procedure

- Two runs per file, one per mode, as the Phase 7 criterion specifies ("same burst run with
  `prewarm: none` and `warm_pool`"). No repeats, so no statistical claim is made.
- Config: `gateway/config.yaml` with only `prewarm` changed; the script prints the keys that differ
  (`['prewarm']`). Same image, thresholds, timers, slots and max_replicas in both runs.
- Each run: fresh gateway, r1 READY, 15 s idle (in `warm_pool` the spare loads during this time),
  then 14 concurrent clients with identical prompts and max_tokens, then the gateway stops.
  `none` ran first, then `warm_pool`.
- All times come from the gateway log's monotonic `mono=` field.

## Real-model results (first scale-up of each run)

| | prewarm: none | prewarm: warm_pool |
|---|---|---|
| How the scale-up was done | start a new container (r2) | promote spare r2 |
| New replica's model load (`load_s`) | 1.96 s, during the burst (on the critical path) | 1.87 s, 12.9 s **before** the burst (off the critical path) |
| Trigger -> new replica routable | 1.58 s | 0.00 s |
| **Trigger -> first request dispatched to it** | **1.59 s** | **0.00 s** |
| Trigger -> first token from it | 6.87 s | 8.17 s |

Second scale-up: `none` 2.18 s to first dispatch (start r3, load_s 2.50); `warm_pool` 0.00 s
(promote r3, which had loaded during the burst, before the trigger).

Burst latency, all 14 clients (arrival -> first token / -> complete), one run each:

| | mean first token | mean complete | burst makespan |
|---|---|---|---|
| none | 30.19 s | 53.42 s | 81.09 s |
| warm_pool | 26.89 s | 44.68 s | 66.23 s |

## What this shows, and what it does not

- **Measured as required:** the warm pool removes the model-load wait from scale-up. A new
  replica receives its first request immediately on promotion instead of ~1.6-2.2 s later.
- **Model load is not the dominant scale-up delay on this machine.** Decision 8 assumed it is
  ("to be quantified in Phase 7"). Measured: model load ~2 s, against the 5 s `scale_up_after_s`
  persistence window before any scale-up, and ~5-8 s for the new replica's first token (prompt
  processing on a 1-CPU replica with 2 concurrent requests). The model file was in the page cache
  (it had been read many times); a cold load, e.g. after a reboot, may be slower and was not measured.
- **First token on the new replica was not faster with the warm pool in this run** (8.17 s vs
  6.87 s). That time is dominated by prompt processing on the new replica, not by loading; with one
  run per mode the difference is within what run-to-run variation can explain.
- **Burst-level latency was lower with the warm pool** (makespan 66 s vs 81 s), but the gap (15 s)
  is much larger than the ~3.8 s of load time hidden across both scale-ups, so this single pair of
  runs cannot attribute it to pre-warming. Inconclusive without repeated runs.
- Stand-in run: same pattern (2.55 s vs 0.00 s to first dispatch); its 2-3 s load is simulated.

## Measurement details

- With `none`, the `scale up` log line is written after `docker run` returns, about 0.4 s after the
  decision. `load_s` is measured from just before `docker run`, so decision -> routable is closer to
  `load_s` (1.96 s) than to the 1.58 s in the table.
- A routable value printed as `-0.00` is a rounding artefact: the `spare promoted` line is logged
  a fraction of a millisecond before the `scale up` line that records the trigger.
- The warm pool keeps one more container loaded while idle (1 READY + 1 spare): memory and a CPU
  limit are reserved even with no load. That cost is not measured here.
