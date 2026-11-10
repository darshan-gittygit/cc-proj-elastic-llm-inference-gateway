# Phase 8 evidence notes

Written after the runs; the evidence files themselves are unedited output of `scripts/demo5_config.sh`.

| File | Kind |
|---|---|
| `phase8_config_real.txt` | **REAL**: llama.cpp containers, real model (one run) |
| `simulated/phase8_config_standin_SIMULATED.txt` | **SIMULATED**: stand-in image, no model. Gateway behaviour only. |

## Procedure

One gateway process for the whole demo (its PID is checked at the start and the end). It reads a
copy of `gateway/config.yaml` with 2 replicas x 1 slot and max_replicas 3; that copy is edited
between stages and applied with `POST /admin/reload`. The committed `gateway/config.yaml` is not
changed. The same workload runs in both stages (`loadgen/burst.py`): a burst `B,B,L,S,S,S,S`
(two busy requests occupy both slots, then a long and four short requests queue) and a 4-turn session.

## Real-model results

| | Stage A: fcfs, round_robin, prewarm none | Stage B: spf_aging, session_affinity, warm_pool |
|---|---|---|
| Queued requests served in order | L, S1, S2, S3, S4 | S1, S2, S3, S4, L |
| 4 session turns went to | r1, r2, r1, r2 | r1, r1, r1, r1 |
| Warm spare | none started | r3 started at idle, loaded in 1.94 s |

- Reload 1 (HTTP 200) changed exactly `scheduler`, `router` and `prewarm`.
- Reload 2 with `router: fastest` was rejected (HTTP 400, `[CONFIG] reload rejected, nothing
  changed`); the next session turn still used `session_affinity` and the same replica.
- Reload 3 (`prewarm: none`) stopped the spare within about 1 s (next autoscaler tick).
- r1 and r2 were started once; no restart, no code change.

## Limitations

- In both demo runs the queue was empty at the moment of each reload (`queued_moved=0`), so moving
  queued requests to the new scheduler is shown only by the tests
  (`tests/test_phase8.py::test_reload_switches_scheduler_and_carries_queued_requests_over` and
  `test_reload_back_to_fcfs_restores_arrival_order`).
- A request that was put back at the front of the queue for a retry (Phase 5) loses that front
  position if the scheduler is switched while it waits; it is re-queued by arrival time.
- The session-affinity map is kept when the router setting is unchanged; switching to another
  router and back starts a new map (history is in the gateway, so answers stay correct).
- `container_prefix` and `docker_network` cannot change while running (rejected); container
  settings such as `replica_cpus` apply only to replicas started after the reload.
- Reload lines use a `CONFIG` log tag, which is not one of the four decision tags of decision 11.
