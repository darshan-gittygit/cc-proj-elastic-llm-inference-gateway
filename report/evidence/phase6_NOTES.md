# Phase 6 evidence notes

Written after the runs; the evidence files themselves are unedited output of `scripts/phase6_autoscale_check.sh`.

| File | Kind |
|---|---|
| `phase6_autoscale_real.txt` | **REAL**: llama.cpp containers, real model |
| `simulated/phase6_autoscale_standin_SIMULATED.txt` | **SIMULATED**: stand-in image, no model. Gateway behaviour only. |

Both runs use `gateway/config.yaml` unchanged (min 1, max 3, 2 slots, up > 4.0 for 5 s, down < 1.0 for
30 s, `prewarm: warm_pool`, 1 spare, trigger ratio 0.7). Timeline: start, 14 concurrent clients,
then idle until 180 s after start.

## Real-model run

| Time (s after start) | Event |
|---|---|
| 0.0 / 2.7 | r1 started / READY |
| 3.4 / 4.5 | spare r2 started at idle / loaded (WARM_SPARE, not routable) |
| 9.6 | burst of 14 clients begins (signal rises to 13.78) |
| 15.4 | **scale up**: r2 promoted (instant), then spare r3 started because the signal was above 0.7 x 4 |
| 20.7 | **scale up**: r3 promoted; now 3 READY = max_replicas |
| 25.8 | further scale-up wanted but blocked: at max_replicas=3 |
| 59.2 | burst finished (14/14 clients complete) |
| 82.9 | **scale down**: r3 drained (in_flight 0) and stopped |
| 84.3 / 86.4 | a new idle spare r4 started / loaded (capacity allowed it) |
| 113.4 | **scale down**: r2 drained and stopped; 1 READY + 1 spare until the end |

No flapping: both scale-ups happened during the burst, both scale-downs after it, about 30 s apart
(the cooldown). No request was dispatched to a replica before it was READY or promoted. The stand-in
run shows the same sequence.

## Limitations and interpretation

- Both real scale-ups were spare promotions, so this run did not need the "start a new container on
  scale-up" path; that path is covered by `tests/test_phase6.py` (unit tests and
  `test_burst_starts_a_replica_that_gets_requests_only_once_ready`, stand-in end to end).
  Measuring `prewarm: none` against `warm_pool` is Phase 7.
- Spares count toward `max_replicas` (decision 12 sizes max_replicas by containers/CPU; the frozen
  text does not say). So once the burst promoted both spares, no new spare could start until a
  replica was drained.
- After idle scale-down the pool is 1 READY + 1 WARM_SPARE, as decision 8 specifies ("or at idle if
  capacity allows"), so 2 containers stay loaded while idle.
- Signal = (queued + in_flight) / READY replicas, smoothed with an EWMA (weight 0.5 on the new value)
  every 1 s. Neither the weight nor the tick length is a config field; they are constants in
  `gateway/autoscaler.py`.
- Requests whose client left while queued still count in `queued` until the dispatch loop drops them.
