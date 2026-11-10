# Phase 2 evidence notes

Written after the runs; the evidence files themselves are unedited script output.

| File | Kind |
|---|---|
| `phase2_routing_real.txt` | **REAL**: 3 llama.cpp replicas (1 CPU, 1 GiB each) started and removed by the gateway, 10 requests per policy |
| `simulated/phase2_routing_standin_SIMULATED.txt` | **SIMULATED**: same script with the stand-in image (no model). Gateway behaviour only, not model performance. This is the second stand-in run (12:10 UTC), made to confirm a fix to a harmless shell warning in the script; the first stand-in run was replaced. |

## Identical routing choices in the real run

In `phase2_routing_real.txt`, round_robin and least_busy assigned all 10 requests to the
same replicas (r1 r2 r3 r1 r2 r3 r1 r2 r3 r2). This is a property of that workload, not a
sign that the policies are the same:

- Real requests took 2-14 s, far longer than the 0.4 s between arrivals, so all three
  replicas filled evenly. Round robin skips full replicas (e.g. req 10: r1 and r3 were 2/2,
  so it took r2), which made it behave like least_busy here.
- The policies are separate implementations (`gateway/router.py`). Their difference is shown by:
  - the stand-in run above (e.g. req 3: round_robin -> r3, least_busy -> r2, with r2 empty), and
  - `tests/test_phase2.py::test_least_busy_differs_from_round_robin`, which checks through the
    gateway that, with r1 busy, the third request goes to r2 under least_busy and r1 under round_robin.

## Timestamps

Wall-clock log timestamps in these files sometimes run backwards (in the least_busy sections,
`replica ready` shows an earlier time than `replica started`): WSL2 stepped the system clock.
Line order in the logs is the true event order (one process, one log handler), and all durations
(`load_s`, `ttft_s`, `total_s`) were measured with `time.monotonic()`, so no reported number is
affected. Since this checkpoint every log line also carries `mono=<seconds>` (monotonic); use that,
not the wall-clock time, for ordering and plots (Phase 9).
