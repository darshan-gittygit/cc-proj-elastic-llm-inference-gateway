# Phase 5 evidence notes

Written after the runs; the evidence files themselves are unedited output of `scripts/demo4_fault.sh`.

| File | Kind |
|---|---|
| `phase5_fault_real.txt` | **REAL**, run 2: fault 15 s after the clients start, max_tokens 48 (the script's current settings) |
| `phase5_fault_real_run1_early_fault.txt` | **REAL**, run 1: fault at 6 s, max_tokens 40 (the script's first settings, shown in its header) |
| `simulated/phase5_fault_standin_SIMULATED.txt` | **SIMULATED**: stand-in image, no model. Gateway behaviour only. |

Run 1 is kept, not replaced. Its fault came so early that the two requests on r1 had sent only 0 and
1 tokens, which is too little to test assumption #6. That is why run 2 injects the fault later.
Setup in every run: 2 replicas x 2 slots, 4 concurrent clients (2 per replica), r1 fails mid-stream,
config from `gateway/config.yaml` (health_interval_s 1.5, health_fail_limit 3, max_retries 2).

## Real-model results (run 2)

| | docker kill r1 | docker pause r1 |
|---|---|---|
| Detected by | broken stream (decision 9) | 3 failed health probes |
| r1 declared DEAD after the fault | 0.66 s | 7.55 s |
| Replacement r3 READY after the fault | 2.97 s | 9.84 s |
| Requests on r1 | 2, both retried (attempt 1/3) | 2, both retried |
| Tokens the clients already had | 21 and 22 | 26 and 27 |
| Gateway prefix check (assumption #6) | matched 21/21 and 22/22 | matched 26/26 and 27/27 |
| Clients with complete output, no error, no restart event | 4/4 | 4/4 |

## Assumption #6: checked, with one finding

- **Holds where the gateway relies on it.** In all 4 real retries, the replica that took over
  regenerated exactly the tokens the client already had, so the gateway skipped them and continued
  the stream; the clients never saw a duplicate or a restart.
- **Does not hold in general.** For one of the 4 prompts (client 2), the final text differed from a
  fault-free reference run of the same prompt, in both run-2 faults, even though the first 21/26
  tokens matched. In run 1 (kill) the same prompt was regenerated from scratch on r3 and also
  differed. So with the same model and temperature 0, llama.cpp's output for that prompt depends on
  the run, most likely on what else is being processed at the same time (continuous batching on CPU).
  The client still gets one coherent answer: everything after the resume point comes from the same
  regeneration whose prefix matched. But the answer is not guaranteed to equal what an uninterrupted
  run would have produced. The demo records only *whether* the final text matched, not where it
  first diverged.
- **If the prefix ever differs**, the gateway sends `{"llmgw_retry": {"discard_previous_output": true}}`
  and restarts the output. That path is covered by `tests/test_phase5.py::test_different_regenerated_output_makes_client_restart`;
  it did not occur in the real runs.

## Other observations and limitations

- A retry first regenerates the tokens the client already has before it can continue, so the client
  sees a pause of about that many tokens' generation time (for example, about 8 to 17 s in run 2).
- `docker pause` is only noticed by the health probes: detection ~= health_fail_limit x health_interval_s
  + probe timeouts (7.55 s here). Hung streams have no read timeout of their own; the HealthMonitor
  aborts and requeues them (`requeueing in-flight requests of dead replica ... requests=1,3`).
- Wall-clock times in the logs jump (e.g. a `replica started` line stamped earlier than the line
  before it); all durations above use the monotonic `mono=` field.
- The replacement keeps the replica count at `min_replicas` by itself; the autoscaler (Phase 6) does
  not exist yet.
