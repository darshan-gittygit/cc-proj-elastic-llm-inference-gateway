# Phase 10 - two clean dry runs of `scripts/run_all_demos.sh`

Both runs used the real model on the demo machine (WSL2 Ubuntu 24.04, Docker 29.1.3, Python 3.12.3,
llama.cpp server image, Qwen2.5-0.5B-Instruct Q4_K_M). They are rehearsals of the full demo, not
measurement experiments: the runner works in a temporary copy of the project and deletes the demos'
outputs afterwards, so no number from these runs is used in the report and no evidence was written.

## Command (identical for both runs)

```
cd ~/llmgw-wsl-test-copy && source ~/llmgw-venv/bin/activate && bash scripts/run_all_demos.sh real
```

`~/llmgw-wsl-test-copy` is the WSL working copy of this repository (the model file lives there).

## Clean setup before each run

1. Re-sync the WSL copy from the Windows repository (`SYNC OK: 73 files identical to Windows source`).
2. Check: no `llmgw-*` containers, no gateway / stand-in / load-generator / runner processes,
   port 8000 free, no `/tmp/llmgw-demos.*` copies left from an earlier run.
3. Record a checksum over all files in `report/evidence/` (Windows repo and WSL copy).

The same checks were repeated after each run (`dry_run_N.checks.txt`).

## Results

| Demo (phase) | Run 1 | Run 2 |
|---|---|---|
| phase1_stream_check (1) | PASS, 6 checks, 9 s | PASS, 6 checks, 8 s |
| phase2_routing_check (2) | PASS, 16 checks, 35 s | PASS, 16 checks, 45 s |
| demo1_scheduling (3) | PASS, 17 checks, 141 s | PASS, 17 checks, 148 s |
| demo2_affinity (4) | PASS, 25 checks, 113 s | PASS, 25 checks, 113 s |
| demo4_fault (5) | PASS, 10 checks, 137 s | PASS, 10 checks, 143 s |
| phase6_autoscale_check (6) | PASS, 9 checks, 191 s | PASS, 9 checks, 193 s |
| demo3_prewarm (7) | PASS, 13 checks, 163 s | PASS, 13 checks, 144 s |
| demo5_config (8) | PASS, 11 checks, 78 s | PASS, 11 checks, 63 s |
| **Total** | **107 PASS, 0 FAIL, exit 0** (17:20:53-17:34:31 UTC) | **107 PASS, 0 FAIL, exit 0** (17:34:54-17:48:21 UTC) |

After each run: 0 gateway containers, 0 gateway/test/load processes, port 8000 free, 0 temporary
copies, and the evidence checksum unchanged (`87b048a0fd824bca` before and after both runs, in both
the Windows repository and the WSL copy).

## Failure handling (checked separately, stand-in mode, not a dry run)

- No mode, an unknown mode, or a Python without the required packages: refused before anything
  starts, exit 2.
- Every demo cut off after 1 s (`DEMO_TIMEOUT_S=1 ... standin`): all 8 reported `FAIL exit=124`,
  runner exit 1, nothing left behind (0 containers, 0 processes, port free, no temporary copy).
- Not exercised: the branch that stops a gateway left running by a demo (the 1 s cut-off came
  before any gateway had started).

## Files

- `dry_run_1.log`, `dry_run_2.log` - the runner's complete console output.
- `dry_run_1.checks.txt`, `dry_run_2.checks.txt` - clean-setup and post-run state.
