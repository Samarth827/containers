## Experiment Playbook

These runs assume:

1. You are on a Linux host with cgroup v2.
2. `docker compose up -d` is running the three workloads.
3. `controller/controller.py` and `agent/agent.py` are running (ideally in separate terminals).

All timestamps/events land in `logs/events.jsonl`.

### 1. CPU throttling ramp

1. Pin the CPU burner container to a single core to make throttling easier:
   ```bash
   docker update cpu_burner --cpuset-cpus="0"
   ```
2. Observe the controller bumping `cpu.max` until it reaches the hard quota. The agent should emit `cpu_throttle` and possibly `psi_warning` for CPU.
3. Optional: add another stressor with `docker run --rm -it --cpuset-cpus=0 alpine:3.19 sh -c 'while true; do :; done'` to force hard-limit conditions.

Expected: `soft_limit_hit` controller events followed by `hard_limit_hit` once the quota tops out.

### 2. Memory soft vs hard limits

1. Inside `memory_hog`, allocate more aggressively:
   ```bash
   docker exec -it memory_hog python - <<'PY'
   import time
   blocks = []
   for _ in range(100):
       blocks.append(bytearray(10 * 1024 * 1024))
       time.sleep(0.3)
   time.sleep(600)
   PY
   ```
2. Controller should raise `memory.high` whenever the soft limit is reached.
3. If the hard limit is exceeded, expect agent `memory_critical` events because the kernel logs `oom_kill`.

### 3. I/O throttling

1. Identify the block device backing `./experiments/data` (e.g. `lsblk`); update `config/containers.yml` if needed.
2. Run concurrent readers/writers inside `io_tester`:
   ```bash
   docker exec -it io_tester sh -c 'while true; do dd if=/dev/zero of=/data/fill bs=4M count=256 oflag=direct; done'
   ```
3. Controller should increase `io.max` soft limits as throughput grows. The agent should emit `io_pressure` when waits accumulate.

### 4. PSI induced stall

1. Temporarily pause all containers except one to alter pressure: `docker pause memory_hog io_tester`.
2. Launch multiple CPU burners (e.g. stress-ng) on the host to create global pressure: `stress-ng --cpu 8 --timeout 120s`.
3. Observe PSI warnings/stalls without per-cgroup events to contrast host-level vs container-level signals.

### 5. Replay log

After any run, replay events:

```bash
jq '.time, .type, .message' logs/events.jsonl
```

Look for sequences where `soft_limit_hit` precedes `psi_warning` to confirm the controller reacted before the kernel escalated.



