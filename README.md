## Containers Resource Lab

This repo provides a minimal reproducible lab for experimenting with Linux cgroup v2 resource management:

- `docker-compose.yml` spins up three synthetic workloads (CPU burner, memory hog, IO tester).
- `controller/controller.py` applies soft/hard limits for CPU quota, memory, and IO by writing directly to the cgroup filesystem. When a soft limit is hit it bumps the limit (without exceeding the hard ceiling) and emits an event. Hard limit events are reported but not changed.
- `agent/agent.py` watches kernel PSI signals (`/proc/pressure/*`) plus per-cgroup stats to surface stalls, throttling, and OOMs.

### Prerequisites

1. Linux host with cgroup v2 mounted at `/sys/fs/cgroup`.
2. Docker Engine 24+ with Compose V2 (`docker compose` CLI).
3. Python 3.11+.

### Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

docker compose up -d
python controller/controller.py
# In another terminal
python agent/agent.py
```

Events are appended to `logs/events.jsonl`.

### Updating cgroup targets

Each service is tagged with `com.example.cgroup_path` to document the desired cgroup path. Update the host paths in `config/containers.yml` so they match the actual cgroup directories that Docker creates (e.g. `/sys/fs/cgroup/system.slice/docker-<id>.scope`). The controller will create intermediate directories if they do not exist and can optionally attach additional PIDs.

### Experiments

See `experiments/README.md` for a curated set of runs that stress CPU, memory, and IO to trigger both soft and hard limits. The controller changes limits on soft-limit breaches, while the agent logs PSI spikes and throttling so you can correlate behavior.



