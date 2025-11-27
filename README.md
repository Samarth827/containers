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

### Collect training data

The controller and agent now log structured samples to `logs/training_samples.jsonl`. Let the system run under load to collect a few hundred intervals before attempting to train ML policies.

### Prototype ML soft-limit policies

Use the provided regression script to fit a baseline model that predicts the next CPU soft limit:

```bash
python notebooks/soft_limit_regression.py \
  --samples logs/training_samples.jsonl \
  --output models/soft_limit_linear.pkl
```

This outputs both the serialized model and a `.metrics.json` report so you can compare future iterations.

After a model exists at `models/soft_limit_linear.pkl`, the controller automatically loads it (see `config/containers.yml -> ml.model_path`) and will call it whenever CPU throttling occurs. If the model suggests a higher soft quota (but below the hard ceiling), that value is applied; otherwise the legacy fixed-step heuristic is used.

### Evaluate & refresh the model

- `scripts/evaluate_policy.py` summarizes `ml_adjustment`, `ml_effective`, and `ml_no_improvement` events to see whether the ML policy is reducing throttling.
- `scripts/retrain_model.py` reuses the latest samples to fit a fresh model and appends the results to `logs/model_history.jsonl`, making it easy to schedule periodic retraining (e.g., cron/systemd timer).

### Updating cgroup targets

Each service is tagged with `com.example.cgroup_path` to document the desired cgroup path. Update the host paths in `config/containers.yml` so they match the actual cgroup directories that Docker creates (e.g. `/sys/fs/cgroup/system.slice/docker-<id>.scope`). The controller will create intermediate directories if they do not exist and can optionally attach additional PIDs.

### Experiments

See `experiments/README.md` for a curated set of runs that stress CPU, memory, and IO to trigger both soft and hard limits. The controller changes limits on soft-limit breaches, while the agent logs PSI spikes and throttling so you can correlate behavior.



