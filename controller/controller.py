import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import yaml

try:
    from .ml_policy import SoftLimitPolicy
except ImportError:
    from ml_policy import SoftLimitPolicy


def load_config(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def append_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


@dataclass
class ResourceState:
    cpu_soft: Optional[int] = None
    last_cpu_usage: Optional[int] = None
    last_cpu_throttled: Optional[int] = None
    memory_soft: Optional[int] = None
    io_soft_rbps: Optional[int] = None
    io_soft_wbps: Optional[int] = None
    pending_eval: Optional[Dict[str, float]] = None


class Controller:
    def __init__(self, config: Dict, dry_run: bool = False):
        self.config = config
        self.events_path = Path(config["events"]["sink"])
        self.interval = config["events"].get("sample_interval_ms", 2000) / 1000
        metrics_cfg = config.get("metrics", {})
        self.samples_path = Path(metrics_cfg["samples_sink"]) if metrics_cfg.get("samples_sink") else None
        self.dry_run = dry_run
        self.state: Dict[str, ResourceState] = {}
        ml_cfg = config.get("ml", {})
        model_path = ml_cfg.get("model_path")
        self.policy: Optional[SoftLimitPolicy] = None
        if model_path:
            candidate = Path(model_path)
            if candidate.exists():
                self.policy = SoftLimitPolicy(candidate)
            else:
                print(f"[controller] ML model {candidate} not found; falling back to heuristic adjustments.")

    def run(self) -> None:
        while True:
            loop_ts = time.time()
            for name, payload in self.config["containers"].items():
                self.state.setdefault(name, ResourceState())
                self.ensure_base_limits(name, payload)
                stats = self.collect_stats(payload)
                cpu_meta = self.adjust_cpu(name, payload, stats)
                mem_meta = self.adjust_memory(name, payload, stats)
                io_meta = self.adjust_io(name, payload, stats)
                self.record_training_sample(name, payload, stats, cpu_meta, mem_meta, io_meta, loop_ts)
            time.sleep(self.interval)

    def collect_stats(self, payload: Dict) -> Dict:
        cgroup = Path(payload["cgroup_path"])
        cpu_stat = {}
        mem_current = None
        io_metrics = {}
        cpu_path = cgroup / "cpu.stat"
        if cpu_path.exists():
            cpu_stat = self.parse_key_value(cpu_path)
        mem_path = cgroup / "memory.current"
        if mem_path.exists():
            mem_current = self.read_int(mem_path)
        io_path = cgroup / "io.stat"
        if io_path.exists():
            io_metrics = self.parse_io_stat(io_path, payload["io"]["device"])
        return {"cpu_stat": cpu_stat, "memory_current": mem_current, "io_metrics": io_metrics}

    def ensure_base_limits(self, name: str, payload: Dict) -> None:
        cgroup = Path(payload["cgroup_path"])
        cgroup.mkdir(parents=True, exist_ok=True)

        cpu = payload["cpu"]
        soft = cpu["soft_quota_us"]
        hard = cpu["hard_quota_us"]
        if soft > hard:
            raise ValueError(f"{name}: CPU soft_quota_us must be <= hard_quota_us")
        current = self.state[name]
        if current.cpu_soft is None:
            self.write_cpu_max(cgroup, soft, cpu["period_us"])
            current.cpu_soft = soft
        if cpu.get("pids"):
            self.attach_pids(cgroup, cpu["pids"])

        memory = payload["memory"]
        if memory["soft_bytes"] > memory["hard_bytes"]:
            raise ValueError(f"{name}: memory soft limit exceeds hard limit")
        if current.memory_soft is None:
            self.write_memory_limits(cgroup, memory["soft_bytes"], memory["hard_bytes"])
            current.memory_soft = memory["soft_bytes"]

        io = payload["io"]
        if current.io_soft_rbps is None:
            self.write_io_limit(cgroup, io, io["soft_rbps"], io["soft_wbps"])
            current.io_soft_rbps = io["soft_rbps"]
            current.io_soft_wbps = io["soft_wbps"]

    def attach_pids(self, cgroup: Path, pids) -> None:
        procs_file = cgroup / "cgroup.procs"
        for pid in pids:
            self.emit("info", f"Attaching pid {pid} to {cgroup}", {"pid": pid, "cgroup": str(cgroup)})
            if not self.dry_run:
                procs_file.write_text(str(pid), encoding="utf-8")

    def write_cpu_max(self, cgroup: Path, quota: int, period: int) -> None:
        if self.dry_run:
            return
        write_text(cgroup / "cpu.max", f"{quota} {period}")

    def write_memory_limits(self, cgroup: Path, soft: int, hard: int) -> None:
        if self.dry_run:
            return
        write_text(cgroup / "memory.high", str(soft))
        write_text(cgroup / "memory.max", str(hard))

    def write_io_limit(self, cgroup: Path, io_cfg: Dict, rbps: int, wbps: int) -> None:
        if self.dry_run:
            return
        line = f"{io_cfg['device']} rbps={rbps} wbps={wbps}"
        write_text(cgroup / "io.max", line)

    def read_int(self, path: Path) -> int:
        return int(path.read_text(encoding="utf-8").strip())

    def adjust_cpu(self, name: str, payload: Dict, stats: Dict) -> Dict:
        cgroup = Path(payload["cgroup_path"])
        cpu = payload["cpu"]
        state = self.state[name]
        stat = stats["cpu_stat"]
        if not stat:
            return {"usage_delta": 0, "throttled_delta": 0, "usage": 0, "throttled": 0, "soft": state.cpu_soft}
        usage = int(stat.get("usage_usec", 0))
        throttled = int(stat.get("throttled_usec", 0))
        usage_delta = 0
        throttled_delta = 0
        if state.last_cpu_usage is not None:
            usage_delta = max(usage - state.last_cpu_usage, 0)
            throttled_delta = max(throttled - state.last_cpu_throttled, 0)
            if state.pending_eval:
                prev = state.pending_eval
                improvement = prev["prev_delta"] - throttled_delta
                event_type = "ml_effective" if throttled_delta < prev["prev_delta"] else "ml_no_improvement"
                self.emit(
                    event_type,
                    f"{name} ML adjustment impact: Δthrottle={throttled_delta} vs prev {prev['prev_delta']}",
                    {
                        "resource": "cpu",
                        "container": name,
                        "previous_delta": prev["prev_delta"],
                        "current_delta": throttled_delta,
                        "applied_soft_quota_us": prev["new_soft"],
                        "improvement": improvement,
                        "elapsed_sec": time.time() - prev["time"],
                    },
                )
                state.pending_eval = None
            if throttled_delta > 0 and state.cpu_soft < cpu["hard_quota_us"]:
                suggested = self.suggest_cpu_soft_limit(
                    name, payload, stats, cpu_meta={"usage_delta": usage_delta, "throttled_delta": throttled_delta}
                )
                ml_used = suggested is not None
                previous_soft = state.cpu_soft
                new_soft = suggested or min(state.cpu_soft + cpu["adjust_step_us"], cpu["hard_quota_us"])
                self.emit(
                    "soft_limit_hit",
                    f"{name} CPU throttled ({throttled_delta} usec); raising soft quota to {new_soft}",
                    {"resource": "cpu", "container": name, "new_soft_quota_us": new_soft},
                )
                state.cpu_soft = new_soft
                self.write_cpu_max(cgroup, new_soft, cpu["period_us"])
                if ml_used:
                    state.pending_eval = {
                        "prev_delta": throttled_delta,
                        "new_soft": new_soft,
                        "previous_soft": previous_soft,
                        "time": time.time(),
                    }
                    self.emit(
                        "ml_adjustment",
                        f"{name} ML suggested raising CPU soft quota to {new_soft}",
                        {
                            "resource": "cpu",
                            "container": name,
                            "previous_soft_quota_us": previous_soft,
                            "new_soft_quota_us": new_soft,
                            "throttled_delta_usec": throttled_delta,
                        },
                    )
            elif throttled_delta > 0 and state.cpu_soft >= cpu["hard_quota_us"]:
                self.emit(
                    "hard_limit_hit",
                    f"{name} CPU throttled at hard limit ({state.cpu_soft})",
                    {"resource": "cpu", "container": name, "hard_quota_us": cpu["hard_quota_us"]},
                )
        state.last_cpu_usage = usage
        state.last_cpu_throttled = throttled
        return {
            "usage_delta": usage_delta,
            "throttled_delta": throttled_delta,
            "usage": usage,
            "throttled": throttled,
            "soft": state.cpu_soft,
        }

    def suggest_cpu_soft_limit(self, name: str, payload: Dict, stats: Dict, cpu_meta: Dict) -> Optional[int]:
        if not self.policy:
            return None
        state = self.state[name]
        hard_cap = payload["cpu"]["hard_quota_us"]
        soft = state.cpu_soft
        period = payload["cpu"]["period_us"] or 1
        usage_delta = cpu_meta.get("usage_delta", 0)
        throttle_delta = cpu_meta.get("throttled_delta", 0)
        usage_ratio = usage_delta / period
        throttle_ratio = throttle_delta / period
        memory_ratio = None
        memory_current = stats.get("memory_current")
        memory_soft = state.memory_soft
        if memory_current is not None and memory_soft:
            memory_ratio = memory_current / max(memory_soft, 1)
        io_metrics = stats.get("io_metrics") or {}
        features = {
            "usage_ratio": usage_ratio,
            "throttle_ratio": throttle_ratio,
            "memory_ratio": memory_ratio or 0.0,
            "rbps": io_metrics.get("rbps", 0.0),
            "wbps": io_metrics.get("wbps", 0.0),
        }
        return self.policy.suggest(features, hard_cap, soft)

    def adjust_memory(self, name: str, payload: Dict, stats: Dict) -> Dict:
        cgroup = Path(payload["cgroup_path"])
        memory = payload["memory"]
        state = self.state[name]
        current_val = stats["memory_current"]
        if current_val is None:
            return {"current": None, "soft": state.memory_soft}
        if state.memory_soft is None:
            state.memory_soft = memory["soft_bytes"]
        threshold = state.memory_soft * 0.95
        if current_val >= threshold and state.memory_soft < memory["hard_bytes"]:
            new_soft = min(state.memory_soft + memory["adjust_step_bytes"], memory["hard_bytes"])
            self.emit(
                "soft_limit_hit",
                f"{name} memory usage {current_val} >= {threshold}; raising soft limit to {new_soft}",
                {"resource": "memory", "container": name, "new_soft_bytes": new_soft},
            )
            state.memory_soft = new_soft
            self.write_memory_limits(cgroup, new_soft, memory["hard_bytes"])
        elif current_val >= memory["hard_bytes"]:
            self.emit(
                "hard_limit_hit",
                f"{name} memory usage {current_val} exceeded hard limit {memory['hard_bytes']}",
                {"resource": "memory", "container": name, "value": current_val},
            )
        return {"current": current_val, "soft": state.memory_soft, "hard": memory["hard_bytes"]}

    def adjust_io(self, name: str, payload: Dict, stats: Dict) -> Dict:
        cgroup = Path(payload["cgroup_path"])
        io_cfg = payload["io"]
        metrics = stats["io_metrics"]
        if not metrics:
            return {"metrics": {}, "soft_rbps": self.state[name].io_soft_rbps, "soft_wbps": self.state[name].io_soft_wbps}
        state = self.state[name]
        rbps = metrics.get("rbps", 0)
        wbps = metrics.get("wbps", 0)
        soft_hit = False
        new_r = state.io_soft_rbps
        new_w = state.io_soft_wbps
        if rbps >= state.io_soft_rbps and state.io_soft_rbps < io_cfg["hard_rbps"]:
            new_r = min(state.io_soft_rbps + io_cfg["adjust_step_bps"], io_cfg["hard_rbps"])
            soft_hit = True
        if wbps >= state.io_soft_wbps and state.io_soft_wbps < io_cfg["hard_wbps"]:
            new_w = min(state.io_soft_wbps + io_cfg["adjust_step_bps"], io_cfg["hard_wbps"])
            soft_hit = True
        if soft_hit:
            state.io_soft_rbps = new_r
            state.io_soft_wbps = new_w
            self.emit(
                "soft_limit_hit",
                f"{name} IO throughput high; raising limits to {new_r}/{new_w}",
                {"resource": "io", "container": name, "new_soft_rbps": new_r, "new_soft_wbps": new_w},
            )
            self.write_io_limit(cgroup, io_cfg, new_r, new_w)
        elif (rbps >= io_cfg["hard_rbps"]) or (wbps >= io_cfg["hard_wbps"]):
            self.emit(
                "hard_limit_hit",
                f"{name} IO throughput reached hard limit",
                {"resource": "io", "container": name, "rbps": rbps, "wbps": wbps},
            )
        return {
            "metrics": metrics,
            "soft_rbps": state.io_soft_rbps,
            "soft_wbps": state.io_soft_wbps,
            "hard_rbps": io_cfg["hard_rbps"],
            "hard_wbps": io_cfg["hard_wbps"],
        }

    def record_training_sample(
        self,
        name: str,
        payload: Dict,
        stats: Dict,
        cpu_meta: Dict,
        mem_meta: Dict,
        io_meta: Dict,
        timestamp: float,
    ) -> None:
        if self.dry_run or not self.samples_path:
            return
        sample = {
            "time": timestamp,
            "source": "controller",
            "container": name,
            "cpu": {
                "soft_quota_us": cpu_meta.get("soft"),
                "hard_quota_us": payload["cpu"]["hard_quota_us"],
                "period_us": payload["cpu"]["period_us"],
                "usage_usec": cpu_meta.get("usage"),
                "usage_delta_usec": cpu_meta.get("usage_delta"),
                "throttled_usec": cpu_meta.get("throttled"),
                "throttled_delta_usec": cpu_meta.get("throttled_delta"),
            },
            "memory": {
                "current_bytes": mem_meta.get("current"),
                "soft_bytes": mem_meta.get("soft"),
                "hard_bytes": payload["memory"]["hard_bytes"],
            },
            "io": {
                "metrics": io_meta.get("metrics", {}),
                "soft_rbps": io_meta.get("soft_rbps"),
                "soft_wbps": io_meta.get("soft_wbps"),
                "hard_rbps": io_meta.get("hard_rbps", payload["io"]["hard_rbps"]),
                "hard_wbps": io_meta.get("hard_wbps", payload["io"]["hard_wbps"]),
            },
        }
        append_json(self.samples_path, sample)

    def parse_key_value(self, path: Path) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            if " " in line:
                key, value = line.split()
                result[key] = value
        return result

    def parse_io_stat(self, path: Path, device: str) -> Dict[str, int]:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.startswith(device):
                continue
            metrics = {}
            for pair in line.split()[1:]:
                k, v = pair.split("=")
                metrics[k] = int(v)
            return metrics
        return {}

    def emit(self, event_type: str, message: str, data: Dict) -> None:
        payload = {
            "time": time.time(),
            "type": event_type,
            "message": message,
            "data": data,
        }
        if self.dry_run:
            print(json.dumps(payload))
        else:
            append_json(self.events_path, payload)


def parse_args():
    parser = argparse.ArgumentParser(description="Cgroup controller for container workloads")
    parser.add_argument(
        "--config",
        default="config/containers.yml",
        type=Path,
        help="Path to controller configuration YAML",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not write to cgroup files")
    return parser.parse_args()


def main():
    args = parse_args()
    cfg = load_config(args.config)
    controller = Controller(cfg, dry_run=args.dry_run)
    try:
        controller.run()
    except KeyboardInterrupt:
        print("Controller stopped.")


if __name__ == "__main__":
    main()



