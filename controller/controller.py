import argparse
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

import yaml


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


class Controller:
    def __init__(self, config: Dict, dry_run: bool = False):
        self.config = config
        self.events_path = Path(config["events"]["sink"])
        self.interval = config["events"].get("sample_interval_ms", 2000) / 1000
        self.dry_run = dry_run
        self.state: Dict[str, ResourceState] = {}

    def run(self) -> None:
        while True:
            for name, payload in self.config["containers"].items():
                self.state.setdefault(name, ResourceState())
                self.ensure_base_limits(name, payload)
                self.adjust_cpu(name, payload)
                self.adjust_memory(name, payload)
                self.adjust_io(name, payload)
            time.sleep(self.interval)

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

    def adjust_cpu(self, name: str, payload: Dict) -> None:
        cgroup = Path(payload["cgroup_path"])
        cpu = payload["cpu"]
        state = self.state[name]
        stat_path = cgroup / "cpu.stat"
        if not stat_path.exists():
            return
        stat = self.parse_key_value(stat_path)
        usage = int(stat.get("usage_usec", 0))
        throttled = int(stat.get("throttled_usec", 0))
        if state.last_cpu_usage is not None:
            usage_delta = max(usage - state.last_cpu_usage, 0)
            throttled_delta = max(throttled - state.last_cpu_throttled, 0)
            if throttled_delta > 0 and state.cpu_soft < cpu["hard_quota_us"]:
                new_soft = min(state.cpu_soft + cpu["adjust_step_us"], cpu["hard_quota_us"])
                self.emit(
                    "soft_limit_hit",
                    f"{name} CPU throttled ({throttled_delta} usec); raising soft quota to {new_soft}",
                    {"resource": "cpu", "container": name, "new_soft_quota_us": new_soft},
                )
                state.cpu_soft = new_soft
                self.write_cpu_max(cgroup, new_soft, cpu["period_us"])
            elif throttled_delta > 0 and state.cpu_soft >= cpu["hard_quota_us"]:
                self.emit(
                    "hard_limit_hit",
                    f"{name} CPU throttled at hard limit ({state.cpu_soft})",
                    {"resource": "cpu", "container": name, "hard_quota_us": cpu["hard_quota_us"]},
                )
        state.last_cpu_usage = usage
        state.last_cpu_throttled = throttled

    def adjust_memory(self, name: str, payload: Dict) -> None:
        cgroup = Path(payload["cgroup_path"])
        memory = payload["memory"]
        state = self.state[name]
        current_path = cgroup / "memory.current"
        if not current_path.exists():
            return
        current_val = self.read_int(current_path)
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

    def adjust_io(self, name: str, payload: Dict) -> None:
        cgroup = Path(payload["cgroup_path"])
        io_cfg = payload["io"]
        stat_path = cgroup / "io.stat"
        if not stat_path.exists():
            return
        metrics = self.parse_io_stat(stat_path, io_cfg["device"])
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



