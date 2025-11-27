import argparse
import json
import time
from pathlib import Path
from typing import Dict

import yaml


PSI_FILES = {
    "cpu": Path("/proc/pressure/cpu"),
    "memory": Path("/proc/pressure/memory"),
    "io": Path("/proc/pressure/io"),
}


def load_config(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def append_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


class PressureAgent:
    def __init__(self, config: Dict, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.interval = config["events"].get("sample_interval_ms", 2000) / 1000
        self.events_path = Path(config["events"]["sink"])
        metrics_cfg = config.get("metrics", {})
        self.samples_path = Path(metrics_cfg["samples_sink"]) if metrics_cfg.get("samples_sink") else None
        self.cgroup_paths = {name: Path(cfg["cgroup_path"]) for name, cfg in config["containers"].items()}
        self.last_memory_events: Dict[str, Dict[str, int]] = {}
        self.last_cpu_throttled: Dict[str, int] = {}
        self.last_io_stat: Dict[str, Dict[str, int]] = {}

    def run(self) -> None:
        while True:
            loop_ts = time.time()
            psi_snapshot = self.check_system_pressure()
            cgroup_snapshot = self.check_cgroup_stats()
            self.record_training_sample(loop_ts, psi_snapshot, cgroup_snapshot)
            time.sleep(self.interval)

    def check_system_pressure(self) -> Dict[str, Dict]:
        snapshot: Dict[str, Dict] = {}
        for resource, psi_path in PSI_FILES.items():
            if not psi_path.exists():
                continue
            data = self.parse_psi(psi_path)
            snapshot[resource] = data
            if data["some"]["avg10"] >= 0.2:
                self.emit(
                    "psi_warning",
                    f"System {resource} pressure avg10={data['some']['avg10']}",
                    {"resource": resource, "psi": data["some"]},
                )
            if data["full"]["avg10"] >= 0.1:
                self.emit(
                    "psi_stall",
                    f"System {resource} FULL pressure avg10={data['full']['avg10']}",
                    {"resource": resource, "psi": data["full"]},
                )
        return snapshot

    def check_cgroup_stats(self) -> Dict[str, Dict]:
        snapshot: Dict[str, Dict] = {}
        for name, path in self.cgroup_paths.items():
            memory_events = path / "memory.events"
            cgroup_data: Dict[str, Dict] = {}
            if memory_events.exists():
                cgroup_data["memory_events"] = self.detect_memory_events(name, memory_events)
            cpu_stat = path / "cpu.stat"
            if cpu_stat.exists():
                cgroup_data["cpu"] = self.detect_cpu_throttle(name, cpu_stat)
            io_stat = path / "io.stat"
            if io_stat.exists():
                cgroup_data["io"] = self.detect_io_slowdown(name, io_stat)
            if cgroup_data:
                snapshot[name] = cgroup_data
        return snapshot

    def detect_memory_events(self, name: str, path: Path) -> Dict[str, int]:
        stats = self.parse_key_value(path)
        previous = self.last_memory_events.get(name, {})
        for key in ("low", "high", "max", "oom", "oom_kill"):
            current = int(stats.get(key, 0))
            delta = current - int(previous.get(key, 0))
            if delta > 0:
                event_type = "memory_event"
                if key in ("oom", "oom_kill"):
                    event_type = "memory_critical"
                self.emit(
                    event_type,
                    f"{name} memory event {key} x{delta}",
                    {"container": name, "event": key, "count": delta},
                )
        self.last_memory_events[name] = {k: int(v) for k, v in stats.items()}
        return self.last_memory_events[name]

    def detect_cpu_throttle(self, name: str, path: Path) -> Dict[str, int]:
        stats = self.parse_key_value(path)
        throttled = int(stats.get("nr_throttled", 0))
        last = self.last_cpu_throttled.get(name, 0)
        delta = throttled - last
        if delta > 0:
            self.emit(
                "cpu_throttle",
                f"{name} experienced {delta} throttled periods",
                {"container": name, "delta": delta, "total": throttled},
            )
        self.last_cpu_throttled[name] = throttled
        metrics = {"nr_throttled": throttled}
        if "usage_usec" in stats:
            metrics["usage_usec"] = int(stats["usage_usec"])
        if "throttled_usec" in stats:
            metrics["throttled_usec"] = int(stats["throttled_usec"])
        return metrics

    def detect_io_slowdown(self, name: str, path: Path) -> Dict[str, int]:
        lines = path.read_text(encoding="utf-8").splitlines()
        if not lines:
            return {}
        stats: Dict[str, int] = {}
        for line in lines:
            tokens = line.split()
            if not tokens:
                continue
            for pair in tokens[1:]:
                k, v = pair.split("=")
                stats[k] = stats.get(k, 0) + int(v)
        previous = self.last_io_stat.get(name, {})
        deltas = {k: stats.get(k, 0) - previous.get(k, 0) for k in stats}
        if any(value > 0 for key, value in deltas.items() if key.endswith("wait")):
            self.emit(
                "io_pressure",
                f"{name} IO wait increasing",
                {"container": name, "deltas": deltas},
            )
        self.last_io_stat[name] = stats
        return stats

    def parse_psi(self, path: Path) -> Dict[str, Dict[str, float]]:
        out = {"some": {"avg10": 0.0, "avg60": 0.0, "avg300": 0.0}, "full": {"avg10": 0.0, "avg60": 0.0, "avg300": 0.0}}
        for line in path.read_text(encoding="utf-8").splitlines():
            parts = line.split()
            category = parts[0]
            for kv in parts[1:]:
                key, value = kv.split("=")
                if key.startswith("avg"):
                    out[category][key] = float(value)
        return out

    def parse_key_value(self, path: Path) -> Dict[str, str]:
        data: Dict[str, str] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            if " " in line:
                key, value = line.split()
                data[key] = value
        return data

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

    def record_training_sample(self, timestamp: float, psi_snapshot: Dict[str, Dict], cgroup_snapshot: Dict[str, Dict]) -> None:
        if self.dry_run or not self.samples_path:
            return
        sample = {
            "time": timestamp,
            "source": "agent",
            "psi": psi_snapshot,
            "cgroups": cgroup_snapshot,
        }
        append_json(self.samples_path, sample)


def parse_args():
    parser = argparse.ArgumentParser(description="Pressure and cgroup monitoring agent")
    parser.add_argument(
        "--config",
        default="config/containers.yml",
        type=Path,
        help="Path to shared controller/agent configuration",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print events instead of writing the sink")
    return parser.parse_args()


def main():
    args = parse_args()
    cfg = load_config(args.config)
    agent = PressureAgent(cfg, dry_run=args.dry_run)
    try:
        agent.run()
    except KeyboardInterrupt:
        print("Agent stopped.")


if __name__ == "__main__":
    main()



