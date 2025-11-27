"""
Summarize ML controller effectiveness using emitted events.

Usage:
    python scripts/evaluate_policy.py --events logs/events.jsonl
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Evaluate ML soft-limit adjustments")
    parser.add_argument("--events", type=Path, default=Path("logs/events.jsonl"))
    return parser.parse_args()


def load_events(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Events file {path} not found")
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            yield json.loads(line)


def main():
    args = parse_args()
    totals = {"ml_adjustment": 0, "ml_effective": 0, "ml_no_improvement": 0}
    improvements = []
    for event in load_events(args.events):
        etype = event.get("type")
        if etype in totals:
            totals[etype] += 1
            if etype in ("ml_effective", "ml_no_improvement"):
                improvements.append(
                    {
                        "time": event.get("time"),
                        "container": event.get("data", {}).get("container"),
                        "current_delta": event.get("data", {}).get("current_delta"),
                        "previous_delta": event.get("data", {}).get("previous_delta"),
                    }
                )
    if totals["ml_adjustment"] == 0:
        print("No ML adjustment events found yet.")
        return
    success_rate = (
        totals["ml_effective"] / max(totals["ml_effective"] + totals["ml_no_improvement"], 1)
        if (totals["ml_effective"] + totals["ml_no_improvement"]) > 0
        else 0
    )
    print("ML adjustment summary")
    print("---------------------")
    print(f"Adjustments applied: {totals['ml_adjustment']}")
    print(f"Effective episodes: {totals['ml_effective']}")
    print(f"No-improvement episodes: {totals['ml_no_improvement']}")
    print(f"Success rate: {success_rate:.2%}")
    if improvements:
        print("\nSample observations:")
        for item in improvements[-5:]:
            print(
                f"- {item['container']} at {item['time']}: "
                f"throttle {item['current_delta']} vs prev {item['previous_delta']}"
            )


if __name__ == "__main__":
    main()

