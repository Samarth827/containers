"""
Utility to retrain the soft-limit regression model using the latest samples.

Example:
    python scripts/retrain_model.py \
        --samples logs/training_samples.jsonl \
        --output models/soft_limit_linear.pkl
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from notebooks.soft_limit_regression import build_cpu_frame, load_samples, train_model


def parse_args():
    parser = argparse.ArgumentParser(description="Retrain ML model for CPU soft limits")
    parser.add_argument("--samples", type=Path, default=Path("logs/training_samples.jsonl"))
    parser.add_argument("--output", type=Path, default=Path("models/soft_limit_linear.pkl"))
    parser.add_argument("--history", type=Path, default=Path("logs/model_history.jsonl"))
    return parser.parse_args()


def append_history(history_path: Path, payload: dict) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def main():
    args = parse_args()
    samples = load_samples(args.samples)
    df = build_cpu_frame(samples)
    model, metrics = train_model(df)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Reuse joblib via pipeline's own function
    import joblib

    joblib.dump(model, args.output)
    metrics_payload = {
        "time": datetime.utcnow().isoformat() + "Z",
        "model_path": str(args.output),
        "metrics": metrics,
        "samples": len(df),
    }
    append_history(args.history, metrics_payload)
    print("Retrained model saved to", args.output)
    print("Metrics:", json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()

