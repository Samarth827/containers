"""
Train baseline regressors that predict the next CPU soft quota using controller logs.

Usage:
    python notebooks/soft_limit_regression.py \
        --samples logs/training_samples.jsonl \
        --output models/soft_limit_linear.pkl

The script expects controller samples produced by the instrumentation step.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def load_samples(path: Path) -> List[Dict]:
    if not path.exists():
        raise FileNotFoundError(f"Sample file {path} not found; run controller/agent first.")
    rows: List[Dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            rows.append(json.loads(line))
    return rows


def build_cpu_frame(samples: List[Dict]) -> pd.DataFrame:
    controller_rows = [row for row in samples if row.get("source") == "controller"]
    data = []
    for row in controller_rows:
        cpu = row.get("cpu") or {}
        memory = row.get("memory") or {}
        io = row.get("io") or {}
        if not cpu:
            continue
        period = cpu.get("period_us") or 1
        usage_delta = cpu.get("usage_delta_usec")
        throttled_delta = cpu.get("throttled_delta_usec")
        if usage_delta is None or throttled_delta is None:
            continue
        usage_ratio = usage_delta / period
        throttle_ratio = throttled_delta / period
        memory_ratio = None
        if memory.get("soft_bytes") and memory.get("current_bytes"):
            memory_ratio = memory["current_bytes"] / max(memory["soft_bytes"], 1)
        rbps = (io.get("metrics") or {}).get("rbps")
        wbps = (io.get("metrics") or {}).get("wbps")
        timestamp = row.get("time")
        data.append(
            {
                "container": row["container"],
                "timestamp": timestamp,
                "soft_quota_us": cpu.get("soft_quota_us"),
                "hard_quota_us": cpu.get("hard_quota_us"),
                "usage_ratio": usage_ratio,
                "throttle_ratio": throttle_ratio,
                "memory_ratio": memory_ratio,
                "rbps": rbps,
                "wbps": wbps,
            }
        )
    df = pd.DataFrame(data)
    if df.empty:
        raise ValueError("No controller samples with CPU data were found.")
    df.sort_values(["container", "timestamp"], inplace=True)
    # Next-step soft quota is our target.
    df["target_soft_quota_us"] = df.groupby("container")["soft_quota_us"].shift(-1)
    df.dropna(subset=["target_soft_quota_us"], inplace=True)
    return df


def train_model(df: pd.DataFrame):
    feature_cols = ["usage_ratio", "throttle_ratio", "memory_ratio", "rbps", "wbps", "soft_quota_us", "hard_quota_us"]
    X = df[feature_cols].fillna(0.0)
    y = df["target_soft_quota_us"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=True, random_state=42)
    pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=0.1)),
        ]
    )
    pipeline.fit(X_train, y_train)
    preds = pipeline.predict(X_test)
    metrics = {
        "mae": mean_absolute_error(y_test, preds),
        "r2": r2_score(y_test, preds),
        "test_size": len(y_test),
        "train_size": len(y_train),
    }
    return pipeline, metrics


def save_report(metrics: Dict, output_path: Path) -> None:
    report_path = output_path.with_suffix(".metrics.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(description="Train regression model for soft limit prediction")
    parser.add_argument("--samples", type=Path, default=Path("logs/training_samples.jsonl"))
    parser.add_argument("--output", type=Path, default=Path("models/soft_limit_linear.pkl"))
    return parser.parse_args()


def main():
    args = parse_args()
    samples = load_samples(args.samples)
    df = build_cpu_frame(samples)
    model, metrics = train_model(df)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, args.output)
    save_report(metrics, args.output)
    print("Model saved:", args.output)
    print("Metrics:", json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()

