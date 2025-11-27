from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import joblib


class SoftLimitPolicy:
    """Wraps an sklearn model that predicts the next CPU soft quota."""

    def __init__(self, model_path: Path):
        self.model_path = Path(model_path)
        if not self.model_path.exists():
            raise FileNotFoundError(f"Model file {self.model_path} not found")
        self.model = joblib.load(self.model_path)

    def suggest(self, features: Dict[str, float], hard_cap: int, current_soft: Optional[int]) -> Optional[int]:
        if current_soft is None:
            return None
        vector = [
            features.get("usage_ratio", 0.0),
            features.get("throttle_ratio", 0.0),
            features.get("memory_ratio", 0.0),
            features.get("rbps", 0.0),
            features.get("wbps", 0.0),
            current_soft,
            hard_cap,
        ]
        predicted = float(self.model.predict([vector])[0])
        if predicted <= current_soft:
            return None
        return int(min(predicted, hard_cap))

