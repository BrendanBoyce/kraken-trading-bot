"""Explicit asset allowlist; membership does not imply Funded eligibility."""

import json
from pathlib import Path

DEFAULT_ASSETS = ("BTC/USD", "ETH/USD", "SOL/USD")


def normalize_symbol(value: str) -> str:
    parts = value.strip().upper().split("/")
    if len(parts) != 2 or not all(part.isalnum() for part in parts):
        raise ValueError(f"Expected BASE/QUOTE symbol, got {value!r}")
    aliases = {"XBT": "BTC", "XDG": "DOGE"}
    return "/".join(aliases.get(part, part) for part in parts)


def load_assets(path: Path | None = None) -> tuple[str, ...]:
    values = DEFAULT_ASSETS if path is None else json.loads(path.read_text())["assets"]
    if not isinstance(values, (list, tuple)) or not values:
        raise ValueError("assets must be a nonempty list")
    if not all(isinstance(value, str) for value in values):
        raise ValueError("assets must contain strings")
    assets = tuple(normalize_symbol(value) for value in values)
    if len(set(assets)) != len(assets):
        raise ValueError("Duplicate assets in allowlist")
    return assets
