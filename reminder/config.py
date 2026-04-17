from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG: dict[str, Any] = {
    "universe": {
        "stocks": {
            "enabled": True,
            "min_market_cap": 10_000_000_000,
            "max_assets": 800,
        },
        "crypto": {
            "enabled": True,
            "min_market_cap": 1_000_000_000,
            "max_assets": 400,
        },
    },
    "providers": {
        "yahoo": {
            "timeout_sec": 20,
            "max_retries": 4,
            "backoff_sec": 1.5,
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        },
        "nasdaq": {
            "enabled": True,
            "timeout_sec": 20,
            "max_retries": 3,
            "backoff_sec": 1.0,
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        },
        "binance": {
            "enabled": True,
            "base_url": "https://api.binance.com",
            "market_cap_url": "https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list",
            "universe_quote_assets": ["USDT", "FDUSD", "USDC", "BUSD", "TUSD"],
            "universe_fallback_to_coingecko": False,
            "timeout_sec": 20,
            "max_retries": 4,
            "backoff_sec": 1.0,
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "filter_universe": True,
            "fallback_to_coingecko": False,
        },
    },
    "runtime": {
        "cache_dir": ".cache",
        "stocks_universe_cache_ttl_hours": 24,
    },
    "data": {
        "lookback_days": 120,
        "request_sleep_sec": 0.05,
    },
    "rules": [
        {
            "id": "ema15_down_touch_from_below",
            "enabled": True,
            "params": {
                "span": 15,
                "use_only_closed_candle": True,
            },
        },
        {
            "id": "weekly_gain_over_pct_stock",
            "enabled": True,
            "params": {
                "asset_type": "stock",
                "min_market_cap": 10_000_000_000,
                "lookback_days": 7,
                "min_gain_pct": 50.0,
                "use_only_closed_candle": True,
            },
        },
    ],
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: str | Path) -> dict[str, Any]:
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    raw = raw or {}
    if not isinstance(raw, dict):
        raise ValueError("Config root must be a mapping")

    return _deep_merge(DEFAULT_CONFIG, raw)
