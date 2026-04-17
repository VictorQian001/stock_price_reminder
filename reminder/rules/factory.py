from __future__ import annotations

from typing import Any

from reminder.rules.base import Rule
from reminder.rules.ema15_down_touch import EmaDownTouchFromBelowRule
from reminder.rules.weekly_gain import WeeklyGainOverPctRule


def build_rules(rule_configs: list[dict[str, Any]]) -> list[Rule]:
    rules: list[Rule] = []

    for item in rule_configs:
        if not item.get("enabled", True):
            continue

        rule_id = str(item.get("id") or "").strip()
        params = item.get("params") or {}

        if rule_id == "ema15_down_touch_from_below":
            rules.append(
                EmaDownTouchFromBelowRule(
                    span=int(params.get("span", 15)),
                    use_only_closed_candle=bool(params.get("use_only_closed_candle", True)),
                )
            )
            continue

        if rule_id.startswith("weekly_gain_over_pct") or rule_id.startswith("gain_over_pct"):
            asset_type = params.get("asset_type")
            rules.append(
                WeeklyGainOverPctRule(
                    rule_id=rule_id,
                    asset_type=str(asset_type).lower() if asset_type else None,
                    min_market_cap=float(params.get("min_market_cap", 0)),
                    lookback_days=int(params.get("lookback_days", 7)),
                    min_gain_pct=float(params.get("min_gain_pct", 5.0)),
                    use_only_closed_candle=bool(params.get("use_only_closed_candle", True)),
                )
            )

    return rules
