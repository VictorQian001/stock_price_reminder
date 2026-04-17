from __future__ import annotations

from datetime import timedelta

import pandas as pd

from reminder.models import Asset, RuleSignal
from reminder.rules.base import Rule


class WeeklyGainOverPctRule(Rule):
    def __init__(
        self,
        rule_id: str,
        asset_type: str | None = None,
        min_market_cap: float = 0,
        lookback_days: int = 7,
        min_gain_pct: float = 50.0,
        use_only_closed_candle: bool = True,
    ) -> None:
        if lookback_days <= 0:
            raise ValueError("lookback_days must be > 0")
        self.rule_id = rule_id
        self.asset_type = asset_type.lower() if asset_type else None
        self.min_market_cap = float(min_market_cap)
        self.lookback_days = int(lookback_days)
        self.min_gain_pct = float(min_gain_pct)
        self.use_only_closed_candle = use_only_closed_candle

    def evaluate(self, asset: Asset, candles: pd.DataFrame) -> RuleSignal | None:
        if self.asset_type and asset.asset_type.value != self.asset_type:
            return None
        if asset.market_cap < self.min_market_cap:
            return None

        if candles is None or len(candles) < 3:
            return None

        work = candles.iloc[:-1].copy() if self.use_only_closed_candle else candles.copy()
        if len(work) < 2:
            return None

        work = work.sort_index()
        end_ts = pd.Timestamp(work.index[-1])
        start_bound = end_ts - timedelta(days=self.lookback_days)

        baseline_df = work.loc[work.index <= start_bound]
        if baseline_df.empty:
            return None

        start_close = float(baseline_df["close"].iloc[-1])
        end_close = float(work["close"].iloc[-1])
        if start_close <= 0:
            return None

        gain_pct = (end_close / start_close - 1.0) * 100.0
        if gain_pct < self.min_gain_pct:
            return None

        start_date = str(pd.Timestamp(baseline_df.index[-1]).date())
        end_date = str(end_ts.date())
        return RuleSignal(
            rule_id=self.rule_id,
            symbol=asset.symbol,
            name=asset.name,
            asset_type=asset.asset_type.value,
            market_cap=asset.market_cap,
            message=(
                f"Price gained {gain_pct:.2f}% in past {self.lookback_days} days "
                f"({start_date} -> {end_date})"
            ),
            context={
                "start_date": start_date,
                "end_date": end_date,
                "lookback_days": self.lookback_days,
                "start_close": round(start_close, 6),
                "end_close": round(end_close, 6),
                "gain_pct": round(gain_pct, 4),
                "min_gain_pct": self.min_gain_pct,
                "min_market_cap": self.min_market_cap,
            },
        )
