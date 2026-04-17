from __future__ import annotations

import pandas as pd

from reminder.models import Asset, RuleSignal
from reminder.rules.base import Rule


class EmaDownTouchFromBelowRule(Rule):
    rule_id = "ema15_down_touch_from_below"

    def __init__(self, span: int = 15, use_only_closed_candle: bool = True) -> None:
        if span <= 1:
            raise ValueError("span must be > 1")
        self.span = span
        self.use_only_closed_candle = use_only_closed_candle

    def evaluate(self, asset: Asset, candles: pd.DataFrame) -> RuleSignal | None:
        required = self.span + 3
        if candles is None or len(candles) < required:
            return None

        work = candles.iloc[:-1].copy() if self.use_only_closed_candle else candles.copy()
        if len(work) < self.span + 2:
            return None

        ema = work["close"].ewm(span=self.span, adjust=False).mean()

        prev_i = len(work) - 2
        curr_i = len(work) - 1

        ema_prev = float(ema.iloc[prev_i])
        ema_curr = float(ema.iloc[curr_i])
        close_prev = float(work["close"].iloc[prev_i])
        open_curr = float(work["open"].iloc[curr_i])
        high_curr = float(work["high"].iloc[curr_i])
        close_curr = float(work["close"].iloc[curr_i])

        ema_down = ema_curr < ema_prev
        from_below = close_prev < ema_prev and open_curr < ema_curr
        touched_up = high_curr >= ema_curr

        if not (ema_down and from_below and touched_up):
            return None

        bar_date = str(work.index[curr_i].date())
        return RuleSignal(
            rule_id=self.rule_id,
            symbol=asset.symbol,
            name=asset.name,
            asset_type=asset.asset_type.value,
            market_cap=asset.market_cap,
            message=(
                f"EMA{self.span} is falling and price touched EMA from below "
                f"on {bar_date}"
            ),
            context={
                "bar_date": bar_date,
                "ema_prev": round(ema_prev, 6),
                "ema_curr": round(ema_curr, 6),
                "close_prev": round(close_prev, 6),
                "open_curr": round(open_curr, 6),
                "high_curr": round(high_curr, 6),
                "close_curr": round(close_curr, 6),
            },
        )
