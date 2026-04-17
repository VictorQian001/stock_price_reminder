from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from reminder.models import Asset, RuleSignal


class Rule(ABC):
    rule_id: str

    @abstractmethod
    def evaluate(self, asset: Asset, candles: pd.DataFrame) -> RuleSignal | None:
        raise NotImplementedError
