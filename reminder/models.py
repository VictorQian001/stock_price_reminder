from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class AssetType(str, Enum):
    STOCK = "stock"
    CRYPTO = "crypto"


@dataclass(frozen=True)
class Asset:
    symbol: str
    name: str
    asset_type: AssetType
    market_cap: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuleSignal:
    rule_id: str
    symbol: str
    name: str
    asset_type: str
    market_cap: float
    message: str
    context: dict[str, Any] = field(default_factory=dict)
