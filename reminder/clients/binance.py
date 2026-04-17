from __future__ import annotations

import time
from typing import Any, Iterable

import pandas as pd
import requests

from reminder.models import Asset, AssetType


class BinanceClient:
    EXCHANGE_INFO_PATH = "/api/v3/exchangeInfo"
    KLINES_PATH = "/api/v3/klines"

    def __init__(
        self,
        base_url: str = "https://api.binance.com",
        market_cap_url: str = "https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list",
        timeout: float = 20.0,
        max_retries: int = 4,
        backoff_sec: float = 1.0,
        user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        preferred_quote_assets: Iterable[str] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.market_cap_url = market_cap_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_sec = backoff_sec
        self.preferred_quote_assets = tuple(
            x.strip().upper()
            for x in (preferred_quote_assets or ["USDT", "FDUSD", "USDC", "BUSD", "TUSD"])
            if str(x).strip()
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json,text/plain,*/*",
                "Connection": "keep-alive",
            }
        )
        self._symbols_cache: set[str] | None = None

    def get_large_cap_coins(self, min_market_cap: float, max_assets: int = 200) -> list[Asset]:
        body = self._request_json("GET", self.market_cap_url)
        rows = body.get("data") or []
        if not isinstance(rows, list):
            return []

        spot_symbols = self._get_spot_symbols()
        assets: list[Asset] = []
        seen: set[str] = set()

        for row in rows:
            if not isinstance(row, dict):
                continue

            market_cap = _to_float(row.get("marketCap"))
            if market_cap is None or market_cap < min_market_cap:
                continue

            base_asset = str(row.get("baseAsset") or row.get("mapperName") or "").upper().strip()
            quote_asset = str(row.get("quoteAsset") or "").upper().strip()
            listed_symbol = str(row.get("symbol") or "").upper().strip()
            if not base_asset:
                continue

            if self.preferred_quote_assets and quote_asset and quote_asset not in self.preferred_quote_assets:
                continue

            binance_symbol = ""
            if listed_symbol and listed_symbol in spot_symbols:
                binance_symbol = listed_symbol
            else:
                binance_symbol = self.resolve_spot_symbol(base_asset) or ""

            if not binance_symbol or binance_symbol in seen:
                continue

            name = str(row.get("fullName") or row.get("name") or base_asset).strip() or base_asset
            assets.append(
                Asset(
                    symbol=binance_symbol,
                    name=name,
                    asset_type=AssetType.CRYPTO,
                    market_cap=market_cap,
                    metadata={
                        "base_asset": base_asset,
                        "quote_asset": quote_asset,
                        "binance_symbol": binance_symbol,
                        "market_cap_source": "binance",
                    },
                )
            )
            seen.add(binance_symbol)

        assets.sort(key=lambda x: x.market_cap, reverse=True)
        return assets[:max_assets]

    def resolve_spot_symbol(self, base_asset: str) -> str | None:
        if not base_asset:
            return None

        symbols = self._get_spot_symbols()
        base = base_asset.strip().upper()
        for quote in self.preferred_quote_assets:
            candidate = f"{base}{quote}"
            if candidate in symbols:
                return candidate
        return None

    def fetch_daily_klines(self, symbol: str, lookback_days: int) -> pd.DataFrame:
        limit = max(60, min(1000, lookback_days + 40))
        params = {
            "symbol": symbol,
            "interval": "1d",
            "limit": str(limit),
        }
        rows = self._request_json("GET", self.KLINES_PATH, params=params)
        if not isinstance(rows, list) or not rows:
            return pd.DataFrame(columns=["open", "high", "low", "close"])

        parsed_rows = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 5:
                continue
            parsed_rows.append(
                {
                    "ts": row[0],
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                }
            )

        if not parsed_rows:
            return pd.DataFrame(columns=["open", "high", "low", "close"])

        df = pd.DataFrame(parsed_rows)
        df["date"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.date
        df = df.drop(columns=["ts"]).dropna(subset=["open", "high", "low", "close"])
        df = df.set_index(pd.to_datetime(df["date"]))
        df = df.drop(columns=["date"]).sort_index()
        return df.tail(lookback_days + 10)

    def _get_spot_symbols(self) -> set[str]:
        if self._symbols_cache is not None:
            return self._symbols_cache

        body = self._request_json("GET", self.EXCHANGE_INFO_PATH)
        rows = body.get("symbols") or []

        symbols: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("status") != "TRADING":
                continue
            if not bool(row.get("isSpotTradingAllowed", True)):
                continue
            symbol = str(row.get("symbol") or "").upper()
            if not symbol:
                continue
            symbols.add(symbol)

        self._symbols_cache = symbols
        return symbols

    def _request_json(self, method: str, path_or_url: str, **kwargs: Any) -> Any:
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}{path_or_url}"

        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.request(method, url, timeout=self.timeout, **kwargs)

                if response.status_code in (418, 429, 500, 502, 503, 504):
                    if attempt >= self.max_retries:
                        response.raise_for_status()
                    time.sleep(self.backoff_sec * (2**attempt))
                    continue

                response.raise_for_status()
                return response.json()
            except requests.RequestException:
                if attempt >= self.max_retries:
                    raise
                time.sleep(self.backoff_sec * (2**attempt))

        raise RuntimeError("Unexpected retry loop state")


def _to_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None
