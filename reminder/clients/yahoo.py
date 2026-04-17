from __future__ import annotations

from email.utils import parsedate_to_datetime
import time
from typing import Any

import pandas as pd
import requests

from reminder.models import Asset, AssetType


class YahooClient:
    SCREEN_URL = "https://query1.finance.yahoo.com/v1/finance/screener"
    CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

    def __init__(
        self,
        timeout: float = 20.0,
        max_retries: int = 4,
        backoff_sec: float = 1.5,
        user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    ) -> None:
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_sec = backoff_sec
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }
        )

    def get_large_cap_us_stocks(
        self,
        min_market_cap: float,
        max_assets: int = 400,
        batch_size: int = 250,
    ) -> list[Asset]:
        assets: list[Asset] = []
        seen: set[str] = set()
        offset = 0

        while len(assets) < max_assets:
            payload = {
                "offset": offset,
                "size": min(batch_size, max_assets - len(assets)),
                "sortType": "DESC",
                "sortField": "intradaymarketcap",
                "quoteType": "EQUITY",
                "query": {
                    "operator": "and",
                    "operands": [
                        {"operator": "gt", "operands": ["intradaymarketcap", min_market_cap]},
                        {"operator": "eq", "operands": ["region", "us"]},
                        {
                            "operator": "is-in",
                            "operands": ["exchange", ["NMS", "NYQ", "ASE"]],
                        },
                    ],
                },
            }

            body = self._request_json("POST", self.SCREEN_URL, json=payload)

            result = (body.get("finance", {}).get("result") or [None])[0] or {}
            quotes = result.get("quotes") or []
            if not quotes:
                break

            for quote in quotes:
                symbol = quote.get("symbol")
                if not symbol or symbol in seen:
                    continue

                market_cap = _extract_market_cap(quote)
                if market_cap is None or market_cap < min_market_cap:
                    continue

                assets.append(
                    Asset(
                        symbol=symbol,
                        name=quote.get("shortName") or quote.get("longName") or symbol,
                        asset_type=AssetType.STOCK,
                        market_cap=float(market_cap),
                    )
                )
                seen.add(symbol)

                if len(assets) >= max_assets:
                    break

            if len(quotes) < payload["size"]:
                break
            offset += payload["size"]

        return assets

    def fetch_daily_candles(self, symbol: str, lookback_days: int) -> pd.DataFrame:
        url = self.CHART_URL.format(symbol=symbol)
        params = {
            "interval": "1d",
            "range": _range_for_days(lookback_days),
            "includePrePost": "false",
            "events": "div,splits",
        }

        body = self._request_json("GET", url, params=params)

        result = (body.get("chart", {}).get("result") or [None])[0]
        if not result:
            return pd.DataFrame(columns=["open", "high", "low", "close"])

        timestamps = result.get("timestamp") or []
        quote = ((result.get("indicators") or {}).get("quote") or [None])[0] or {}

        if not timestamps:
            return pd.DataFrame(columns=["open", "high", "low", "close"])

        df = pd.DataFrame(
            {
                "ts": timestamps,
                "open": quote.get("open", []),
                "high": quote.get("high", []),
                "low": quote.get("low", []),
                "close": quote.get("close", []),
            }
        )

        df["date"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.date
        df = df.drop(columns=["ts"]).dropna(subset=["open", "high", "low", "close"])
        df = df.set_index(pd.to_datetime(df["date"]))
        df = df.drop(columns=["date"]).sort_index()

        return df.tail(lookback_days + 10)

    def _request_json(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        for attempt in range(self.max_retries + 1):
            response: requests.Response | None = None
            try:
                response = self.session.request(method, url, timeout=self.timeout, **kwargs)

                if response.status_code == 429:
                    if attempt >= self.max_retries:
                        response.raise_for_status()
                    time.sleep(_retry_sleep_seconds(response, self.backoff_sec, attempt))
                    continue

                if response.status_code in (500, 502, 503, 504):
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


def _retry_sleep_seconds(response: requests.Response, backoff_sec: float, attempt: int) -> float:
    retry_after = response.headers.get("Retry-After")
    parsed = _parse_retry_after(retry_after)
    if parsed is not None:
        return max(0.5, min(parsed, 120.0))
    return max(0.5, min(backoff_sec * (2**attempt), 120.0))


def _parse_retry_after(retry_after: str | None) -> float | None:
    if not retry_after:
        return None

    try:
        seconds = float(retry_after)
        return max(0.0, seconds)
    except ValueError:
        pass

    try:
        retry_dt = parsedate_to_datetime(retry_after)
        if retry_dt.tzinfo is None:
            return None
        return (retry_dt - pd.Timestamp.now(tz="UTC").to_pydatetime()).total_seconds()
    except (TypeError, ValueError, OverflowError):
        return None


def _extract_market_cap(quote: dict[str, Any]) -> float | None:
    candidates = [
        quote.get("marketCap"),
        quote.get("intradaymarketcap"),
        quote.get("intradayMarketCap"),
    ]

    for value in candidates:
        if value is None:
            continue
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, dict) and "raw" in value and isinstance(value["raw"], (int, float)):
            return float(value["raw"])

    return None


def _range_for_days(days: int) -> str:
    if days <= 30:
        return "3mo"
    if days <= 90:
        return "6mo"
    if days <= 180:
        return "1y"
    if days <= 365:
        return "2y"
    return "5y"
