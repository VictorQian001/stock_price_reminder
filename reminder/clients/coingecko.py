from __future__ import annotations

import pandas as pd
import requests

from reminder.models import Asset, AssetType


class CoinGeckoClient:
    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout
        self.session = requests.Session()

    def get_large_cap_coins(self, min_market_cap: float, max_assets: int = 200) -> list[Asset]:
        assets: list[Asset] = []
        page = 1
        per_page = 250

        while len(assets) < max_assets:
            url = f"{self.BASE_URL}/coins/markets"
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": per_page,
                "page": page,
                "sparkline": "false",
            }
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            if not data:
                break

            should_stop = False
            for row in data:
                market_cap = float(row.get("market_cap") or 0)
                if market_cap < min_market_cap:
                    should_stop = True
                    break

                symbol = str(row.get("symbol", "")).upper()
                coin_id = row.get("id")
                if not symbol or not coin_id:
                    continue

                assets.append(
                    Asset(
                        symbol=f"{symbol}-USD",
                        name=row.get("name") or symbol,
                        asset_type=AssetType.CRYPTO,
                        market_cap=market_cap,
                        metadata={"coingecko_id": coin_id, "raw_symbol": symbol},
                    )
                )

                if len(assets) >= max_assets:
                    break

            if should_stop or len(data) < per_page:
                break
            page += 1

        return assets

    def fetch_daily_ohlc(self, coin_id: str, lookback_days: int) -> pd.DataFrame:
        days = _coingecko_days(lookback_days + 30)
        url = f"{self.BASE_URL}/coins/{coin_id}/ohlc"
        params = {
            "vs_currency": "usd",
            "days": days,
        }

        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        rows = response.json()

        if not rows:
            return pd.DataFrame(columns=["open", "high", "low", "close"])

        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close"])
        df["date"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.date
        df = df.drop(columns=["ts"]).dropna(subset=["open", "high", "low", "close"])
        df = df.set_index(pd.to_datetime(df["date"]))
        df = df.drop(columns=["date"]).sort_index()

        return df.tail(lookback_days + 10)


def _coingecko_days(days: int) -> str:
    allowed = [1, 7, 14, 30, 90, 180, 365]
    for value in allowed:
        if days <= value:
            return str(value)
    return "max"
