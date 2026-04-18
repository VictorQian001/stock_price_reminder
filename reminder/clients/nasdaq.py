from __future__ import annotations

import re
import time
from typing import Any

import requests

from reminder.models import Asset, AssetType


class NasdaqClient:
    SCREEN_URL = "https://api.nasdaq.com/api/screener/stocks"

    def __init__(
        self,
        timeout: float = 20.0,
        max_retries: int = 3,
        backoff_sec: float = 1.0,
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
                "Origin": "https://www.nasdaq.com",
                "Referer": "https://www.nasdaq.com/",
            }
        )

    def get_large_cap_us_stocks(
        self,
        min_market_cap: float,
        max_assets: int = 400,
        page_size: int = 5000,
    ) -> list[Asset]:
        params = {
            "tableonly": "true",
            "limit": str(max(500, page_size)),
            "offset": "0",
            "download": "true",
        }

        body = self._request_json(self.SCREEN_URL, params=params)
        rows = ((body.get("data") or {}).get("rows") or [])

        assets: list[Asset] = []
        seen: set[str] = set()

        for row in rows:
            raw_symbol = str(row.get("symbol") or "").strip().upper()
            if not raw_symbol:
                continue

            # Yahoo chart symbol compatibility for names like BRK.B -> BRK-B
            symbol = raw_symbol.replace(".", "-")
            if symbol in seen:
                continue

            if "^" in symbol or "/" in symbol:
                continue

            market_cap = _parse_market_cap(row.get("marketCap"))
            if market_cap is None or market_cap < min_market_cap:
                continue

            country = str(row.get("country") or "").strip().lower()
            if country and "united states" not in country and country not in {"usa", "us"}:
                continue

            sector = str(row.get("sector") or "").strip() or None
            industry = str(row.get("industry") or "").strip() or None
            assets.append(
                Asset(
                    symbol=symbol,
                    name=str(row.get("name") or symbol),
                    asset_type=AssetType.STOCK,
                    market_cap=market_cap,
                    metadata={
                        "source": "nasdaq",
                        "raw_symbol": raw_symbol,
                        "sector": sector,
                        "industry": industry,
                    },
                )
            )
            seen.add(symbol)

        assets.sort(key=lambda a: a.market_cap, reverse=True)
        return assets[:max_assets]

    def _request_json(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)

                if response.status_code in (429, 500, 502, 503, 504):
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


def _parse_market_cap(value: Any) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric if numeric > 0 else None

    if not isinstance(value, str):
        return None

    s = value.strip().upper().replace("$", "").replace(",", "")
    if not s or s in {"N/A", "NA", "--"}:
        return None

    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*([KMBT]?)", s)
    if not match:
        return None

    number = float(match.group(1))
    suffix = match.group(2)
    multiplier = {
        "": 1.0,
        "K": 1_000.0,
        "M": 1_000_000.0,
        "B": 1_000_000_000.0,
        "T": 1_000_000_000_000.0,
    }[suffix]
    return number * multiplier
