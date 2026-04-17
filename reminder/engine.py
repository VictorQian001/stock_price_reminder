from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any

import pandas as pd

from reminder.clients.binance import BinanceClient
from reminder.clients.coingecko import CoinGeckoClient
from reminder.clients.nasdaq import NasdaqClient
from reminder.clients.yahoo import YahooClient
from reminder.models import Asset, AssetType
from reminder.rules.factory import build_rules


class ReminderEngine:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

        yahoo_cfg = (self.config.get("providers", {}).get("yahoo", {}) or {})
        self.yahoo = YahooClient(
            timeout=float(yahoo_cfg.get("timeout_sec", 20.0)),
            max_retries=int(yahoo_cfg.get("max_retries", 4)),
            backoff_sec=float(yahoo_cfg.get("backoff_sec", 1.5)),
            user_agent=str(
                yahoo_cfg.get(
                    "user_agent",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                )
            ),
        )

        nasdaq_cfg = (self.config.get("providers", {}).get("nasdaq", {}) or {})
        self.nasdaq: NasdaqClient | None = None
        if bool(nasdaq_cfg.get("enabled", True)):
            self.nasdaq = NasdaqClient(
                timeout=float(nasdaq_cfg.get("timeout_sec", 20.0)),
                max_retries=int(nasdaq_cfg.get("max_retries", 3)),
                backoff_sec=float(nasdaq_cfg.get("backoff_sec", 1.0)),
                user_agent=str(
                    nasdaq_cfg.get(
                        "user_agent",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                    )
                ),
            )

        binance_cfg = (self.config.get("providers", {}).get("binance", {}) or {})
        self.binance: BinanceClient | None = None
        if bool(binance_cfg.get("enabled", True)):
            self.binance = BinanceClient(
                base_url=str(binance_cfg.get("base_url", "https://api.binance.com")),
                market_cap_url=str(
                    binance_cfg.get(
                        "market_cap_url",
                        "https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list",
                    )
                ),
                timeout=float(binance_cfg.get("timeout_sec", 20.0)),
                max_retries=int(binance_cfg.get("max_retries", 4)),
                backoff_sec=float(binance_cfg.get("backoff_sec", 1.0)),
                user_agent=str(
                    binance_cfg.get(
                        "user_agent",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                    )
                ),
                preferred_quote_assets=binance_cfg.get(
                    "universe_quote_assets", ["USDT", "FDUSD", "USDC", "BUSD", "TUSD"]
                ),
            )

        self.binance_filter_universe = bool(binance_cfg.get("filter_universe", True))
        self.crypto_fallback_to_coingecko = bool(binance_cfg.get("fallback_to_coingecko", False))
        self.crypto_universe_fallback_to_coingecko = bool(
            binance_cfg.get("universe_fallback_to_coingecko", False)
        )

        self.coingecko = CoinGeckoClient()

        runtime_cfg = (self.config.get("runtime", {}) or {})
        self.cache_dir = Path(str(runtime_cfg.get("cache_dir", ".cache")))
        if not self.cache_dir.is_absolute():
            self.cache_dir = Path.cwd() / self.cache_dir
        self.stocks_universe_cache_ttl_hours = float(
            runtime_cfg.get("stocks_universe_cache_ttl_hours", 24.0)
        )

        self.rules = build_rules(config.get("rules") or [])
        if not self.rules:
            raise ValueError("No enabled rules found in config")

    def run(self) -> dict[str, Any]:
        universe, bootstrap_errors = self._load_universe()

        lookback_days = int(self.config.get("data", {}).get("lookback_days", 120))
        sleep_sec = float(self.config.get("data", {}).get("request_sleep_sec", 0.0))

        scanned_by_type = {"stock": 0, "crypto": 0}
        for asset in universe:
            scanned_by_type[asset.asset_type.value] = scanned_by_type.get(asset.asset_type.value, 0) + 1

        signals: list[dict[str, Any]] = []
        errors: list[str] = list(bootstrap_errors)

        for asset in universe:
            try:
                candles = self._load_candles(asset, lookback_days)
                if candles.empty:
                    continue

                for rule in self.rules:
                    signal = rule.evaluate(asset, candles)
                    if signal:
                        signals.append(asdict(signal))
            except Exception as exc:
                errors.append(f"{asset.symbol}: {exc}")

            if sleep_sec > 0:
                time.sleep(sleep_sec)

        return {
            "run_at": datetime.now(timezone.utc).isoformat(),
            "scanned_assets": len(universe),
            "scanned_by_type": scanned_by_type,
            "signals": signals,
            "errors": errors,
        }

    def _load_universe(self) -> tuple[list[Asset], list[str]]:
        universe: list[Asset] = []
        errors: list[str] = []

        stocks_cfg = self.config.get("universe", {}).get("stocks", {})
        crypto_cfg = self.config.get("universe", {}).get("crypto", {})

        if stocks_cfg.get("enabled", True):
            min_market_cap = float(stocks_cfg.get("min_market_cap", 20_000_000_000))
            max_assets = int(stocks_cfg.get("max_assets", 400))
            stocks: list[Asset] = []
            yahoo_exc: Exception | None = None
            nasdaq_exc: Exception | None = None

            try:
                stocks = self.yahoo.get_large_cap_us_stocks(
                    min_market_cap=min_market_cap,
                    max_assets=max_assets,
                )
                if not stocks:
                    raise ValueError("Yahoo screener returned empty stock universe")
                self._save_stocks_cache(stocks)
            except Exception as exc:
                yahoo_exc = exc

            if not stocks and self.nasdaq is not None:
                try:
                    stocks = self.nasdaq.get_large_cap_us_stocks(
                        min_market_cap=min_market_cap,
                        max_assets=max_assets,
                    )
                    if not stocks:
                        raise ValueError("Nasdaq screener returned empty stock universe")
                    self._save_stocks_cache(stocks)
                    errors.append(
                        "stocks universe loaded from Nasdaq fallback after Yahoo failure: "
                        f"{_exc_desc(yahoo_exc)}"
                    )
                except Exception as exc:
                    nasdaq_exc = exc

            if not stocks:
                stocks = self._load_stocks_cache(min_market_cap=min_market_cap, max_assets=max_assets)
                if stocks:
                    errors.append(
                        "stocks universe loaded from cache after provider failure: "
                        f"yahoo={_exc_desc(yahoo_exc)}; nasdaq={_exc_desc(nasdaq_exc)}"
                    )
                else:
                    errors.append(
                        "stocks universe unavailable (provider failed and cache missing/expired): "
                        f"yahoo={_exc_desc(yahoo_exc)}; nasdaq={_exc_desc(nasdaq_exc)}"
                    )

            universe.extend(stocks)

        if crypto_cfg.get("enabled", True):
            min_market_cap = float(crypto_cfg.get("min_market_cap", 1_000_000_000))
            max_assets = int(crypto_cfg.get("max_assets", 200))

            cryptos: list[Asset] = []
            binance_exc: Exception | None = None
            coingecko_exc: Exception | None = None

            if self.binance is not None:
                try:
                    cryptos = self.binance.get_large_cap_coins(
                        min_market_cap=min_market_cap,
                        max_assets=max_assets,
                    )
                    if not cryptos:
                        raise ValueError("Binance market-cap universe returned empty result")
                except Exception as exc:
                    binance_exc = exc

            if not cryptos and self.crypto_universe_fallback_to_coingecko:
                try:
                    cryptos = self.coingecko.get_large_cap_coins(
                        min_market_cap=min_market_cap,
                        max_assets=max_assets,
                    )
                    if self.binance is not None:
                        cryptos, dropped = self._bind_crypto_to_binance(cryptos)
                        if dropped > 0:
                            errors.append(
                                "crypto universe filtered by Binance symbols after CoinGecko fallback: "
                                f"kept={len(cryptos)} dropped={dropped}"
                            )
                    if cryptos:
                        errors.append(
                            "crypto universe loaded from CoinGecko fallback after Binance failure: "
                            f"{_exc_desc(binance_exc)}"
                        )
                except Exception as exc:
                    coingecko_exc = exc

            if cryptos:
                universe.extend(cryptos)
            else:
                errors.append(
                    "crypto universe unavailable: "
                    f"binance={_exc_desc(binance_exc)}; coingecko={_exc_desc(coingecko_exc)}"
                )

        return universe, errors

    def _load_candles(self, asset: Asset, lookback_days: int) -> pd.DataFrame:
        if asset.asset_type == AssetType.STOCK:
            return self.yahoo.fetch_daily_candles(asset.symbol, lookback_days)

        if self.binance is not None:
            binance_symbol = str(asset.metadata.get("binance_symbol") or asset.symbol or "")
            if binance_symbol:
                try:
                    return self.binance.fetch_daily_klines(binance_symbol, lookback_days)
                except Exception as binance_exc:
                    if not self.crypto_fallback_to_coingecko:
                        raise

                    coin_id = asset.metadata.get("coingecko_id")
                    if not coin_id:
                        raise ValueError("Missing coingecko_id in crypto metadata") from binance_exc
                    return self.coingecko.fetch_daily_ohlc(str(coin_id), lookback_days)

        if self.crypto_fallback_to_coingecko:
            coin_id = asset.metadata.get("coingecko_id")
            if not coin_id:
                raise ValueError("Missing coingecko_id in crypto metadata")
            return self.coingecko.fetch_daily_ohlc(str(coin_id), lookback_days)

        raise ValueError("Binance is disabled and crypto fallback is also disabled")

    def _bind_crypto_to_binance(self, assets: list[Asset]) -> tuple[list[Asset], int]:
        if self.binance is None:
            return assets, 0

        bound_assets: list[Asset] = []
        dropped = 0

        for asset in assets:
            symbol_hint = self._crypto_symbol_hint(asset)
            binance_symbol = self.binance.resolve_spot_symbol(symbol_hint)
            if not binance_symbol:
                dropped += 1
                if not self.binance_filter_universe:
                    bound_assets.append(asset)
                continue

            metadata = dict(asset.metadata)
            metadata["binance_symbol"] = binance_symbol
            metadata["market_cap_source"] = metadata.get("market_cap_source") or "coingecko"
            bound_assets.append(
                Asset(
                    symbol=binance_symbol,
                    name=asset.name,
                    asset_type=asset.asset_type,
                    market_cap=asset.market_cap,
                    metadata=metadata,
                )
            )

        return bound_assets, dropped

    def _crypto_symbol_hint(self, asset: Asset) -> str:
        raw = asset.metadata.get("raw_symbol") or asset.metadata.get("base_asset")
        if raw:
            return str(raw).upper()

        symbol = str(asset.symbol or "").upper()
        for suffix in ("-USD", "-USDT", "USD", "USDT"):
            if symbol.endswith(suffix):
                return symbol[: -len(suffix)]
        return symbol

    def _stocks_cache_path(self) -> Path:
        return self.cache_dir / "stocks_universe.json"

    def _save_stocks_cache(self, assets: list[Asset]) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "assets": [
                {
                    "symbol": a.symbol,
                    "name": a.name,
                    "market_cap": a.market_cap,
                    "metadata": a.metadata,
                }
                for a in assets
                if a.asset_type == AssetType.STOCK
            ],
        }
        self._stocks_cache_path().write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_stocks_cache(self, min_market_cap: float, max_assets: int) -> list[Asset]:
        path = self._stocks_cache_path()
        if not path.exists():
            return []

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []

        saved_at_raw = payload.get("saved_at")
        try:
            saved_at = datetime.fromisoformat(str(saved_at_raw).replace("Z", "+00:00"))
        except ValueError:
            return []

        if saved_at.tzinfo is None:
            saved_at = saved_at.replace(tzinfo=timezone.utc)

        age_hours = (datetime.now(timezone.utc) - saved_at).total_seconds() / 3600.0
        if age_hours > self.stocks_universe_cache_ttl_hours:
            return []

        rows = payload.get("assets") or []
        assets: list[Asset] = []
        for row in rows:
            try:
                market_cap = float(row.get("market_cap") or 0)
            except (TypeError, ValueError):
                continue

            if market_cap < min_market_cap:
                continue

            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                continue

            assets.append(
                Asset(
                    symbol=symbol,
                    name=str(row.get("name") or symbol),
                    asset_type=AssetType.STOCK,
                    market_cap=market_cap,
                    metadata=row.get("metadata") or {},
                )
            )

            if len(assets) >= max_assets:
                break

        return assets


def _exc_desc(exc: Exception | None) -> str:
    if exc is None:
        return "n/a"
    return f"{type(exc).__name__}: {exc}"
