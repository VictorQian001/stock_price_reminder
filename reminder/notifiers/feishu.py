from __future__ import annotations

import json
from typing import Any

import requests


# Rule label mapping: rule_id -> display label
_RULE_LABELS: dict[str, str] = {
    "gain_over_pct_1d": "1日涨幅 >5%",
    "gain_over_pct_7d": "1周涨幅 >20%",
    "gain_over_pct_30d": "1月涨幅 >30%",
    "gain_over_pct_90d": "3月涨幅 >50%",
}

_RULE_ORDER = [
    "gain_over_pct_1d",
    "gain_over_pct_7d",
    "gain_over_pct_30d",
    "gain_over_pct_90d",
]

# 美股 sector 英文 -> 中文
_SECTOR_ZH: dict[str, str] = {
    "Technology": "科技",
    "Healthcare": "医疗",
    "Health Care": "医疗",
    "Financials": "金融",
    "Financial Services": "金融",
    "Consumer Discretionary": "可选消费",
    "Consumer Cyclical": "可选消费",
    "Consumer Staples": "必需消费",
    "Consumer Defensive": "必需消费",
    "Communication Services": "通信",
    "Industrials": "工业",
    "Energy": "能源",
    "Materials": "材料",
    "Real Estate": "房地产",
    "Utilities": "公用事业",
}

# Crypto 主流币兜底分类（Binance tags 为空时使用）
_CRYPTO_CATEGORY: dict[str, str] = {
    "BTC": "Layer 1", "ETH": "Layer 1", "BNB": "Layer 1",
    "SOL": "Layer 1", "ADA": "Layer 1", "AVAX": "Layer 1",
    "TRX": "Layer 1", "TON": "Layer 1", "DOT": "Layer 1",
    "ATOM": "Layer 1", "NEAR": "Layer 1", "APT": "Layer 1",
    "SUI": "Layer 1", "ICP": "Layer 1", "HBAR": "Layer 1",
    "XRP": "Layer 1", "LTC": "Layer 1", "BCH": "Layer 1",
    "ALGO": "Layer 1", "FTM": "Layer 1", "ONE": "Layer 1",
    "MATIC": "Layer 2", "POL": "Layer 2", "ARB": "Layer 2",
    "OP": "Layer 2", "STRK": "Layer 2", "ZK": "Layer 2",
    "IMX": "Layer 2", "MNT": "Layer 2",
    "UNI": "DeFi", "AAVE": "DeFi", "MKR": "DeFi",
    "CRV": "DeFi", "SNX": "DeFi", "COMP": "DeFi",
    "LDO": "DeFi", "RUNE": "DeFi", "BAL": "DeFi",
    "SUSHI": "DeFi", "1INCH": "DeFi", "JUP": "DeFi",
    "LINK": "Oracle", "BAND": "Oracle", "API3": "Oracle",
    "FIL": "存储", "AR": "存储", "GRT": "数据",
    "FET": "AI", "AGIX": "AI", "RENDER": "AI",
    "WLD": "AI", "TAO": "AI",
    "DOGE": "Meme", "SHIB": "Meme", "PEPE": "Meme",
    "BONK": "Meme", "WIF": "Meme", "FLOKI": "Meme",
    "AXS": "GameFi", "SAND": "GameFi", "MANA": "GameFi",
    "GALA": "GameFi", "ENJ": "GameFi",
    "USDT": "稳定币", "USDC": "稳定币", "DAI": "稳定币",
    "FDUSD": "稳定币", "TUSD": "稳定币",
    "XMR": "隐私", "ZEC": "隐私", "SCRT": "隐私",
}

# Binance tags 关键词 -> 中文分类（优先级从高到低）
_TAG_KEYWORDS: list[tuple[str, str]] = [
    ("layer-2", "Layer 2"),
    ("layer-1", "Layer 1"),
    ("defi", "DeFi"),
    ("ai", "AI"),
    ("meme", "Meme"),
    ("gamefi", "GameFi"),
    ("nft", "GameFi"),
    ("oracle", "Oracle"),
    ("storage", "存储"),
    ("stablecoin", "稳定币"),
    ("privacy", "隐私"),
    ("exchange", "交易所"),
]


def _stock_sector(metadata: dict[str, Any]) -> str:
    raw = str(metadata.get("sector") or "").strip()
    return _SECTOR_ZH.get(raw, raw) if raw else "其他"


def _crypto_category(metadata: dict[str, Any]) -> str:
    # 优先从 Binance tags 推断
    tags: list[str] = metadata.get("tags") or []
    tags_lower = [t.lower() for t in tags]
    for keyword, label in _TAG_KEYWORDS:
        if any(keyword in t for t in tags_lower):
            return label

    # 兜底：按 base_asset 查硬编码表
    base = str(metadata.get("base_asset") or "").upper().strip()
    if base in _CRYPTO_CATEGORY:
        return _CRYPTO_CATEGORY[base]

    return "加密货币"


def _category_label(sig: dict[str, Any]) -> str:
    metadata = sig.get("context", {}).get("metadata") or sig.get("metadata") or {}
    if sig.get("asset_type") == "stock":
        return _stock_sector(metadata)
    return _crypto_category(metadata)


def _build_card(run_date: str, signals: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a Feishu interactive card payload."""

    by_rule: dict[str, list[dict[str, Any]]] = {}
    for sig in signals:
        rid = sig.get("rule_id", "")
        by_rule.setdefault(rid, []).append(sig)

    has_signals = bool(signals)
    total = len(signals)
    elements: list[dict[str, Any]] = []

    if not has_signals:
        elements.append({
            "tag": "div",
            "text": {"tag": "plain_text", "content": "今日无符合条件的标的。"},
        })
    else:
        for rule_id in _RULE_ORDER:
            group = by_rule.get(rule_id, [])
            if not group:
                continue

            label = _RULE_LABELS.get(rule_id, rule_id)
            group_sorted = sorted(
                group,
                key=lambda s: float(s.get("context", {}).get("gain_pct", 0)),
                reverse=True,
            )

            lines = []
            for sig in group_sorted:
                ctx = sig.get("context", {})
                gain_pct = ctx.get("gain_pct", 0)
                atype = "美股" if sig.get("asset_type") == "stock" else "加密"
                category = _category_label(sig)
                symbol = sig.get("symbol", "")
                name = sig.get("name") or symbol
                lines.append(
                    f"• [{atype}·{category}] {symbol} {name}  +{gain_pct:.1f}%"
                )

            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**{label}** ({len(group)}只)\n" + "\n".join(lines),
                },
            })
            elements.append({"tag": "hr"})

        if elements and elements[-1].get("tag") == "hr":
            elements.pop()

    card: dict[str, Any] = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {
                "tag": "plain_text",
                "content": f"价格异动提醒  {run_date}",
            },
            "template": "blue" if has_signals else "grey",
        },
        "elements": elements + [
            {"tag": "hr"},
            {
                "tag": "note",
                "elements": [
                    {
                        "tag": "plain_text",
                        "content": (
                            f"共 {total} 个信号 | "
                            "美股市值≥200亿 / 加密市值≥100亿 | "
                            "数据截至前一交易日收盘"
                        ),
                    }
                ],
            },
        ],
    }
    return {"msg_type": "interactive", "card": json.dumps(card, ensure_ascii=False)}


def send_feishu_alert(
    webhook_url: str,
    run_date: str,
    signals: list[dict[str, Any]],
    timeout: float = 15.0,
) -> None:
    """Send a Feishu card message via webhook."""
    payload = _build_card(run_date, signals)
    resp = requests.post(
        webhook_url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("code", 0) != 0:
        raise RuntimeError(f"Feishu webhook error: {body}")
