from __future__ import annotations

import json
from typing import Any

import requests


# Rule label mapping: rule_id prefix -> display label
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


def _format_market_cap(mc: float) -> str:
    if mc >= 1_000_000_000_000:
        return f"{mc / 1_000_000_000_000:.2f}T"
    if mc >= 1_000_000_000:
        return f"{mc / 1_000_000_000:.1f}B"
    if mc >= 1_000_000:
        return f"{mc / 1_000_000:.0f}M"
    return str(mc)


def _asset_type_label(asset_type: str) -> str:
    return "美股" if asset_type == "stock" else "加密"


def _build_card(run_date: str, signals: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a Feishu interactive card payload."""

    # Group signals by rule_id
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
            "text": {
                "tag": "plain_text",
                "content": "今日无符合条件的标的。",
            },
        })
    else:
        for rule_id in _RULE_ORDER:
            group = by_rule.get(rule_id, [])
            if not group:
                continue

            label = _RULE_LABELS.get(rule_id, rule_id)
            # sort by gain_pct desc
            group_sorted = sorted(
                group,
                key=lambda s: float(s.get("context", {}).get("gain_pct", 0)),
                reverse=True,
            )

            lines = []
            for sig in group_sorted:
                ctx = sig.get("context", {})
                gain_pct = ctx.get("gain_pct", 0)
                mc = _format_market_cap(float(sig.get("market_cap") or 0))
                atype = _asset_type_label(sig.get("asset_type", ""))
                name = sig.get("name") or sig.get("symbol", "")
                symbol = sig.get("symbol", "")
                lines.append(
                    f"• [{atype}] {symbol} {name}  +{gain_pct:.1f}%  市值{mc}"
                )

            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**{label}** ({len(group)}只)\n" + "\n".join(lines),
                },
            })
            elements.append({"tag": "hr"})

        # remove trailing hr
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
    """Send a Feishu card message via webhook.

    Raises requests.HTTPError on non-2xx response.
    """
    payload = _build_card(run_date, signals)
    resp = requests.post(
        webhook_url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    resp.raise_for_status()
    body = resp.json()
    # Feishu returns {"code": 0, ...} on success
    if body.get("code", 0) != 0:
        raise RuntimeError(f"Feishu webhook error: {body}")
