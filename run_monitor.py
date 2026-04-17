#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from reminder.config import load_config
from reminder.engine import ReminderEngine


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Price reminder runner (for crontab)")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name("config.yaml")),
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional JSON output path for current run result",
    )
    return parser.parse_args()


def _beijing_date() -> str:
    """Return yesterday's date in Beijing time (UTC+8), as YYYY-MM-DD."""
    now_bj = datetime.now(timezone(timedelta(hours=8)))
    yesterday = now_bj - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def main() -> int:
    args = parse_args()

    try:
        config = load_config(args.config)
        result = ReminderEngine(config).run()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    scanned_by_type = result.get("scanned_by_type") or {}
    print(
        f"[SUMMARY] scanned={result['scanned_assets']} "
        f"stocks={scanned_by_type.get('stock', 0)} "
        f"crypto={scanned_by_type.get('crypto', 0)} "
        f"signals={len(result['signals'])} errors={len(result['errors'])}"
    )

    for signal in result["signals"]:
        context = signal.get("context") or {}
        event_date = context.get("bar_date") or context.get("end_date") or "n/a"
        print(
            "[ALERT] "
            f"{signal['asset_type']} {signal['symbol']} "
            f"rule={signal['rule_id']} "
            f"event_date={event_date}"
        )

    if result["errors"]:
        for err in result["errors"]:
            print(f"[WARN] {err}", file=sys.stderr)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── 飞书推送 ──────────────────────────────────────────────────
    feishu_cfg = config.get("feishu") or {}
    webhook_url = str(feishu_cfg.get("webhook_url") or "").strip()
    if webhook_url:
        try:
            from reminder.notifiers.feishu import send_feishu_alert
            run_date = _beijing_date()
            send_feishu_alert(
                webhook_url=webhook_url,
                run_date=run_date,
                signals=result["signals"],
                timeout=float(feishu_cfg.get("timeout_sec", 15)),
            )
            print(f"[FEISHU] 推送成功，日期={run_date}，信号数={len(result['signals'])}")
        except Exception as exc:
            print(f"[FEISHU][ERROR] 推送失败: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
