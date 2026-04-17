# price_reminder

定时扫描「美股 + 加密货币」价格异动，北京时间 08:00 通过飞书推送提醒。

## 监控规则

| 规则 | 时间窗口 | 阈值 |
|------|---------|------|
| `gain_over_pct_1d`  | 前1日   | 涨幅 ≥ 5%   |
| `gain_over_pct_7d`  | 前7日   | 涨幅 ≥ 30%  |
| `gain_over_pct_30d` | 前30日  | 涨幅 ≥ 50%  |
| `gain_over_pct_90d` | 前90日  | 涨幅 ≥ 100% |

## 标的池

- **美股**：市值 ≥ 500亿 USD（`min_market_cap: 50000000000`）
- **加密**：市值 ≥ 100亿 USD（`min_market_cap: 10000000000`）

所有规则对两类资产均生效，市值过滤在 universe 层完成，规则层无需重复过滤。

## 数据源

- 美股 universe：Yahoo screener（失败时回退 Nasdaq，再回退本地缓存）
- 美股日线：Yahoo chart
- 加密 universe（市值筛选）：Binance `marketing/symbol/list`
- 加密日线：Binance Spot `1d` Kline

## 安装

```bash
cd /path/to/price_reminder
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.example.yaml config.yaml
```

## 配置飞书 Webhook

1. 在飞书目标群 → 设置 → 机器人 → 添加「自定义机器人」
2. 复制生成的 Webhook URL
3. 填入 `config.yaml`：

```yaml
feishu:
  webhook_url: "https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxxxx"
```

留空则不发送，仅输出到终端/日志。

## 运行

```bash
python run_monitor.py --config config.yaml --output ./out/latest_alerts.json
```

## crontab（北京时间 08:00 执行）

```cron
# 北京时间 08:00 = UTC 00:00
0 0 * * * cd /path/to/price_reminder && .venv/bin/python run_monitor.py --config config.yaml --output ./out/latest_alerts.json >> ./out/cron.log 2>&1
```

## Provider 回退顺序（股票 universe）

- 默认顺序：`Yahoo screener -> Nasdaq screener -> 本地缓存`
- 任一 provider 成功后写入 `runtime.cache_dir/stocks_universe.json`
- 两个 provider 都失败时，若缓存未过期（`stocks_universe_cache_ttl_hours`），自动回退缓存

## 目录结构

```
run_monitor.py              crontab 入口
reminder/
  engine.py                 统一调度
  config.py                 配置加载
  models.py                 数据模型
  clients/
    yahoo.py                美股筛选 + 日线
    nasdaq.py               美股筛选回退源
    binance.py              加密市值筛选 + 日线 K 线
    coingecko.py            可选回退源
  rules/
    base.py                 规则基类
    weekly_gain.py          涨幅规则（支持任意时间窗口）
    ema15_down_touch.py     EMA15 触碰规则
    factory.py              规则工厂
  notifiers/
    feishu.py               飞书 Webhook 推送
```

## 扩展方式

1. 在 `reminder/rules/` 新增规则类，并在 `factory.py` 注册
2. 在 `config.yaml` 增加规则配置（`id/enabled/params`）
3. 若需更多数据源，新增 client 并在 `engine.py` 接入
