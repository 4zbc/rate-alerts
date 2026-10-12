# rate-alerts: API data pipeline with change alerts

A small, production-shaped Python job that shows the pattern most business automations need:
**fetch from an API → normalize JSON → store with history → detect a condition → notify a webhook.**

Demo project. The data source is the public [Frankfurter](https://www.frankfurter.app/) API (European Central Bank rates), chosen because it needs no API key, so anyone can run this in 10 seconds.

## What it does

1. Pulls today's exchange rates for a base currency and a list of symbols.
2. Stores them in SQLite (one row per day/symbol, idempotent re-runs).
3. Compares with the previous stored day and flags moves above a threshold.
4. Exports the full history to CSV.
5. Posts an alert to any webhook URL: Slack, Discord, n8n, Make, Zapier catch hook, or a Telegram relay.

Retries with exponential backoff, structured logging, non-zero exit code on failure so cron or GitHub Actions can notice.

## Run it

```bash
python rate_alerts.py --dry-run      # no files written, no webhook
python rate_alerts.py                # real run, uses config.json
python rate_alerts.py --config prod.json -v
```

Standard library only. Python 3.9+.

## Schedule it

cron, every weekday at 17:00 CET (ECB publishes around 16:00):

```
0 17 * * 1-5 cd /opt/rate-alerts && /usr/bin/python3 rate_alerts.py >> run.log 2>&1
```

GitHub Actions: see `.github/workflows/daily.yml`.

## Configure

| key | meaning |
|---|---|
| `base`, `symbols` | which rates to track |
| `alert_threshold_pct` | minimum absolute daily move to alert on |
| `webhook_url` | leave empty to disable notifications |
| `db_path`, `csv_path` | where history goes |

## Adapting it to a real client

Swap `fetch_rates` for any REST endpoint (Shopify orders, a CRM, a supplier price list) and `detect_moves` for the business rule (new order, stock below minimum, price change). The storage, alerting, retry and scheduling parts stay the same. That swap is usually a 2 to 4 hour job.
