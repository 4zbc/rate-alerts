"""Scheduled API data pipeline with change alerts.

Pulls daily exchange rates from a public REST API (Frankfurter, ECB data),
stores them in SQLite, detects moves above a threshold versus the previous
stored day, exports CSV, and posts alerts to any webhook (Slack, Telegram
bot relay, Discord, n8n, Make).

Usage:
    python rate_alerts.py                 # run once with config.json
    python rate_alerts.py --dry-run       # fetch and report, no webhook, no DB write
    python rate_alerts.py --config my.json

Standard library only: nothing to install.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

log = logging.getLogger("rate_alerts")

DEFAULT_CONFIG = {
    "api_url": "https://api.frankfurter.app/latest",
    "base": "EUR",
    "symbols": ["USD", "GBP", "CHF", "TRY", "JPY"],
    "alert_threshold_pct": 0.5,
    "db_path": "rates.sqlite",
    "csv_path": "rates.csv",
    "webhook_url": "",
    "retries": 3,
    "timeout_seconds": 15,
}


# ---------- HTTP ----------

def http_json(url: str, *, method: str = "GET", payload: dict | None = None,
              retries: int = 3, timeout: int = 15) -> dict:
    """GET or POST JSON with exponential backoff. Raises after the last retry."""
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Accept": "application/json", "User-Agent": "rate-alerts/1.0"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)

    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode()
                return json.loads(body) if body else {}
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
            if attempt == retries:
                raise
            wait = 2 ** attempt
            log.warning("request failed (%s), retry %d/%d in %ss", exc, attempt, retries, wait)
            time.sleep(wait)
    raise RuntimeError("unreachable")


def fetch_rates(cfg: dict) -> tuple[str, dict[str, float]]:
    query = urllib.parse.urlencode({"base": cfg["base"], "symbols": ",".join(cfg["symbols"])})
    body = http_json(f'{cfg["api_url"]}?{query}', retries=cfg["retries"], timeout=cfg["timeout_seconds"])
    rates = body.get("rates")
    if not isinstance(rates, dict) or "date" not in body:
        raise ValueError(f"unexpected API response: {body!r}")
    return body["date"], {k: float(v) for k, v in rates.items()}


# ---------- storage ----------

def open_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.execute(
        """CREATE TABLE IF NOT EXISTS rates (
               day TEXT NOT NULL, base TEXT NOT NULL, symbol TEXT NOT NULL,
               rate REAL NOT NULL, PRIMARY KEY (day, base, symbol))"""
    )
    return con


def previous_rates(con: sqlite3.Connection, base: str, before_day: str) -> dict[str, float]:
    row = con.execute(
        "SELECT MAX(day) FROM rates WHERE base = ? AND day < ?", (base, before_day)
    ).fetchone()
    if not row or not row[0]:
        return {}
    cur = con.execute("SELECT symbol, rate FROM rates WHERE base = ? AND day = ?", (base, row[0]))
    return dict(cur.fetchall())


def store_rates(con: sqlite3.Connection, day: str, base: str, rates: dict[str, float]) -> None:
    con.executemany(
        "INSERT OR REPLACE INTO rates (day, base, symbol, rate) VALUES (?, ?, ?, ?)",
        [(day, base, s, r) for s, r in rates.items()],
    )
    con.commit()


def export_csv(con: sqlite3.Connection, path: str) -> int:
    rows = con.execute("SELECT day, base, symbol, rate FROM rates ORDER BY day, symbol").fetchall()
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["day", "base", "symbol", "rate"])
        writer.writerows(rows)
    return len(rows)


# ---------- analysis ----------

def detect_moves(today: dict[str, float], prev: dict[str, float], threshold_pct: float) -> list[dict]:
    moves = []
    for symbol, rate in today.items():
        old = prev.get(symbol)
        if not old:
            continue
        change = (rate - old) / old * 100
        if abs(change) >= threshold_pct:
            moves.append({"symbol": symbol, "old": old, "new": rate, "change_pct": round(change, 3)})
    return sorted(moves, key=lambda m: -abs(m["change_pct"]))


def format_alert(base: str, day: str, moves: list[dict]) -> str:
    lines = [f"Rate alert for {day} (base {base}):"]
    for m in moves:
        arrow = "up" if m["change_pct"] > 0 else "down"
        lines.append(f"  {base}/{m['symbol']} {arrow} {m['change_pct']:+.2f}%  ({m['old']:.4f} -> {m['new']:.4f})")
    return "\n".join(lines)


def send_webhook(url: str, text: str, moves: list[dict], cfg: dict) -> None:
    payload = {"text": text, "content": text, "moves": moves}  # Slack uses text, Discord uses content
    http_json(url, method="POST", payload=payload, retries=cfg["retries"], timeout=cfg["timeout_seconds"])


# ---------- main ----------

def load_config(path: str | None) -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if path:
        cfg.update(json.loads(Path(path).read_text(encoding="utf-8")))
    return cfg


def run(cfg: dict, dry_run: bool = False) -> int:
    day, rates = fetch_rates(cfg)
    log.info("fetched %d rates for %s (base %s)", len(rates), day, cfg["base"])

    con = open_db(":memory:" if dry_run else cfg["db_path"])
    prev = previous_rates(con, cfg["base"], day)
    moves = detect_moves(rates, prev, cfg["alert_threshold_pct"])

    if not dry_run:
        store_rates(con, day, cfg["base"], rates)
        n = export_csv(con, cfg["csv_path"])
        log.info("stored %d rows total, CSV at %s", n, cfg["csv_path"])

    if moves:
        text = format_alert(cfg["base"], day, moves)
        print(text)
        if cfg["webhook_url"] and not dry_run:
            send_webhook(cfg["webhook_url"], text, moves, cfg)
            log.info("alert sent to webhook")
    else:
        why = "no previous day stored yet" if not prev else f"no move above {cfg['alert_threshold_pct']}%"
        print(f"{day}: no alerts ({why})")
        for s, r in sorted(rates.items()):
            print(f"  {cfg['base']}/{s} = {r:.4f}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", help="path to a JSON config (see config.json)")
    parser.add_argument("--dry-run", action="store_true", help="fetch and report only; no DB write, no webhook")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    try:
        return run(load_config(args.config), dry_run=args.dry_run)
    except Exception as exc:  # the scheduler needs a non-zero exit to notice failures
        log.error("run failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
