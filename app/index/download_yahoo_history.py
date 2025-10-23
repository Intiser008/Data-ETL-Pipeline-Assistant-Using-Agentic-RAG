#!/usr/bin/env python3
"""
Utility script to download historical price data from Yahoo Finance.

The script hits the public chart endpoint (no crumb handling needed), converts
the JSON payload into a tabular structure, and writes it out as a CSV file.

Example:
    python app/index/download_yahoo_history.py \
        --ticker AAPL \
        --start 1990-11-01 \
        --end 2025-10-23 \
        --interval 1d \
        --output data/etl/raw/prices/aapl_history.csv
"""
from __future__ import annotations

import argparse
import datetime as dt
import csv
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


CHART_ENDPOINT = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"


def parse_date_token(value: Optional[str], default: Optional[dt.datetime] = None) -> int:
    """Return a UTC epoch seconds integer for either a date string or raw epoch."""
    if value is None:
        if default is None:
            raise ValueError("Either provide a value or a default for the date parameter.")
        target_dt = default
    else:
        token = value.strip()
        if token.isdigit():
            return int(token)
        try:
            target_dt = dt.datetime.fromisoformat(token)
        except ValueError as exc:
            raise ValueError(
                f"Could not parse date token '{value}'. "
                "Use YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, or raw epoch seconds."
            ) from exc

    if target_dt.tzinfo is None:
        target_dt = target_dt.replace(tzinfo=dt.timezone.utc)
    else:
        target_dt = target_dt.astimezone(dt.timezone.utc)
    return int(target_dt.timestamp())


def fetch_chart_payload(
    ticker: str,
    start_epoch: int,
    end_epoch: int,
    interval: str,
) -> Dict[str, Any]:
    """Fetch raw chart JSON from Yahoo Finance and return the parsed payload."""
    params = {
        "period1": start_epoch,
        "period2": end_epoch,
        "interval": interval,
        "events": "history",
        "includeAdjustedClose": "true",
    }
    url = CHART_ENDPOINT.format(ticker=ticker)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/127.0.0.0 Safari/537.36"
        )
    }

    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=30,
            )
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else 2 ** attempt
                time.sleep(wait_seconds)
                continue
            response.raise_for_status()
            break
        except requests.RequestException as exc:
            last_exc = exc
            time.sleep(2 ** attempt)
    else:
        raise RuntimeError("Failed to fetch Yahoo Finance data") from last_exc

    payload = response.json()

    chart = payload.get("chart", {})
    error = chart.get("error")
    if error:
        raise RuntimeError(f"Yahoo Finance returned an error: {json.dumps(error)}")

    results = chart.get("result")
    if not results:
        raise RuntimeError("Yahoo Finance response did not contain any chart results.")
    return results[0]


def safe_value(series: List[Any], index: int) -> Any:
    """Return the element at index or None when the list is shorter."""
    if index < len(series):
        return series[index]
    return None


def to_float(value: Any) -> Optional[float]:
    """Convert a value to float when possible."""
    if value in (None, "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> Optional[int]:
    """Convert a value to int when possible."""
    if value in (None, "null"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def chart_payload_to_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Transform chart payload into a list of row dictionaries."""
    timestamps: List[int] = payload.get("timestamp") or []
    if not timestamps:
        raise ValueError("Chart payload did not include any timestamps.")

    quote_blocks: List[Dict[str, List[Any]]] = payload.get("indicators", {}).get("quote") or []
    if not quote_blocks:
        raise ValueError("Chart payload did not include quote indicator data.")
    quote = quote_blocks[0]

    adj_series = (
        payload.get("indicators", {})
        .get("adjclose", [{}])[0]
        .get("adjclose", [])
    )

    timezone_name = payload.get("meta", {}).get("timezone", "UTC")
    tzinfo = dt.timezone.utc

    rows: List[Dict[str, Any]] = []
    for idx, timestamp in enumerate(timestamps):
        dt_obj = dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc)
        rows.append(
            {
                "Date": dt_obj.astimezone(tzinfo).date().isoformat(),
                "Open": to_float(safe_value(quote.get("open", []), idx)),
                "High": to_float(safe_value(quote.get("high", []), idx)),
                "Low": to_float(safe_value(quote.get("low", []), idx)),
                "Close": to_float(safe_value(quote.get("close", []), idx)),
                "Adj Close": to_float(safe_value(adj_series, idx)),
                "Volume": to_int(safe_value(quote.get("volume", []), idx)),
                "Ticker": payload.get("meta", {}).get("symbol"),
                "Currency": payload.get("meta", {}).get("currency"),
                "ExchangeTimezone": timezone_name,
            }
        )

    return rows


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Download historical price data from Yahoo Finance.",
    )
    parser.add_argument("--ticker", required=True, help="Ticker symbol, e.g. AAPL.")
    parser.add_argument(
        "--start",
        required=True,
        help="Start date (YYYY-MM-DD) or epoch seconds, matching Yahoo's period1.",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date (YYYY-MM-DD) or epoch seconds, matching Yahoo's period2.",
    )
    parser.add_argument(
        "--interval",
        default="1d",
        choices={"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"},
        help="Sampling interval to request from Yahoo (default: 1d).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Destination CSV path for the downloaded data.",
    )

    args = parser.parse_args(argv)

    now_utc = dt.datetime.now(tz=dt.timezone.utc)
    start_epoch = parse_date_token(args.start)
    end_epoch = parse_date_token(args.end, default=now_utc)

    if end_epoch <= start_epoch:
        raise ValueError("End date must be greater than start date.")

    payload = fetch_chart_payload(args.ticker, start_epoch, end_epoch, args.interval)
    rows = chart_payload_to_rows(payload)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "Date",
                "Open",
                "High",
                "Low",
                "Close",
                "Adj Close",
                "Volume",
                "Ticker",
                "Currency",
                "ExchangeTimezone",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main(sys.argv[1:])
