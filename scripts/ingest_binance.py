"""Fetch daily OHLCV candles from the public Binance REST API and write parquet files.

No API key is required: /api/v3/klines is a public endpoint.
Handles rate limits (HTTP 429) and transient errors (5xx) with retries.
"""
import argparse
import logging
import time
from datetime import datetime, timezone

import pandas as pd
import requests

API_URL = "https://api.binance.com/api/v3/klines"
COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades", "taker_buy_base",
    "taker_buy_quote", "ignore",
]
MAX_RETRIES = 5


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """Fetch klines in 1000-candle pages. Retries with backoff on 429/5xx."""
    all_rows = []
    cursor = start_ms
    while cursor < end_ms:
        params = {
            "symbol": symbol, "interval": interval,
            "startTime": cursor, "endTime": end_ms, "limit": 1000,
        }
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(API_URL, params=params, timeout=30)
            except requests.RequestException as exc:
                logging.warning("network error for %s: %s", symbol, exc)
                time.sleep(2 ** attempt)
                continue
            if resp.status_code == 429:          # rate limited
                logging.warning("rate limited, sleeping %ss", 2 ** attempt)
                time.sleep(2 ** attempt)
                continue
            if resp.status_code >= 500:          # transient server error
                time.sleep(2 ** attempt)
                continue
            if resp.status_code == 404:
                raise ValueError("symbol %s not found (404)" % symbol)
            resp.raise_for_status()
            rows = resp.json()
            break
        else:
            raise RuntimeError("failed to fetch %s after %d attempts" % (symbol, MAX_RETRIES))

        if not rows:
            break
        all_rows.extend(rows)
        cursor = rows[-1][0] + 1  # next page starts after last open_time
        time.sleep(0.25)          # be polite to the public API
    df = pd.DataFrame(all_rows, columns=COLUMNS)
    if df.empty:
        return df
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume", "quote_volume"]:
        df[col] = df[col].astype(float)
    df["trades"] = df["trades"].astype(int)
    df["symbol"] = symbol
    df["open_date"] = df["open_time"].dt.date  # real dates -> parquet date32
    return df[["symbol", "open_time", "open", "high", "low", "close", "volume",
               "close_time", "quote_volume", "trades", "open_date"]]



def write_parquet(df: "pd.DataFrame", path: "pathlib.Path") -> None:
    """Write parquet with explicit arrow types so BigQuery DATE/TIMESTAMP match."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([
        ("symbol", pa.string()),
        ("open_time", pa.timestamp("us", tz="UTC")),
        ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()),
        ("close", pa.float64()), ("volume", pa.float64()),
        ("close_time", pa.timestamp("us", tz="UTC")),
        ("quote_volume", pa.float64()),
        ("trades", pa.int64()),
        ("open_date", pa.date32()),
    ])
    pq.write_table(pa.Table.from_pandas(df, schema=schema), str(path))


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    start_ms = ms(datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc))
    end_ms = ms(datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc))

    for symbol in args.symbols:
        try:
            df = fetch_klines(symbol, args.interval, start_ms, end_ms)
        except Exception:
            logging.exception("skipping %s due to error", symbol)
            continue
        if df.empty:
            logging.warning("no data for %s in range", symbol)
            continue
        out = pathlib.Path(args.output_dir) / f"symbol={symbol}"
        out.mkdir(parents=True, exist_ok=True)
        write_parquet(df, out / "data.parquet")
        logging.info("wrote %d rows for %s -> %s", len(df), symbol, out)


if __name__ == "__main__":
    main()
