"""
Send synthetic price messages to SQS for local testing.
Simulates what the data-ingestion Lambda will eventually push.

Usage:
  python scripts/send_test_messages.py
  python scripts/send_test_messages.py --symbols AAPL BTC --messages 24 --interval 5
"""

import argparse
import json
import os
import random
import sys
from datetime import datetime, timezone, timedelta

import boto3
from dotenv import dotenv_values

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config   = dotenv_values(os.path.join(ROOT_DIR, ".env"))

QUEUE_URL   = config.get("SQS_QUEUE_URL") or sys.exit("ERROR: SQS_QUEUE_URL not set in .env")
SOURCE_NAME = "alpha_vantage_price"

BASE_PRICES: dict = {
    "AAPL": 195.0,
    "BTC":  65000.0,
    "MSFT": 420.0,
    "GOOG": 175.0,
}


def _random_walk(base: float, n: int, volatility: float = 0.002) -> list:
    price, prices = base, []
    for _ in range(n):
        price *= 1 + random.gauss(0, volatility)
        prices.append(round(price, 2))
    return prices


def send(symbols: list, num_messages: int, interval_minutes: int, start: datetime) -> None:
    sqs = boto3.client(
        "sqs",
        region_name=config.get("AWS_DEFAULT_REGION", "eu-west-1"),
    )

    total = 0
    for symbol in symbols:
        base    = BASE_PRICES.get(symbol, 100.0)
        prices  = _random_walk(base, num_messages)
        entries = []

        for i, price in enumerate(prices):
            ts     = start + timedelta(minutes=i * interval_minutes)
            volume = max(0, int(random.gauss(1_000_000, 200_000)))
            entries.append({
                "Id":          f"{symbol}-{i}",
                "MessageBody": json.dumps({
                    "source":  SOURCE_NAME,
                    "version": "1.0",
                    "payload": {
                        "symbol":    symbol,
                        "price":     price,
                        "volume":    volume,
                        "timestamp": ts.isoformat(),
                    },
                }),
            })

        for batch_start in range(0, len(entries), 10):
            batch  = entries[batch_start : batch_start + 10]
            resp   = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=batch)
            failed = resp.get("Failed", [])
            if failed:
                print(f"  WARNING: {len(failed)} messages failed for {symbol}")
            total += len(batch) - len(failed)

        print(f"  {symbol}: {len(entries)} messages queued")

    print(f"\nTotal sent: {total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send test price messages to SQS")
    parser.add_argument("--symbols",   nargs="+", default=["AAPL"])
    parser.add_argument("--messages",  type=int,  default=12, help="messages per symbol (12 = 1h at 5min)")
    parser.add_argument("--interval",  type=int,  default=5,  help="minutes between ticks")
    parser.add_argument("--start",     type=str,  default=None, help="ISO-8601 start time (default: now minus window)")
    args = parser.parse_args()

    if args.start:
        start = datetime.fromisoformat(args.start)
    else:
        start = datetime.now(timezone.utc) - timedelta(minutes=args.messages * args.interval)

    print(f"Sending {args.messages} messages per symbol for {args.symbols}")
    print(f"Time range: {start.isoformat()} → {(start + timedelta(minutes=args.messages * args.interval)).isoformat()}\n")
    send(args.symbols, args.messages, args.interval, start)
