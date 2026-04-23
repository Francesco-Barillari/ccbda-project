"""
Glue Python Shell job: reads SQS messages from all enabled sources in parallel,
processes them through the appropriate processor, and writes Forecast-ready CSVs to S3.

S3 output layout per run:
  forecast-ready/target/{SYMBOL}/{RUN_TS}.csv
  forecast-ready/related/{SERIES}/{SYMBOL}/{RUN_TS}.csv
  forecast-ready/metadata/items.csv

--- Adding a new data source ---
1. Create glue/data_preparation/processors/your_api.py (extend BaseProcessor).
2. Register it in glue/data_preparation/processors/__init__.py.
3. Add a source entry in config/pipeline.json pointing to its queue.
4. Re-run setup_phase2.py to re-upload the processors zip and this script.
"""

import io
import sys
import json
import csv
import zipfile
import boto3
from datetime import datetime, timezone
from io import StringIO
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, [
        "config_s3_bucket",
        "config_s3_key",
        "assets_s3_key",
        "processors_zip_key",
    ])
except ImportError:
    # Local testing fallback
    args = {
        "config_s3_bucket":   "YOUR_BUCKET",
        "config_s3_key":      "config/pipeline.json",
        "assets_s3_key":      "config/assets.json",
        "processors_zip_key": "glue-scripts/data_preparation/processors.zip",
    }

BUCKET     = args["config_s3_bucket"]
s3_client  = boto3.client("s3")
sqs_client = boto3.client("sqs")

# Download processors package from S3 and add to sys.path at runtime.
# --extra-py-files does not reliably add zips to sys.path in Python Shell.
_buf = io.BytesIO()
s3_client.download_fileobj(BUCKET, args["processors_zip_key"], _buf)
_buf.seek(0)
with zipfile.ZipFile(_buf) as _zf:
    _zf.extractall("/tmp/glue_processors")
sys.path.insert(0, "/tmp/glue_processors")

from processors import get_processor  # noqa: E402


# =============================================================================
# Helpers
# =============================================================================

def load_json_from_s3(key: str) -> dict:
    return json.loads(s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def drain_queue(queue_url: str, max_messages: int) -> list:
    """Pull all available messages up to max_messages (handles SQS 10-msg cap per call)."""
    messages = []
    while len(messages) < max_messages:
        resp  = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=min(10, max_messages - len(messages)),
            WaitTimeSeconds=1,
        )
        batch = resp.get("Messages", [])
        if not batch:
            break
        messages.extend(batch)
    return messages


def delete_messages(queue_url: str, messages: list) -> None:
    for i in range(0, len(messages), 10):
        batch   = messages[i : i + 10]
        entries = [{"Id": str(j), "ReceiptHandle": m["ReceiptHandle"]} for j, m in enumerate(batch)]
        sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=entries)


def write_csv(rows: list, fieldnames: list, s3_key: str) -> None:
    if not rows:
        return
    buf    = StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    s3_client.put_object(
        Bucket=BUCKET, Key=s3_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"  Wrote {len(rows)} rows → s3://{BUCKET}/{s3_key}")


# =============================================================================
# Per-source processing (runs in a thread per source)
# =============================================================================

def process_source(source_cfg: dict, max_sqs_messages: int) -> tuple:
    """Drains SQS for one source and returns (tts_rows, rts_dict). Thread-safe."""
    name      = source_cfg["name"]
    processor = get_processor(source_cfg["processor"])
    messages  = drain_queue(source_cfg["queue_url"], max_sqs_messages)
    print(f"  [{name}] received {len(messages)} messages")

    tts: list       = []
    rts: dict       = defaultdict(list)
    processed: list = []

    for msg in messages:
        try:
            envelope = json.loads(msg["Body"])
            payload  = envelope.get("payload", envelope)
            record   = processor.parse(payload)
            tts.append(processor.to_tts_row(record))
            rts_result = processor.to_rts_rows(record)
            if rts_result:
                for series, row in rts_result.items():
                    rts[series].append(row)
            processed.append(msg)
        except Exception as exc:
            print(f"  [{name}] ERROR on {msg['MessageId']}: {exc} — will retry after visibility timeout")

    delete_messages(source_cfg["queue_url"], processed)
    print(f"  [{name}] processed {len(processed)}, skipped {len(messages) - len(processed)}")
    return tts, dict(rts)


# =============================================================================
# Main
# =============================================================================

def main():
    config     = load_json_from_s3(args["config_s3_key"])
    assets_cfg = load_json_from_s3(args["assets_s3_key"])
    run_ts     = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
    s3_cfg     = config["s3"]
    max_msgs   = config["glue"]["max_sqs_messages"]

    enabled = [s for s in config["sources"] if s.get("enabled", True)]
    if not enabled:
        print("No enabled sources. Exiting.")
        return

    tts_rows: list = []
    rts_rows: dict = defaultdict(list)

    n_workers = min(len(enabled), 8)
    print(f"\nProcessing {len(enabled)} source(s) with {n_workers} thread(s)")

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {pool.submit(process_source, src, max_msgs): src["name"] for src in enabled}
        for future in as_completed(futures):
            src_tts, src_rts = future.result()
            tts_rows.extend(src_tts)
            for series, rows in src_rts.items():
                rts_rows[series].extend(rows)

    # Write Target Time Series
    print(f"\nWriting {len(tts_rows)} TTS rows...")
    by_symbol: dict = defaultdict(list)
    for row in tts_rows:
        by_symbol[row["item_id"]].append(row)
    for symbol, rows in by_symbol.items():
        write_csv(rows, ["item_id", "timestamp", "target_value"],
                  f"{s3_cfg['target_prefix']}/{symbol}/{run_ts}.csv")

    # Write Related Time Series
    total_rts = sum(len(v) for v in rts_rows.values())
    print(f"Writing {total_rts} RTS rows across {len(rts_rows)} series...")
    for series_name, rows in rts_rows.items():
        by_symbol = defaultdict(list)
        for row in rows:
            by_symbol[row["item_id"]].append(row)
        for symbol, sym_rows in by_symbol.items():
            write_csv(sym_rows, list(sym_rows[0].keys()),
                      f"{s3_cfg['related_prefix']}/{series_name}/{symbol}/{run_ts}.csv")

    # Write Item Metadata (static, from assets config)
    metadata = assets_cfg.get("metadata", {})
    if metadata:
        meta_rows = [{"item_id": sym, **attrs} for sym, attrs in metadata.items()]
        write_csv(meta_rows, ["item_id", "sector", "asset_class"],
                  f"{s3_cfg['metadata_prefix']}/items.csv")

    print(f"\nDone. TTS: {len(tts_rows)} rows, RTS series: {list(rts_rows)}")


main()
