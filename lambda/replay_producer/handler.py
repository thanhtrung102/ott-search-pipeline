"""
replay-producer — Iteration 1.

Reads Parquet files from S3, enriches each row (datetime clean, genre classify,
platform bucket), then replays to Kinesis at 336× speedup so 14 days of data
complete in ~60 minutes.

Publish rate: 500 records/call, ≤1,500 records/second (§4.5).
Speedup calculation (§6.1): 14d = 1,209,600 s; 60 min = 3,600 s; factor ≈ 336.

Event body: original fields + derived_genre + platform_group (used by Firehose
dynamic partitioning and by anomaly-detector for (derived_genre, hour) grouping).

Environment variables:
  STREAM_NAME       Kinesis stream name
  SPEEDUP_FACTOR    default 336
  PARQUET_S3_PREFIX s3://bucket/raw-source/log_search/
"""
import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import pyarrow.parquet as pq

# genre_classifier is deployed as a Lambda layer (see ingestion_stack.py).
# During local testing, add the repo root to sys.path.
try:
    from genre_classifier.rules import classify_keyword, bucket_platform
except ImportError:
    import sys
    sys.path.insert(0, "/opt/python")   # Lambda layer path
    from genre_classifier.rules import classify_keyword, bucket_platform  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

STREAM_NAME    = os.environ["STREAM_NAME"]
SPEEDUP_FACTOR = float(os.environ.get("SPEEDUP_FACTOR", "336"))
S3_PREFIX      = os.environ["PARQUET_S3_PREFIX"].rstrip("/")  # s3://bucket/key-prefix

_KINESIS  = boto3.client("kinesis")
_S3       = boto3.client("s3")

_AR_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

_BATCH_SIZE   = 500    # Kinesis PutRecords max
_MAX_REC_SEC  = 1_500  # rate limit per spec §4.5


# ── Datetime helpers (§6.7) ───────────────────────────────────────────────────

def _clean_dt(dt_str: str | None) -> str | None:
    if dt_str is None:
        return None
    s = dt_str.translate(_AR_DIGITS)
    if s.startswith("2565"):
        s = "2022" + s[4:]
    return s


def _parse_ts(dt_clean: str | None) -> float | None:
    if not dt_clean:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(dt_clean, fmt).replace(
                tzinfo=timezone.utc
            ).timestamp()
        except ValueError:
            continue
    return None


# ── S3 helpers ────────────────────────────────────────────────────────────────

def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Return (bucket, key_prefix) from s3://bucket/prefix."""
    without = uri[len("s3://"):]
    bucket, _, prefix = without.partition("/")
    return bucket, prefix


def _list_parquet_keys(bucket: str, prefix: str) -> list[str]:
    paginator = _S3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".parquet") or k.endswith(".snappy.parquet"):
                keys.append(k)
    return keys


def _read_parquet_from_s3(bucket: str, key: str) -> list[dict]:
    """Stream a Parquet file from S3 and return rows as dicts."""
    import io
    resp = _S3.get_object(Bucket=bucket, Key=key)
    table = pq.read_table(io.BytesIO(resp["Body"].read()))
    return table.to_pylist()


# ── Enrichment ────────────────────────────────────────────────────────────────

def _enrich_row(row: dict) -> dict:
    dt_clean      = _clean_dt(row.get("datetime"))
    kw_norm       = (row.get("keyword") or "").lower().strip() or None
    category_norm = classify_keyword(kw_norm)
    platform_grp  = bucket_platform(row.get("platform"))

    enriched = {k: (v.isoformat() if hasattr(v, "isoformat") else v)
                for k, v in row.items()}
    enriched["datetime_clean"]  = dt_clean
    enriched["keyword_norm"]    = kw_norm
    enriched["derived_genre"]   = category_norm
    enriched["platform_group"]  = platform_grp
    return enriched


# ── Kinesis publishing ────────────────────────────────────────────────────────

def _put_batch(records: list[dict]) -> int:
    """Push one batch of ≤500 records. Returns failed-record count."""
    kinesis_records = [
        {
            "Data": json.dumps(r, ensure_ascii=False).encode(),
            "PartitionKey": r.get("derived_genre", "UNKNOWN"),
        }
        for r in records
    ]
    resp = _KINESIS.put_records(
        StreamName=STREAM_NAME,
        Records=kinesis_records,
    )
    failed = resp.get("FailedRecordCount", 0)
    if failed:
        logger.warning("PutRecords: %d/%d records failed", failed, len(records))
    return failed


# ── Main handler ──────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context: object) -> dict:
    """Entry point — invoke manually to start the replay."""
    bucket, prefix = _parse_s3_uri(S3_PREFIX)
    keys = _list_parquet_keys(bucket, prefix)
    logger.info("Found %d parquet files under s3://%s/%s", len(keys), bucket, prefix)

    # Collect all rows, find min timestamp for replay offset calculation
    all_rows: list[dict] = []
    for key in keys:
        rows = _read_parquet_from_s3(bucket, key)
        enriched = [_enrich_row(r) for r in rows]
        all_rows.extend(enriched)
    logger.info("Total rows: %d", len(all_rows))

    # Sort by original event timestamp for correct replay ordering
    def _ts(r: dict) -> float:
        return _parse_ts(_clean_dt(r.get("datetime"))) or 0.0

    all_rows.sort(key=_ts)
    timestamps = [_ts(r) for r in all_rows]
    min_ts = timestamps[0] if timestamps else 0.0
    now_epoch = time.time()

    total_published = 0
    total_failed    = 0

    for batch_start in range(0, len(all_rows), _BATCH_SIZE):
        batch = all_rows[batch_start: batch_start + _BATCH_SIZE]

        # Respect replay timeline — sleep until this batch's replay time
        last_row_ts = timestamps[min(batch_start + _BATCH_SIZE - 1, len(timestamps) - 1)]
        replay_ts   = now_epoch + (last_row_ts - min_ts) / SPEEDUP_FACTOR
        sleep_sec   = replay_ts - time.time()
        if sleep_sec > 0:
            time.sleep(min(sleep_sec, 1.0))  # cap single sleep to avoid Lambda timeout

        t0 = time.time()
        failed = _put_batch(batch)
        total_published += len(batch) - failed
        total_failed    += failed

        # Rate-limit: hold ≥ _BATCH_SIZE/_MAX_REC_SEC between batch starts
        elapsed = time.time() - t0
        time.sleep(max(0, _BATCH_SIZE / _MAX_REC_SEC - elapsed))

    summary = {
        "files_processed": len(keys),
        "rows_total": len(all_rows),
        "records_published": total_published,
        "records_failed": total_failed,
    }
    logger.info("Replay complete: %s", summary)
    return summary
