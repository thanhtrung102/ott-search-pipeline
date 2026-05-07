"""
anomaly-detector — Iteration 3.

Kinesis event-source mapping trigger (batch 100, bisect-on-error).
Groups enter-only events by (derived_genre, hour_of_day_vn), fetches
7-day rolling baseline from DynamoDB, computes z-score.

z-score > 3.0 → publishes SearchAnomalyDetected to EventBridge custom bus
              → writes to DynamoDB ott-anomaly-events
              → emits AnomalyScore CloudWatch metric

Always emits SearchEventsPerMinute metric.

Environment variables:
  BASELINE_TABLE   DynamoDB table name (ott-baseline-stats)
  ANOMALY_TABLE    DynamoDB table name (ott-anomaly-events)
  EVENT_BUS_NAME   EventBridge bus name (ott-search-events)
  CW_NAMESPACE     CloudWatch namespace (OTT/SearchPipeline)
"""
import base64
import json
import logging
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BASELINE_TABLE = os.environ.get("BASELINE_TABLE", "ott-baseline-stats")
ANOMALY_TABLE  = os.environ.get("ANOMALY_TABLE",  "ott-anomaly-events")
EVENT_BUS_NAME = os.environ.get("EVENT_BUS_NAME", "ott-search-events")
CW_NAMESPACE   = os.environ.get("CW_NAMESPACE",   "OTT/SearchPipeline")

_dynamodb = boto3.resource("dynamodb")
_events   = boto3.client("events")
_cw       = boto3.client("cloudwatch")

_VN_OFFSET   = timedelta(hours=7)
_Z_THRESHOLD = 3.0


def _vn_hour_from_record(record: dict) -> int:
    """Parse Vietnam-timezone hour from datetime_clean or datetime field."""
    for field in ("datetime_clean", "datetime"):
        val = record.get(field)
        if not val:
            continue
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                dt_utc = datetime.strptime(str(val), fmt).replace(tzinfo=timezone.utc)
                return (dt_utc + _VN_OFFSET).hour
            except ValueError:
                continue
    return (datetime.now(timezone.utc) + _VN_OFFSET).hour


def _decode_records(kinesis_event: dict) -> list[dict]:
    records = []
    for r in kinesis_event.get("Records", []):
        raw_data = r["kinesis"]["data"]
        try:
            payload = json.loads(base64.b64decode(raw_data).decode("utf-8"))
            records.append(payload)
        except Exception as exc:
            logger.warning("Failed to decode record: %s", exc)
    return records


def _batch_get_baselines(
    table, keys: list[tuple[str, int]]
) -> dict[tuple[str, int], tuple[float, float]]:
    """Fetch all (mean, std) for the given (genre, hour) keys in one batch request."""
    if not keys:
        return {}
    request_keys = [{"genre_hour": {"S": f"{g}#{h}"}} for g, h in keys]
    resp = _dynamodb.meta.client.batch_get_item(
        RequestItems={table.name: {"Keys": [{"genre_hour": {"S": f"{g}#{h}"}} for g, h in keys]}}
    )
    result: dict[tuple[str, int], tuple[float, float]] = {}
    for item in resp.get("Responses", {}).get(table.name, []):
        gh = item["genre_hour"]["S"]
        genre, _, hour_str = gh.partition("#")
        result[(genre, int(hour_str))] = (
            float(item.get("rolling_mean", {}).get("N", 0)),
            float(item.get("rolling_std",  {}).get("N", 0)),
        )
    return result


def _put_anomaly(
    table,
    category: str,
    hour: int,
    count: int,
    z_score: float,
    mean: float,
    std: float,
) -> str:
    now = datetime.now(timezone.utc)
    anomaly_id = f"{category}#{hour}#{uuid.uuid4().hex[:8]}"
    score_ts   = now.isoformat()
    ttl        = int((now + timedelta(days=30)).timestamp())
    anomaly_type = "SPIKE" if z_score > 0 else "DROP"
    table.put_item(Item={
        "anomaly_id":     anomaly_id,
        "score_ts":       score_ts,
        "derived_genre":  category,
        "hour_of_day_vn": hour,
        "observed_count": count,
        "rolling_mean":   str(Decimal(str(mean)).quantize(Decimal("0.0001"))),
        "rolling_std":    str(Decimal(str(std)).quantize(Decimal("0.0001"))),
        "z_score":        str(Decimal(str(z_score)).quantize(Decimal("0.0001"))),
        "is_anomaly":     True,
        "anomaly_type":   anomaly_type,
        "ttl":            ttl,
    })
    return anomaly_id


def _put_cw_metrics(metric_data: list[dict]) -> None:
    for i in range(0, len(metric_data), 20):
        _cw.put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=metric_data[i: i + 20],
        )


def lambda_handler(event: dict, context: object) -> None:
    records = _decode_records(event)
    if not records:
        return

    enter_events = [r for r in records if r.get("category") == "enter"]

    # Group enter events by (derived_genre, hour_of_day_vn) for anomaly scoring
    enter_buckets: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for r in enter_events:
        category = r.get("derived_genre") or "UNKNOWN"
        hour     = _vn_hour_from_record(r)
        enter_buckets[(category, hour)].append(r)

    baseline_table = _dynamodb.Table(BASELINE_TABLE)
    anomaly_table  = _dynamodb.Table(ANOMALY_TABLE)
    metric_data: list[dict] = []

    baselines = _batch_get_baselines(baseline_table, list(enter_buckets.keys()))

    for (category, hour), group in enter_buckets.items():
        count      = len(group)
        mean, std  = baselines.get((category, hour), (0.0, 0.0))
        z_score    = (count - mean) / max(std, 1.0)

        metric_data.append({
            "MetricName": "AnomalyScore",
            "Dimensions": [
                {"Name": "Genre", "Value": category},
                {"Name": "Hour",  "Value": str(hour)},
            ],
            "Value": abs(z_score),
            "Unit":  "None",
        })

        if abs(z_score) > _Z_THRESHOLD:
            anomaly_id = _put_anomaly(
                anomaly_table, category, hour, count, z_score, mean, std
            )
            logger.warning(
                "Anomaly detected category=%s hour=%d count=%d z=%.2f id=%s",
                category, hour, count, z_score, anomaly_id,
            )
            anomaly_type = "SPIKE" if z_score > 0 else "DROP"
            _events.put_events(Entries=[{
                "Source":     "ott.anomaly-detector",
                "DetailType": "SearchAnomalyDetected",
                "Detail":     json.dumps({
                    "derived_genre":  category,
                    "hour_of_day_vn": hour,
                    "observed_count": count,
                    "z_score":        z_score,
                    "anomaly_type":   anomaly_type,
                    "anomaly_id":     anomaly_id,
                }),
                "EventBusName": EVENT_BUS_NAME,
            }])

    # Throughput metrics per (Genre, PlatformGroup) — §4.15
    quit_events = [r for r in records if r.get("category") == "quit"]
    for metric_name, event_list in (
        ("SearchEnterEventsPerMinute", enter_events),
        ("SearchQuitEventsPerMinute",  quit_events),
    ):
        gp_counts: dict[tuple[str, str], int] = defaultdict(int)
        for r in event_list:
            genre    = r.get("derived_genre") or "UNKNOWN"
            platform = r.get("platform_group") or "Unknown"
            gp_counts[(genre, platform)] += 1
        for (genre, platform), count in gp_counts.items():
            metric_data.append({
                "MetricName": metric_name,
                "Dimensions": [
                    {"Name": "Genre",         "Value": genre},
                    {"Name": "PlatformGroup", "Value": platform},
                ],
                "Value": count,
                "Unit":  "Count",
            })

    if metric_data:
        _put_cw_metrics(metric_data)

    logger.info(
        "Processed %d records: %d enter / %d buckets",
        len(records), len(enter_events), len(enter_buckets),
    )
