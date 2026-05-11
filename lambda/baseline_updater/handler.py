"""
baseline-updater — Iteration 3.

Invoked in two modes:
  1. EventBridge schedule (daily 01:00 UTC+7 = 18:00 UTC) — event has no taskToken
  2. Step Functions waitForTaskToken — event.taskToken must be ack'd on completion

Queries Athena curated layer for 7-day rolling enter-event counts per
(derived_genre, hour_of_day_vn) grouped by calendar day. Computes per-slot
mean and std across days, then batch-writes up to 216 items (9 genres × 24 hours)
to DynamoDB ott-baseline-stats.

DynamoDB item schema:
  genre_hour (PK, string)  e.g. "THE_THAO#20"
  rolling_mean (N)
  rolling_std  (N)
  sample_days  (N)
  updated_at   (S)           ISO-8601 UTC

Environment variables:
  BASELINE_TABLE      DynamoDB table name
  ATHENA_WORKGROUP    ott-analytics
  ATHENA_OUTPUT       s3://bucket/athena-results/
  CURATED_DATABASE    ott_search_curated
  CURATED_TABLE       search_enriched
"""
import json
import logging
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BASELINE_TABLE   = os.environ.get("BASELINE_TABLE",   "ott-baseline-stats")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP",  "ott-analytics")
ATHENA_OUTPUT    = os.environ["ATHENA_OUTPUT"]
CURATED_DB       = os.environ.get("CURATED_DATABASE",  "ott_search_curated")
CURATED_TABLE    = os.environ.get("CURATED_TABLE",     "search_enriched")
# Optional: override the 7-day window with a fixed start date (e.g., for demos
# with historical data).  Set BASELINE_DATE_FROM to 'YYYY-MM-DD'.
_DATE_FROM_OVERRIDE = os.environ.get("BASELINE_DATE_FROM", "")

_athena   = boto3.client("athena")
_dynamodb = boto3.resource("dynamodb")
_sfn      = boto3.client("stepfunctions")

# Per-day counts grouped by (derived_genre, hour_of_day_vn)
_DATE_WHERE = (
    f"dt >= '{_DATE_FROM_OVERRIDE}'"
    if _DATE_FROM_OVERRIDE
    else "dt BETWEEN DATE_FORMAT(DATE_ADD('day', -7, CURRENT_DATE), '%Y-%m-%d')"
         " AND DATE_FORMAT(DATE_ADD('day', -1, CURRENT_DATE), '%Y-%m-%d')"
)
_ROLLING_DAYS_SQL = """
SELECT
  derived_genre,
  hour_of_day_vn,
  dt,
  COUNT(*) AS daily_enter_count
FROM {db}.{tbl}
WHERE {date_where}
  AND session_action = 'enter'
  AND is_cross_partition_date = false
GROUP BY derived_genre, hour_of_day_vn, dt
""".format(db=CURATED_DB, tbl=CURATED_TABLE, date_where=_DATE_WHERE)

_POLL_INTERVAL_SEC = 5


def _run_athena_query(sql: str) -> list[dict]:
    """Execute SQL, poll until done, return rows as list of dicts."""
    start_resp = _athena.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    query_id = start_resp["QueryExecutionId"]
    logger.info("Athena query started: %s", query_id)

    deadline = time.time() + 240  # 4-minute cap; Lambda timeout is 5 min
    while True:
        if time.time() > deadline:
            _athena.stop_query_execution(QueryExecutionId=query_id)
            raise RuntimeError(f"Athena query {query_id} timed out after 240 s")
        resp  = _athena.get_query_execution(QueryExecutionId=query_id)
        state = resp["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {query_id} {state}: {reason}")
        time.sleep(_POLL_INTERVAL_SEC)

    rows = []
    paginator = _athena.get_paginator("get_query_results")
    header: list[str] | None = None
    for page in paginator.paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"]:
            values = [c.get("VarCharValue", "") for c in row["Data"]]
            if header is None:
                header = values
                continue
            rows.append(dict(zip(header, values)))

    logger.info("Athena returned %d rows", len(rows))
    return rows


def _compute_baseline(rows: list[dict]) -> dict[tuple[str, int], dict]:
    daily: dict[tuple[str, int], list[int]] = defaultdict(list)
    for r in rows:
        genre = r.get("derived_genre") or "UNKNOWN"
        try:
            hour  = int(r.get("hour_of_day_vn", 0))
            count = int(r.get("daily_enter_count", 0))
        except (TypeError, ValueError):
            continue
        daily[(genre, hour)].append(count)

    result = {}
    for (genre, hour), counts in daily.items():
        n    = len(counts)
        mean = sum(counts) / n
        variance = sum((c - mean) ** 2 for c in counts) / n
        std  = math.sqrt(variance)
        result[(genre, hour)] = {
            "rolling_mean": Decimal(str(round(mean, 4))),
            "rolling_std":  Decimal(str(round(std,  4))),
            "sample_days":  n,
        }
    return result


def _write_baseline(stats: dict[tuple[str, int], dict]) -> int:
    table   = _dynamodb.Table(BASELINE_TABLE)
    now_iso = datetime.now(timezone.utc).isoformat()
    with table.batch_writer() as batch:
        for (genre, hour), s in stats.items():
            batch.put_item(Item={
                "genre_hour":   f"{genre}#{hour}",
                "rolling_mean": s["rolling_mean"],
                "rolling_std":  s["rolling_std"],
                "sample_days":  s["sample_days"],
                "updated_at":   now_iso,
            })
    return len(stats)


def _run() -> dict:
    rows   = _run_athena_query(_ROLLING_DAYS_SQL)
    stats  = _compute_baseline(rows)
    written = _write_baseline(stats)
    logger.info("Baseline updated: %d genre/hour slots written", written)
    return {"slots_written": written, "athena_rows": len(rows)}


def lambda_handler(event: dict, context: object) -> dict:
    task_token = event.get("taskToken")
    try:
        result = _run()
        if task_token:
            _sfn.send_task_success(
                taskToken=task_token,
                output=json.dumps(result),
            )
        return result
    except Exception as exc:
        logger.exception("Baseline update failed")
        if task_token:
            _sfn.send_task_failure(
                taskToken=task_token,
                error="BaselineUpdateError",
                cause=str(exc),
            )
        raise
