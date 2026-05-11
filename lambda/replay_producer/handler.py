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
  STREAM_NAME            Kinesis stream name
  SPEEDUP_FACTOR         default 336
  PARQUET_S3_PREFIX      s3://bucket/raw-source/log_search/
  NOVA_FALLBACK_ENABLED  set to "1" to re-classify UNKNOWN rows via Nova Micro
                         after the main waterfall; new classifications are written
                         back to lut_extended.json on S3 so the Glue job benefits
                         too. Requires bedrock:InvokeModel on the Lambda role.
  BEDROCK_MODEL_ID       override Nova model (default apac.amazon.nova-micro-v1:0)
  LUT_EXT_S3_KEY         S3 key for lut_extended.json within PARQUET_S3_PREFIX's
                         bucket (default glue-scripts/lut_extended.json)
"""
import json
import logging
import os
import re
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
_MAX_REC_SEC  = int(os.environ.get("MAX_REC_SEC", "1500"))  # rate limit per spec §4.5


# ── Nova Micro fallback (§6.4 optional stage, item ADR-07 extension) ─────────

_NOVA_FALLBACK  = os.environ.get("NOVA_FALLBACK_ENABLED", "0") == "1"
_BEDROCK_REGION = os.environ.get("BEDROCK_REGION", "ap-southeast-1")
_BEDROCK_MODEL  = os.environ.get("BEDROCK_MODEL_ID", "apac.amazon.nova-micro-v1:0")
_LUT_EXT_S3_KEY = os.environ.get("LUT_EXT_S3_KEY", "glue-scripts/lut_extended.json")

_BEDROCK = (
    boto3.client("bedrock-runtime", region_name=_BEDROCK_REGION)
    if _NOVA_FALLBACK else None
)

_VALID_GENRES = frozenset({
    "NHAC", "THE_THAO", "ANIME", "PHIM_TRUNG", "PHIM_VIET",
    "PHIM_AU_MY", "PHIM_HAN", "TRUYEN_HINH", "UNKNOWN",
})
_GENRE_ALIAS = {
    "PHIM_CHINA": "PHIM_TRUNG", "PHIM_CHINESE": "PHIM_TRUNG",
    "PHIM_TQ": "PHIM_TRUNG", "PHIM_TRUNG_QUOC": "PHIM_TRUNG",
    "PHIM_CHIEU_RAP": "PHIM_AU_MY",
    "PHIM_KOREAN": "PHIM_HAN", "PHIM_KOREA": "PHIM_HAN",
    "KDRAMA": "PHIM_HAN", "K_DRAMA": "PHIM_HAN", "K-DRAMA": "PHIM_HAN",
    "PHIM_JAPAN": "ANIME", "PHIM_NHAT": "ANIME", "MANGA": "ANIME",
    "CARTOON": "ANIME", "HOAT_HINH": "ANIME",
    "KPOP": "NHAC", "K_POP": "NHAC", "K-POP": "NHAC",
    "VARIETY": "TRUYEN_HINH", "SHOW": "TRUYEN_HINH",
    "PHIM_BO": "PHIM_VIET", "PHIM_LE": "PHIM_VIET",
}
_NOVA_PROMPT = (
    "You are a genre classifier for FPT Play, a Vietnamese OTT platform.\n"
    "Classify search keywords into genres. Most keywords are movie/drama titles.\n"
    "Genre codes: PHIM_HAN PHIM_TRUNG PHIM_VIET PHIM_AU_MY ANIME THE_THAO NHAC TRUYEN_HINH UNKNOWN\n"
    "- PHIM_HAN: Korean dramas/movies  - PHIM_TRUNG: Chinese dramas/movies\n"
    "- PHIM_VIET: Vietnamese content    - PHIM_AU_MY: Western/Hollywood films\n"
    "- ANIME: Japanese anime/manga      - THE_THAO: Sports\n"
    "- NHAC: Music/songs/concerts       - TRUYEN_HINH: TV channels/variety/live\n"
    "- UNKNOWN: Only truly ambiguous (single chars, gibberish, meta-queries)\n"
    "Output: JSON {{keyword: genre}}. Return ONLY the JSON.\nKeywords:\n{kws}"
)
_SANITIZE_RE = re.compile(r"[\x00-\x1f\x7f\\]")


def _normalize_genre(v: str) -> str:
    if not isinstance(v, str):
        return "UNKNOWN"
    u = _GENRE_ALIAS.get(v.upper().strip(), v.upper().strip())
    return u if u in _VALID_GENRES else "UNKNOWN"


def _nova_classify_batch(keywords: list[str]) -> dict[str, str]:
    """Classify up to 50 keywords via Nova Micro. Returns {kw: genre} for non-UNKNOWN."""
    if not keywords or _BEDROCK is None:
        return {}
    san_to_orig: dict[str, str] = {}
    for kw in keywords:
        s = _SANITIZE_RE.sub(" ", kw).strip()
        if s:
            san_to_orig.setdefault(s, kw)
    clean = list(san_to_orig)
    try:
        body = json.dumps({
            "messages": [{"role": "user", "content": [{"text": _NOVA_PROMPT.format(kws="\n".join(clean))}]}],
            "inferenceConfig": {"maxTokens": 1024, "temperature": 0},
        })
        resp = _BEDROCK.invoke_model(
            modelId=_BEDROCK_MODEL,
            contentType="application/json",
            accept="application/json",
            body=body,
        )
        raw = json.loads(resp["body"].read())["output"]["message"]["content"][0]["text"]
        parsed = json.loads(re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE))
        # Detect and fix inverted {genre: keyword} responses from Nova
        if parsed and sum(1 for k in parsed if isinstance(k, str) and k.upper() in _VALID_GENRES) / len(parsed) > 0.5:
            parsed = {v: k for k, v in parsed.items() if isinstance(v, str)}
        kw_set = set(keywords)
        return {
            san_to_orig.get(k, k): _normalize_genre(v)
            for k, v in parsed.items()
            if san_to_orig.get(k, k) in kw_set and _normalize_genre(v) != "UNKNOWN"
        }
    except Exception as exc:
        logger.warning("Nova batch classify failed: %s", exc)
        return {}


def _apply_nova_fallback(rows: list[dict]) -> list[dict]:
    """Re-classify UNKNOWN-tagged rows via Nova Micro.

    Loads lut_extended.json from S3 to skip already-seen keywords (free).
    Writes new classifications back to S3 so the Glue batch job benefits too.
    """
    unknown_kws = list({
        r["keyword_norm"] for r in rows
        if r.get("derived_genre") == "UNKNOWN" and r.get("keyword_norm")
    })
    if not unknown_kws:
        return rows

    s3_bucket = _parse_s3_uri(S3_PREFIX)[0]
    existing: dict[str, str] = {}
    try:
        obj = _S3.get_object(Bucket=s3_bucket, Key=_LUT_EXT_S3_KEY)
        existing = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as exc:
        logger.warning("Could not load lut_extended from S3: %s", exc)

    new_kws = [kw for kw in unknown_kws if kw not in existing]
    logger.info(
        "Nova fallback: %d UNKNOWN keywords (%d new, %d already cached)",
        len(unknown_kws), len(new_kws), len(unknown_kws) - len(new_kws),
    )

    nova_results: dict[str, str] = {}
    for i in range(0, len(new_kws), 50):
        nova_results.update(_nova_classify_batch(new_kws[i:i + 50]))
        if i + 50 < len(new_kws):
            time.sleep(0.7)  # Nova Micro ~100 RPM on-demand limit

    if nova_results:
        logger.info("Nova classified %d / %d new keywords", len(nova_results), len(new_kws))
        merged = {**existing, **nova_results}
        try:
            _S3.put_object(
                Bucket=s3_bucket, Key=_LUT_EXT_S3_KEY,
                Body=json.dumps(merged, ensure_ascii=False, sort_keys=True).encode("utf-8"),
                ContentType="application/json",
            )
            logger.info("lut_extended updated on S3: %d → %d entries", len(existing), len(merged))
        except Exception as exc:
            logger.warning("Failed to write lut_extended back to S3: %s", exc)

    combined = {**existing, **nova_results}
    for row in rows:
        if row.get("derived_genre") == "UNKNOWN":
            kw = row.get("keyword_norm")
            if kw and kw in combined:
                row["derived_genre"] = combined[kw]
    return rows


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


_VN_OFFSET_SEC = 7 * 3600

def _vn_hour(row: dict) -> int:
    """Return Vietnam-timezone hour from a row's datetime field."""
    ts = _parse_ts(_clean_dt(row.get("datetime")))
    if ts is None:
        return -1
    return int(((ts + _VN_OFFSET_SEC) % 86400) // 3600)


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
    """Entry point — invoke manually to start the replay.

    Optional event fields for anomaly injection demo:
      inject_anomaly    (bool)   : if true, duplicate records for anomaly_genre at anomaly_hour
      anomaly_genre     (str)    : derived_genre to spike (default "THE_THAO")
      anomaly_hour      (int)    : Vietnam-timezone hour to spike (default 20)
      anomaly_multiplier(int)    : how many extra copies to inject (default 10)
    """
    inject         = event.get("inject_anomaly", False)
    anomaly_genre  = event.get("anomaly_genre",  "THE_THAO")
    anomaly_hour   = int(event.get("anomaly_hour", 20))
    anomaly_mult   = int(event.get("anomaly_multiplier", 10))

    s3_prefix = event.get("s3_prefix", S3_PREFIX).rstrip("/")
    bucket, prefix = _parse_s3_uri(s3_prefix)
    keys = _list_parquet_keys(bucket, prefix)
    logger.info("Found %d parquet files under s3://%s/%s", len(keys), bucket, prefix)

    all_rows: list[dict] = []
    for key in keys:
        rows = _read_parquet_from_s3(bucket, key)
        enriched = [_enrich_row(r) for r in rows]
        all_rows.extend(enriched)
    logger.info("Total rows after waterfall classify: %d", len(all_rows))

    # Nova fallback: re-classify UNKNOWN rows in one batched pass
    if _NOVA_FALLBACK:
        before = sum(1 for r in all_rows if r.get("derived_genre") == "UNKNOWN")
        all_rows = _apply_nova_fallback(all_rows)
        after = sum(1 for r in all_rows if r.get("derived_genre") == "UNKNOWN")
        logger.info("Nova fallback resolved %d UNKNOWN → classified (%d remaining)", before - after, after)

    if inject:
        spike_rows = [
            r for r in all_rows
            if r.get("derived_genre") == anomaly_genre
            and _vn_hour(r) == anomaly_hour
        ]
        injected = spike_rows * (anomaly_mult - 1)
        all_rows.extend(injected)
        logger.info(
            "Anomaly injection: %d spike rows × %d = %d extra records "
            "(genre=%s hour=%d)",
            len(spike_rows), anomaly_mult - 1, len(injected), anomaly_genre, anomaly_hour,
        )

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

        last_row_ts = timestamps[min(batch_start + _BATCH_SIZE - 1, len(timestamps) - 1)]
        replay_ts   = now_epoch + (last_row_ts - min_ts) / SPEEDUP_FACTOR
        sleep_sec   = replay_ts - time.time()
        if sleep_sec > 0:
            time.sleep(min(sleep_sec, 1.0))

        t0 = time.time()
        failed = _put_batch(batch)
        total_published += len(batch) - failed
        total_failed    += failed

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
