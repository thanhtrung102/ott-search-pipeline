"""
search-enrichment-job — Iteration 2.

Glue 4.0 (Spark 3.3 / Python 3.10), G.1X, 2 DPUs, job bookmark enabled.
Reads last 2 days to ensure cross-day is_repeat_search window is correct.

18-step enrichment pipeline (§4.4 — sequence is load-bearing):
  1.  clean_datetime UDF → datetime_clean  (§6.7)
  2.  to_timestamp → event_ts
  3.  DROP year(event_ts) < 2015              (corrupt year 0004, §6.7 Type 3)
  4.  hour_of_day_vn = hour(event_ts + 7h)
  5.  session_action = category passthrough
  6.  is_search_abandoned = (category = 'quit')
  7.  user_is_authenticated = (user_id IS NOT NULL)
  8.  user_id_hashed = sha2(user_id, 256)    [null-safe]
  9.  keyword_norm = lower(trim(keyword))
  10. derived_genre from keyword_norm         (rule-based + optional LLM fallback)
  11. platform_group from platform            (§6.5)
  12. network_type_norm from networkType      (§6.6)
  13. isp_segment = upper(proxy_isp)          (§6.2 — ISP market segment)
  14. has_premium from userPlansMap array     (§2)
  15. subscription_count = len(userPlansMap)  [null-safe]
  16. search_session_id                       (auth vs anon paths)
  17. is_repeat_search (LAG window, 24h)
  18. is_cross_partition_date flag
      WRITE partitioned by dt, derived_genre

Output: s3://ott-search-{account}-{env}/curated/search_enriched/
        dt={YYYY-MM-DD}/derived_genre={VALUE}/
"""
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window, functions as F
from pyspark.sql.functions import (
    col, expr, floor, hash as spark_hash, hour, lit,
    lower, sha2, to_timestamp, trim, udf, when, year,
)
from pyspark.sql.types import ArrayType, BooleanType, IntegerType, StringType, StructField, StructType

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_BUCKET", "CURATED_DB", "RAW_DB", "LLM_ENABLED"],
)
# Optional override — useful for historical backfills or demo runs.
# Reads --PUSH_DOWN_PREDICATE if provided; falls back to last 2 days.
try:
    _pdp_args = getResolvedOptions(sys.argv, ["PUSH_DOWN_PREDICATE"])
    _PUSH_DOWN_PREDICATE = _pdp_args["PUSH_DOWN_PREDICATE"]
except Exception:
    _PUSH_DOWN_PREDICATE = "dt >= date_format(date_sub(current_date(), 2), 'yyyy-MM-dd')"
import logging as _logging
logger      = _logging.getLogger(__name__)
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
# Belt-and-suspenders: return null for any remaining unparseable timestamp
# rather than throwing (Spark 3.3 default is EXCEPTION).
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET   = args["S3_BUCKET"]
CURATED_DB  = args["CURATED_DB"]
RAW_DB      = args["RAW_DB"]
LLM_ENABLED = args.get("LLM_ENABLED", "false").lower() == "true"

CURATED_PATH = f"s3://{S3_BUCKET}/curated/search_enriched/"

# ── Step 1/2: Datetime normalisation (§6.7) ───────────────────────────────────

_AR = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩"   # Arabic-Indic    U+0660–U+0669
    "۰۱۲۳۴۵۶۷۸۹",  # Ext. Arabic-Indic U+06F0–U+06F9
    "01234567890123456789"
)


import re as _re

_DT_RE = _re.compile(
    r'^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?'
)


@udf(StringType())
def clean_datetime(dt_str: str) -> str | None:
    if dt_str is None:
        return None
    s = dt_str.strip().translate(_AR)   # Type 1: Arabic numerals
    if s.startswith("2565"):            # Type 2: Buddhist Era year
        s = "2022" + s[4:]
    # Normalise to YYYY-MM-DD HH:MM:SS.mmm:
    #   - zero-pad single-digit hour (e.g. "6:…" → "06:…")
    #   - truncate ms to 3 digits
    #   - strip trailing timezone suffix (" CH", " ICT", " +07:00", …)
    m = _DT_RE.match(s)
    if not m:
        return None
    date_p, hh, mm, ss, frac = m.groups()
    ms = (frac or "000" + "000")[:3]
    return f"{date_p} {hh.zfill(2)}:{mm}:{ss}.{ms}"


# Load classifier from extra-py-files (genre_classifier.zip bundled with job)
try:
    from genre_classifier.rules import classify_keyword as _classify_kw, \
        bucket_platform as _bucket_platform, \
        normalize_network_type as _norm_nt
except ImportError:
    def _classify_kw(kw): return "UNKNOWN"
    def _bucket_platform(p): return "Other"
    def _norm_nt(n): return "Unknown"


classify_keyword_udf    = udf(_classify_kw,    StringType())
bucket_platform_udf     = udf(_bucket_platform, StringType())
normalize_network_type_udf = udf(_norm_nt,      StringType())


# ── Step 14: Premium flag (§2) ────────────────────────────────────────────────

_PREMIUM_PLANS = {"VIP", "HBO GO+", "K+", "MAX", "MAX XMAS"}


@udf(BooleanType())
def has_premium_udf(plans) -> bool:
    if plans is None:
        return False
    for entry in plans:
        name = entry.split(":")[0].strip() if ":" in entry else entry.strip()
        if name in _PREMIUM_PLANS:
            return True
    return False


@udf(IntegerType())
def subscription_count_udf(plans) -> int | None:
    if plans is None:
        return None       # unauthenticated — null, not 0
    return len(plans)


# ── Bedrock fallback (batch path only, §6.4 / ADR-07) ────────────────────────

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


def _normalize_genre(v) -> str:
    if not isinstance(v, str):
        return "UNKNOWN"
    u = v.upper().strip()
    u = _GENRE_ALIAS.get(u, u)
    return u if u in _VALID_GENRES else "UNKNOWN"


def _load_lut_ext_from_s3(bucket: str, key: str) -> dict:
    """Load lut_extended.json from S3; return {} on any error."""
    import json
    import boto3
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as exc:
        logger.warning("Could not load lut_extended from s3://%s/%s: %s", bucket, key, exc)
        return {}


def _apply_llm_fallback(df):
    """Classify residual UNKNOWN keywords via Amazon Nova Micro.

    Strategy (cost + reproducibility):
    1. Load lut_extended.json from S3 (built offline by scripts/build_extended_lut.py).
       Keywords already in lut_extended get zero LLM calls this run.
    2. Classify only NEW unknown keywords (unseen in lut_extended) via Nova Micro.
    3. Merge new classifications into lut_extended and write back to S3 so they
       are free on subsequent runs (incremental, self-improving LUT).
    """
    import json, os, re, io
    import boto3

    bucket = S3_BUCKET
    lut_ext_key = "glue-scripts/lut_extended.json"

    # Load existing extended LUT from S3
    lut_ext = _load_lut_ext_from_s3(bucket, lut_ext_key)
    logger.info("lut_extended loaded: %d entries", len(lut_ext))

    # Collect all distinct UNKNOWN keywords (top 2000 by frequency)
    all_unknown = [
        r.keyword_norm
        for r in df.filter(
            (col("derived_genre") == "UNKNOWN") & col("keyword_norm").isNotNull()
        ).groupBy("keyword_norm").count().orderBy(F.desc("count")).limit(2000)
        .select("keyword_norm").collect()
    ]
    # Only send keywords NOT already in lut_extended to Nova
    new_kws = [kw for kw in all_unknown if kw not in lut_ext]
    logger.info("UNKNOWN keywords: %d total, %d already in lut_ext, %d to classify",
                len(all_unknown), len(all_unknown) - len(new_kws), len(new_kws))

    lut_updates: dict[str, str] = {}
    if new_kws:
        bedrock = boto3.client("bedrock-runtime", region_name="ap-southeast-1")
        model_id = os.environ.get("BEDROCK_MODEL_ID", "apac.amazon.nova-micro-v1:0")
        prompt_tmpl = (
            "You are a genre classifier for FPT Play, a Vietnamese OTT platform.\n"
            "Classify search keywords into genres. Most keywords are movie/drama titles or partial titles.\n"
            "When a keyword looks like a drama/movie title, prefer a specific genre over UNKNOWN.\n"
            "Genre rules:\n"
            "- PHIM_HAN: Korean dramas/movies, K-drama titles, names like 'oh soo jae', 'jun', 'ji'\n"
            "- PHIM_TRUNG: Chinese dramas/movies, C-drama titles, wuxia, xianxia\n"
            "- PHIM_VIET: Vietnamese dramas/movies, local titles in Vietnamese\n"
            "- PHIM_AU_MY: Hollywood/Western films and series\n"
            "- ANIME: Japanese anime, manga titles\n"
            "- THE_THAO: Sports (football, basketball, esports...)\n"
            "- NHAC: Music, songs, music videos, concerts\n"
            "- TRUYEN_HINH: TV channels, variety/reality shows, live broadcasts\n"
            "- UNKNOWN: Only truly ambiguous (single chars, meta queries like 'voice search')\n"
            "Output: JSON where KEYS=keywords, VALUES=genre codes.\n"
            "Example: {{\"why her?\": \"PHIM_HAN\", \"fairy tail\": \"ANIME\", \"tấm cám\": \"PHIM_VIET\"}}\n"
            "Return ONLY the JSON object.\nKeywords:\n{kws}"
        )
        def _sanitize_kw(kw: str) -> str:
            return re.sub(r"[\x00-\x1f\x7f\\]", " ", kw).strip()

        errors = 0
        for i in range(0, len(new_kws), 50):
            batch = new_kws[i: i + 50]
            batch_set = set(batch)
            # Map sanitized → original to recover keys from Nova's response
            san_to_orig = {}
            for kw in batch:
                s = _sanitize_kw(kw)
                if s and s not in san_to_orig:
                    san_to_orig[s] = kw
            clean_batch = list(san_to_orig.keys())
            try:
                body = json.dumps({
                    "messages": [{"role": "user", "content": [{"text": prompt_tmpl.format(kws=chr(10).join(clean_batch))}]}],
                    "inferenceConfig": {"maxTokens": 1024, "temperature": 0},
                })
                resp = bedrock.invoke_model(
                    modelId=model_id,
                    contentType="application/json",
                    accept="application/json",
                    body=body,
                )
                raw = json.loads(resp["body"].read())["output"]["message"]["content"][0]["text"]
                cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
                parsed = json.loads(cleaned)
                # Detect and fix inverted {genre: keyword} responses
                keys_are_genres = sum(1 for k in parsed if isinstance(k, str) and k.upper() in _VALID_GENRES)
                if keys_are_genres / max(len(parsed), 1) > 0.5:
                    parsed = {v: k for k, v in parsed.items() if isinstance(v, str)}
                for k, v in parsed.items():
                    orig = san_to_orig.get(k, k)
                    genre = _normalize_genre(v)
                    if orig in batch_set and genre != "UNKNOWN":
                        lut_updates[orig] = genre
            except Exception as exc:
                errors += 1
                logger.warning("LLM batch %d/%d failed: %s", i // 50 + 1, (len(new_kws) + 49) // 50, exc)
        logger.info("Nova classified %d new keywords, %d batch errors", len(lut_updates), errors)

        # Write updated lut_extended back to S3 (incremental growth)
        if lut_updates:
            merged = {**lut_ext, **lut_updates}
            try:
                s3 = boto3.client("s3")
                s3.put_object(
                    Bucket=bucket, Key=lut_ext_key,
                    Body=json.dumps(merged, ensure_ascii=False, sort_keys=True).encode("utf-8"),
                    ContentType="application/json",
                )
                logger.info("lut_extended updated on S3: %d → %d entries", len(lut_ext), len(merged))
            except Exception as exc:
                logger.warning("Failed to write lut_extended back to S3: %s", exc)

    # Combined broadcast: lut_ext + new classifications
    combined = {**lut_ext, **lut_updates}
    if not combined:
        return df

    combined_bc = spark.sparkContext.broadcast(combined)

    @udf(StringType())
    def lut_genre_udf(kw: str, current_genre: str) -> str:
        if current_genre != "UNKNOWN" or kw is None:
            return current_genre
        return combined_bc.value.get(kw, "UNKNOWN")

    return df.withColumn(
        "derived_genre",
        lut_genre_udf(col("keyword_norm"), col("derived_genre")),
    )


# ── Pipeline ──────────────────────────────────────────────────────────────────

_DATA_SCHEMA = StructType([
    StructField("eventid",      StringType(), True),
    StructField("datetime",     StringType(), True),
    StructField("user_id",      StringType(), True),
    StructField("keyword",      StringType(), True),
    StructField("category",     StringType(), True),
    StructField("proxy_isp",    StringType(), True),
    StructField("platform",     StringType(), True),
    StructField("networktype",  StringType(), True),
    StructField("action",       StringType(), True),
    StructField("userplansmap", ArrayType(StringType()), True),
])


def run() -> None:
    # Read raw events directly from S3 with explicit schema to avoid duplicate-
    # column conflict: the Parquet files physically embed derived_genre and
    # platform_group as data columns AND the partition paths repeat them.
    # Using an explicit schema (data cols only) forces column projection so Spark
    # reads only the 10 named columns from each file; basePath adds the 4
    # partition columns from the directory structure without overlap.
    raw_path = f"s3://{S3_BUCKET}/raw/events/"
    df = (
        spark.read
        .schema(_DATA_SCHEMA)
        .option("basePath", raw_path)
        .option("mergeSchema", "false")
        .parquet(raw_path)
        .filter(_PUSH_DOWN_PREDICATE)
    )

    # ── Steps 1-2: Datetime clean + cast ─────────────────────────────────────
    df = (
        df
        .withColumn("datetime_clean", clean_datetime(col("datetime")))
        .withColumn(
            "event_ts",
            to_timestamp(col("datetime_clean"), "yyyy-MM-dd HH:mm:ss.SSS"),
        )
    )

    # ── Step 3: Drop corrupt year 0004 rows ──────────────────────────────────
    df = df.filter(year(col("event_ts")) >= 2015)

    # ── Step 4: Vietnam timezone hour ────────────────────────────────────────
    df = df.withColumn(
        "hour_of_day_vn",
        hour(col("event_ts") + expr("interval 7 hours")).cast(IntegerType()),
    )

    # ── Steps 5-6: Session action + abandonment flag ─────────────────────────
    df = (
        df
        .withColumn("session_action", col("category"))
        .withColumn("is_search_abandoned", col("category") == lit("quit"))
    )

    # ── Steps 7-8: Auth flag + hashed user_id ────────────────────────────────
    df = (
        df
        .withColumn("user_is_authenticated", col("user_id").isNotNull())
        .withColumn(
            "user_id_hashed",
            when(col("user_id").isNotNull(), sha2(col("user_id"), 256)).otherwise(lit(None)),
        )
    )

    # ── Step 9: Normalise keyword ─────────────────────────────────────────────
    df = df.withColumn(
        "keyword_norm",
        when(col("keyword").isNotNull(), lower(trim(col("keyword")))).otherwise(lit(None)),
    )

    # ── Step 10: Genre classification → derived_genre ────────────────────────
    df = df.withColumn(
        "derived_genre",
        when(col("keyword_norm").isNotNull(), classify_keyword_udf(col("keyword_norm")))
        .otherwise(lit("UNKNOWN")),
    )
    if LLM_ENABLED:
        df = _apply_llm_fallback(df)

    # ── Step 11: Platform group ───────────────────────────────────────────────
    df = df.withColumn("platform_group", bucket_platform_udf(col("platform")))

    # ── Step 12: Network type normalisation ──────────────────────────────────
    df = df.withColumn("network_type_norm", normalize_network_type_udf(col("networkType")))

    # ── Step 13: ISP segment (ADR-06 — VPN detection removed, all ISPs Vietnamese)
    df = df.withColumn("isp_segment", F.upper(col("proxy_isp")))

    # ── Steps 14-15: Premium flag + subscription count ───────────────────────
    df = (
        df
        .withColumn("has_premium",         has_premium_udf(col("userPlansMap")))
        .withColumn("subscription_count",  subscription_count_udf(col("userPlansMap")))
    )

    # ── Step 16: Search session ID ────────────────────────────────────────────
    unix_ts = col("event_ts").cast("long")
    half_hour_bucket = floor(unix_ts / lit(1800)).cast("string")

    auth_session_input  = F.concat(col("user_id"),    lit("|"), half_hour_bucket)
    anon_session_input  = F.concat(col("proxy_isp"), lit("|"), col("platform"),
                                   lit("|"), half_hour_bucket)

    df = df.withColumn(
        "search_session_id",
        when(
            col("user_id").isNotNull(),
            sha2(auth_session_input, 256),
        ).otherwise(
            sha2(anon_session_input, 256),
        ),
    )

    # ── Step 17: is_repeat_search (LAG over session, 24h window) ─────────────
    # rangeBetween with timestamp ordering fails in Spark 3.3+ (type mismatch
    # between timestamp column and integer offset). rowsBetween(unboundedPreceding, -1)
    # gives identical semantics: all rows physically preceding the current row.
    session_window = (
        Window
        .partitionBy("search_session_id")
        .orderBy(col("event_ts"))
        .rowsBetween(
            Window.unboundedPreceding,
            -1,
        )
    )
    prev_keyword = F.last(
        when(col("keyword_norm").isNotNull(), col("keyword_norm")),
        ignorenulls=True,
    ).over(session_window)

    df = df.withColumn(
        "is_repeat_search",
        when(
            col("keyword_norm").isNull(),
            lit(None).cast(BooleanType()),
        ).otherwise(
            col("keyword_norm") == prev_keyword,
        ),
    )

    # ── Step 18: Cross-partition date flag ────────────────────────────────────
    # dt is a date string like "2022-06-01" or "2565-06-01"; extract year prefix.
    df = df.withColumn(
        "is_cross_partition_date",
        year(col("event_ts")) != col("dt").substr(1, 4).cast(IntegerType()),
    )

    # ── Rename / select output columns ───────────────────────────────────────
    df = df.select(
        col("eventID").alias("event_id"),
        col("event_ts"),
        col("hour_of_day_vn"),
        col("user_id_hashed"),
        col("user_is_authenticated"),
        col("session_action"),
        col("is_search_abandoned"),
        col("keyword_norm"),
        when(col("keyword").isNull(), lit(True)).otherwise(lit(False)).alias("keyword_is_null"),
        col("derived_genre"),
        col("platform_group"),
        col("network_type_norm"),
        col("isp_segment"),
        col("has_premium"),
        col("subscription_count"),
        col("search_session_id"),
        col("is_repeat_search"),
        col("is_cross_partition_date"),
        col("dt"),
    )

    # ── Write curated layer ───────────────────────────────────────────────────
    # Use Spark native writer to avoid Glue DynamicFrame type inference, which
    # tries to parse string columns as timestamps and fails on values with
    # timezone suffixes like "2022-06-01 10:39:27.633 CH".
    (df.write
     .mode("overwrite")
     .option("compression", "snappy")
     .partitionBy("dt", "derived_genre")
     .parquet(CURATED_PATH))


run()
job.commit()
