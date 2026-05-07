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
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET   = args["S3_BUCKET"]
CURATED_DB  = args["CURATED_DB"]
RAW_DB      = args["RAW_DB"]
LLM_ENABLED = args.get("LLM_ENABLED", "false").lower() == "true"

CURATED_PATH = f"s3://{S3_BUCKET}/curated/search_enriched/"

# ── Step 1/2: Datetime normalisation (§6.7) ───────────────────────────────────

_AR = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


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


# ── LLM fallback (batch path only, §6.4 / ADR-07) ────────────────────────────

def _apply_llm_fallback(df):
    """Call Gemini for rows where category_norm = 'UNKNOWN'. Batch of 20."""
    # Collect distinct UNKNOWN keywords (bounded — long tail only)
    unknown_kws = [
        r.keyword_norm
        for r in df.filter(
            (col("derived_genre") == "UNKNOWN") & col("keyword_norm").isNotNull()
        ).select("keyword_norm").distinct().limit(2000).collect()
    ]
    if not unknown_kws:
        return df

    # Build batches of 20 and call Gemini
    import json, os
    import google.generativeai as genai  # in Lambda layer / Glue extra lib

    api_key = _get_secret("ott-gemini-api-key")  # from Secrets Manager
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")

    genres = [
        "NHAC", "THE_THAO", "ANIME", "PHIM_TRUNG",
        "PHIM_VIET", "PHIM_AU_MY", "TRUYEN_HINH", "UNKNOWN",
    ]
    lut_updates: dict[str, str] = {}

    for i in range(0, len(unknown_kws), 20):
        batch = unknown_kws[i: i + 20]
        prompt = (
            "Classify each Vietnamese OTT search keyword into exactly one genre: "
            f"{genres}. Return JSON only: {{\"keyword\": \"genre\"}}.\n"
            "Keywords:\n" + "\n".join(batch)
        )
        try:
            resp = model.generate_content(prompt)
            parsed = json.loads(resp.text)
            lut_updates.update(parsed)
        except Exception:
            pass  # LLM errors degrade gracefully to UNKNOWN

    if not lut_updates:
        return df

    # Create a broadcast map and apply corrections
    lut_bc = spark.sparkContext.broadcast(lut_updates)

    @udf(StringType())
    def llm_genre_udf(kw: str, current_genre: str) -> str:
        if current_genre != "UNKNOWN" or kw is None:
            return current_genre
        return lut_bc.value.get(kw, "UNKNOWN")

    return df.withColumn(
        "derived_genre",
        llm_genre_udf(col("keyword_norm"), col("derived_genre")),
    )


def _get_secret(secret_name: str) -> str:
    import boto3
    client = boto3.client("secretsmanager")
    return client.get_secret_value(SecretId=secret_name)["SecretString"]


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
