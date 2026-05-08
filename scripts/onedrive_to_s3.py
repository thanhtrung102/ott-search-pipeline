"""
onedrive_to_s3.py — Download parquet files from a OneDrive shared folder
and upload each one to S3 under raw-source/log_search/{date}/.

Date assignment: reads each parquet file's datetime column and uses the
modal date (most-common day) to determine the S3 partition date.
Falls back to sequential assignment if datetime parsing fails.

Usage:
    python onedrive_to_s3.py \
        --share-url "https://1drv.ms/f/..." \
        --bucket ott-search-703668403514-demo \
        --region ap-southeast-1 \
        [--s3-prefix raw-source/log_search] \
        [--staging-dir C:/tmp] \
        [--dry-run]
"""
import argparse
import base64
import io
import logging
import os
import sys
import tempfile
from collections import Counter
from pathlib import Path

import boto3
import pyarrow.parquet as pq
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


def _share_id(url: str) -> str:
    """Encode a OneDrive share URL as a Graph API share ID."""
    encoded = base64.urlsafe_b64encode(("u!" + url).encode()).rstrip(b"=").decode()
    return encoded


def list_drive_items(share_url: str) -> list[dict]:
    """Return all file items in a OneDrive shared folder."""
    sid = _share_id(share_url)
    items = []
    url = f"{GRAPH_BASE}/shares/{sid}/driveItem/children?$select=name,size,@microsoft.graph.downloadUrl"
    while url:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 401:
            log.error("OneDrive folder requires authentication — make sure the link is set to 'Anyone with the link'")
            sys.exit(1)
        resp.raise_for_status()
        data = resp.json()
        items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return items


def _modal_date(buf: bytes) -> str | None:
    """Read parquet bytes and return the modal dt=YYYY-MM-DD from datetime column."""
    try:
        table = pq.read_table(io.BytesIO(buf), columns=["datetime"])
        dates = []
        for val in table.column("datetime").to_pylist():
            if val and len(str(val)) >= 10:
                dates.append(str(val)[:10].replace("-", ""))
        if dates:
            return Counter(dates).most_common(1)[0][0]
    except Exception as exc:
        log.warning("Could not parse datetime from parquet: %s", exc)
    return None


def run(share_url: str, bucket: str, region: str, s3_prefix: str,
        staging_dir: Path, dry_run: bool) -> None:
    log.info("Listing files in shared OneDrive folder...")
    items = list_drive_items(share_url)
    parquet_items = [i for i in items if i["name"].endswith(".parquet") or ".snappy.parquet" in i["name"]]
    log.info("Found %d parquet file(s)", len(parquet_items))

    s3 = boto3.client("s3", region_name=region)
    results = []

    for item in parquet_items:
        name = item["name"]
        download_url = item.get("@microsoft.graph.downloadUrl")
        if not download_url:
            log.warning("No download URL for %s — skipping", name)
            continue

        log.info("Downloading %s (%.2f MB)...", name, item.get("size", 0) / 1_048_576)
        resp = requests.get(download_url, timeout=120)
        resp.raise_for_status()
        buf = resp.content

        date_str = _modal_date(buf)
        if not date_str:
            log.warning("Could not determine date for %s — skipping", name)
            continue

        local_dir = staging_dir / date_str
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = local_dir / name
        local_path.write_bytes(buf)
        log.info("Staged → %s", local_path)

        s3_key = f"{s3_prefix}/{date_str}/{name}"
        if dry_run:
            log.info("[DRY RUN] Would upload to s3://%s/%s", bucket, s3_key)
        else:
            s3.upload_file(str(local_path), bucket, s3_key)
            log.info("Uploaded → s3://%s/%s", bucket, s3_key)

        results.append({"file": name, "date": date_str, "s3_key": s3_key, "size_mb": round(len(buf) / 1_048_576, 2)})

    print("\n── Summary ──────────────────────────────────────")
    for r in sorted(results, key=lambda x: x["date"]):
        status = "[DRY RUN]" if dry_run else "✓"
        print(f"  {status} {r['date']}  {r['file'][:20]}...  {r['size_mb']} MB")
    print(f"\n  Total: {len(results)} file(s) processed")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--share-url", required=True, help="OneDrive shared folder URL")
    ap.add_argument("--bucket", default="ott-search-703668403514-demo")
    ap.add_argument("--region", default="ap-southeast-1")
    ap.add_argument("--s3-prefix", default="raw-source/log_search")
    ap.add_argument("--staging-dir", default="C:/tmp", type=Path)
    ap.add_argument("--dry-run", action="store_true", help="Download and stage only, skip S3 upload")
    args = ap.parse_args()

    run(
        share_url=args.share_url,
        bucket=args.bucket,
        region=args.region,
        s3_prefix=args.s3_prefix,
        staging_dir=args.staging_dir,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
