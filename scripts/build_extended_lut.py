"""
Build lut_extended.json from UNKNOWN keyword CSV via Amazon Nova Micro.

Usage:
    python scripts/build_extended_lut.py \
        --input  C:/tmp/unknown_top20k.csv \
        --output genre_classifier/lut_extended.json \
        --s3-upload  s3://ott-search-703668403514-demo/glue-scripts/lut_extended.json \
        [--limit 20000] [--batch 50] [--region ap-southeast-1]

Input CSV: two columns "keyword_norm","cnt" (header row, sorted by cnt desc).
Existing lut_extended.json is loaded first and only NEW keywords are sent to the LLM,
making reruns incremental (cost-free for already-classified entries).
"""
import argparse
import csv
import json
import os
import re
import sys
import time

import boto3

# ── Taxonomy ─────────────────────────────────────────────────────────────────

VALID_GENRES = {
    "NHAC", "THE_THAO", "ANIME", "PHIM_TRUNG", "PHIM_VIET",
    "PHIM_AU_MY", "PHIM_HAN", "TRUYEN_HINH", "UNKNOWN",
}

ALIAS = {
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
    "PHIM_THAI": "UNKNOWN",
}

PROMPT_SYSTEM = """\
You are a genre classifier for FPT Play, a Vietnamese OTT streaming platform.
Most keywords are movie/drama titles or partial titles — classify them by their most likely genre.
When a keyword looks like a drama or movie title, prefer a specific genre over UNKNOWN.
Genre definitions:
- PHIM_TRUNG   : Chinese drama / movie titles, wuxia, historical Chinese, C-drama
- PHIM_VIET    : Vietnamese drama / movie titles, local content in Vietnamese
- PHIM_HAN     : Korean drama / movie (K-drama) titles, Korean names, Korean variety
- PHIM_AU_MY   : Western / Hollywood / American / European films and series
- ANIME        : Japanese anime, manga-based animation, Japanese drama
- THE_THAO     : Sports (football, basketball, esports, tennis …)
- NHAC         : Music, songs, music videos, concerts, K-pop, V-pop
- TRUYEN_HINH  : TV channels (VTV, HTV …), reality / variety shows, live broadcast
- UNKNOWN      : ONLY truly unclassifiable: single characters, meta-queries like "voice search", gibberish
"""

PROMPT_TEMPLATE = """\
Task: classify Vietnamese OTT search keywords into genres.

Output format: JSON object where KEYS are the original keywords and VALUES are genres.
IMPORTANT: keys must be the exact keywords, values must be genre codes.

Correct example:
{{"fairy tail": "ANIME", "why her?": "PHIM_HAN", "tấm cám": "PHIM_VIET", "bong da": "THE_THAO"}}

Wrong format (do NOT do this):
{{"ANIME": "fairy tail"}}

Genre codes: PHIM_TRUNG PHIM_VIET PHIM_HAN PHIM_AU_MY ANIME THE_THAO NHAC TRUYEN_HINH UNKNOWN

Keywords to classify:
{keywords}

Return ONLY the JSON object, no explanation."""


def normalize(v: str) -> str:
    if not isinstance(v, str):
        return "UNKNOWN"
    u = v.upper().strip()
    u = ALIAS.get(u, u)
    return u if u in VALID_GENRES else "UNKNOWN"


def _detect_and_fix_inverted(parsed: dict, batch_set: set) -> dict:
    """If Nova returns {genre: keyword} instead of {keyword: genre}, flip it."""
    if not parsed:
        return parsed
    # Check if most keys look like genre codes
    keys_are_genres = sum(1 for k in parsed if k.upper() in VALID_GENRES) / len(parsed)
    if keys_are_genres > 0.5:
        # Inverted: {genre: keyword} → {keyword: genre}
        return {v: k for k, v in parsed.items() if isinstance(v, str)}
    return parsed


def _sanitize_for_prompt(kw: str) -> str:
    """Strip control chars and backslashes that break Nova's JSON output."""
    return re.sub(r"[\x00-\x1f\x7f\\]", " ", kw).strip()


def classify_batch(bedrock, model_id: str, batch: list[str]) -> dict[str, str]:
    # Map sanitized → original so we can look up the original key in the result
    sanitized_to_orig: dict[str, str] = {}
    for kw in batch:
        s = _sanitize_for_prompt(kw)
        if s and s not in sanitized_to_orig:
            sanitized_to_orig[s] = kw
    clean_batch = list(sanitized_to_orig.keys())

    prompt = PROMPT_TEMPLATE.format(keywords="\n".join(clean_batch))
    body = json.dumps({
        "messages": [{"role": "user", "content": [{"text": PROMPT_SYSTEM + "\n" + prompt}]}],
        "inferenceConfig": {"maxTokens": 2048, "temperature": 0},
    })
    try:
        resp = bedrock.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=body,
        )
        raw = json.loads(resp["body"].read())["output"]["message"]["content"][0]["text"]
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
        parsed = json.loads(cleaned)
        parsed = _detect_and_fix_inverted(parsed, set(clean_batch))
        result = {}
        for k, v in parsed.items():
            orig = sanitized_to_orig.get(k, k)
            genre = normalize(v)
            if orig in batch and genre != "UNKNOWN":
                result[orig] = genre
        return result
    except Exception as exc:
        print(f"  [WARN] batch failed: {exc}", file=sys.stderr)
        return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",     required=True)
    ap.add_argument("--output",    required=True)
    ap.add_argument("--s3-upload", default="")
    ap.add_argument("--limit",     type=int, default=20000)
    ap.add_argument("--batch",     type=int, default=50)
    ap.add_argument("--region",    default="ap-southeast-1")
    ap.add_argument("--model",     default="apac.amazon.nova-micro-v1:0")
    args = ap.parse_args()

    # Load existing LUT (incremental reruns)
    existing: dict[str, str] = {}
    if os.path.exists(args.output):
        with open(args.output, encoding="utf-8") as f:
            existing = json.load(f)
        print(f"Loaded {len(existing)} existing entries from {args.output}")

    # Read input keywords
    keywords: list[str] = []
    with open(args.input, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            kw = row.get("keyword_norm", "").strip().strip('"')
            if kw and kw not in existing:
                keywords.append(kw)
            if len(keywords) >= args.limit:
                break
    print(f"New keywords to classify: {len(keywords)} (skipped {len(existing)} already in LUT)")

    bedrock = boto3.client("bedrock-runtime", region_name=args.region)
    lut = dict(existing)
    total_batches = (len(keywords) + args.batch - 1) // args.batch
    classified = 0
    errors = 0

    for i in range(0, len(keywords), args.batch):
        batch = keywords[i: i + args.batch]
        batch_num = i // args.batch + 1
        result = classify_batch(bedrock, args.model, batch)
        lut.update(result)
        classified += len(result)
        if not result:
            errors += 1
        if batch_num % 20 == 0 or batch_num == total_batches:
            pct = (i + len(batch)) / len(keywords) * 100
            print(f"  [{batch_num}/{total_batches}] {pct:.0f}% — classified so far: {classified}, errors: {errors}")
        # Throttle: Nova Micro allows ~100 RPM on-demand
        time.sleep(0.7)

    # Write output
    # Exclude UNKNOWN entries (not worth storing)
    clean_lut = {k: v for k, v in lut.items() if v != "UNKNOWN"}
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(clean_lut, f, ensure_ascii=False, indent=2, sort_keys=True)
    print(f"\nWrote {len(clean_lut)} entries to {args.output}")

    # Upload to S3 if requested
    if args.s3_upload:
        import subprocess
        subprocess.run(
            ["aws", "s3", "cp", args.output, args.s3_upload,
             "--region", args.region],
            check=True,
        )
        print(f"Uploaded to {args.s3_upload}")


if __name__ == "__main__":
    main()
