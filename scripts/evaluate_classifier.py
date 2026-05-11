"""
Evaluate genre classifier quality against Amazon Nova Micro as oracle.

Pulls every distinct (keyword_norm, derived_genre, search_count) from the
Athena gold layer, Nova-judges each keyword independently, then computes:
  - Per-genre precision / recall / F1 (count-weighted and volume-weighted)
  - Full 9×9 confusion matrix
  - Top misclassified keywords ranked by search volume
  - Overall volume-weighted accuracy headline

Nova results are cached to --cache so re-runs against updated Athena data
never re-classify already-judged keywords (cost = $0 for cached entries).

Usage:
    python scripts/evaluate_classifier.py \\
        [--workgroup ott-analytics-demo] \\
        [--region ap-southeast-1] \\
        [--cache C:/tmp/eval_cache.json] \\
        [--out   C:/tmp/eval_report.csv] \\
        [--batch 75] \\
        [--include-unknown]   # also evaluate UNKNOWN-labelled keywords
"""
import argparse
import csv
import json
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).parent.parent))

GENRES = [
    "THE_THAO", "NHAC", "ANIME", "PHIM_HAN",
    "PHIM_AU_MY", "PHIM_TRUNG", "PHIM_VIET", "TRUYEN_HINH", "UNKNOWN",
]

_VALID_GENRES = frozenset(GENRES)
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

_NOVA_JUDGE_PROMPT = """\
You are an expert genre classifier for FPT Play, a Vietnamese OTT platform.
For each keyword below, state the single most likely genre — independent of any
existing label. Most keywords are movie/drama titles or partial titles.

Genre codes (use EXACTLY these strings):
  PHIM_HAN    Korean drama / movie titles, K-drama
  PHIM_TRUNG  Chinese drama / movie titles, wuxia, C-drama
  PHIM_VIET   Vietnamese drama / movie, local content
  PHIM_AU_MY  Western / Hollywood / European films and series
  ANIME       Japanese anime, manga-based animation
  THE_THAO    Sports (football, basketball, esports, tennis …)
  NHAC        Music, songs, music videos, concerts, K-pop, V-pop
  TRUYEN_HINH TV channels (VTV, HTV …), reality/variety shows, live broadcast
  UNKNOWN     Only truly unclassifiable: single characters, gibberish,
              meta-queries like "voice search"

Output: a single JSON object where KEYS are the exact keywords and VALUES are
genre codes. Return ONLY the JSON — no explanation.

Example:
{{"fairy tail": "ANIME", "why her?": "PHIM_HAN", "tấm cám": "PHIM_VIET"}}

Keywords:
{keywords}"""

_SANITIZE_RE = re.compile(r"[\x00-\x1f\x7f\\]")


def _normalize(v: str) -> str:
    if not isinstance(v, str):
        return "UNKNOWN"
    u = v.upper().strip()
    u = _GENRE_ALIAS.get(u, u)
    return u if u in _VALID_GENRES else "UNKNOWN"


# ── Athena helpers ────────────────────────────────────────────────────────────

def _wait_athena(athena, qid: str, poll_sec: float = 3.0) -> str:
    """Poll until query finishes. Returns 'SUCCEEDED' or raises."""
    while True:
        r = athena.get_query_execution(QueryExecutionId=qid)
        state = r["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return state
        if state in ("FAILED", "CANCELLED"):
            reason = r["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(poll_sec)


def _paginate_results(athena, qid: str) -> list[dict]:
    """Fetch all rows from a completed Athena query."""
    rows: list[dict] = []
    paginator = athena.get_paginator("get_query_results")
    headers: list[str] = []
    for page_num, page in enumerate(paginator.paginate(QueryExecutionId=qid)):
        result_rows = page["ResultSet"]["Rows"]
        if page_num == 0:
            headers = [c["VarCharValue"] for c in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            values = [c.get("VarCharValue", "") for c in row["Data"]]
            rows.append(dict(zip(headers, values)))
    return rows


def fetch_keywords(athena, workgroup: str, include_unknown: bool) -> list[dict]:
    """Query Athena for distinct (keyword_norm, derived_genre, total_cnt)."""
    genre_filter = "" if include_unknown else "AND derived_genre <> 'UNKNOWN'"
    sql = f"""
SELECT
    keyword_norm,
    derived_genre,
    SUM(search_count) AS total_cnt
FROM ott_search_gold.keyword_trends
WHERE keyword_norm IS NOT NULL
  AND LENGTH(keyword_norm) >= 2
  {genre_filter}
GROUP BY keyword_norm, derived_genre
ORDER BY total_cnt DESC
"""
    print("Querying Athena for classified keywords …")
    resp = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        ResultConfiguration={
            "OutputLocation": f"s3://ott-search-703668403514-demo/athena-results/"
        },
    )
    qid = resp["QueryExecutionId"]
    _wait_athena(athena, qid)
    rows = _paginate_results(athena, qid)

    # A keyword can appear with multiple derived_genre values (different run dates).
    # Keep only the genre with the highest total_cnt per keyword.
    best: dict[str, dict] = {}
    for row in rows:
        kw   = row["keyword_norm"]
        cnt  = int(row["total_cnt"] or 0)
        genre = row["derived_genre"]
        if kw not in best or cnt > best[kw]["cnt"]:
            best[kw] = {"keyword_norm": kw, "derived_genre": genre, "cnt": cnt}

    result = list(best.values())
    print(f"Fetched {len(result):,} distinct keywords "
          f"({sum(r['cnt'] for r in result):,} total searches)")
    return result


# ── Nova judging ──────────────────────────────────────────────────────────────

def nova_judge_batch(bedrock, model_id: str, keywords: list[str]) -> dict[str, str]:
    """Ask Nova to independently classify a batch. Returns {kw: genre}."""
    san_to_orig: dict[str, str] = {}
    for kw in keywords:
        s = _SANITIZE_RE.sub(" ", kw).strip()
        if s:
            san_to_orig.setdefault(s, kw)
    clean = list(san_to_orig)

    body = json.dumps({
        "messages": [{
            "role": "user",
            "content": [{"text": _NOVA_JUDGE_PROMPT.format(keywords="\n".join(clean))}],
        }],
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
        # Detect and fix inverted {genre: keyword} Nova responses
        if parsed and sum(1 for k in parsed if isinstance(k, str) and k.upper() in _VALID_GENRES) / len(parsed) > 0.5:
            parsed = {v: k for k, v in parsed.items() if isinstance(v, str)}
        kw_set = set(keywords)
        return {
            san_to_orig.get(k, k): _normalize(v)
            for k, v in parsed.items()
            if san_to_orig.get(k, k) in kw_set
        }
    except Exception as exc:
        print(f"  [WARN] Nova batch failed: {exc}", file=sys.stderr)
        return {}


def judge_all(bedrock, model_id: str, keywords: list[str], cache_path: str,
              batch_size: int) -> dict[str, str]:
    """Nova-judge all keywords, loading/saving cache to avoid re-calls."""
    cache: dict[str, str] = {}
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            cache = json.load(f)
        print(f"Loaded {len(cache):,} cached Nova judgments from {cache_path}")

    new_kws = [kw for kw in keywords if kw not in cache]
    total_batches = (len(new_kws) + batch_size - 1) // batch_size
    print(f"Judging {len(new_kws):,} new keywords "
          f"({len(keywords) - len(new_kws):,} already cached) …")

    for i in range(0, len(new_kws), batch_size):
        batch = new_kws[i:i + batch_size]
        result = nova_judge_batch(bedrock, model_id, batch)
        cache.update(result)
        # Keywords not returned by Nova → mark UNKNOWN
        for kw in batch:
            if kw not in cache:
                cache[kw] = "UNKNOWN"
        batch_num = i // batch_size + 1
        if batch_num % 10 == 0 or batch_num == total_batches:
            print(f"  [{batch_num}/{total_batches}] {batch_num * batch_size}/{len(new_kws)} keywords")
        time.sleep(0.7)

    # Persist cache
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)
    print(f"Saved {len(cache):,} judgments → {cache_path}")
    return cache


# ── Metrics ───────────────────────────────────────────────────────────────────

def compute_metrics(rows: list[dict], nova_cache: dict[str, str]):
    """
    Returns:
      comparisons  list[dict] — per-keyword comparison rows
      conf_matrix  dict[rules_genre][nova_genre] = {count, volume}
      per_genre    dict[genre] = {tp, fp, fn, tp_vol, fp_vol, fn_vol}
    """
    comparisons: list[dict] = []
    conf_matrix: dict[str, dict[str, dict]] = {
        g: {g2: {"count": 0, "volume": 0} for g2 in GENRES}
        for g in GENRES
    }
    per_genre: dict[str, dict] = {
        g: {"tp": 0, "fp": 0, "fn": 0, "tp_vol": 0, "fp_vol": 0, "fn_vol": 0}
        for g in GENRES
    }

    for row in rows:
        kw         = row["keyword_norm"]
        rules_g    = row["derived_genre"]
        nova_g     = nova_cache.get(kw, "UNKNOWN")
        cnt        = row["cnt"]
        agree      = rules_g == nova_g

        comparisons.append({
            "keyword_norm":  kw,
            "rules_genre":   rules_g,
            "nova_genre":    nova_g,
            "agree":         agree,
            "search_count":  cnt,
        })

        # Confusion matrix cell: rows = rules prediction, cols = Nova (oracle)
        conf_matrix[rules_g][nova_g]["count"]  += 1
        conf_matrix[rules_g][nova_g]["volume"] += cnt

        # Per-genre TP / FP / FN using Nova as ground truth
        if agree:
            per_genre[rules_g]["tp"]     += 1
            per_genre[rules_g]["tp_vol"] += cnt
        else:
            # rules said rules_g but Nova says nova_g
            per_genre[rules_g]["fp"]     += 1   # false positive for rules_g
            per_genre[rules_g]["fp_vol"] += cnt
            per_genre[nova_g]["fn"]      += 1   # false negative for nova_g
            per_genre[nova_g]["fn_vol"]  += cnt

    return comparisons, conf_matrix, per_genre


# ── Reporting ─────────────────────────────────────────────────────────────────

def _prf(tp, fp, fn) -> tuple[float, float, float]:
    p = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f = 2 * p * r / (p + r) if (p + r) > 0 else 0.0
    return p, r, f


def print_report(comparisons: list[dict], conf_matrix, per_genre):
    total_count  = len(comparisons)
    total_vol    = sum(r["search_count"] for r in comparisons)
    agree_count  = sum(1 for r in comparisons if r["agree"])
    agree_vol    = sum(r["search_count"] for r in comparisons if r["agree"])

    print("\n" + "═" * 72)
    print("  GENRE CLASSIFIER EVALUATION  (Nova Micro as oracle)")
    print("═" * 72)
    print(f"  Keywords evaluated : {total_count:>7,}")
    print(f"  Total search volume: {total_vol:>7,}")
    print(f"  Count accuracy     : {agree_count / total_count * 100:>6.1f}%  "
          f"({agree_count:,} / {total_count:,} keywords)")
    print(f"  Volume accuracy    : {agree_vol / total_vol * 100:>6.1f}%  "
          f"({agree_vol:,} / {total_vol:,} searches)")

    # ── Per-genre table ───────────────────────────────────────────────────────
    print("\n  PER-GENRE METRICS\n")
    hdr = f"  {'Genre':<14} {'Precision':>10} {'Recall':>8} {'F1':>8}  {'Support':>8}  {'Vol-Prec':>10}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    genre_rows = []
    for g in GENRES:
        s = per_genre[g]
        p,  r,  f  = _prf(s["tp"],     s["fp"],     s["fn"])
        pv, rv, fv = _prf(s["tp_vol"], s["fp_vol"], s["fn_vol"])
        support = s["tp"] + s["fn"]
        genre_rows.append((g, p, r, f, pv, rv, fv, support))
        print(f"  {g:<14} {p*100:>9.1f}% {r*100:>7.1f}% {f*100:>7.1f}%  "
              f"{support:>8,}  {pv*100:>9.1f}%")

    # macro-F1
    macro_f1 = sum(row[4] for row in genre_rows) / len(genre_rows)
    print(f"\n  Macro F1 (volume-weighted): {macro_f1 * 100:.1f}%")

    # ── Confusion matrix (counts) ─────────────────────────────────────────────
    active_genres = [g for g in GENRES if any(
        conf_matrix[g][g2]["count"] > 0 or conf_matrix[g2][g]["count"] > 0
        for g2 in GENRES
    )]
    print("\n  CONFUSION MATRIX (keyword count)  rows=rules  cols=nova\n")
    col_w = 10
    header = "  " + f"{'':14}" + "".join(f"{g[:8]:>{col_w}}" for g in active_genres)
    print(header)
    print("  " + "-" * (14 + col_w * len(active_genres) + 2))
    for rg in active_genres:
        row_str = f"  {rg:<14}"
        for cg in active_genres:
            v = conf_matrix[rg][cg]["count"]
            marker = "◆" if rg == cg and v > 0 else ""
            cell = f"{v}{marker}" if v > 0 else "."
            row_str += f"{cell:>{col_w}}"
        print(row_str)

    # ── Top misclassifications by volume ──────────────────────────────────────
    wrong = [r for r in comparisons if not r["agree"]]
    wrong.sort(key=lambda x: -x["search_count"])
    print(f"\n  TOP 30 MISCLASSIFICATIONS BY SEARCH VOLUME\n")
    print(f"  {'Keyword':<40} {'Rules':>12} {'Nova':>12} {'Searches':>10}")
    print("  " + "-" * 78)
    for row in wrong[:30]:
        kw = row["keyword_norm"][:39]
        print(f"  {kw:<40} {row['rules_genre']:>12} {row['nova_genre']:>12} "
              f"{row['search_count']:>10,}")
    if len(wrong) > 30:
        remaining_vol = sum(r["search_count"] for r in wrong[30:])
        print(f"  … {len(wrong) - 30} more misclassified keywords "
              f"({remaining_vol:,} searches)")

    print("\n" + "═" * 72)


def write_csv(comparisons: list[dict], out_path: str):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "keyword_norm", "rules_genre", "nova_genre", "agree", "search_count"
        ])
        w.writeheader()
        w.writerows(sorted(comparisons, key=lambda r: -r["search_count"]))
    print(f"\nFull results → {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workgroup",       default="ott-analytics-demo")
    ap.add_argument("--region",          default="ap-southeast-1")
    ap.add_argument("--model",           default="apac.amazon.nova-micro-v1:0")
    ap.add_argument("--cache",           default="C:/tmp/eval_cache.json")
    ap.add_argument("--out",             default="C:/tmp/eval_report.csv")
    ap.add_argument("--batch",           type=int, default=75)
    ap.add_argument("--include-unknown", action="store_true",
                    help="Also evaluate UNKNOWN-labelled keywords")
    args = ap.parse_args()

    athena  = boto3.client("athena",           region_name=args.region)
    bedrock = boto3.client("bedrock-runtime",  region_name=args.region)

    # 1. Pull all distinct keywords from Athena gold layer
    keyword_rows = fetch_keywords(athena, args.workgroup, args.include_unknown)

    # 2. Nova-judge every keyword (cached)
    keywords = [r["keyword_norm"] for r in keyword_rows]
    nova_cache = judge_all(bedrock, args.model, keywords, args.cache, args.batch)

    # 3. Compute metrics
    comparisons, conf_matrix, per_genre = compute_metrics(keyword_rows, nova_cache)

    # 4. Print report
    print_report(comparisons, conf_matrix, per_genre)

    # 5. Write full CSV
    write_csv(comparisons, args.out)


if __name__ == "__main__":
    main()
