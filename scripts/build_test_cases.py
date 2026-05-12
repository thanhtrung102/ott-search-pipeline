"""
Build a stable ground-truth test set from the full classification dataset.

Strategy
--------
  Agree  (rules == nova, 1340 keywords): ground_truth = that label, confidence=high.
  Dispute (rules != nova,  616 keywords): adjudicated by Claude Haiku via Bedrock.
    - Haiku is shown the keyword + both candidate labels and must pick one (or a
      third genre if both are wrong).
    - Results cached to --cache so reruns are free.

Output: tests/test_cases.json
  [{"keyword_norm": ..., "expected_genre": ..., "search_count": ...,
    "confidence": "high"|"adjudicated", "source": "agree"|"haiku"}]

Usage:
    python scripts/build_test_cases.py \\
        [--report  C:/tmp/eval_report_v2.csv] \\
        [--cache   C:/tmp/adjudication_cache.json] \\
        [--out     tests/test_cases.json] \\
        [--model   apac.anthropic.claude-haiku-3-5-20241022-v2:0] \\
        [--batch   40] \\
        [--region  ap-southeast-1]
"""
import argparse
import csv
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from genre_classifier.rules import VALID_GENRES, normalize_genre  # noqa: E402

_ADJUDICATE_PROMPT = """\
You are an expert at classifying Vietnamese OTT (FPT Play) search keywords into genres.
Each line shows a keyword and EXACTLY TWO candidate genres that disagree.
Pick the MORE ACCURATE genre for each keyword. You MUST choose one of the two candidates.
Only provide a third genre if BOTH candidates are clearly, unambiguously wrong.

Critical rules:
- Generic music terms (bolero, trữ tình, nhạc vàng, remix) → always NHAC.
- Hollywood titles (harry potter, avengers, justice league, batman) → always PHIM_AU_MY.
- Vietnamese titles of Korean dramas → PHIM_HAN (classify by origin, not language of title).
- Vietnamese titles of Chinese dramas → PHIM_TRUNG (classify by origin).
- Variety/reality shows and TV channels → TRUYEN_HINH.

Input:  keyword  |  candidate_A  |  candidate_B
Output: JSON object where keys=exact keywords, values=chosen genre code.
Return ONLY the JSON object — no explanation.

Keywords:
{items}"""

_SANITIZE_RE = re.compile(r"[\x00-\x1f\x7f\\]")
_CODEBLOCK_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


def _parse_response(raw: str, san_to_orig: dict, kw_set: set) -> dict[str, str]:
    cleaned = _CODEBLOCK_RE.sub("", raw.strip())
    parsed  = json.loads(cleaned)
    if parsed and (
        sum(1 for k in parsed if isinstance(k, str) and k.upper() in VALID_GENRES)
        / max(len(parsed), 1) > 0.5
    ):
        parsed = {v: k for k, v in parsed.items() if isinstance(v, str)}
    return {
        san_to_orig.get(k, k): normalize_genre(v)
        for k, v in parsed.items()
        if san_to_orig.get(k, k) in kw_set
    }


def _call_anthropic(client, model_id: str, prompt: str,
                    san_to_orig: dict, kw_set: set) -> dict[str, str]:
    try:
        msg = client.messages.create(
            model=model_id,
            max_tokens=2048,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        return _parse_response(msg.content[0].text, san_to_orig, kw_set)
    except Exception as exc:
        print(f"  [WARN] Anthropic batch failed: {exc}", file=sys.stderr)
        return {}


def _call_bedrock(bedrock, model_id: str, prompt: str,
                  san_to_orig: dict, kw_set: set) -> dict[str, str]:
    body = {
        "inferenceConfig": {"maxTokens": 2048, "temperature": 0},
        "messages": [{"role": "user", "content": [{"text": prompt}]}],
    }
    try:
        resp = bedrock.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(body),
        )
        raw = json.loads(resp["body"].read())["output"]["message"]["content"][0]["text"]
        return _parse_response(raw, san_to_orig, kw_set)
    except Exception as exc:
        print(f"  [WARN] Bedrock batch failed: {exc}", file=sys.stderr)
        return {}


def _adjudicate_batch(client, model_id: str, provider: str,
                      disputes: list[dict]) -> dict[str, str]:
    san_to_orig: dict[str, str] = {}
    lines: list[str] = []
    kw_set: set[str] = set()
    for d in disputes:
        kw = d["keyword_norm"]
        kw_set.add(kw)
        s = _SANITIZE_RE.sub(" ", kw).strip()
        if s:
            san_to_orig.setdefault(s, kw)
            lines.append(f"{s}  |  {d['rules_genre']}  |  {d['nova_genre']}")

    prompt = _ADJUDICATE_PROMPT.format(items="\n".join(lines))

    if provider == "anthropic":
        result = _call_anthropic(client, model_id, prompt, san_to_orig, kw_set)
    else:
        result = _call_bedrock(client, model_id, prompt, san_to_orig, kw_set)

    rules_map = {d["keyword_norm"]: d["rules_genre"] for d in disputes}
    return {kw: result.get(kw, rules_map[kw]) for kw in kw_set}


def adjudicate_all(client, model_id: str, provider: str,
                   disputes: list[dict],
                   cache_path: str, batch_size: int) -> dict[str, str]:
    cache: dict[str, str] = {}
    try:
        with open(cache_path, encoding="utf-8") as f:
            cache = json.load(f)
        print(f"Loaded {len(cache):,} cached adjudications from {cache_path}")
    except FileNotFoundError:
        pass

    new   = [d for d in disputes if d["keyword_norm"] not in cache]
    total = (len(new) + batch_size - 1) // batch_size
    print(f"Adjudicating {len(new):,} new disputes "
          f"({len(disputes) - len(new):,} cached) via {provider}/{model_id} …")

    for i in range(0, len(new), batch_size):
        batch    = new[i:i + batch_size]
        result   = _adjudicate_batch(client, model_id, provider, batch)
        cache.update(result)
        batch_num = i // batch_size + 1
        if batch_num % 5 == 0 or batch_num == total:
            print(f"  [{batch_num}/{total}]  {min(i + batch_size, len(new))}/{len(new)}")
        if i + batch_size < len(new):
            time.sleep(0.3)

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)
    print(f"Saved {len(cache):,} adjudications → {cache_path}")
    return cache


def build(rows: list[dict], adj_cache: dict[str, str]) -> list[dict]:
    cases: list[dict] = []
    for row in rows:
        kw    = row["keyword_norm"]
        cnt   = int(row["search_count"])
        agree = row["agree"] == "True"

        if agree:
            confidence, source, genre = "high", "agree", row["rules_genre"]
        else:
            genre = adj_cache.get(kw)
            if not genre or genre not in VALID_GENRES:
                genre = row["rules_genre"]
            confidence, source = "adjudicated", "haiku"

        cases.append({
            "keyword_norm":   kw,
            "expected_genre": genre,
            "search_count":   cnt,
            "confidence":     confidence,
            "source":         source,
        })

    cases.sort(key=lambda x: -x["search_count"])
    return cases


def print_summary(cases: list[dict]) -> None:
    total     = len(cases)
    high      = sum(1 for c in cases if c["confidence"] == "high")
    total_vol = sum(c["search_count"] for c in cases)

    genre_count: Counter[str] = Counter()
    genre_vol:   dict[str, int] = {}
    for c in cases:
        g = c["expected_genre"]
        genre_count[g] += 1
        genre_vol[g] = genre_vol.get(g, 0) + c["search_count"]

    print(f"\n{'═'*60}")
    print(f"  TEST SET SUMMARY")
    print(f"{'═'*60}")
    print(f"  Total test cases   : {total:,}")
    print(f"    High-confidence  : {high:,}  ({high/total*100:.1f}%)")
    print(f"    Haiku-adjudicated: {total-high:,}  ({(total-high)/total*100:.1f}%)")
    print(f"  Total search volume: {total_vol:,}")

    print(f"\n  Genre distribution:")
    for g, cnt in sorted(genre_count.items(), key=lambda x: -x[1]):
        print(f"    {g:<14} {cnt:>5,} keywords   {genre_vol[g]:>9,} searches")
    print(f"{'═'*60}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report",   default="C:/tmp/eval_report_v2.csv")
    ap.add_argument("--cache",    default="C:/tmp/adjudication_cache.json")
    ap.add_argument("--out",      default="tests/test_cases.json")
    ap.add_argument("--model",    default="claude-sonnet-4-6")
    ap.add_argument("--provider", default="anthropic",
                    choices=["anthropic", "bedrock"],
                    help="anthropic = Anthropic API (ANTHROPIC_API_KEY); "
                         "bedrock = AWS Bedrock (uses --region)")
    ap.add_argument("--batch",    type=int, default=40)
    ap.add_argument("--region",   default="ap-southeast-1")
    args = ap.parse_args()

    with open(args.report, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    disputes = [
        {"keyword_norm": r["keyword_norm"],
         "rules_genre":  r["rules_genre"],
         "nova_genre":   r["nova_genre"]}
        for r in rows if r["agree"] == "False"
    ]
    print(f"Dataset: {len(rows):,} keywords  "
          f"({len(rows)-len(disputes):,} agree, {len(disputes):,} disputes)")

    if args.provider == "anthropic":
        import anthropic as _anthropic
        client = _anthropic.Anthropic()
    else:
        import boto3
        client = boto3.client("bedrock-runtime", region_name=args.region)

    adj_cache = adjudicate_all(
        client, args.model, args.provider, disputes, args.cache, args.batch
    )

    cases = build(rows, adj_cache)
    print_summary(cases)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(cases, f, ensure_ascii=False, indent=2)
    print(f"\nWrote {len(cases):,} test cases → {args.out}")


if __name__ == "__main__":
    main()
