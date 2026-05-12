"""
Apply classification fixes derived from evaluate_classifier.py output.

Two fix tiers:
  Tier 1 — lut_extended.json corrections:
    For every misclassified keyword that lives in lut_extended.json, update
    its genre to Nova's judgment — IF Nova suggests a specific genre (not
    UNKNOWN) and search volume >= MIN_VOL.
    Skips keywords where rules used lut.json (hand-curated, higher trust).

  Tier 2 — lut.json curated overrides:
    High-volume regex-classified keywords that Nova clearly disagrees with
    are added to lut.json so they win at stage 1 (before regex fires).
    Only applied for keywords with volume >= CURATED_MIN_VOL.

Usage:
    python scripts/apply_eval_fixes.py \\
        [--report  C:/tmp/eval_report.csv] \\
        [--lut     genre_classifier/lut.json] \\
        [--lut-ext genre_classifier/lut_extended.json] \\
        [--min-vol 100]           # min search volume for lut_ext fixes
        [--curated-min-vol 200]   # min volume to promote a fix to lut.json
        [--dry-run]               # print changes without writing
"""
import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from genre_classifier.rules import (  # noqa: E402
    LUT, LUT_EXT, VALID_GENRES, _COMPILED, _GENRE_ORDER, _preprocess,
)

# apply_eval_fixes never writes UNKNOWN to LUTs — filter it out at point of use
_FIXABLE_GENRES = VALID_GENRES - {"UNKNOWN"}

# Keywords where Nova is known to be wrong — skip even if volume is high
_NOVA_SKIP = {
    "lưỡi gươm diệt quỷ",          # Demon Slayer → ANIME; fixed separately below
    "học viện anh hùng",            # My Hero Academia → ANIME; rules regex already correct
    "cô nàng trong trắng oh woo ri",# Korean drama → PHIM_HAN; Nova says PHIM_TRUNG
    "shooting stars",               # Korean drama → PHIM_HAN; Nova says TRUYEN_HINH
    "runningman",                   # Running Man variety → TRUYEN_HINH; Nova says PHIM_HAN
    "running",                      # too generic; sports context likely valid
    "youtube",                      # TRUYEN_HINH is defensible as a streaming channel
    "trực tiếp bong da",            # THE_THAO is correct; Nova says UNKNOWN
    "truc tiep bong da",            # same
    "cá mập con baby shark dance | + tuyển tập | pinkfong! - nhạc thiếu nhi",  # NHAC correct
}

# Corrections Nova missed — applied directly to lut_extended regardless of Nova judgment
_DIRECT_LUT_EXT_FIXES = {
    "lưỡi gươm diệt quỷ": "ANIME",   # Demon Slayer; wrongly PHIM_TRUNG in lut_ext
    "shooting stars":     "PHIM_HAN", # Korean drama; Nova said TRUYEN_HINH
    "cô nàng trong trắng oh woo ri": "PHIM_HAN",  # Korean drama Oh Woo-ri
    "runningman":         "TRUYEN_HINH",  # Running Man variety show
}


def load_csv(path: str) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _regex_genre(kw: str) -> str | None:
    for g in _GENRE_ORDER:
        if any(p.search(kw) for p in _COMPILED[g]):
            return g
    return None


def plan_fixes(
    rows: list[dict],
    lut: dict,
    lut_ext: dict,
    min_vol: int,
    curated_min_vol: int,
) -> tuple[dict, dict]:
    """Return (lut_fixes, lut_ext_fixes) dicts."""
    lut_fixes: dict[str, str] = {}
    lut_ext_fixes: dict[str, str] = {}

    for row in rows:
        if row["agree"] == "True":
            continue

        kw       = row["keyword_norm"]
        kw_prep  = _preprocess(kw)
        rules_g  = row["rules_genre"]
        nova_g   = row["nova_genre"]
        vol      = int(row["search_count"])

        # Never apply UNKNOWN as a "fix" — keeps the old label
        if nova_g not in _FIXABLE_GENRES:
            continue

        if kw in _NOVA_SKIP or kw_prep in _NOVA_SKIP:
            continue

        if kw_prep in lut:
            # Curated LUT — hand-verified entries; only override if extremely
            # high volume and Nova is unambiguous
            if vol >= 5000:
                lut_fixes[kw_prep] = nova_g
            continue

        if kw_prep in lut_ext:
            if vol >= min_vol:
                lut_ext_fixes[kw_prep] = nova_g
            continue

        # Regex-classified keyword: promote to lut.json so it beats the regex
        src = _regex_genre(kw_prep)
        if src and src == rules_g and vol >= curated_min_vol:
            lut_fixes[kw_prep] = nova_g

    return lut_fixes, lut_ext_fixes


def print_plan(lut_fixes: dict, lut_ext_fixes: dict, lut: dict, lut_ext: dict):
    print(f"\nlut.json  fixes: {len(lut_fixes)}")
    for kw, new_g in sorted(lut_fixes.items(), key=lambda x: x[0]):
        old_g = lut.get(kw, "regex")
        print(f"  {kw:<45} {old_g:>12} → {new_g}")

    print(f"\nlut_extended.json fixes: {len(lut_ext_fixes)}")
    for kw, new_g in sorted(lut_ext_fixes.items(), key=lambda x: x[0]):
        old_g = lut_ext.get(kw, "?")
        if old_g != new_g:
            print(f"  {kw:<45} {old_g:>12} → {new_g}")

    # Summarise lut_ext fixes by (old → new) pair
    dist: dict[str, int] = {}
    for kw, new_g in lut_ext_fixes.items():
        old_g = lut_ext.get(kw, "?")
        dist[f"{old_g} → {new_g}"] = dist.get(f"{old_g} → {new_g}", 0) + 1
    print("\nlut_ext fix breakdown:")
    for pair, cnt in sorted(dist.items(), key=lambda x: -x[1]):
        print(f"  {pair:<30} {cnt:>4}")


def apply(lut_fixes: dict, lut_ext_fixes: dict,
          lut_path: str, lut_ext_path: str,
          lut: dict, lut_ext: dict,
          direct_ext: dict | None = None):
    changed_lut = changed_ext = 0

    if lut_fixes:
        for kw, g in lut_fixes.items():
            if lut.get(kw) != g:
                lut[kw] = g
                changed_lut += 1
        with open(lut_path, "w", encoding="utf-8") as f:
            json.dump(lut, f, ensure_ascii=False, indent=2)
        print(f"\nWrote {changed_lut} changes → {lut_path}")

    all_ext_fixes = {**(direct_ext or {}), **lut_ext_fixes}
    if all_ext_fixes:
        for kw, g in all_ext_fixes.items():
            if lut_ext.get(kw) != g:
                lut_ext[kw] = g
                changed_ext += 1
        with open(lut_ext_path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(lut_ext, f, ensure_ascii=False, indent=2, sort_keys=True)
        print(f"Wrote {changed_ext} changes → {lut_ext_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report",          default="C:/tmp/eval_report.csv")
    ap.add_argument("--lut",             default="genre_classifier/lut.json")
    ap.add_argument("--lut-ext",         default="genre_classifier/lut_extended.json")
    ap.add_argument("--min-vol",         type=int, default=100)
    ap.add_argument("--curated-min-vol", type=int, default=200)
    ap.add_argument("--dry-run",         action="store_true")
    args = ap.parse_args()

    rows = load_csv(args.report)
    print(f"Loaded {len(rows):,} rows from {args.report} "
          f"({sum(1 for r in rows if r['agree']=='False'):,} misclassified)")

    with open(args.lut, encoding="utf-8") as f:
        lut = json.load(f)
    with open(args.lut_ext, encoding="utf-8") as f:
        lut_ext = json.load(f)

    lut_fixes, lut_ext_fixes = plan_fixes(
        rows, lut, lut_ext, args.min_vol, args.curated_min_vol
    )

    print_plan(lut_fixes, lut_ext_fixes, lut, lut_ext)

    if args.dry_run:
        print("\n[dry-run] No files written.")
        return

    apply(lut_fixes, lut_ext_fixes, args.lut, args.lut_ext, lut, lut_ext,
          direct_ext=_DIRECT_LUT_EXT_FIXES)


if __name__ == "__main__":
    main()
