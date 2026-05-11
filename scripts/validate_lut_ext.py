"""
Audit lut_extended.json for entries that conflict with regex patterns.

A conflict means Nova classified a keyword as genre A, but a strong regex
signal points to genre B — likely a hallucination worth reviewing manually.

Usage:
    python scripts/validate_lut_ext.py \
        [--lut-ext genre_classifier/lut_extended.json] \
        [--top 50] \
        [--fix]   # auto-correct unambiguous conflicts (regex wins)

Output: sorted list of conflicts with counts, printed to stdout.
Pass --fix to write corrections back to lut_extended.json in-place.
"""
import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from genre_classifier.rules import _COMPILED, _GENRE_ORDER  # noqa: E402

# Regex patterns with strong signal (not easily confused)
_STRONG_GENRES = {"THE_THAO", "ANIME", "NHAC", "PHIM_HAN", "TRUYEN_HINH"}


def find_conflicts(lut_ext: dict[str, str]) -> list[tuple[str, str, str]]:
    """Return list of (keyword, lut_genre, regex_genre) conflicts."""
    conflicts = []
    for kw, lut_genre in lut_ext.items():
        for g in _GENRE_ORDER:
            if g not in _STRONG_GENRES:
                continue
            if any(p.search(kw) for p in _COMPILED[g]):
                if g != lut_genre:
                    conflicts.append((kw, lut_genre, g))
                break
    return conflicts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lut-ext", default="genre_classifier/lut_extended.json")
    ap.add_argument("--top", type=int, default=100)
    ap.add_argument("--fix", action="store_true",
                    help="Overwrite lut_extended.json with regex-corrected values")
    args = ap.parse_args()

    path = Path(args.lut_ext)
    with open(path, encoding="utf-8") as f:
        lut_ext: dict[str, str] = json.load(f)

    conflicts = find_conflicts(lut_ext)
    conflicts.sort(key=lambda x: x[0])

    print(f"Found {len(conflicts)} conflicts in {len(lut_ext):,} entries "
          f"({len(conflicts)/len(lut_ext)*100:.2f}%)\n")

    print(f"{'KEYWORD':<40} {'LUT_EXT':>12} {'REGEX':>12}")
    print("-" * 66)
    for kw, lut_g, reg_g in conflicts[: args.top]:
        print(f"{kw:<40} {lut_g:>12} → {reg_g:>12}")

    if len(conflicts) > args.top:
        print(f"  ... and {len(conflicts) - args.top} more")

    dist: dict[str, int] = {}
    for _, lut_g, reg_g in conflicts:
        key = f"{lut_g} → {reg_g}"
        dist[key] = dist.get(key, 0) + 1
    print("\nConflict type breakdown:")
    for pair, cnt in sorted(dist.items(), key=lambda x: -x[1]):
        print(f"  {pair:<30} {cnt:>4}")

    if args.fix:
        fixed = 0
        for kw, _, reg_g in conflicts:
            lut_ext[kw] = reg_g
            fixed += 1
        with open(path, "w", encoding="utf-8") as f:
            json.dump(lut_ext, f, ensure_ascii=False, indent=2, sort_keys=True)
        print(f"\nFixed {fixed} entries → wrote {path}")


if __name__ == "__main__":
    main()
