"""
Shared utility module — used by replay-producer Lambda and Glue ETL job.

Three classifiers (§6.4, §6.5, §6.6):
  classify_keyword(keyword_norm)   → derived_genre  (8 genres + UNKNOWN = 9 values total)
  bucket_platform(platform)        → platform_group (5 values + Other)
  normalize_network_type(nt)       → network_type_norm (5 values)

classify_keyword waterfall (§6.4):
  0. _preprocess: collapse spaces, strip episode suffix (tap/ep N), dedupe chars
  1. LUT exact match (lut.json — curated, ~70 entries)
  2. Extended LUT exact match (lut_extended.json — LLM-built, ~100K entries)
  2b. LUT_EXT no-diacritics match (handles queries typed without Vietnamese marks)
  3. Regex pattern match (covers partial matches and transliterations)
  4a. Fuzzy LUT match via difflib (cutoff=0.85, keywords ≥4 chars)
  4b. Fuzzy LUT_EXT multi-word match (cutoff=0.92, keywords ≥8 chars with space)
  5. FastText ML classifier (optional; requires FASTTEXT_MODEL_PATH env var)
  → UNKNOWN for no match; Bedrock fallback applied only in the Glue batch path.

lut_extended.json is built offline by scripts/build_extended_lut.py and bundled
into genre_classifier.zip alongside lut.json.  Rebuild when genre distribution
shifts noticeably (run build_extended_lut.py → commit → rebuild zip → re-upload).
"""
import difflib as _difflib
import importlib.resources
import json
import re
import unicodedata
from pathlib import Path


def _load_json_from_pkg(filename: str) -> dict:
    try:
        _pkg = importlib.resources.files("genre_classifier")
        return json.loads((_pkg / filename).read_text(encoding="utf-8"))
    except Exception:
        path = Path(__file__).parent / filename
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}


LUT: dict[str, str] = _load_json_from_pkg("lut.json")
LUT_EXT: dict[str, str] = _load_json_from_pkg("lut_extended.json")


def _strip_diacritics(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


# No-diacritics index for LUT_EXT — handles "tieu ly phi dao" matching "tiểu lý phi đao"
_LUT_EXT_NODIAC: dict[str, str] = {_strip_diacritics(k): v for k, v in LUT_EXT.items()}

# ── Regex patterns per genre (§6.4) ──────────────────────────────────────────
_RAW_PATTERNS: dict[str, list[str]] = {
    "THE_THAO": [
        r"bóng đá", r"bong da", r"thể thao", r"the thao", r"nexsport",
        r"trực tiếp.*vs", r"\bvs\b.*việt nam", r"\bvs\b.*viet nam",
        r"tennis", r"bóng rổ", r"futsal", r"world cup", r"v-?league",
        r"\bk\+\b",          # K+ sports premium channel
    ],
    "NHAC": [
        r"bolero", r"trữ tình", r"tru tinh", r"nhạc", r"nhac",
        r"ca nhạc", r"ca nhac", r"vpop", r"nhạc trẻ", r"nhạc vàng",
        r"nhac vang", r"remix", r"mv\b",
    ],
    "ANIME": [
        r"\banime\b", r"hoạt hình nhật", r"hoat hinh nhat",
        r"\bnaruto\b", r"\bone piece\b", r"\bkirito\b",
        r"chuyển sinh", r"cậu mang", r"học viện anh hùng",
        r"\bdragon ball\b", r"\bsword art online\b", r"\bbleach\b",
        r"\battack on titan\b", r"\bconan\b",
    ],
    "TRUYEN_HINH": [
        r"vtv[1-9]", r"htv[1-9]", r"\bkênh\b", r"\bkenh\b",
        r"trực tiếp$", r"truc tiep$", r"\bvtv\b", r"\bhtv\b",
    ],
    "PHIM_VIET": [
        r"phim việt", r"phim viet", r"phim bộ việt", r"phim bo viet",
        r"đài truyền hình", r"dai truyen hinh",
        r"ngôi nhà", r"cô gái", r"chàng trai",
    ],
    "PHIM_AU_MY": [
        r"\bmarvel\b", r"\bdisney\b", r"\bnetflix\b", r"\bhbo\b",
        r"\bavengers\b", r"\bbatman\b", r"\bspiderman\b", r"\bsuperman\b",
        r"\bstar wars\b", r"\bfast.furious\b",
    ],
    "PHIM_TRUNG": [
        r"kiếm hiệp", r"kiem hiep", r"cổ trang", r"co trang",
        r"tiên hiệp", r"tien hiep", r"lương sơn bá", r"luong son ba",
        r"hoa thiên cốt", r"diên hy công lược", r"trường nguyệt tẫn minh",
    ],
    "PHIM_HAN": [
        r"phim hàn", r"phim han\b", r"hàn quốc", r"han quoc",
        r"kdrama", r"k-?drama", r"phim hàn quốc",
        r"crash landing", r"goblin", r"descendants.*sun",
        r"my love from.*star", r"reply \d{4}",
    ],
}

_COMPILED: dict[str, list[re.Pattern]] = {
    genre: [re.compile(pat, re.IGNORECASE) for pat in pats]
    for genre, pats in _RAW_PATTERNS.items()
}

# Pattern evaluation order — most-selective first to minimise false positives
_GENRE_ORDER = [
    "THE_THAO", "ANIME", "NHAC", "TRUYEN_HINH",
    "PHIM_AU_MY", "PHIM_HAN", "PHIM_TRUNG", "PHIM_VIET",
]

# Fuzzy match candidates:
#   - Stage 4a: curated LUT only  (cutoff=0.85, any length ≥4)
#   - Stage 4b: LUT_EXT subsample (cutoff=0.92, length ≥8, multi-word only)
#     We sample only multi-word LUT_EXT keys to keep O(n) manageable and avoid
#     single-word false positives (e.g. "running" ≠ "running man").
_LUT_KEYS: list[str] = list(LUT.keys())
_LUT_EXT_FUZZY_KEYS: list[str] = [k for k in LUT_EXT if len(k) >= 8 and " " in k]

_EPISODE_RE = re.compile(
    r"\s+(?:tap|ep|episode|phan|part)\s*\d+\s*$", re.IGNORECASE
)
_REPEAT_RE = re.compile(r"(.)\1{2,}")


def _preprocess(kw: str) -> str:
    """Normalise keyword before lookup: collapse spaces, strip episode suffix, dedupe chars."""
    kw = " ".join(kw.split())
    kw = _EPISODE_RE.sub("", kw).strip()
    kw = _REPEAT_RE.sub(r"\1\1", kw)  # keep max 2 repeats to avoid over-collapsing
    return kw


# FastText model — optional Stage 3; degrades to UNKNOWN if unavailable
try:
    from genre_classifier.fasttext_model import predict_genre as _ft_predict
    _FASTTEXT_AVAILABLE = True
except Exception:
    _FASTTEXT_AVAILABLE = False
    _ft_predict = None  # type: ignore[assignment]


def classify_keyword(keyword_norm: str | None) -> str:
    """Return derived_genre for a normalised keyword string.

    Args:
        keyword_norm: lower-cased, stripped keyword (may be None or empty).

    Returns:
        One of NHAC | THE_THAO | ANIME | PHIM_TRUNG | PHIM_VIET |
               PHIM_AU_MY | PHIM_HAN | TRUYEN_HINH | UNKNOWN
    """
    if not keyword_norm:
        return "UNKNOWN"

    kw = _preprocess(keyword_norm)
    if not kw:
        return "UNKNOWN"

    # 1. Curated LUT exact match (small, hand-verified, highest precision)
    genre = LUT.get(kw)
    if genre:
        return genre

    # 2. Extended LUT exact match (LLM-built title catalog, ~100K entries)
    genre = LUT_EXT.get(kw)
    if genre:
        return genre

    # 2b. LUT_EXT no-diacritics match — handles queries typed without Vietnamese marks
    genre = _LUT_EXT_NODIAC.get(_strip_diacritics(kw))
    if genre:
        return genre

    # 3. Regex patterns
    for g in _GENRE_ORDER:
        if any(p.search(kw) for p in _COMPILED[g]):
            return g

    # 4a. Fuzzy match against curated LUT (cutoff=0.85, length ≥4)
    if len(kw) >= 4:
        matches = _difflib.get_close_matches(kw, _LUT_KEYS, n=1, cutoff=0.85)
        if matches:
            return LUT[matches[0]]

    # 4b. Fuzzy match against multi-word LUT_EXT subset (cutoff=0.92, length ≥8)
    #     High cutoff prevents false positives across large key space.
    if len(kw) >= 8 and " " in kw:
        matches = _difflib.get_close_matches(kw, _LUT_EXT_FUZZY_KEYS, n=1, cutoff=0.92)
        if matches:
            return LUT_EXT[matches[0]]

    # 5. FastText ML classifier — optional, requires trained model binary
    if _FASTTEXT_AVAILABLE and _ft_predict is not None:
        result = _ft_predict(kw)
        if result != "UNKNOWN":
            return result

    return "UNKNOWN"


# ── Platform bucketing (§6.5) ─────────────────────────────────────────────────

def bucket_platform(platform: str | None) -> str:
    """Map 35-value device-model string to 5-bucket platform_group.

    Returns one of: OTTBox | SmartTV | Android | iOS | Web | Other
    """
    if not platform:
        return "Other"
    p = platform.lower().strip()

    if p.startswith("fplay-ottbox") or p in {
        "androidtvboxhis22", "fplay-gr-android-box",
    }:
        return "OTTBox"
    if "smarttv" in p or "smart-tv" in p:
        return "SmartTV"
    if p in {"android", "vsmart"}:
        return "Android"
    if p == "ios":
        return "iOS"
    if "web" in p:
        return "Web"
    return "Other"


# ── NetworkType normalisation (§6.6) ─────────────────────────────────────────

def normalize_network_type(nt: str | None) -> str:
    """Collapse 9 dirty networkType values into 5 canonical labels.

    Returns one of: WiFi | Mobile | Ethernet | Cable | Unknown
    """
    if nt is None:
        return "Unknown"
    n = nt.upper().strip()

    if n in {"WIFI", "WLAN"}:
        return "WiFi"
    if n in {"WWAN", "3G", "4G", "LTE", "5G"}:
        return "Mobile"
    if n == "ETHERNET":
        return "Ethernet"
    if n in {"CAB", "CABLE"}:
        return "Cable"
    # Covers: ###, NO-INTERNET, Bluetooth Tethering, empty, unknown values
    return "Unknown"
