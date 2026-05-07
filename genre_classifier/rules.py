"""
Shared utility module — used by replay-producer Lambda and Glue ETL job.

Three classifiers (§6.4, §6.5, §6.6):
  classify_keyword(keyword_norm)   → derived_genre  (8 values + UNKNOWN)
  bucket_platform(platform)        → platform_group (5 values + Other)
  normalize_network_type(nt)       → network_type_norm (5 values)

classify_keyword uses:
  1. LUT exact match (lut.json, from key_search_by_category.csv)
  2. Regex pattern match (covers partial matches and transliterations)
  → UNKNOWN for no match; LLM fallback is applied only in the Glue batch path.
"""
import importlib.resources
import json
import re
from pathlib import Path

# importlib.resources works from both plain directory (Lambda) and zip archive (Glue extra-py-files)
try:
    _pkg = importlib.resources.files("genre_classifier")
    LUT: dict[str, str] = json.loads((_pkg / "lut.json").read_text(encoding="utf-8"))
except Exception:
    _LUT_PATH = Path(__file__).parent / "lut.json"
    try:
        with open(_LUT_PATH, encoding="utf-8") as _f:
            LUT = json.load(_f)
    except FileNotFoundError:
        LUT = {}

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
        r"trực tiếp$", r"truc tiep$", r"\bvtv\b$",
    ],
    "PHIM_VIET": [
        r"phim việt", r"phim viet", r"phim bộ việt", r"phim bo viet",
        r"\bvtv\b", r"\bhtv\b", r"đài truyền hình", r"dai truyen hinh",
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
}

_COMPILED: dict[str, list[re.Pattern]] = {
    genre: [re.compile(pat, re.IGNORECASE) for pat in pats]
    for genre, pats in _RAW_PATTERNS.items()
}

# Pattern evaluation order — most-selective first to minimise false positives
_GENRE_ORDER = [
    "THE_THAO", "ANIME", "NHAC", "TRUYEN_HINH",
    "PHIM_AU_MY", "PHIM_TRUNG", "PHIM_VIET",
]


def classify_keyword(keyword_norm: str | None) -> str:
    """Return derived_genre for a normalised keyword string.

    Args:
        keyword_norm: lower-cased, stripped keyword (may be None or empty).

    Returns:
        One of NHAC | THE_THAO | ANIME | PHIM_TRUNG | PHIM_VIET |
               PHIM_AU_MY | TRUYEN_HINH | UNKNOWN
    """
    if not keyword_norm:
        return "UNKNOWN"

    # 1. Exact-match LUT (fastest, highest precision)
    genre = LUT.get(keyword_norm)
    if genre:
        return genre

    # 2. Regex patterns
    for g in _GENRE_ORDER:
        if any(p.search(keyword_norm) for p in _COMPILED[g]):
            return g

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
