"""
influencer_collab.py  —  Detect & save Influencer Collaborations from Brand posts
ColabMind — Instagram Intelligence Platform

Mirrors brand.py but from the BRAND's perspective:
  → Brand posts are scanned to detect which influencers they collaborate with
  → Saves to:  instagram_db → influencer_collabs

Both `brands` and `influencer_collabs` collections are written
simultaneously every time a brand account is scraped.
"""

import re
from collections import defaultdict
from datetime import datetime, timezone


# ══════════════════════════════════════════════════════════════
#  COLLAB DETECTION CONSTANTS  (mirrors brand.py exactly)
# ══════════════════════════════════════════════════════════════

COLLAB_HASHTAGS = [
    "#ad", "#ads", "#advertisement", "#sponsored", "#sponsoredpost", "#sponsoredcontent",
    "#paidpartnership", "#paidpromotion", "#paidpost", "#partner", "#partnership",
    "#brandpartner", "#brandambassador", "#collaboration", "#collab", "#gifted",
    "#giftedbybrand", "#promo", "#promotion", "#spon", "#sp", "#ambassador", "#endorsement",
]

COLLAB_KEYWORDS = [
    "paid partnership", "paid promotion", "in partnership with", "partnered with",
    "collaboration with", "sponsored by", "gifted by", "gifted from", "use my code",
    "use code", "promo code", "discount code", "link in bio", "swipe up", "affiliate",
    "ambassador for", "collab with", "working with", "thanks to", "thank you to",
    "meet our ambassador", "our brand ambassador", "brand collab", "featuring",
]

COLLAB_RULES = [
    ("ADVERTISEMENT",        ["#ad","#ads","#advertisement"],                   ["advertisement"]),
    ("SPONSORED_POST",       ["#sponsored","#sponsoredpost","#spon","#sp"],     ["sponsored by"]),
    ("GIFTED_PRODUCT",       ["#gifted","#giftedbybrand"],                      ["gifted by","gifted from"]),
    ("BRAND_PARTNERSHIP",    ["#partner","#partnership","#brandpartner"],       ["in partnership with","partnered with"]),
    ("BRAND_AMBASSADOR",     ["#ambassador","#brandambassador"],                ["ambassador for","brand ambassador","meet our ambassador"]),
    ("COLLABORATION",        ["#collaboration","#collab"],                      ["collaboration with","collab with","featuring"]),
    ("AFFILIATE_PROMO_CODE", [],                                                 ["use my code","use code","promo code","discount code"]),
    ("AFFILIATE_LINK",       [],                                                 ["link in bio","swipe up","affiliate","use my link"]),
    ("PAID_PROMOTION",       ["#paidpartnership","#paidpromotion","#paidpost"], ["paid partnership","paid promotion"]),
]

COLLAB_TYPES_ALL = [
    "ADVERTISEMENT", "SPONSORED_POST", "GIFTED_PRODUCT", "BRAND_PARTNERSHIP",
    "BRAND_AMBASSADOR", "COLLABORATION", "AFFILIATE_PROMO_CODE", "AFFILIATE_LINK", "PAID_PROMOTION",
]

COLLAB_COLORS = {
    "ADVERTISEMENT":        "badge-red",
    "SPONSORED_POST":       "badge-orange",
    "GIFTED_PRODUCT":       "badge-yellow",
    "BRAND_PARTNERSHIP":    "badge-green",
    "BRAND_AMBASSADOR":     "badge-purple",
    "COLLABORATION":        "badge-blue",
    "AFFILIATE_PROMO_CODE": "badge-blue",
    "AFFILIATE_LINK":       "badge-blue",
    "PAID_PROMOTION":       "badge-orange",
    "ORGANIC":              "",
}


# ══════════════════════════════════════════════════════════════
#  DETECTION HELPERS
# ══════════════════════════════════════════════════════════════

def classify_influencer_collab(caption: str, hashtags: list[str]) -> list[str]:
    """Classify brand post collab type — same logic as classify_collaboration in brand.py."""
    caption_l  = caption.lower()
    hashtags_l = [h.lower() for h in hashtags]
    types      = []
    for collab_type, tag_triggers, kw_triggers in COLLAB_RULES:
        if any(t in hashtags_l for t in tag_triggers) or any(k in caption_l for k in kw_triggers):
            types.append(collab_type)
    return types if types else ["ORGANIC"]


def extract_influencer_mentions(caption: str) -> list[str]:
    """
    Extract @mentions from brand post captions.
    From a brand's perspective, these @mentions are likely the influencers they work with.
    """
    return list(set(re.findall(r"@([\w.]+)", caption)))


def extract_promo_codes(caption: str) -> list[str]:
    """Extract promo/discount codes from caption — same as brand.py."""
    codes = []
    for pattern in [
        r"(?:use|code|promo|discount)[:\s]+([A-Z0-9]{3,15})",
        r"([A-Z]{2,}[0-9]{1,4})\s+(?:for|to get)",
    ]:
        codes.extend(re.findall(pattern, caption, re.IGNORECASE))
    return list(set(codes))


def estimate_collab_value(followers: int, eng_rate: float, collab_types: list[str]) -> float:
    """
    Estimate the campaign spend from the brand's side.
    Same multiplier table as brand.py for consistency.
    """
    mults = {
        "PAID_PROMOTION":       1.2,
        "ADVERTISEMENT":        1.3,
        "SPONSORED_POST":       1.2,
        "BRAND_AMBASSADOR":     2.0,
        "AFFILIATE_PROMO_CODE": 0.5,
        "GIFTED_PRODUCT":       0.3,
        "COLLABORATION":        1.0,
        "AFFILIATE_LINK":       0.4,
        "BRAND_PARTNERSHIP":    1.3,
        "ORGANIC":              0.0,
    }
    multiplier = max([mults.get(t, 1.0) for t in collab_types])
    clamped_er = max(min(eng_rate * 100, 3.0), 0.5)
    return round((followers / 10_000) * 100 * clamped_er * multiplier, 2)


# ══════════════════════════════════════════════════════════════
#  PARSE INFLUENCER COLLAB ROWS + INFLUENCER ROWS
# ══════════════════════════════════════════════════════════════

def parse_influencer_collabs(
    post_rows: list[dict],
    brand_record: dict,
) -> tuple[list[dict], list[dict], dict]:
    """
    Given post_rows (already enriched by brand_profile.parse_brand_profile)
    and the brand_record, returns:

      collab_rows     : filtered list of brand posts that are influencer collabs
      influencer_rows : per-influencer aggregation (mirrors brand_rows in brand.py)
      collab_summary  : dict with type breakdown + totals to merge into brand_record
    """
    followers       = brand_record.get("follower_count", 1)
    brand_username  = brand_record.get("username", "")
    collab_rows     = []
    all_influencers = set()
    total_value     = 0.0
    collab_type_counts = defaultdict(int)
    influencer_map     = defaultdict(list)

    for post in post_rows:
        if not post.get("is_collaboration", False):
            continue

        # Re-enrich if collab keys are missing (backward-compat)
        caption   = post.get("caption", "")
        hashtags  = post.get("hashtags", [])
        mentions  = post.get("mentions") or extract_influencer_mentions(caption)
        codes     = post.get("promo_codes") or extract_promo_codes(caption)
        c_types   = post.get("collab_types") or classify_influencer_collab(caption, hashtags)
        eng_rate  = post.get("engagement_rate", 0.0)
        est_value = post.get("estimated_value_usd") or estimate_collab_value(followers, eng_rate, c_types)

        enriched_post = {
            **post,
            "mentions":            mentions,
            "promo_codes":         codes,
            "collab_types":        c_types,
            "estimated_value_usd": est_value,
        }
        collab_rows.append(enriched_post)

        total_value += est_value
        for ct in c_types:
            collab_type_counts[ct] += 1
        for m in mentions:
            if m.strip():
                all_influencers.add(m.strip())
                influencer_map[m.strip()].append(enriched_post)

    # Per-influencer aggregation (mirrors brand_rows in brand.py)
    influencer_rows = [
        {
            "influencer_username":  influencer,
            "brand":                brand_username,
            "total_collab_posts":   len(bp),
            "collab_types":         list(set(t for p in bp for t in p["collab_types"])),
            "avg_engagement_rate":  round(sum(p["engagement_rate"] for p in bp) / len(bp), 6),
            "estimated_value_usd":  round(sum(p["estimated_value_usd"] for p in bp), 2),
        }
        for influencer, bp in influencer_map.items()
    ]

    collab_summary = {
        "collab_posts_count":           len(collab_rows),
        "collab_rate":                  round(len(collab_rows) / max(brand_record.get("posts_scraped", 1), 1), 3),
        "unique_influencers_mentioned": len(all_influencers),
        "influencers_list":             ", ".join(all_influencers),
        "total_estimated_spend_usd":    round(total_value, 2),
        "most_common_collab_type":      max(collab_type_counts, key=collab_type_counts.get) if collab_type_counts else "NONE",
        **{k: collab_type_counts.get(k, 0) for k in COLLAB_TYPES_ALL},
    }

    return collab_rows, influencer_rows, collab_summary


# ══════════════════════════════════════════════════════════════
#  MONGODB — influencer_collabs collection
# ══════════════════════════════════════════════════════════════

def save_influencer_collabs_to_mongodb(
    collection,
    brand_record: dict,
    collab_rows: list[dict],
    influencer_rows: list[dict],
    collab_summary: dict,
) -> str:
    """
    Upserts influencer collab data into the `influencer_collabs` collection.
    Returns: "inserted" | "updated" | "no_change"
    Change-detection: only writes when collab_posts_count or total_estimated_spend_usd changed.
    """
    brand_username = brand_record["username"]
    now            = datetime.now(timezone.utc)

    existing = collection.find_one({"brand_username": brand_username})

    doc = {
        "brand_username":            brand_username,
        "brand_id":                  brand_record.get("user_id", ""),
        "follower_count":            brand_record.get("follower_count", 0),
        "collab_posts":              collab_rows,
        "influencer_summary":        influencer_rows,
        "total_collabs":             len(collab_rows),
        "unique_influencers":        collab_summary.get("unique_influencers_mentioned", 0),
        "influencers_list":          collab_summary.get("influencers_list", ""),
        "total_estimated_spend_usd": collab_summary.get("total_estimated_spend_usd", 0),
        "collab_type_breakdown":     {k: collab_summary.get(k, 0) for k in COLLAB_TYPES_ALL},
        "updated_at":                now,
    }

    if not existing:
        collection.insert_one(doc)
        return "inserted"

    if (
        existing.get("total_collabs")              != len(collab_rows)
        or existing.get("total_estimated_spend_usd") != collab_summary.get("total_estimated_spend_usd", 0)
    ):
        collection.update_one({"brand_username": brand_username}, {"$set": doc})
        return "updated"

    return "no_change"


# ══════════════════════════════════════════════════════════════
#  HIGH-LEVEL ORCHESTRATOR
# ══════════════════════════════════════════════════════════════

def process_influencer_collabs(
    brand_record: dict,
    post_rows: list[dict],
    influencer_collection=None,
) -> dict:
    """
    Given already-parsed brand profile + posts, extracts influencer collaboration data
    and optionally saves to MongoDB influencer_collabs collection.

    Returns:
      {
        "collab_rows":      list[dict],
        "influencer_rows":  list[dict],
        "collab_summary":   dict,
        "action":           "inserted" | "updated" | "no_change" | None,
      }
    """
    collab_rows, influencer_rows, collab_summary = parse_influencer_collabs(post_rows, brand_record)

    action = None
    if influencer_collection is not None:
        action = save_influencer_collabs_to_mongodb(
            influencer_collection, brand_record, collab_rows, influencer_rows, collab_summary
        )

    return {
        "collab_rows":      collab_rows,
        "influencer_rows":  influencer_rows,
        "collab_summary":   collab_summary,
        "action":           action,
    }
