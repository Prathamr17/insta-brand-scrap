"""
brand_profile.py  —  Fetch, parse, and save Brand Instagram profiles
ColabMind — Instagram Intelligence Platform

Mirrors profile.py but designed for brand accounts (Nike, Puma, Adidas, etc.)

Saves to:  instagram_db → brands
"""

import requests
import math
import re
from datetime import datetime, timezone

SEARCHAPI_URL = "https://www.searchapi.io/api/v1/search"


# ══════════════════════════════════════════════════════════════
#  RAW FETCH FROM SEARCHAPI  (same engine as profile.py)
# ══════════════════════════════════════════════════════════════

def fetch_brand_instagram_raw(username: str, api_key: str, max_posts: int = 30) -> dict:
    """
    Hits SearchAPI instagram_profile engine and returns raw API response dict.
    Returns {"status": "not_found"} if profile missing,
            {"status": "error", "message": ...} on failure.
    """
    try:
        resp = requests.get(
            SEARCHAPI_URL,
            params={"engine": "instagram_profile", "username": username, "api_key": api_key},
            timeout=20,
        )
        if resp.status_code != 200:
            return {"status": "error", "message": f"API returned HTTP {resp.status_code}"}

        data    = resp.json()
        profile = data.get("profile", {})

        if not profile or not profile.get("username"):
            return {"status": "not_found"}

        posts = data.get("posts", [])[:max_posts]
        return {"status": "ok", "profile": profile, "posts": posts}

    except Exception as e:
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════════════
#  NORMALISE INTO INTERNAL DICTS
# ══════════════════════════════════════════════════════════════

def normalise_brand_api_response(raw: dict) -> tuple[dict, list[dict]]:
    """
    Converts the flat SearchAPI profile/posts shape into the richer internal
    `brand` and `posts` dicts that parse_brand_profile() expects.
    """
    profile = raw["profile"]
    posts   = raw["posts"]

    brand = {
        "username":                     profile.get("username"),
        "full_name":                    profile.get("name"),
        "id":                           profile.get("id"),
        "edge_followed_by":             {"count": profile.get("followers", 0)},
        "edge_follow":                  {"count": profile.get("following", 0)},
        "edge_owner_to_timeline_media": {"count": profile.get("posts", 0)},
        "is_verified":                  profile.get("verified", False),
        "is_business_account":          profile.get("is_business", False),
        "biography":                    profile.get("biography", ""),
        "external_url":                 profile.get("external_url", ""),
        "category_name":                profile.get("category", ""),
        "profile_pic_url":              profile.get("profile_picture", ""),
    }

    formatted_posts = []
    for post in posts:
        formatted_posts.append({
            "shortcode":              post.get("id"),
            "is_video":               post.get("type") == "reel",
            "video_view_count":       post.get("views", 0),
            "edge_liked_by":          {"count": post.get("likes", 0)},
            "edge_media_to_comment":  {"count": post.get("comments", 0)},
            "taken_at_timestamp":     int(datetime.now().timestamp()),
            "edge_media_to_caption":  {
                "edges": [{"node": {"text": post.get("caption", "")}}]
            },
        })

    return brand, formatted_posts


# ══════════════════════════════════════════════════════════════
#  PARSE INTO BRAND RECORD + POST ROWS
# ══════════════════════════════════════════════════════════════

def parse_brand_profile(brand: dict, posts: list[dict], collab_classifier=None) -> tuple[dict, list[dict]]:
    """
    Builds:
      - brand_record : flat dict of all brand-level metrics
      - post_rows    : list of per-post dicts (optionally enriched with influencer collab metadata)

    If `collab_classifier` is truthy, each post row is enriched with
    influencer collab signals from influencer_collab.py.
    """
    username  = brand.get("username", "")
    followers = brand.get("edge_followed_by", {}).get("count", 0)
    following = brand.get("edge_follow", {}).get("count", 0)
    n         = len(posts) or 1

    likes, comments, views, hashtag_counts, timestamps, media_types = [], [], [], [], [], []
    post_rows = []

    for post in posts:
        caption_edges = post.get("edge_media_to_caption", {}).get("edges", [])
        caption   = (caption_edges[0].get("node", {}).get("text", "") if caption_edges else "") or ""
        like_c    = post.get("edge_liked_by", {}).get("count", 0) or post.get("edge_media_preview_like", {}).get("count", 0)
        comment_c = post.get("edge_media_to_comment", {}).get("count", 0)
        view_c    = post.get("video_view_count", 0) or 0
        is_video  = post.get("is_video", False)
        shortcode = post.get("shortcode", "")
        timestamp = post.get("taken_at_timestamp", 0)
        ts_str    = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""
        mtype     = "video" if is_video else "image"
        tags      = re.findall(r"#\w+", caption)
        eng_rate  = (like_c + comment_c) / max(followers, 1)

        likes.append(like_c);  comments.append(comment_c);  views.append(view_c)
        hashtag_counts.append(len(tags))
        if ts_str: timestamps.append(ts_str)
        media_types.append(mtype)

        pr = {
            "brand_username":  username,
            "post_id":         shortcode,
            "timestamp":       ts_str,
            "media_type":      mtype,
            "like_count":      like_c,
            "comment_count":   comment_c,
            "view_count":      view_c,
            "engagement_rate": round(eng_rate, 6),
            "engagement_%":    round(eng_rate * 100, 4),
            "hashtag_count":   len(tags),
            "hashtags":        tags,
            "caption":         caption[:500],
            "post_url":        f"https://www.instagram.com/p/{shortcode}/",
        }

        # Optionally enrich with influencer collab metadata from influencer_collab.py
        if collab_classifier:
            from influencer_collab import (
                classify_influencer_collab, extract_influencer_mentions,
                extract_promo_codes, estimate_collab_value,
                COLLAB_HASHTAGS, COLLAB_KEYWORDS,
            )
            mentions     = extract_influencer_mentions(caption)
            codes        = extract_promo_codes(caption)
            collab_types = classify_influencer_collab(caption, tags)
            is_collab    = (
                any(t.lower() in COLLAB_HASHTAGS for t in tags)
                or any(k in caption.lower() for k in COLLAB_KEYWORDS)
                or len(mentions) > 0
            )
            est_value    = estimate_collab_value(followers, eng_rate, collab_types) if is_collab else 0
            pr.update({
                "mentions":            mentions,
                "promo_codes":         codes,
                "is_collaboration":    is_collab,
                "collab_types":        collab_types,
                "estimated_value_usd": est_value,
            })

        post_rows.append(pr)

    # ── Aggregate metrics ─────────────────────────────────────
    avg_likes    = sum(likes)    / n
    avg_comments = sum(comments) / n
    avg_views    = sum(views)    / n
    eng_rate_avg = (avg_likes + avg_comments) / max(followers, 1)

    post_freq = 0.0
    if len(timestamps) > 1:
        first     = datetime.strptime(timestamps[0],  "%Y-%m-%d %H:%M:%S")
        last      = datetime.strptime(timestamps[-1], "%Y-%m-%d %H:%M:%S")
        days      = max((last - first).days, 1)
        post_freq = round(n / days * 7, 3)

    eng_std = 0.0
    if len(likes) > 1:
        mean_l  = sum(likes) / n
        eng_std = round(math.sqrt(sum((x - mean_l) ** 2 for x in likes) / n), 2)

    video_count = media_types.count("video")

    brand_record = {
        "username":                  username,
        "full_name":                 brand.get("full_name", ""),
        "user_id":                   brand.get("id", ""),
        "follower_count":            followers,
        "follower_count_log":        round(math.log1p(followers), 4),
        "following_count":           following,
        "post_count":                brand.get("edge_owner_to_timeline_media", {}).get("count", 0),
        "follower_following_ratio":  round(followers / max(following, 1), 3),
        "is_verified":               brand.get("is_verified", False),
        "is_business":               brand.get("is_business_account", False),
        "bio":                       brand.get("biography", ""),
        "external_url":              brand.get("external_url", ""),
        "category":                  brand.get("category_name", ""),
        "profile_pic_url":           brand.get("profile_pic_url", ""),
        "engagement_rate":           round(eng_rate_avg, 6),
        "engagement_%":              round(eng_rate_avg * 100, 4),
        "like_count_avg":            round(avg_likes, 2),
        "comment_count_avg":         round(avg_comments, 2),
        "view_count_avg":            round(avg_views, 2),
        "engagement_std":            eng_std,
        "hashtag_density_avg":       round(sum(hashtag_counts) / n, 3),
        "posting_frequency_weekly":  post_freq,
        "video_count":               video_count,
        "image_count":               n - video_count,
        "video_ratio":               round(video_count / n, 3),
        "image_ratio":               round((n - video_count) / n, 3),
        "posts_scraped":             n,
        "scraped_at":                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }

    return brand_record, post_rows


# ══════════════════════════════════════════════════════════════
#  MONGODB — brands collection
# ══════════════════════════════════════════════════════════════

def save_brand_profile_to_mongodb(
    collection,
    username: str,
    brand_record: dict,
    post_rows: list[dict],
) -> str:
    """
    Upserts brand profile + posts into the `brands` collection.
    Returns: "inserted" | "updated" | "no_change"
    Change-detection: only writes when follower_count or post_count changed.
    """
    now      = datetime.now(timezone.utc)
    existing = collection.find_one({"profile.username": username})

    doc = {
        "profile":    brand_record,
        "posts":      post_rows,
        "updated_at": now,
    }

    if not existing:
        collection.insert_one(doc)
        return "inserted"

    old = existing.get("profile", {})
    if (
        old.get("follower_count") != brand_record.get("follower_count")
        or old.get("post_count")  != brand_record.get("post_count")
    ):
        collection.update_one({"profile.username": username}, {"$set": doc})
        return "updated"

    return "no_change"


# ══════════════════════════════════════════════════════════════
#  HIGH-LEVEL ORCHESTRATOR
# ══════════════════════════════════════════════════════════════

def process_brand_profile(
    username: str,
    api_key: str,
    brands_collection=None,
    max_posts: int = 30,
    with_collab: bool = True,
) -> dict:
    """
    Full pipeline:
      1. Fetch raw data from SearchAPI
      2. Normalise & parse into brand_record + post_rows
      3. Optionally save to MongoDB brands collection

    Returns:
      {
        "status":       "success" | "not_found" | "error",
        "action":       "inserted" | "updated" | "no_change" | None,
        "brand_record": dict,
        "post_rows":    list[dict],
        "message":      str   (only on error),
      }
    """
    raw = fetch_brand_instagram_raw(username, api_key, max_posts)

    if raw["status"] == "not_found":
        return {"status": "not_found"}
    if raw["status"] == "error":
        return {"status": "error", "message": raw["message"]}

    brand, formatted_posts = normalise_brand_api_response(raw)
    brand_record, post_rows = parse_brand_profile(brand, formatted_posts, collab_classifier=with_collab)

    action = None
    if brands_collection is not None:
        action = save_brand_profile_to_mongodb(brands_collection, username, brand_record, post_rows)

    return {
        "status":       "success",
        "action":       action,
        "brand_record": brand_record,
        "post_rows":    post_rows,
    }
