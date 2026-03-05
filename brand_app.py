"""
ColabMind — Instagram Intelligence Platform
brand_app.py  ·  Streamlit entry point for Brand Intelligence

Depends on:
  brand_profile.py    —  fetch, parse, save brand profiles
  influencer_collab.py —  detect influencer collabs, save influencer_collabs

Both `brands` and `influencer_collabs` collections are written
simultaneously every time a brand account is scraped.
"""

import os
import pandas as pd
import streamlit as st
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pymongo import MongoClient
import dotenv

from brand_profile import process_brand_profile
from influencer_collab import (
    process_influencer_collabs,
    COLLAB_COLORS, COLLAB_TYPES_ALL,
)

dotenv.load_dotenv()


# ══════════════════════════════════════════════════════════════
#  PAGE CONFIG
# ══════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="ColabMind — Brand Intelligence",
    page_icon="🏷",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .stApp { background-color: #0f1117; color: #e0e0e0; }
    section[data-testid="stSidebar"] { background-color: #1a1d27; border-right: 1px solid #2a2d3a; }
    .card  { background: #1a1d27; border: 1px solid #2a2d3a; border-radius: 12px; padding: 20px; margin-bottom: 16px; }
    .card-blue { background: linear-gradient(135deg, #0d1828, #1a1d2d); border: 1px solid #2d4a8a; border-radius: 12px; padding: 20px; margin-bottom: 16px; }
    .metric-box { background: #12151f; border: 1px solid #2a2d3a; border-radius: 8px; padding: 14px 18px; text-align: center; }
    .metric-value { font-size: 24px; font-weight: 700; color: #60a5fa; }
    .metric-label { font-size: 12px; color: #888; margin-top: 4px; }
    .badge { display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 11px; font-weight: 600; margin: 2px; }
    .badge-green  { background: #14532d; color: #4ade80; }
    .badge-blue   { background: #1e3a5f; color: #60a5fa; }
    .badge-orange { background: #431407; color: #fb923c; }
    .badge-purple { background: #3b0764; color: #c084fc; }
    .badge-red    { background: #450a0a; color: #f87171; }
    .badge-yellow { background: #422006; color: #fbbf24; }
    .username-title { font-size: 22px; font-weight: 700; color: #ffffff; }
    .section-title  { font-size: 13px; font-weight: 600; color: #9ca3af; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid #2a2d3a; }
    .collab-row { background: #12151f; border: 1px solid #1e2130; border-radius: 8px; padding: 12px 16px; margin-bottom: 8px; }
    .stTextInput > div > div > input { background-color: #12151f !important; border: 1px solid #2a2d3a !important; color: #e0e0e0 !important; border-radius: 8px !important; }
    .stButton > button { background: linear-gradient(135deg, #1e3a8a, #1d4ed8) !important; color: white !important; border: none !important; border-radius: 8px !important; font-weight: 600 !important; width: 100% !important; }
    .stProgress > div > div { background-color: #60a5fa !important; }
    .stTabs [data-baseweb="tab"] { background: #1a1d27 !important; color: #9ca3af !important; }
    .stTabs [aria-selected="true"] { background: #1e3a8a !important; color: white !important; }
    .mongo-ok  { padding: 8px 12px; border-radius: 8px; font-size: 12px; background: #14532d; color: #4ade80; }
    .mongo-err { padding: 8px 12px; border-radius: 8px; font-size: 12px; background: #450a0a; color: #f87171; }
    .mongo-na  { padding: 8px 12px; border-radius: 8px; font-size: 12px; background: #1e2130; color: #6b7280; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════

def fmt_number(n):
    n = int(n) if n else 0
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return str(n)


def collab_badge(ct):
    return f'<span class="badge {COLLAB_COLORS.get(ct, "")}">{ct.replace("_", " ")}</span>'


# ══════════════════════════════════════════════════════════════
#  MONGODB CONNECTION  (persisted in session_state)
# ══════════════════════════════════════════════════════════════

def get_mongo_client(uri: str):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return client, None
    except Exception as e:
        return None, str(e)


for _k, _v in [("mongo_client", None), ("mongo_ok", False), ("mongo_uri_active", "")]:
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ══════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("""
    <div style="text-align:center; padding:20px 0 10px;">
        <div style="font-size:28px; font-weight:800; color:#60a5fa;">🏷 ColabMind</div>
        <div style="font-size:12px; color:#6b7280; margin-top:4px;">Brand Intelligence Platform</div>
    </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── SearchAPI Key ─────────────────────────────────────────
    st.markdown("### 🔑 SearchAPI")

    searchapi_input = st.text_input(
        "SearchAPI Key",
        type="password",
        placeholder="Enter your SearchAPI key"
    )

    if searchapi_input:
        st.session_state["searchapi_key"] = searchapi_input
        st.markdown('<div class="mongo-ok">✓ SearchAPI Key Loaded</div>', unsafe_allow_html=True)
    else:
        st.session_state["searchapi_key"] = ""
        st.markdown('<div class="mongo-err">✗ SearchAPI Key Required</div>', unsafe_allow_html=True)

    st.markdown("---")

    # ── MongoDB (Auto from .env) ──────────────────────────────
    st.markdown("### 🗄 MongoDB Atlas")

    MONGO_URI = os.getenv("MONGO_URI", "")

    if MONGO_URI and not st.session_state["mongo_ok"]:
        with st.spinner("Connecting to MongoDB…"):
            client, err = get_mongo_client(MONGO_URI)

        if client:
            st.session_state["mongo_client"]     = client
            st.session_state["mongo_ok"]         = True
            st.session_state["mongo_uri_active"] = MONGO_URI
        else:
            st.session_state["mongo_client"] = None
            st.session_state["mongo_ok"]     = False
            st.session_state["mongo_err"]    = err

    if st.session_state["mongo_ok"]:
        st.markdown('<div class="mongo-ok">✓ Connected to MongoDB Atlas</div>', unsafe_allow_html=True)
    elif not MONGO_URI:
        st.markdown('<div class="mongo-err">✗ MongoDB URI missing in .env</div>', unsafe_allow_html=True)
    else:
        err_msg = st.session_state.get("mongo_err", "Connection failed")
        st.markdown(f'<div class="mongo-err">✗ {err_msg}</div>', unsafe_allow_html=True)

    # ── Settings ──────────────────────────────────────────────
    st.markdown("### ⚙️ Settings")
    max_posts_ui = st.slider("Posts per brand",   12, 50, 30)
    workers_ui   = st.slider("Parallel workers",   1,  3,  3)

    st.markdown("---")
    st.markdown("""<div style="color:#6b7280; font-size:11px; line-height:1.9;">
    📦 Saves to MongoDB simultaneously:<br>
    <code>instagram_db → brands</code><br>
    <code>instagram_db → influencer_collabs</code><br><br>
    🔍 Detects 9 collab types<br>
    👤 Identifies influencer partners<br>
    📊 Change-detection before every write<br>
    ⚡ Up to 3 parallel workers
    </div>""", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("""<div style="color:#6b7280; font-size:11px;">
    🌿 Switch to <strong style="color:#4ade80;">Influencer Mode</strong>:<br>
    <code>streamlit run app.py</code>
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
#  MAIN HEADER
# ══════════════════════════════════════════════════════════════

st.markdown("""
<div style="padding:10px 0 24px;">
    <h1 style="font-size:32px; font-weight:800; color:#ffffff; margin:0;">
        🏷 Brand Intelligence
    </h1>
    <p style="color:#9ca3af; margin-top:6px;">
        Scrape brand profiles · Detect influencer collaborations · Store brands &amp; influencer data to MongoDB Atlas simultaneously
    </p>
</div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
#  BRAND USERNAME INPUT
# ══════════════════════════════════════════════════════════════

st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Enter up to 5 Brand Instagram Usernames</div>', unsafe_allow_html=True)

placeholders    = ["nike", "puma", "adidas", "redbull", "zara"]
usernames_input = []
cols = st.columns(5)
for i, col in enumerate(cols):
    with col:
        val = st.text_input(f"Brand {i+1}", placeholder=placeholders[i], key=f"b{i}")
        usernames_input.append(val.strip())

st.markdown("</div>", unsafe_allow_html=True)

usernames_clean = [u for u in usernames_input if u]
_, c2, _ = st.columns([2, 1, 2])
with c2:
    run_btn = st.button("🚀 Start Scraping", disabled=len(usernames_clean) == 0)


# ══════════════════════════════════════════════════════════════
#  SCRAPING EXECUTION
# ══════════════════════════════════════════════════════════════

def scrape_and_save_brand(username: str, max_posts: int, brands_col, influencer_col, api_key: str) -> dict:
    """
    Full pipeline for one brand username:
      1. Fetch + parse brand profile & posts      (brand_profile.py)
      2. Parse influencer collab data             (influencer_collab.py)
      3. Save to BOTH collections simultaneously
    Returns a result dict for the UI.
    """
    # ── Step 1: brand profile ─────────────────────────────────
    b_result = process_brand_profile(
        username         = username,
        api_key          = api_key,
        brands_collection= brands_col,
        max_posts        = max_posts,
        with_collab      = True,
    )

    if b_result["status"] != "success":
        return {"status": b_result["status"], "username": username,
                "message": b_result.get("message", "")}

    brand_record = b_result["brand_record"]
    post_rows    = b_result["post_rows"]
    b_action     = b_result["action"]

    # ── Step 2: influencer collabs ───────────────────────────
    ic_result = process_influencer_collabs(
        brand_record         = brand_record,
        post_rows            = post_rows,
        influencer_collection= influencer_col,
    )

    collab_rows      = ic_result["collab_rows"]
    influencer_rows  = ic_result["influencer_rows"]
    collab_summary   = ic_result["collab_summary"]
    ic_action        = ic_result["action"]

    # Merge collab_summary into brand_record for UI display
    brand_record.update(collab_summary)

    return {
        "status":          "success",
        "username":        username,
        "brand_record":    brand_record,
        "post_rows":       post_rows,
        "collab_rows":     collab_rows,
        "influencer_rows": influencer_rows,
        "collab_summary":  collab_summary,
        "b_action":        b_action,
        "ic_action":       ic_action,
    }


if run_btn and usernames_clean:
    st.markdown("---")
    st.markdown(f"### ⏳ Scraping {len(usernames_clean)} brand(s) with {workers_ui} worker(s)…")

    progress_bar = st.progress(0)
    status_text  = st.empty()
    completed    = [0]

    mongo_ok     = st.session_state["mongo_ok"]
    mongo_client = st.session_state["mongo_client"]
    api_key      = st.session_state.get("searchapi_key", "")

    brands_col    = None
    influencer_col = None
    if mongo_ok and mongo_client:
        db             = mongo_client["instagram_db"]
        brands_col     = db["brands"]
        influencer_col = db["influencer_collabs"]

    results_store = {}
    errors_store  = {}

    def _fetch(username):
        return scrape_and_save_brand(username, max_posts_ui, brands_col, influencer_col, api_key)

    with ThreadPoolExecutor(max_workers=workers_ui) as executor:
        futures = {executor.submit(_fetch, u): u for u in usernames_clean}
        for future in futures:
            res = future.result()
            completed[0] += 1
            progress_bar.progress(completed[0] / len(usernames_clean))
            u = res["username"]
            status_text.markdown(f"✅ Processed **@{u}**")

            if res["status"] == "success":
                results_store[u] = res

                if mongo_ok:
                    ba  = res.get("b_action")  or "—"
                    ica = res.get("ic_action") or "—"
                    st.caption(f"🗄 @{u} → brands: **{ba}** · influencer_collabs: **{ica}**")
                else:
                    st.caption(f"⚠️ @{u}: MongoDB not connected — skipping save")
            else:
                errors_store[u] = res.get("message", res["status"])

    progress_bar.progress(1.0)
    status_text.markdown("✅ **Scraping complete!**")

    st.session_state["brand_results"] = results_store
    st.session_state["brand_errors"]  = errors_store

    for u, e in errors_store.items():
        st.error(f"✗ @{u}: {e}")

    if mongo_ok and results_store:
        st.success(
            f"✅ {len(results_store)}/{len(usernames_clean)} brand(s) saved to MongoDB Atlas → `instagram_db`\n\n"
            "Collections: `brands` (profile + posts) · `influencer_collabs` (collab & influencer data)"
        )


# ══════════════════════════════════════════════════════════════
#  UI RENDER FUNCTIONS
# ══════════════════════════════════════════════════════════════

def render_brand_card(p: dict):
    er_pct   = round(p.get("engagement_%", p.get("engagement_rate", 0) * 100), 2)
    er_color = "#60a5fa" if er_pct > 3 else "#fbbf24" if er_pct > 1 else "#f87171"
    verified = "✓" if p.get("is_verified") else ""
    biz      = "🏢" if p.get("is_business") else "🏷"

    st.markdown(f"""
    <div class="card">
        <div style="margin-bottom:12px;">
            <div class="username-title">{biz} @{p['username']}
                <span style="color:#60a5fa">{verified}</span>
            </div>
            <div style="color:#9ca3af; font-size:14px; margin-top:4px;">
                {p.get('full_name','')}
                {'• ' + p['category'] if p.get('category') else ''}
            </div>
            <div style="color:#6b7280; font-size:13px; margin-top:6px;">
                {p.get('bio','')[:120]}
            </div>
        </div>
        <div style="display:grid; grid-template-columns:repeat(4,1fr); gap:10px;">
            <div class="metric-box"><div class="metric-value">{fmt_number(p['follower_count'])}</div><div class="metric-label">Followers</div></div>
            <div class="metric-box"><div class="metric-value">{fmt_number(p['following_count'])}</div><div class="metric-label">Following</div></div>
            <div class="metric-box"><div class="metric-value">{fmt_number(p['post_count'])}</div><div class="metric-label">Total Posts</div></div>
            <div class="metric-box"><div class="metric-value" style="color:{er_color}">{er_pct}%</div><div class="metric-label">Engagement Rate</div></div>
        </div>
        <div style="display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-top:10px;">
            <div class="metric-box"><div class="metric-value">{fmt_number(int(p['like_count_avg']))}</div><div class="metric-label">Avg Likes</div></div>
            <div class="metric-box"><div class="metric-value">{fmt_number(int(p['comment_count_avg']))}</div><div class="metric-label">Avg Comments</div></div>
            <div class="metric-box"><div class="metric-value">{p['posting_frequency_weekly']}</div><div class="metric-label">Posts / Week</div></div>
            <div class="metric-box"><div class="metric-value">{p['posts_scraped']}</div><div class="metric-label">Posts Scraped</div></div>
        </div>
    </div>""", unsafe_allow_html=True)


def render_posts_performance(post_rows: list[dict]):
    if not post_rows:
        st.warning("No posts available.")
        return

    st.markdown('<div class="section-title">📋 Posts Performance</div>', unsafe_allow_html=True)
    df = pd.DataFrame(post_rows)

    display_cols = ["post_id","timestamp","media_type","like_count","comment_count","view_count","engagement_%"]
    df_show      = df[[c for c in display_cols if c in df.columns]].copy()
    df_show["engagement_%"] = df_show["engagement_%"].apply(lambda x: f"{x:.4f}%")

    st.dataframe(df_show, use_container_width=True, hide_index=True)

    avg_eng = df["engagement_%"].mean() if "engagement_%" in df.columns else 0
    st.metric("Average Engagement %", f"{avg_eng:.4f}%")


def render_influencer_collab_summary(p: dict):
    type_badges = "".join(
        f'<span class="badge {COLLAB_COLORS.get(ct,"")}">{ct.replace("_"," ")} ({p.get(ct,0)})</span>'
        for ct in COLLAB_TYPES_ALL if p.get(ct, 0) > 0
    )
    st.markdown(f"""
    <div class="card-blue">
        <div class="section-title">👤 Influencer Collaboration Overview</div>
        <div style="display:grid; grid-template-columns:repeat(4,1fr); gap:10px;">
            <div class="metric-box"><div class="metric-value">{p.get('collab_posts_count',0)}</div><div class="metric-label">Collab Posts</div></div>
            <div class="metric-box"><div class="metric-value">{round(p.get('collab_rate',0)*100,1)}%</div><div class="metric-label">Collab Rate</div></div>
            <div class="metric-box"><div class="metric-value">{p.get('unique_influencers_mentioned',0)}</div><div class="metric-label">Unique Influencers</div></div>
            <div class="metric-box"><div class="metric-value">${fmt_number(int(p.get('total_estimated_spend_usd',0)))}</div><div class="metric-label">Est. Spend</div></div>
        </div>
        <div style="margin-top:14px;">
            <div style="color:#9ca3af; font-size:12px; margin-bottom:8px;">TYPE BREAKDOWN</div>
            <div style="display:flex; flex-wrap:wrap; gap:6px;">
                {type_badges or '<span style="color:#6b7280">None detected</span>'}
            </div>
        </div>
    </div>""", unsafe_allow_html=True)


def render_collab_posts(collab_rows: list[dict]):
    if not collab_rows:
        st.markdown('<div class="card"><p style="color:#6b7280; text-align:center; padding:20px;">No influencer collaboration posts detected</p></div>', unsafe_allow_html=True)
        return

    st.markdown(f'<div class="section-title">📋 Influencer Collab Posts ({len(collab_rows)})</div>', unsafe_allow_html=True)
    for post in collab_rows:
        types_html    = "".join(collab_badge(t) for t in post.get("collab_types", []))
        mentions_html = "".join(f'<span class="badge badge-blue">@{m}</span>' for m in post.get("mentions", [])[:5])
        codes_html    = "".join(f'<span class="badge badge-yellow">🎟 {c}</span>' for c in post.get("promo_codes", []))
        est_val       = post.get("estimated_value_usd", 0)
        val_str       = f"~${est_val:,.0f}" if est_val > 0 else ""
        er_pct        = round(post.get("engagement_rate", 0) * 100, 2)

        st.markdown(f"""
        <div class="collab-row">
            <div style="display:flex; justify-content:space-between;">
                <div style="flex:1;">
                    <div style="margin-bottom:6px;">{types_html}</div>
                    <div style="color:#d1d5db; font-size:13px; margin-bottom:8px; line-height:1.5;">
                        {post.get('caption','')[:200]}{'...' if len(post.get('caption',''))>200 else ''}
                    </div>
                    <div style="display:flex; flex-wrap:wrap; gap:4px;">{mentions_html}{codes_html}</div>
                </div>
                <div style="text-align:right; margin-left:16px; min-width:110px;">
                    <div style="color:#60a5fa; font-size:14px; font-weight:700;">{val_str}</div>
                    <div style="color:#9ca3af; font-size:12px;">ER: {er_pct}%</div>
                    <div style="color:#6b7280; font-size:11px;">
                        ❤ {fmt_number(post.get('like_count',0))}
                        💬 {fmt_number(post.get('comment_count',0))}
                    </div>
                    <div style="color:#374151; font-size:11px; margin-top:4px;">
                        {post.get('timestamp','')[:10]}
                    </div>
                </div>
            </div>
            <div style="margin-top:6px;">
                <a href="{post.get('post_url','#')}" target="_blank" style="color:#60a5fa; font-size:12px;">🔗 View Post</a>
            </div>
        </div>""", unsafe_allow_html=True)


def render_influencer_table(influencer_rows: list[dict]):
    if not influencer_rows:
        return
    st.markdown('<div class="section-title">👤 Influencer Summary</div>', unsafe_allow_html=True)
    df = pd.DataFrame(influencer_rows)
    df["collab_types"]        = df["collab_types"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
    df["avg_engagement_rate"] = df["avg_engagement_rate"].apply(lambda x: f"{round(x*100,2)}%")
    df["estimated_value_usd"] = df["estimated_value_usd"].apply(lambda x: f"${x:,.0f}")
    df = df.rename(columns={
        "influencer_username":  "Influencer",
        "total_collab_posts":   "Posts",
        "collab_types":         "Types",
        "avg_engagement_rate":  "Avg ER",
        "estimated_value_usd":  "Est. Value",
    })
    st.dataframe(df[["Influencer","Posts","Types","Avg ER","Est. Value"]], use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════
#  RESULTS DISPLAY
# ══════════════════════════════════════════════════════════════

if "brand_results" in st.session_state and st.session_state["brand_results"]:
    results = st.session_state["brand_results"]
    st.markdown("---")

    total_followers   = sum(r["brand_record"]["follower_count"]                    for r in results.values())
    total_collabs     = sum(r["brand_record"].get("collab_posts_count", 0)         for r in results.values())
    total_influencers = sum(r["brand_record"].get("unique_influencers_mentioned", 0) for r in results.values())

    st.markdown(f"""
    <div style="display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:24px;">
        <div class="metric-box"><div class="metric-value">{len(results)}</div><div class="metric-label">Brands Scraped</div></div>
        <div class="metric-box"><div class="metric-value">{fmt_number(total_followers)}</div><div class="metric-label">Total Reach</div></div>
        <div class="metric-box"><div class="metric-value">{total_collabs}</div><div class="metric-label">Collab Posts</div></div>
        <div class="metric-box"><div class="metric-value">{total_influencers}</div><div class="metric-label">Unique Influencers</div></div>
    </div>""", unsafe_allow_html=True)

    tab_names = [f"@{u}" for u in results] + ["📊 Compare All"]
    tabs      = st.tabs(tab_names)

    for i, (username, res) in enumerate(results.items()):
        p  = res["brand_record"]
        pr = res["post_rows"]
        cr = res["collab_rows"]
        ir = res["influencer_rows"]

        with tabs[i]:
            render_brand_card(p)

            left, right = st.columns(2)
            with left:
                render_influencer_collab_summary(p)
            with right:
                st.markdown(f"""
                <div class="card">
                    <div class="section-title">📈 Content Analytics</div>
                    <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
                        <div class="metric-box"><div class="metric-value">{round(p.get('video_ratio',0)*100)}%</div><div class="metric-label">Video Content</div></div>
                        <div class="metric-box"><div class="metric-value">{round(p.get('image_ratio',0)*100)}%</div><div class="metric-label">Image Content</div></div>
                        <div class="metric-box"><div class="metric-value">{p.get('hashtag_density_avg',0)}</div><div class="metric-label">Avg Hashtags</div></div>
                        <div class="metric-box"><div class="metric-value">{p.get('follower_following_ratio',0)}</div><div class="metric-label">F/F Ratio</div></div>
                    </div>
                </div>""", unsafe_allow_html=True)

            # Posts performance table
            with st.expander("📊 Posts Performance Table"):
                st.markdown('<div class="card">', unsafe_allow_html=True)
                render_posts_performance(pr)
                st.markdown("</div>", unsafe_allow_html=True)

            # Influencer table
            if ir:
                st.markdown('<div class="card">', unsafe_allow_html=True)
                render_influencer_table(ir)
                st.markdown("</div>", unsafe_allow_html=True)

            # Collab posts detail
            st.markdown('<div class="card">', unsafe_allow_html=True)
            render_collab_posts(cr)
            st.markdown("</div>", unsafe_allow_html=True)

            # Raw posts table
            with st.expander(f"📋 All {len(pr)} Posts — Raw Data"):
                df_p = pd.DataFrame(pr)
                if "collab_types" in df_p.columns:
                    df_p["collab_types"] = df_p["collab_types"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
                if "hashtags" in df_p.columns:
                    df_p["hashtags"]     = df_p["hashtags"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)

                show_cols = [c for c in [
                    "timestamp","media_type","like_count","comment_count","view_count",
                    "engagement_rate","engagement_%","hashtag_count",
                    "is_collaboration","collab_types","post_url",
                ] if c in df_p.columns]
                st.dataframe(df_p[show_cols], use_container_width=True, hide_index=True)

    # ── Compare All tab ───────────────────────────────────────
    with tabs[-1]:
        st.markdown("### 📊 Side-by-Side Brand Comparison")

        compare_data = [
            {
                "Brand":              f"@{u}",
                "Followers":          fmt_number(p["follower_count"]),
                "Eng. Rate":          f"{round(p.get('engagement_%', p.get('engagement_rate',0)*100), 2)}%",
                "Avg Likes":          fmt_number(int(p["like_count_avg"])),
                "Avg Comments":       fmt_number(int(p["comment_count_avg"])),
                "Posts/Week":         p["posting_frequency_weekly"],
                "Collab Posts":       p.get("collab_posts_count", 0),
                "Collab Rate":        f"{round(p.get('collab_rate',0)*100, 1)}%",
                "Unique Influencers": p.get("unique_influencers_mentioned", 0),
                "Est. Spend":         f"${fmt_number(int(p.get('total_estimated_spend_usd',0)))}",
                "Top Collab":         p.get("most_common_collab_type","NONE"),
                "Verified":           "✓" if p.get("is_verified") else "—",
                "Video %":            f"{round(p.get('video_ratio',0)*100)}%",
            }
            for u, res in results.items()
            for p in [res["brand_record"]]
        ]
        st.dataframe(pd.DataFrame(compare_data), use_container_width=True, hide_index=True)

        st.markdown("### 💾 Download")
        dc1, dc2 = st.columns(2)
        with dc1:
            brands_csv = pd.DataFrame([r["brand_record"] for r in results.values()])
            st.download_button(
                "⬇️ Brands CSV",
                brands_csv.to_csv(index=False).encode("utf-8"),
                "brand_profiles.csv", "text/csv",
                use_container_width=True,
            )
        with dc2:
            all_collabs = [post for r in results.values() for post in r["collab_rows"]]
            if all_collabs:
                df_c = pd.DataFrame(all_collabs)
                if "collab_types" in df_c.columns:
                    df_c["collab_types"] = df_c["collab_types"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
                if "hashtags" in df_c.columns:
                    df_c["hashtags"]     = df_c["hashtags"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
                st.download_button(
                    "⬇️ Influencer Collabs CSV",
                    df_c.to_csv(index=False).encode("utf-8"),
                    "influencer_collabs.csv", "text/csv",
                    use_container_width=True,
                )

elif "brand_results" not in st.session_state:
    st.markdown("""
    <div class="card" style="text-align:center; padding:60px 20px; margin-top:20px;">
        <div style="font-size:52px; margin-bottom:16px;">🏷</div>
        <div style="font-size:20px; font-weight:600; color:#9ca3af; margin-bottom:8px;">
            Enter brand usernames above and click Scrape
        </div>
        <div style="color:#6b7280; font-size:14px; line-height:1.8;">
            Scrapes up to 5 public brand Instagram accounts simultaneously<br>
            Detects 9 influencer collaboration types · Estimates campaign spend<br>
            Saves brands <strong style="color:#60a5fa">&amp;</strong> influencer collabs to MongoDB Atlas at the same time
        </div>
    </div>""", unsafe_allow_html=True)
