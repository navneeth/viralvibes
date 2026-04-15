"""
Application constants for ViralVibes.
Centralized configuration for UI components, styling, and feature data.
"""

from fasthtml.components import Path, Svg
import os

# =============================================================================
# WORKER CONFIGURATION
# =============================================================================
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))

# =============================================================================
# ASSET PATHS
# =============================================================================
ICONS_PATH = "assets/icons"

# =============================================================================
# AUTH ROUTES
# =============================================================================
AUTH_SIGNUP_EMAIL = "/sign-up/email"
AUTH_LOGIN = "/login"

# =============================================================================
# CSS CLASS CONSTANTS
# =============================================================================
# Layout
FLEX_COL = "flex flex-col"
FLEX_CENTER = "flex items-center"
FLEX_BETWEEN = "flex justify-between"
GAP_2 = "flex gap-2"


# Sections
SECTION_BASE = "pt-8 px-4 pb-24 gap-8 lg:gap-16 lg:pt-16 lg:px-16"
SECTION_WRAPPER = f"{FLEX_COL} {SECTION_BASE}"
between = FLEX_BETWEEN

# Cards
CARD_BASE = (
    "max-w-2xl mx-auto my-12 p-8 shadow-lg rounded-xl bg-white text-gray-900 "
    "hover:shadow-xl transition-shadow duration-300"
)
HEADER_CARD = (
    "bg-gradient-to-r from-rose-500 via-red-600 to-red-700 text-white "
    "py-8 px-6 text-center rounded-xl"
)

# Single source of truth for standard surface cards (forms/newsletter)
FORM_CARD = (
    "max-w-420px; margin: 3rem auto; padding: 2rem; "
    "box-shadow: 0 4px 24px rgba(0,0,0,0.06); "
    "border-radius: 1.2rem; background: #fff; color: #333; "
    "transition: all 0.3s ease;"
)
NEWSLETTER_CARD = FORM_CARD  # Keep backwards compatibility

# Convenience aliases for common CSS patterns
CARD_BASE_CLS = CARD_BASE
HEADER_CARD_CLS = HEADER_CARD
CARD_INLINE_STYLE = FORM_CARD
FORM_CARD_CLS = FORM_CARD
NEWSLETTER_CARD_CLS = NEWSLETTER_CARD
FLEX_COL_CENTER_CLS = FLEX_COL + " " + FLEX_CENTER

# Centralized theme for styles (reduces repetition)
THEME = {
    "primary_bg": "bg-red-500",
    "primary_hover": "hover:bg-red-600",
    "secondary_text": "text-blue-700",
    "neutral_bg": "bg-gray-100",
    "card_base": "bg-white rounded-2xl shadow-lg p-8 border border-gray-200",
    "flex_col": "flex flex-col",
    "flex_row": "flex-row",
    "flex_center": "flex items-center justify-center",
    "gap_sm": "gap-4",
    # Add more as needed
}

# --- centralized reusable Tailwind clusters (start small) ---
STYLES = {
    "hero_title": "text-4xl md:text-5xl font-poppins font-extrabold text-gray-900 leading-tight",
    "hero_subhead": "mt-4 text-lg text-gray-700 max-w-xl",
    "cta_primary": "bg-red-500 hover:bg-red-600 text-white px-6 py-3 rounded-lg shadow-md",
    "cta_secondary": "ml-4 inline-flex items-center px-4 py-3 rounded-md text-gray-700 bg-gray-100 hover:bg-gray-200",
    "trust_line": "mt-4 text-sm text-gray-500",
    "card_thumbnail": "w-full rounded-2xl object-cover shadow-2xl",
    "hero_grid": "grid grid-cols-1 md:grid-cols-2 gap-8 items-center max-w-7xl mx-auto px-4 sm:px-6 py-12",
    # New consolidated clusters
    "btn_full": "w-full py-3 px-6 text-base font-semibold rounded-lg shadow-lg bg-gradient-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 text-white border-0 focus:ring-4 focus:ring-red-200 transition-all duration-200 hover:scale-105 active:scale-95 transform",
    "btn_refresh": "w-full py-3 px-6 text-base font-semibold rounded-lg shadow-lg bg-gradient-to-r from-orange-500 to-orange-600 hover:from-orange-600 hover:to-orange-700 text-white border-0 focus:ring-4 focus:ring-orange-200 transition-all duration-200 hover:scale-105 active:scale-95 transform",
    "badge_small": "inline-flex items-center px-2 py-1 bg-gray-50 rounded-md text-xs font-medium",
    "badge_info": "inline-flex items-center px-2 py-1 rounded-md text-xs font-medium bg-blue-50 text-blue-700",
    "progress_meter": "w-full h-2 rounded-full bg-gray-200 [&::-webkit-progress-value]:rounded-full [&::-webkit-progress-value]:bg-red-600 [&::-moz-progress-bar]:bg-red-600",
    "table_base": "w-full text-sm text-gray-700 border border-gray-200 rounded-lg shadow-sm overflow-hidden",
}


# Step configurations for playlist analysis flow
PLAYLIST_STEPS_CONFIG = [
    ("Paste Playlist URL", "📋", "Copy and paste any YouTube playlist URL"),
    ("Validate URL", "✓", "We verify it's a valid YouTube playlist"),
    ("Fetch Video Data", "📊", "Retrieve video statistics and metadata"),
    ("Calculate Metrics", "🔢", "Process views, likes, and engagement rates"),
    ("Display Results", "📈", "View comprehensive analysis in charts"),
]

# =============================================================================
# VALUE PROPOSITION (Creator-First Messaging)
# =============================================================================
HERO_HEADLINE = "Find YouTube creators who actually drive results"
HERO_SUBHEADLINE = (
    "Browse ranked creator lists by niche, country, and growth. "
    "Track engagement, growth velocity, and revenue potential "
    "Spot high-engagement channels and rising stars for brand campaigns. "
    "Spend less time researching and more time creating campaigns that pay off."
)


# Social proof metrics
TOTAL_CREATORS = "1 Million+"
TOTAL_CATEGORIES = "25+"
COUNTRIES_COVERED = "150+"

# Trust markers
TRUST_MARKERS = [
    "No credit card required",
    "Real performance data, not vanity metrics",
    "1 Million+ ranked creators",
    "Updated daily",
]

# =============================================================================
# SOCIALS (single source of truth — update once)
# =============================================================================
SOCIALS = {
    "x": "https://x.com/viralvibesfyi",
    "linkedin": "https://linkedin.com/company/viralvibesfyi",  # placeholder — swap when real
    "youtube": "https://www.youtube.com/@viralvibesfyi",
}

# Core value propositions — 3-pillar product + ROAS framing
CORE_VALUE_PROPS = [
    {
        "icon": "list",
        "title": "Creator Lists",
        "headline": "Pre-ranked by niche & country",
        "description": "Browse 1M+ creators organized by category, geography, and "
        "engagement quality — not subscriber count. Filter to a shortlist in minutes.",
    },
    {
        "icon": "user-check",
        "title": "Creator Profiles",
        "headline": "Every metric that predicts ROAS",
        "description": "Engagement rate, 30-day growth velocity, content consistency, "
        "and audience quality — on any public channel, no OAuth required.",
    },
    {
        "icon": "play-circle",
        "title": "Playlist Analysis",
        "headline": "Decode any content library",
        "description": "Paste any public playlist URL. Get views, engagement patterns, "
        "controversy signals, and viral scoring in under 2 seconds.",
    },
    {
        "icon": "dollar-sign",
        "title": "Estimate",
        "headline": "Revenue potential",
        "description": "Calculate estimated monthly earnings based on views, niche, "
        "and country. Compare creator ROI instantly.",
    },
]

# =============================================================================
# FEATURE & BENEFIT CONFIGURATIONS (Legacy - keeping for backwards compatibility)
# =============================================================================
FEATURES = [
    (
        "Real Engagement Signals",
        "Filter by engagement quality — not inflated subscriber counts. "
        "Find creators whose audiences actually watch and interact.",
        "bar-chart-2",
    ),
    (
        "Growth Velocity",
        "See who is rising, plateauing, or declining right now. "
        "Catch rising stars before they become expensive.",
        "trending-up",
    ),
    (
        "No Login for Playlist Analysis",
        "Paste any YouTube playlist URL and get instant viral pattern detection. "
        "No account, no friction.",
        "unlock",
    ),
]

BENEFITS = [
    (
        "Real-time Analysis",
        "Track trends as they emerge with live data processing.",
        "activity",
    ),
    (
        "Engagement Metrics",
        "Understand what drives likes, shares, and comments.",
        "heart",
    ),
    (
        "Top Creator Insights",
        "Identify breakout content and rising stars.",
        "star",
    ),
]

# =============================================================================
# LISTS FEATURE TABS
# =============================================================================
LISTS_FEATURE_TABS = [
    {
        "id": "top-rated",
        "label": "Top Rated",
        "icon": "star",
        "description": "Creators ranked by engagement quality, not subscriber count. "
        "Filter by niche, country, and growth trend.",
        "highlight": "1M+ ranked creators",
    },
    {
        "id": "by-country",
        "label": "By Country",
        "icon": "globe",
        "description": "Find creators in specific markets. Compare engagement benchmarks "
        "across 150+ countries without manual research.",
        "highlight": "150+ countries",
    },
    {
        "id": "by-category",
        "label": "By Category",
        "icon": "grid",
        "description": "Discover top creators in Tech, Gaming, Finance, Beauty, "
        "and 25+ other categories scored against their peers.",
        "highlight": "25+ categories",
    },
    {
        "id": "rising-stars",
        "label": "Rising Stars",
        "icon": "trending-up",
        "description": "Channels gaining momentum right now — before their rates double. "
        "Ranked by 30-day subscriber and engagement velocity.",
        "highlight": "Updated daily",
    },
]

# =============================================================================
# TESTIMONIALS
# =============================================================================
TESTIMONIALS = [
    (
        "ViralVibes helped me decode my channel's growth patterns. The dashboard is a game changer!",
        "Alex Kim",
        "YouTube Creator",
        "TechExplained",
        "/static/testimonials/alex-kim.png",
    ),
    (
        "The playlist analytics are incredibly detailed and easy to understand. Highly recommended.",
        "Priya Singh",
        "Content Strategist",
        "MediaPulse",
        "/static/testimonials/priya-singh.png",
    ),
    (
        "I love how ViralVibes visualizes engagement and controversy. It's a must-have for creators.",
        "Jordan Lee",
        "Growth Hacker",
        "ViralBoost",
        "/static/testimonials/jordan-lee.png",
    ),
]

# =============================================================================
# FAQ DATA
# =============================================================================
FAQS = [
    (
        "What is ViralVibes best used for?",
        "ViralVibes is a creator intelligence platform for brands and agencies running YouTube "
        "campaigns. Instead of picking creators by subscriber count, you get ranked lists built "
        "on real engagement signals, growth velocity, and consistency — the inputs that actually "
        "predict whether a campaign will convert. You can also analyze any public playlist for "
        "free, with no account required.",
    ),
    (
        "What is ROAS and why does it matter for creator campaigns?",
        "ROAS stands for Return on Ad Spend. If you spend $1,000 on a creator campaign and "
        "generate $3,000 in revenue, your ROAS is 3x. Most creator campaigns look good in "
        "reports — views, likes — but fail on ROAS because selection was based on vanity metrics. "
        "ViralVibes surfaces signals that correlate with conversion, not just attention.",
    ),
    (
        "How does ViralVibes rank creators?",
        "We analyze large-scale public YouTube data and normalize metrics across categories and "
        "geographies — so a niche creator in Japan can be compared fairly to one in the US. "
        "Rankings factor in engagement quality, content consistency, 30-day momentum, and "
        "risk signals. The result is a composite score you can see and understand — not a "
        "black-box recommendation.",
    ),
    (
        "How is ViralVibes different from SocialBlade or YouTube Analytics?",
        "SocialBlade shows raw subscriber and view counts. YouTube Analytics only works on "
        "channels you own. ViralVibes works on any public channel, focuses on engagement quality "
        "over volume, surfaces momentum shifts (who is rising vs. declining), and ranks creators "
        "in context — so you can compare across niches and geographies without manual research.",
    ),
    (
        "Is it free to use?",
        "Yes — creator lists, search, and playlist analysis are all free to explore with no "
        "credit card required. Pro unlocks deeper rankings, exports, and saved shortlists "
        "for agency workflows.",
    ),
    (
        "How often is creator data updated?",
        "Creator rankings are refreshed on a rolling basis. The platform tracks 1 Million+ channels "
        "across 150+ countries, with momentum signals updated continuously. Playlist analysis "
        "fetches live data from YouTube's API when triggered.",
    ),
    (
        "Who is ViralVibes for?",
        "Brands running YouTube campaigns who want to improve ROAS. Agencies managing creator "
        "rosters who need scalable discovery without manual research. And creators who want to "
        "benchmark themselves and understand their own performance signals.",
    ),
]

# =============================================================================
# DATABASE TABLE CONSTANTS
# =============================================================================
PLAYLIST_STATS_TABLE = "playlist_stats"
PLAYLIST_JOBS_TABLE = "playlist_jobs"
SIGNUPS_TABLE = "signups"
CREATORS_TABLE = "creators"

# Creator tables (matching actual DB schema)
CREATOR_TABLE = "creators"
CREATOR_SYNC_JOBS_TABLE = "creator_sync_jobs"
USER_FAVOURITE_CREATORS_TABLE = "user_favourite_creators"

# Update frequency (frugal)
CREATOR_WORKER_BATCH_SIZE = 2  # Process at a time
CREATOR_WORKER_POLL_INTERVAL = 300  # 5 minutes
CREATOR_UPDATE_INTERVAL_DAYS = 30  # Scheduled background sync interval
CREATOR_REDISCOVERY_THRESHOLD_DAYS = 7  # Sync when rediscovered in playlists (fresher data)
CREATOR_WORKER_SYNC_TIMEOUT = 60
CREATOR_WORKER_MAX_RETRIES = 3
CREATOR_WORKER_RETRY_BASE = 3600  # 1 hour
CREATOR_WORKER_MAX_RUNTIME = 86400  # 24 hours

# Start with a few known creators
KNOWN_CREATORS = [
    # Add 3-5 creators you know:
    {
        "channel_id": "UCX6OQ3DkcsbYNE6H8uQQuVA",
        "channel_name": "Mr Beast",
        "channel_url": "https://www.youtube.com/channel/UCX6OQ3DkcsbYNE6H8uQQuVA",
    },
]

# =============================================================================
# SAMPLE PLAYLISTS (Fallback only - prefer DB fetch)
# =============================================================================
# NOTE: These are fallback samples. The app should fetch fresh playlists
# from the database using db.fetch_playlists() for up-to-date data.
KNOWN_PLAYLISTS = [
    {
        "title": "Most Viewed Videos of All Time",
        "url": "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC",
        "video_count": 539,
    },
    {
        "title": "Best Remixes of Popular Songs",
        "url": "https://www.youtube.com/playlist?list=PLxA687tYuMWjS8IGRWkCzwTn10XcEccaZ",
        "video_count": 100,
    },
    {
        "title": "Viral Songs Right Now",
        "url": "https://www.youtube.com/playlist?list=PL6vc4PXosXVvX3S4RYOS9CC_F4dFJs_q2",
        "video_count": 50,
    },
    {
        "title": "NFL Top 100 Greatest Players",
        "url": "https://www.youtube.com/playlist?list=PL0xvhH4iaYhy4ulh0h-dn4mslO6B8nj0-",
        "video_count": 100,
    },
    {
        "title": "Video Game Top 10's",
        "url": "https://www.youtube.com/playlist?list=PL3662B2B44970D6CC",
        "video_count": 50,
    },
]

# =============================================================================
# CONFIGURATION
# =============================================================================
# Maximum playlists to show in sample selector
MAX_SAMPLE_PLAYLISTS = 5

# Cache TTL for playlist data (in seconds)
PLAYLIST_CACHE_TTL = 3600  # 1 hour

# Backwards compatibility aliases
testimonials = TESTIMONIALS
faqs = FAQS

# Define the YouTube SVG component
youtube_icon = Svg(
    Path(
        d="M2.5 17a24.12 24.12 0 0 1 0-10 2 2 0 0 1 1.4-1.4 49.56 49.56 0 0 1 16.2 0A2 2 0 0 1 21.5 7a24.12 24.12 0 0 1 0 10 2 2 0 0 1-1.4 1.4 49.55 49.55 0 0 1-16.2 0A2 2 0 0 1 2.5 17"
    ),
    Path(d="m10 15 5-3-5-3z"),
    xmlns="http://www.w3.org/2000/svg",
    width="24",
    height="24",
    viewBox="0 0 24 24",
    fill="none",
    stroke="currentColor",
    stroke_width="2",
    stroke_linecap="round",
    stroke_linejoin="round",
    cls="lucide lucide-youtube text-red-600",  # Added text-red-600 for YouTube branding color
)

"""
Centralized constants for the application.
"""


# Job Status Constants
class JobStatus:
    """Job status values used across the application."""

    QUEUED = "queued"
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETE = "complete"
    DONE = "done"  # Legacy, same as COMPLETE
    FAILED = "failed"
    BLOCKED = "blocked"

    # Convenience sets
    ACTIVE = {QUEUED, PENDING, PROCESSING}
    FINISHED = {COMPLETE, DONE, FAILED, BLOCKED}
    SUCCESS = {COMPLETE, DONE}
    ERROR = {FAILED, BLOCKED}


# Time Constants
class TimeEstimates:
    """Time estimation constants."""

    SECONDS_PER_VIDEO = 2.5  # Average processing time per video
    MIN_ESTIMATE_MINUTES = 1  # Minimum time estimate to show
