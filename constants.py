"""
Application constants for ViralVibes.
Centralized configuration for UI components, styling, and feature data.
"""

from fasthtml.components import Path, Svg
from monsterui.all import StepT

# =============================================================================
# ASSET PATHS
# =============================================================================
ICONS_PATH = "assets/icons"

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

section_base = f"{FLEX_COL} {SECTION_BASE}"
between = "flex justify-between"

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


# =============================================================================
# STEP COMPONENTS
# =============================================================================
# Base class: vertical on mobile, horizontal on desktop
STEPS_CLS = f"{StepT.neutral} steps-vertical md:steps-horizontal w-full"

# Step configurations for playlist analysis flow
PLAYLIST_STEPS_CONFIG = [
    ("Paste Playlist URL", "📋", "Copy and paste any YouTube playlist URL"),
    ("Validate URL", "✓", "We verify it's a valid YouTube playlist"),
    ("Fetch Video Data", "📊", "Retrieve video statistics and metadata"),
    ("Calculate Metrics", "🔢", "Process views, likes, and engagement rates"),
    ("Display Results", "📈", "View comprehensive analysis in charts"),
]

# =============================================================================
# FEATURE & BENEFIT CONFIGURATIONS
# =============================================================================
FEATURES = [
    (
        "Uncover Viral Secrets",
        "Paste a playlist and uncover the secrets behind viral videos.",
        "search",
    ),
    (
        "Instant Playlist Insights",
        "Get instant info on trending videos and engagement patterns.",
        "zap",
    ),
    (
        "No Login Required",
        "Just paste a link and go. No signup needed!",
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
        "What kinds of things can I do with ViralVibes?",
        "ViralVibes is designed for analyzing YouTube playlists and surfacing insights. "
        "You can use it to track engagement, spot viral trends, compare creators, or run "
        "deep dives on specific niches. It works equally well for quick experiments, "
        "dashboards, research projects, or as part of a bigger content strategy.",
    ),
    (
        "Where can I deploy ViralVibes?",
        "ViralVibes is a Python-based web app. You can run it locally, or deploy it to "
        "services like Railway, Vercel, Hugging Face Spaces, Replit, or any VPS/server "
        "with Python installed. It's lightweight and runs on all major operating systems.",
    ),
    (
        "How is ViralVibes different from other analytics tools?",
        "Most analytics tools focus on generic stats. ViralVibes is built specifically for "
        "YouTube playlists — it digs into engagement rate, controversy score, likes vs "
        "dislikes, and audience sentiment in a way that off-the-shelf dashboards don't.",
    ),
    (
        "Does this only work for static reports, or can it be interactive?",
        "It's fully interactive. ViralVibes surfaces real-time insights, lets you explore "
        "data visually, and updates dynamically as you validate playlists. You're not just "
        "looking at static charts — you're exploring living data.",
    ),
    (
        "How accurate is the data?",
        "ViralVibes pulls data directly from YouTube's official API, ensuring accuracy. "
        "Results are cached for performance, so you might see a slight delay if a playlist "
        "was recently analyzed. You can always trigger a fresh analysis.",
    ),
]

# =============================================================================
# DATABASE TABLE CONSTANTS
# =============================================================================
PLAYLIST_STATS_TABLE = "playlist_stats"
PLAYLIST_JOBS_TABLE = "playlist_jobs"
SIGNUPS_TABLE = "signups"

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
icons = ICONS_PATH
between = FLEX_BETWEEN
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
