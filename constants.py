"""
Application constants for ViralVibes.
Centralized configuration for UI components, styling, and feature data.
"""

from monsterui.all import StepsT, StepT

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
GAP_4 = "flex gap-4"

# Sections
SECTION_BASE = "pt-8 px-4 pb-24 gap-8 lg:gap-16 lg:pt-16 lg:px-16"

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


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def maxpx(px: int) -> str:
    """Generate max-width class in pixels."""
    return f"w-full max-w-[{px}px]"


def maxrem(rem: int) -> str:
    """Generate max-width class in rem units."""
    return f"w-full max-w-[{rem}rem]"


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
