from monsterui.all import LiStep, Steps, StepsT, StepT

icons = "assets/icons"
between = "flex justify-between"

# CSS Class Constants
FLEX_COL = "flex flex-col"
FLEX_CENTER = "flex items-center"
FLEX_BETWEEN = "flex justify-between"
GAP_2 = "flex gap-2"
GAP_4 = "flex gap-4"
SECTION_BASE = "pt-8 px-4 pb-24 gap-8 lg:gap-16 lg:pt-16 lg:px-16"
CARD_BASE = "max-w-2xl mx-auto my-12 p-8 shadow-lg rounded-xl bg-white text-gray-900 hover:shadow-xl transition-shadow duration-300"
HEADER_CARD = "bg-gradient-to-r from-rose-500 via-red-600 to-red-700 text-white py-8 px-6 text-center rounded-xl"


def maxpx(px):
    return f"w-full max-w-[{px}px]"


def maxrem(rem):
    return f"w-full max-w-[{rem}rem]"


# Single source of truth for standard surface cards (forms/newsletter)
FORM_CARD = (
    "max-w-420px; margin: 3rem auto; padding: 2rem; box-shadow: 0 4px 24px #0001; "
    "border-radius: 1.2rem; background: #fff; color: #333; transition: all 0.3s ease; hover:shadow-xl"
)
# Keep backwards compatibility: newsletter uses the same style
NEWSLETTER_CARD = FORM_CARD

# Base class: vertical on mobile, horizontal on desktop
# Tailwind responsive classes: vertical on small, horizontal on md+
STEPS_CLS = f"{StepT.neutral} steps-vertical md:steps-horizontal w-full"

# Step configurations
PLAYLIST_STEPS_CONFIG = [
    ("Paste Playlist URL", "📋", "Copy and paste any YouTube playlist URL"),
    ("Validate URL", "✓", "We verify it's a valid YouTube playlist"),
    ("Fetch Video Data", "📊", "Retrieve video statistics and metadata"),
    ("Calculate Metrics", "🔢", "Process views, likes, and engagement rates"),
    ("Display Results", "📈", "View comprehensive analysis in a table"),
]

# Feature and Benefit configurations
FEATURES = [
    (
        "Uncover Viral Secrets",
        "Paste a playlist and uncover the secrets behind viral videos.",
        "search",
    ),
    ("Instant Playlist Insights", "Get instant info on trending videos.", "zap"),
    ("No Login Required", "Just paste a link and go. No signup needed!", "unlock"),
]

BENEFITS = [
    ("Real-time Analysis", "Track trends as they emerge.", "activity"),
    (
        "Engagement Metrics",
        "Understand what drives likes, shares, and comments.",
        "heart",
    ),
    ("Top Creator Insights", "Identify breakout content and rising stars.", "star"),
]

benefits_lst = [
    (
        "Get started fast",
        "A single Python file is all that's needed to create any app you can think of. Or bring in any Python or JS library you like.",
    ),
    (
        "Flexibility",
        "FastHTML provides full access to HTTP, HTML, JS, and CSS, bringing the foundations of the web to you. There's no limits to what you can build.",
    ),
    (
        "Speed & scale",
        "FastHTML applications are fast and scalable. They're also easy to deploy, since you can use any hosting service that supports Python.",
    ),
]

testimonials = [
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
        "I love how ViralVibes visualizes engagement and controversy. It’s a must-have for creators.",
        "Jordan Lee",
        "Growth Hacker",
        "ViralBoost",
        "/static/testimonials/jordan-lee.png",
    ),
]

faqs = [
    (
        "What kinds of things can I do with ViralVibes?",
        "ViralVibes is designed for analyzing YouTube playlists and surfacing insights. You can use it to track engagement, spot viral trends, compare creators, or run deep dives on specific niches. It works equally well for quick experiments, dashboards, research projects, or as part of a bigger content strategy.",
    ),
    (
        "Where can I deploy ViralVibes?",
        "ViralVibes is a Python-based web app. You can run it locally, or deploy it to services like Railway, Vercel, Hugging Face Spaces, Replit, or any VPS/server with Python installed. It’s lightweight and runs on all major operating systems.",
    ),
    (
        "How is ViralVibes different from other analytics tools?",
        "Most analytics tools focus on generic stats. ViralVibes is built specifically for YouTube playlists — it digs into engagement rate, controversy score, likes vs dislikes, and audience sentiment in a way that off-the-shelf dashboards don’t.",
    ),
    (
        "Does this only work for static reports, or can it be interactive?",
        "It’s fully interactive. ViralVibes surfaces real-time insights, lets you explore data visually, and updates dynamically as you validate playlists. You’re not just looking at static charts — you’re exploring living data.",
    ),
]


# YouTube Playlist Constants
KNOWN_PLAYLISTS = [
    {
        "title": "Most Viewed Videos of All Time",
        "url": "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC",
        "video_count": 539,
        "channel": None,
        "query_used": "most viewed videos playlist",
    },
    {
        "title": "Best Remixes of Popular Songs",
        "url": "https://www.youtube.com/playlist?list=PLxA687tYuMWjS8IGRWkCzwTn10XcEccaZ",
        "video_count": None,
        "channel": None,
        "query_used": "Best remixes of popular songs",
    },
    {
        "title": "Viral Songs Right Now 🔝 Most Popular Songs",
        "url": "https://www.youtube.com/playlist?list=PL6vc4PXosXVvX3S4RYOS9CC_F4dFJs_q2",
        "video_count": 50,
        "channel": None,
        "query_used": "Viral Songs Right Now",
    },
    {
        "title": "NFL Top 100 Greatest Players Of All Time",
        "url": "https://www.youtube.com/playlist?list=PL0xvhH4iaYhy4ulh0h-dn4mslO6B8nj0-",
        "video_count": 100,
        "channel": None,
        "query_used": "Viral Songs Right Now",
    },
    {
        "title": "Video Game Top 10's",
        "url": "https://www.youtube.com/playlist?list=PL3662B2B44970D6CC",
        "video_count": 50,
        "channel": None,
        "query_used": "Viral Songs Right Now",
    },
    {
        "title": "Pranks",
        "url": "https://www.youtube.com/playlist?list=PLyzfQprOqzrCGTKaMUPZnwuuo58WinckO",
        "video_count": 5,
        "channel": None,
        "query_used": "Viral Songs Right Now",
    },
]

SEARCH_QUERIES = [
    "EDM remix playlist",
    "pop remix playlist",
    "lofi remix mix",
    "hip hop remix",
    "TikTok remix playlist",
    "2024 DJ remix",
    "club remix playlist",
    "remix hits",
    "best remix playlist",
    "dj remix playlist",
    "2024 remix playlist",
    "hip hop playlist",
    "pop playlist",
    "club playlist",
    "dance playlist",
    "party playlist",
    "lofi playlist",
    "tiktok playlist",
    "summer playlist",
    "workout playlist",
    "chill playlist",
    "house music playlist",
    "electronic playlist",
    "indie playlist",
    "rock playlist",
    "old school playlist",
]

# Table name constants
PLAYLIST_STATS_TABLE = "playlist_stats"
PLAYLIST_JOBS_TABLE = "playlist_jobs"
SIGNUPS_TABLE = "signups"
# Add other table names as needed
