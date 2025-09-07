from monsterui.all import LiStep, Steps, StepsT, StepT

# CSS Class Constants
FLEX_COL = "flex flex-col"
FLEX_CENTER = "flex items-center"
FLEX_BETWEEN = "flex justify-between"
GAP_2 = "flex gap-2"
GAP_4 = "flex gap-4"
SECTION_BASE = "pt-8 px-4 pb-24 gap-8 lg:gap-16 lg:pt-16 lg:px-16"
CARD_BASE = "max-w-2xl mx-auto my-12 p-8 shadow-lg rounded-xl bg-white text-gray-900 hover:shadow-xl transition-shadow duration-300"
HEADER_CARD = "bg-gradient-to-r from-rose-500 via-red-600 to-red-700 text-white py-8 px-6 text-center rounded-xl"

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
