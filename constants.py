# CSS Class Constants
FLEX_COL = "flex flex-col"
FLEX_CENTER = "flex items-center"
FLEX_BETWEEN = "flex justify-between"
GAP_2 = "flex gap-2"
GAP_4 = "flex gap-4"
SECTION_BASE = "pt-8 px-4 pb-24 gap-8 lg:gap-16 lg:pt-16 lg:px-16"
CARD_BASE = "max-w-2xl mx-auto my-12 p-8 shadow-lg rounded-xl bg-white text-gray-900 hover:shadow-xl transition-shadow duration-300"
HEADER_CARD = "bg-gradient-to-r from-rose-500 via-red-600 to-red-700 text-white py-8 px-6 text-center rounded-xl"
FORM_CARD = "max-w-420px; margin: 3rem auto; padding: 2rem; box-shadow: 0 4px 24px #0001; border-radius: 1.2rem; background: #fff; color: #333; transition: all 0.3s ease; hover:shadow-xl"
NEWSLETTER_CARD = "max-w-420px; margin: 3rem auto; padding: 2rem; box-shadow: 0 4px 24px #0001; border-radius: 1.2rem; background: #fff; color: #333; transition: all 0.3s ease; hover:shadow-xl"
STEPS_CLS = (
    "uk-steps uk-steps-horizontal min-h-[200px] my-4 mx-auto max-w-4xl "
    "text-center flex justify-center items-center")

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
    ("Uncover Viral Secrets",
     "Paste a playlist and uncover the secrets behind viral videos.",
     "search"),
    ("Instant Playlist Insights", "Get instant info on trending videos.",
     "zap"),
    ("No Login Required", "Just paste a link and go. No signup needed!",
     "unlock"),
]

BENEFITS = [
    ("Real-time Analysis", "Track trends as they emerge.", "activity"),
    ("Engagement Metrics",
     "Understand what drives likes, shares, and comments.", "heart"),
    ("Top Creator Insights", "Identify breakout content and rising stars.",
     "star"),
]

# YouTube Playlist Constants
KNOWN_PLAYLISTS = [
    {
        "title": "Most Viewed Videos of All Time",
        "url":
        "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC",
        "video_count": None,  # to be updated after validation
        "channel": None,  # to be updated
        "query_used": "most viewed videos playlist"
    },
    {
        "title": "Best Remixes of Popular Songs",
        "url":
        "https://www.youtube.com/playlist?list=PLxA687tYuMWjS8IGRWkCzwTn10XcEccaZ",
        "video_count": None,
        "channel": None,
        "query_used": "Best remixes of popular songs"
    },
]

SEARCH_QUERIES = [
    "EDM remix playlist", "pop remix playlist", "lofi remix mix",
    "hip hop remix", "TikTok remix playlist", "2024 DJ remix",
    "club remix playlist", "remix hits", "best remix playlist",
    "dj remix playlist", "2024 remix playlist", "hip hop playlist",
    "pop playlist", "club playlist", "dance playlist", "party playlist",
    "lofi playlist", "tiktok playlist", "summer playlist", "workout playlist",
    "chill playlist", "house music playlist", "electronic playlist",
    "indie playlist", "rock playlist", "old school playlist"
]
