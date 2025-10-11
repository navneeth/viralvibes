# Standard library and typing
import random
from typing import Dict, List, Optional, Tuple

# Third-party libraries
import polars as pl
from fasthtml.common import *
from monsterui.all import *

# Local modules
from charts import (
    chart_bubble_engagement_vs_views,
    chart_controversy_score,
    chart_duration_vs_engagement,
    chart_engagement_rate,
    chart_likes_vs_dislikes,
    chart_polarizing_videos,
    chart_scatter_likes_dislikes,
    chart_total_engagement,
    chart_treemap_views,
    chart_video_radar,
    chart_views_by_video,
)
from constants import (
    BENEFITS,
    CARD_BASE,
    FEATURES,
    FLEX_BETWEEN,
    FLEX_CENTER,
    FLEX_COL,
    FORM_CARD,
    HEADER_CARD,
    KNOWN_PLAYLISTS,
    NEWSLETTER_CARD,
    PLAYLIST_STEPS_CONFIG,
    STEPS_CLS,
    THEME,
    faqs,
    maxpx,
    testimonials,
)
from db import fetch_playlists, get_cached_playlist_stats
from utils import format_number

"""Define reusable UI components for the ViralVibes application."""
icons = "assets/icons"
col = "flex flex-col"
center = "flex items-center"
section_base1 = "pt-8 px-4 pb-24 gap-8 lg:gap-16 lg:pt-16 lg:px-16"
section_base = f"{col} {section_base1}"
between = "flex justify-between"
gap2 = "flex gap-2"
inset = "shadow-[0_2px_2px_rgba(255,255,255,0.5),0_3px_3px_rgba(0,0,0,0.2)]"
bnset = "shadow-[inset_0_2px_4px_rgba(255,255,255,0.1),0_4px_8px_rgba(0,0,0,0.5)]"


# Helper Functions to make the component file self-contained
def DivCentered(*args, **kwargs) -> Div:
    """A Div with flexbox for centering content."""
    return Div(*args, **kwargs, cls=f"{FLEX_COL} {FLEX_CENTER}")


def DivHStacked(*args, **kwargs) -> Div:
    """A horizontal stack of Divs with a gap."""
    return Div(*args, **kwargs, cls=f"flex gap-4")


def DivFullySpaced(*args, **kwargs) -> Div:
    """A Div with full space between items."""
    return Div(*args, **kwargs, cls=f"flex justify-between items-center")


def styled_div(*children, cls: str = "", **kwargs) -> Div:
    """Flexible Div factory with theme integration."""
    full_cls = f"{THEME['flex_col']} {cls}" if "flex-col" in cls else cls
    return Div(*children, cls=full_cls, **kwargs)


def maxrem(rem):
    return f"w-full max-w-[{rem}rem]"


def benefit(title: str, content: str) -> Div:
    return styled_div(
        H3(title, cls="text-white text-xl font-bold"),
        P(content, cls="text-gray-200 text-base mt-4"),
        cls=f"w-full p-6 {THEME['primary_bg']} rounded-2xl lg:h-[22rem] lg:w-[26rem]",
    )


def accordion(
    id: str,
    question: str,
    answer: str,
    question_cls: str = "",
    answer_cls: str = "",
    container_cls: str = "",
) -> Div:
    return Div(
        Input(
            id=f"collapsible-{id}",
            type="checkbox",
            cls=f"collapsible-checkbox peer/collapsible hidden",
        ),
        Label(
            P(question, cls=f"flex-grow {question_cls}"),
            Img(
                src=f"{icons}/plus-icon.svg",
                alt="Expand",
                cls=f"plus-icon w-6 h-6",
            ),
            Img(
                src=f"{icons}/minus-icon.svg",
                alt="Collapse",
                cls=f"minus-icon w-6 h-6",
            ),
            _for=f"collapsible-{id}",
            cls="flex items-center cursor-pointer py-4 lg:py-6 pl-6 lg:pl-8 pr-4 lg:pr-6",
        ),
        P(
            answer,
            cls=f"overflow-hidden max-h-0 pl-6 lg:pl-8 pr-4 lg:pr-6 peer-checked/collapsible:max-h-[30rem] peer-checked/collapsible:pb-4 peer-checked/collapsible:lg:pb-6 transition-all duration-300 ease-in-out {answer_cls}",
        ),
        cls=container_cls,
    )


def faq_item(question: str, answer: str, id: int) -> Div:
    return accordion(
        id=str(id),
        question=question,
        answer=answer,
        question_cls="text-black text-sm font-medium",
        answer_cls="text-black/80 text-sm",
        container_cls=f"bg-blue-50 rounded-2xl shadow-inner",
    )


def HeaderCard() -> Card:
    """Simplified, flat header card for ViralVibes — no nested components."""
    return CardTitle(
        Div(
            # Text content (left)
            Div(
                H1(
                    "Welcome to ViralVibes",
                    cls="text-4xl md:text-5xl font-poppins text-blue-700 mb-4",
                ),
                P(
                    "Decode YouTube virality. Instantly.",
                    cls="text-xl text-gray-700 mb-3",
                ),
                P(
                    "Analyze any YouTube playlist to uncover engagement trends, viral patterns, and creator insights — instantly.",
                    cls="text-base text-gray-500 max-w-lg",
                ),
                Button(
                    UkIcon("chart-bar", cls="mr-2"),
                    "Start Analyzing",
                    onclick="document.querySelector('#analyze-section').scrollIntoView({behavior:'smooth'})",
                    # cls="mt-6 bg-red-600 text-white font-semibold px-6 py-3 rounded-lg hover:bg-red-700 transition-all duration-200 shadow-md hover:shadow-lg",
                    cls=f"mt-6 {THEME['primary_bg']} {THEME['primary_hover']} text-white px-6 py-3 rounded-lg shadow-md",
                ),
                cls="flex-1",
            ),
            # Image (right)
            Img(
                src="/static/thumbnail.png",
                alt="YouTube Analytics Dashboard",
                cls="flex-1 w-64 md:w-80 lg:w-96 rounded-2xl shadow-2xl",
                loading="lazy",
            ),
            # Overall container
            cls="flex flex-col md:flex-row gap-10 items-center justify-between",
        ),
        cls=THEME["card_base"],
        uk_scrollspy="cls: uk-animation-slide-bottom-small",
    )


def hero_section():
    """
    Responsive hero that scales between mobile and desktop:
    - hides horizontal overflow at the section level to avoid "floating" scroll.
    - uses a single responsive decorative SVG sized with vw units so it scales,
      avoiding fixed min-widths that produce horizontal scrollbars.
    - uses viewport-based min-heights so mobile/desktop feel proportional.
    """
    return Section(
        # Decorative background — responsive width using vw so it scales smoothly.
        styled_div(
            File("assets/waves.svg"),
            cls="absolute z-0 left-1/2 -translate-x-1/2 pointer-events-none opacity-80",
            style="width: clamp(100vw, 120vw, 2200px); top: -12vh;",
        ),
        # Content container — hide horizontal overflow here to prevent floating scroll
        styled_div(
            styled_div(cls="lg:flex-1 max-lg:basis-[152px]"),
            styled_div(
                H1(
                    "ViralVibes",
                    cls=f"text-4xl md:text-5xl font-poppins {THEME['secondary_text']}",
                ),
                P(
                    "Decode YouTube virality. Instantly.\nAnalyze your YouTube playlists with creator-first insights.\nUnlock curated insights into your audience instantly.",
                    cls="text-lg md:text-xl font-inter text-gray-800 max-w-[40rem] text-center leading-relaxed",
                ),
                cls=f"flex-1 {col} items-center justify-center gap-6 text-center w-full text-black",
            ),
            styled_div(
                A(
                    "🚀 Try it now",
                    href="#analysis-form",
                    cls=f"shadow-inner m-body px-4 py-1 rounded-full {THEME['primary_bg']} {THEME['primary_hover']} text-white h-[76px] w-full max-w-[350px] {THEME['flex_center']}",
                    uk_scroll="offset: 80",
                ),
                cls=f"{THEME['flex_col']} flex-1 relative px-4 lg:px-16",
            ),
            # Responsive min-heights:
            # - mobile: comfortable fraction of viewport (65vh)
            # - tablet/desktop: larger min-height (75vh -> 90vh) for visual presence
            cls=(
                f"{col} relative w-full min-h-[65vh] md:min-h-[75vh] lg:min-h-[90vh] "
                "max-h-[1024px] overflow-x-hidden overflow-y-visible bg-grey"
            ),
        ),
    )


def PlaylistSteps(completed_steps: int = 0) -> Steps:
    """Create a Steps component explaining the playlist submission process."""
    steps = []
    for i, (title, icon, description) in enumerate(PLAYLIST_STEPS_CONFIG):
        if i < completed_steps:
            # completed → green
            step_cls = StepT.success
        elif i == completed_steps:
            # current → highlight
            step_cls = StepT.primary
        else:
            # pending → gray# pending → gray
            step_cls = StepT.neutral

        steps.append(
            LiStep(title, cls=step_cls, data_content=icon, description=description)
        )

    return Steps(*steps, cls=STEPS_CLS)


def AnalysisFormCard() -> Div:
    """Single-layer Analysis Form card with clean layout (no nested Card)."""

    # Get a random prefill URL from the known playlists
    prefill_url = random.choice(KNOWN_PLAYLISTS)["url"] if KNOWN_PLAYLISTS else ""

    return styled_div(
        # --- Hero image and heading ---
        Img(
            src="/static/celebration.webp",
            alt="YouTube Analytics Celebration",
            cls="w-28 mx-auto drop-shadow-lg mb-4",
            loading="lazy",
        ),
        H2(
            "Analyze Your YouTube Playlist",
            cls="text-3xl font-bold text-red-600 text-center",
        ),
        P(
            "Get deep insights into views, engagement, and virality patterns",
            cls="text-red-500 text-center mb-4",
        ),
        # --- Steps Placeholder ---
        styled_div(
            id="steps-container",
            children=[PlaylistSteps(completed_steps=0)],
            cls=(
                "flex justify-center mb-6 transition-opacity duration-300 "
                "min-h-[120px] opacity-60"
            ),
        ),
        # --- Playlist Form ---
        Form(
            LabelInput(
                "Playlist URL",
                type="text",
                name="playlist_url",
                placeholder="Paste YouTube Playlist URL",
                value=prefill_url,
                className=(
                    "px-4 py-2 w-full border rounded-md text-gray-900 placeholder-gray-500 "
                    "focus:ring-2 focus:ring-red-500 focus:border-red-500 transition"
                ),
                style="color: #333;",
            ),
            P(
                "💡 Works with any public playlist",
                cls="italic text-yellow-600 text-xs mt-1",
            ),
            # Action button
            Button(
                Span(UkIcon("chart-bar", cls="mr-2"), "Analyze Playlist"),
                type="submit",
                cls=f"{ButtonT.primary} w-full {THEME['primary_hover']} transition-transform mt-4",
            ),
            # Quick action buttons for demo playlists
            (
                Details(
                    Summary(
                        "Try sample playlists",
                        cls="text-sm text-gray-600 cursor-pointer hover:text-gray-800",
                    ),
                    SamplePlaylistButtons(),
                    cls="mt-3 max-h-40 overflow-y-auto",
                )
                if KNOWN_PLAYLISTS
                else None
            ),
            # Loading indicator with better positioning
            Loading(
                id="loading",
                cls=(LoadingT.bars, LoadingT.lg),
                htmx_indicator=True,
            ),
            hx_post="/validate/url",
            hx_target="#validation-feedback",
            hx_swap="innerHTML",
            hx_indicator="#loading",
            cls="space-y-3 mt-6",
        ),
        # --- Feedback + Results sections ---
        styled_div(id="validation-feedback", cls="mt-6 text-gray-900"),
        styled_div(id="preview-box", cls="mt-6 text-gray-900"),
        styled_div(
            id="result",
            cls="mt-8 min-h-[400px] border-t pt-6 text-gray-900 {THEME['neutral_bg']} rounded-lg",
        ),
        # --- Styling (outermost container only) ---
        cls=f"{THEME['card_base']} space-y-4 w-full my-12",
        style=FORM_CARD,
        uk_scrollspy="cls: uk-animation-slide-bottom-small",
        id="analysis-form",
    )


# Helper: render sample playlist quick-fill buttons
def SamplePlaylistButtons(input_name: str = "playlist_url", max_items: int = 5) -> Div:
    """Render quick action buttons from cached playlists in DB."""
    known_playlists = fetch_playlists(max_items=max_items, randomize=True)
    if not known_playlists:
        return Div()

    buttons = []
    for pl in known_playlists[:max_items]:
        title = pl.get("title", "Sample")
        short = f"{title[:30]}{'...' if len(title) > 30 else ''}"
        buttons.append(
            Button(
                f"📺 {short}",
                type="button",
                cls=(
                    "text-left text-xs text-blue-600 hover:text-blue-800 "
                    "hover:bg-blue-50 px-2 py-1 rounded transition-colors w-full"
                ),
                onclick=(
                    'document.querySelector("input[name=\\"%s\\"]").value = \'%s\''
                    % (input_name, pl.get("url", ""))
                ),
            )
        )

    return Div(*buttons, cls="mt-2 space-y-1 p-2 bg-gray-50 rounded-md border")


def _build_icon(name: str) -> "Component":
    """Build an icon component with consistent styling."""
    return UkIcon(name, cls="text-red-500 text-3xl mb-2")


def _build_info_items(config: List[Tuple[str, str, str]]) -> List["Component"]:
    """Build a list of info item components from a configuration."""
    return [
        Div(
            _build_icon(icon),
            H4(title, cls="mb-2 mt-2"),
            P(desc, cls="text-gray-600 text-sm text-center"),
            cls=f"{FLEX_COL} {FLEX_CENTER}",
        )
        for title, desc, icon in config
    ]


def create_info_card(
    title: str,
    items: List[Tuple[str, str, str]],
    img_src: Optional[str] = None,
    img_alt: Optional[str] = None,
) -> Card:
    """Helper function to create Feature and Benefit cards."""
    cards = _build_info_items(items)
    img_component = (
        Img(
            src=img_src,
            style="width:120px; margin: 0 auto 2rem auto; display:block;",
            alt=img_alt,
        )
        if img_src
        else ""
    )
    return Card(
        img_component,
        Grid(*cards),
        header=CardTitle(title, cls="text-2xl font-semibold mb-4 text-center"),
        cls=CARD_BASE,
        body_cls="space-y-6",
        uk_scrollspy="cls: uk-animation-slide-bottom-small",
    )


def FeaturesCard() -> Card:
    """Create the features card component."""
    return create_info_card(
        "What is ViralVibes?",
        FEATURES,
        "/static/virality.webp",
        "Illustration of video viral insights",
    )


def BenefitsCard() -> Card:
    """Create the benefits card component."""
    return create_info_card("Why You'll Love It", BENEFITS)


def NewsletterCard() -> Card:
    return Card(
        P(
            "Enter your email to get early access and updates. No spam ever.",
            cls="mb-4",
        ),
        Form(
            LabelInput(
                "Email",
                type="email",
                name="email",
                required=True,
                pattern="[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$",
                title="Please enter a valid email address",
                placeholder="you@example.com",
                className="px-4 py-2 w-full max-w-sm border rounded focus:ring-2 focus:ring-red-500 focus:border-red-500 transition-all invalid:border-red-500 invalid:focus:ring-red-500",
            ),
            Button(
                "Notify Me",
                type="submit",
                className=f"{ButtonT.primary} {THEME['primary_hover']} transition-transform",
            ),
            Loading(
                id="loading",
                cls=(LoadingT.bars, LoadingT.lg),
                style="margin-top:1rem; color:#393e6e;",
                htmx_indicator=True,
            ),
            className=f"{FLEX_COL} {FLEX_CENTER} space-y-4",
            hx_post="/newsletter",
            hx_target="#newsletter-result",
            hx_indicator="#loading",
        ),
        Div(id="newsletter-result", style="margin-top:1rem;"),
        header=CardTitle("Be the first to try it", cls="text-xl font-bold mb-4"),
        cls=NEWSLETTER_CARD,
        style=NEWSLETTER_CARD,
        body_cls="space-y-6",
        uk_scrollspy="cls: uk-animation-slide-bottom-small",
    )


def SummaryStatsCard(summary: Dict) -> Card:
    stats = [
        (
            "eye",
            "Total Views",
            format_number(summary.get("total_views", 0)),
            "text-blue-500",
        ),
        (
            "heart",
            "Total Likes",
            format_number(summary.get("total_likes", 0)),
            "text-red-500",
        ),
        (
            "percent",
            "Average Engagement",
            f"{summary.get('avg_engagement', 0):.2f}%",
            "text-green-500",
        ),
    ]

    return Card(
        Grid(
            *[
                DivCentered(
                    UkIcon(icon, height=32, cls=f"{color} mb-2"),
                    H3(value, cls="text-2xl font-bold"),
                    P(label, cls=TextPresets.muted_sm),
                )
                for icon, label, value, color in stats
            ],
            cols_md=3,
        ),
        cls=CardT.hover,
    )


def create_tabs(
    tabs: List[Tuple[str, "Component", Optional[str]]],
    tabs_id: str,
    alt: bool = True,
    tab_cls: str = "uk-active",
    container_cls: str = "space-y-4",
) -> Div:
    """Creates a MonsterUI tab component with optional icons and improved styling."""
    tab_links = []
    tab_content = []

    for i, tab in enumerate(tabs):
        if len(tab) == 3:
            title, content, icon = tab
            label = Div(
                UkIcon(icon, cls="mr-2 text-lg") if icon else None,
                Span(title),
                cls="flex items-center",
            )
        else:
            title, content = tab[:2]
            label = Span(title)

        # Active class applied to <li>, not <a>
        li_cls = tab_cls if i == 0 else ""
        tab_links.append(Li(A(label, href="#"), cls=li_cls))

        # Guard against None content
        tab_content.append(Li(Div(content, cls="p-2")))  # ✅ wrap to ensure div exists

    return Container(
        TabContainer(
            *tab_links,
            uk_switcher=f"connect: #{tabs_id}; animation: uk-animation-fade",
            alt=alt,
            cls="mb-4 flex gap-2",
        ),
        Ul(id=tabs_id, cls="uk-switcher")(*tab_content),
        cls=container_cls,
    )


def HomepageAccordion() -> Div:
    """Create an accordion section containing Features, Benefits, and Newsletter cards."""
    return styled_div(
        H2("Explore ViralVibes", cls="text-3xl font-bold text-center mb-8"),
        Accordion(
            AccordionItem(
                "🔍 What is ViralVibes?",
                FeaturesCard(),
                li_kwargs={"id": "features-section"},
            ),
            AccordionItem(
                "💡 Why You'll Love It",
                BenefitsCard(),
                li_kwargs={"id": "benefits-section"},
            ),
            AccordionItem(
                "📧 Stay Updated",
                NewsletterCard(),
                li_kwargs={"id": "newsletter-section"},
            ),
            multiple=False,
            animation=True,
            cls="max-w-4xl mx-auto",
        ),
        cls="space-y-8",
    )


def CachedResultsBanner(cached_at: str) -> Div:
    """Display a banner indicating results are from cache."""
    return styled_div(
        styled_div(
            UkIcon("check-circle", cls="text-green-500 mr-2", height=20, width=20),
            Span("Instant Results from Cache", cls="font-semibold text-green-700"),
            Span(f" • Last analyzed: {cached_at}", cls="text-gray-600 text-sm ml-2"),
            cls="flex items-center",
        ),
        cls="bg-green-50 border border-green-200 rounded-lg p-4 mb-6",
    )


def AnalyticsDashboardSection(
    df,
    summary: Dict,
    playlist_name: str,
    channel_name: str,
    playlist_thumbnail: str = None,
    channel_thumbnail: str = None,
    channel_url: str = None,
    from_cache: bool = False,
    cached_at: str = None,
):
    """Create an analytics dashboard section for a playlist with enhanced header."""
    actual_playlist_count = summary.get("actual_playlist_count", 0)
    processed_count = summary.get(
        "processed_video_count", len(df) if df is not None and not df.is_empty() else 0
    )

    # Extract additional data from summary for enhanced header
    total_views = summary.get("total_views", 0)
    engagement_rate = summary.get("avg_engagement", 0)
    processed_date = summary.get("processed_date") or cached_at

    return Section(
        # Show cache banner if results are from DB
        (CachedResultsBanner(cached_at) if from_cache and cached_at else None),
        # Enhanced professional header with all DB fields
        AnalyticsHeader(
            playlist_title=playlist_name,
            channel_name=channel_name,
            total_videos=actual_playlist_count,
            processed_videos=processed_count,
            playlist_thumbnail=playlist_thumbnail,
            channel_thumbnail=channel_thumbnail,
            channel_url=channel_url,
            processed_date=processed_date,
            engagement_rate=engagement_rate,
            total_views=total_views,
        ),
        # Playlist Metrics Overview
        PlaylistMetricsOverview(df, summary),
        # Header
        Div(
            H2("📊 Playlist Analytics", cls="text-3xl font-bold text-gray-900"),
            P(
                "Dive deep into your playlist's performance, audience engagement, and content patterns.",
                cls="text-gray-500 mt-2 mb-8 text-lg",
            ),
            cls="text-center",
        ),
        # Group 1: Content Overview & Performance
        Div(
            H3(
                "📺 Content Overview & Performance",
                cls="text-2xl font-semibold text-gray-800 mb-4",
            ),
            P(
                "See how individual videos contribute to your playlist's overall reach and performance.",
                cls="text-gray-500 mb-6",
            ),
            Grid(
                chart_views_by_video(
                    df, "views-by-video"
                ),  # Shows performance hierarchy
                chart_treemap_views(
                    df, "treemap-views"
                ),  # Shows contribution distribution
                cls="grid-cols-1 md:grid-cols-2 gap-10",
            ),
            cls="mb-16",
        ),
        # Group 2: Audience Engagement Analysis
        Div(
            H3(
                "💬 Audience Engagement Analysis",
                cls="text-2xl font-semibold text-gray-800 mb-4",
            ),
            P(
                "Understand how actively your audience participates - likes, comments, and overall engagement patterns.",
                cls="text-gray-500 mb-6",
            ),
            Div(
                H4(
                    "ℹ️ How is Engagement Rate calculated?",
                    cls="text-md font-semibold text-gray-800 mb-2 flex items-center",
                ),
                P(
                    "Engagement Rate (%) = ((Likes + Comments) ÷ Views) × 100",
                    cls="text-sm text-gray-600 bg-gray-50 p-3 rounded-lg border border-gray-200",
                ),
                cls="mb-6",
            ),
            # Grid(
            #     chart_engagement_rate(
            #         df, "engagement-rate"
            #     ),  # Individual video engagement
            #     chart_total_engagement(
            #         summary, "total-engagement"
            #     ),  # Overall engagement split
            #     cls="grid-cols-1 md:grid-cols-2 gap-10",
            # ),
            cls="mb-16",
        ),
        # Group 3: Audience Sentiment & Polarization
        Div(
            H3(
                "🔥 Audience Sentiment & Polarization",
                cls="text-2xl font-semibold text-gray-800 mb-4",
            ),
            P(
                "Discover which content creates strong reactions and splits audience opinion.",
                cls="text-gray-500 mb-6",
            ),
            # Grid(
            #     chart_likes_vs_dislikes(
            #         df, "likes-vs-dislikes"
            #     ),  # Direct comparison of sentiment
            #     chart_polarizing_videos(
            #         df, "polarizing-videos"
            #     ),  # Polarization with context (bubble shows views)
            #     cls="grid-cols-1 md:grid-cols-2 gap-10",
            # ),
            cls="mb-16",
        ),
        # Group 4: Advanced Insights & Patterns
        Div(
            H3(
                "📈 Advanced Insights & Patterns",
                cls="text-2xl font-semibold text-gray-800 mb-4",
            ),
            P(
                "Uncover deeper relationships between viewership, engagement, and controversy across your content.",
                cls="text-gray-500 mb-6",
            ),
            # Grid(
            #     chart_scatter_likes_dislikes(
            #         df, "scatter-likes"
            #     ),  # Correlation analysis
            #     chart_bubble_engagement_vs_views(
            #         df, "bubble-engagement"
            #     ),  # Multi-dimensional analysis
            #     # chart_duration_vs_engagement(df, "duration-engagement"),
            #     chart_video_radar(df, "video-radar"),
            #     cls="grid-cols-1 md:grid-cols-2 gap-10",
            # ),
            cls="mb-16",
        ),
        # Group 5: Content Strategy Insights (if we have controversy data)
        Div(
            H3(
                "🎯 Content Strategy Insights",
                cls="text-2xl font-semibold text-gray-800 mb-4",
            ),
            P(
                "Strategic insights to help optimize your content mix and audience targeting.",
                cls="text-gray-500 mb-6",
            ),
            # Single chart that gives strategic insight
            chart_controversy_score(df, "controversy-score"),
            cls="mb-16",
        ),
        cls="mt-20 pt-12 border-t border-gray-200 space-y-12",
    )


def ViralVibesButton(
    text: str,
    icon: str = "chart-bar",
    button_type: str = "button",
    full_width: bool = False,
    **kwargs,
) -> Button:
    """Create a consistently styled ViralVibes button."""
    width_class = "w-full" if full_width else ""

    return Button(
        Span(UkIcon(icon, cls="mr-2"), text),
        type=button_type,
        cls=(
            f"{width_class} py-3 px-6 text-base font-semibold rounded-lg shadow-lg "
            "bg-gradient-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 "
            "text-white border-0 focus:ring-4 focus:ring-red-200 "
            "transition-all duration-200 hover:scale-105 active:scale-95 transform"
        ),
        **kwargs,
    )


def AnalyticsHeader(
    playlist_title: str,
    channel_name: str,
    total_videos: int,
    processed_videos: int,
    playlist_thumbnail: Optional[str] = None,
    channel_url: Optional[str] = None,
    channel_thumbnail: Optional[str] = None,
    processed_date: Optional[str] = None,
    engagement_rate: Optional[float] = None,
    total_views: Optional[int] = None,
) -> Div:
    """Create a professional header for the analytics dashboard with rich DB data."""
    video_info = f"{total_videos} videos"
    if processed_videos < total_videos:
        video_info = f"{processed_videos} of {total_videos} videos analyzed"

    return Div(
        # Main header content
        Div(
            # Left side: Combined playlist and channel info
            Div(
                # Playlist thumbnail (larger)
                (
                    Img(
                        src=playlist_thumbnail,
                        alt=f"{playlist_title} thumbnail",
                        cls="w-20 h-20 rounded-lg shadow-md mr-4 object-cover",
                        style="min-width: 80px; min-height: 80px;",
                    )
                    if playlist_thumbnail
                    else ""
                ),
                Div(
                    # Title and channel with thumbnail
                    Div(
                        H1(
                            playlist_title,
                            cls="text-2xl md:text-3xl font-bold text-gray-900 mb-2",
                        ),
                        # Channel info with thumbnail
                        Div(
                            (
                                Img(
                                    src=channel_thumbnail,
                                    alt=f"{channel_name} avatar",
                                    cls="w-6 h-6 rounded-full border border-gray-300",
                                )
                                if channel_thumbnail
                                else UkIcon("user", cls="w-5 h-5 text-gray-500")
                            ),
                            Span(
                                "by ",
                                (
                                    A(
                                        str(channel_name or "Unknown Channel"),
                                        href=channel_url,
                                        target="_blank",
                                        cls="text-blue-600 hover:text-blue-800 hover:underline",
                                    )
                                    if channel_url
                                    else Span(str(channel_name or "Unknown Channel"))
                                ),
                                cls="text-gray-700",
                            ),
                            Span("•", cls="text-gray-400 mx-2"),
                            Span(video_info, cls="text-gray-600"),
                            cls="flex items-center gap-2 text-sm md:text-base",
                        ),
                        # Quick stats row
                        (
                            Div(
                                # Views badge
                                (
                                    Span(
                                        UkIcon("eye", cls="w-4 h-4 mr-1"),
                                        format_number(total_views),
                                        cls="inline-flex items-center px-2 py-1 bg-blue-50 text-blue-700 rounded-md text-xs font-medium",
                                    )
                                    if total_views
                                    else None
                                ),
                                # Engagement badge
                                (
                                    Span(
                                        UkIcon("heart", cls="w-4 h-4 mr-1"),
                                        f"{engagement_rate:.1f}% engagement",
                                        cls="inline-flex items-center px-2 py-1 bg-red-50 text-red-700 rounded-md text-xs font-medium",
                                    )
                                    if engagement_rate
                                    else None
                                ),
                                # Date badge
                                (
                                    Span(
                                        UkIcon("calendar", cls="w-4 h-4 mr-1"),
                                        f"Analyzed {processed_date}",
                                        cls="inline-flex items-center px-2 py-1 bg-gray-50 text-gray-700 rounded-md text-xs font-medium",
                                    )
                                    if processed_date
                                    else None
                                ),
                                cls="flex flex-wrap items-center gap-2 mt-2",
                            )
                            if (total_views or engagement_rate or processed_date)
                            else None
                        ),
                    ),
                    cls="flex-1",
                ),
                cls="flex items-start" if playlist_thumbnail else "",
            ),
            # Right side: Status badge
            Div(
                Div(
                    UkIcon(
                        "check-circle",
                        cls="text-green-500 mr-2",
                        height=20,
                        width=20,
                    ),
                    Span("Analysis Complete", cls="text-sm text-green-700 font-medium"),
                    cls="flex items-center px-3 py-2 bg-green-100/40 rounded-lg border border-green-200",
                ),
                cls="hidden md:flex items-center",
            ),
            cls="flex flex-col md:flex-row md:items-start md:justify-between gap-4",
        ),
        # Solid bar background with padding
        cls="p-6 mb-8 rounded-lg shadow-md",
        style="background: linear-gradient(to right, #f0f4f8, #e2e8f0);",
    )


def PlaylistPreviewCard(
    playlist_name: str,
    channel_name: str,
    channel_thumbnail: str,
    playlist_length: Optional[int],
    playlist_url: str,
    playlist_thumbnail: Optional[str] = None,
    total_views: Optional[int] = None,
    engagement_rate: Optional[float] = None,
    last_analyzed: Optional[str] = None,
    video_count: Optional[int] = None,
    meter_id: str = "fetch-progress-meter",
    show_refresh: bool = False,
) -> Card:
    """Display enhanced playlist preview with rich database information."""

    # Use video_count from DB if available, fallback to playlist_length
    actual_video_count = video_count or playlist_length or 0

    return Card(
        # Visual header with both thumbnails
        Div(
            # Playlist thumbnail (if available) as background accent
            (
                Div(
                    Img(
                        src=playlist_thumbnail,
                        alt="Playlist thumbnail",
                        cls="w-full h-full object-cover opacity-20",
                    ),
                    cls="absolute inset-0 rounded-t-2xl overflow-hidden",
                )
                if playlist_thumbnail
                else None
            ),
            # Channel thumbnail (foreground)
            DivCentered(
                Img(
                    src=channel_thumbnail,
                    alt=f"{channel_name} thumbnail",
                    cls="w-20 h-20 rounded-full shadow-lg border-4 border-white relative z-10",
                ),
                cls="relative pt-8",
            ),
            cls="relative mb-4",
        ),
        # Playlist Info
        DivCentered(
            H3(
                playlist_name,
                cls="text-lg font-semibold text-gray-900 text-center px-4",
            ),
            P(f"by {channel_name}", cls="text-sm text-gray-600"),
            cls="text-center space-y-1 mb-4",
        ),
        # Stats badges (if from cache/DB)
        (
            Div(
                # Video count
                Div(
                    UkIcon("film", cls="w-4 h-4 text-gray-600"),
                    Span(f"{actual_video_count} videos", cls="text-sm font-medium"),
                    cls="flex items-center gap-1.5 px-3 py-1.5 bg-gray-50 rounded-lg",
                ),
                # Total views (if available)
                (
                    Div(
                        UkIcon("eye", cls="w-4 h-4 text-blue-600"),
                        Span(
                            format_number(total_views),
                            cls="text-sm font-medium text-blue-700",
                        ),
                        cls="flex items-center gap-1.5 px-3 py-1.5 bg-blue-50 rounded-lg",
                    )
                    if total_views
                    else None
                ),
                # Engagement rate (if available)
                (
                    Div(
                        UkIcon("heart", cls="w-4 h-4 text-red-600"),
                        Span(
                            f"{engagement_rate:.1f}%",
                            cls="text-sm font-medium text-red-700",
                        ),
                        cls="flex items-center gap-1.5 px-3 py-1.5 bg-red-50 rounded-lg",
                    )
                    if engagement_rate
                    else None
                ),
                cls="flex flex-wrap justify-center gap-2 mb-4",
            )
            if (actual_video_count or total_views or engagement_rate)
            else None
        ),
        # Cache indicator (if showing cached data)
        (
            Div(
                UkIcon("database", cls="w-4 h-4 text-green-600 mr-1.5"),
                Span(
                    f"Cached analysis from {last_analyzed}",
                    cls="text-xs text-green-700",
                ),
                cls="flex items-center justify-center px-3 py-2 bg-green-50 rounded-lg border border-green-200 mb-4",
            )
            if show_refresh and last_analyzed
            # Progress bar for new analysis
            else (
                Div(
                    P(
                        f"Ready to analyze {actual_video_count} videos",
                        cls="text-sm font-medium text-gray-700 text-center mb-2",
                    ),
                    Progress(
                        value=0,
                        max=actual_video_count or 1,
                        id=meter_id,
                        cls=(
                            "w-full h-2 rounded-full bg-gray-200 "
                            "[&::-webkit-progress-bar]:bg-gray-200 "
                            "[&::-webkit-progress-value]:rounded-full "
                            "[&::-webkit-progress-value]:transition-all "
                            "[&::-webkit-progress-value]:duration-300 "
                            "[&::-webkit-progress-value]:bg-red-600 "
                            "[&::-moz-progress-bar]:bg-red-600"
                        ),
                    ),
                    cls="space-y-2",
                )
                if not show_refresh
                else None
            )
        ),
        # Action buttons
        Div(
            (
                # Refresh button (for cached results)
                Button(
                    Span(UkIcon("refresh-cw", cls="mr-2"), "Refresh Analysis"),
                    hx_post="/validate/full",
                    hx_vals={
                        "playlist_url": playlist_url,
                        "meter_id": meter_id,
                        "meter_max": actual_video_count,
                        "force_refresh": "true",
                    },
                    hx_target="#results-box",
                    hx_indicator="#loading-bar",
                    hx_swap="beforeend",
                    cls=(
                        "w-full py-3 px-6 text-base font-semibold rounded-lg shadow-lg "
                        "bg-gradient-to-r from-orange-500 to-orange-600 hover:from-orange-600 hover:to-orange-700 "
                        "text-white border-0 focus:ring-4 focus:ring-orange-200 "
                        "transition-all duration-200 hover:scale-105 active:scale-95 transform"
                    ),
                )
                if show_refresh
                # Start analysis button (for new analysis)
                else Button(
                    Span(UkIcon("chart-bar", cls="mr-2"), "Start Full Analysis"),
                    hx_post="/validate/full",
                    hx_vals={
                        "playlist_url": playlist_url,
                        "meter_id": meter_id,
                        "meter_max": actual_video_count,
                    },
                    hx_target="#results-box",
                    hx_indicator="#loading-bar",
                    hx_swap="beforeend",
                    cls=(
                        "w-full py-3 px-6 text-base font-semibold rounded-lg shadow-lg "
                        "bg-gradient-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 "
                        "text-white border-0 focus:ring-4 focus:ring-red-200 "
                        "transition-all duration-200 hover:scale-105 active:scale-95 transform"
                    ),
                )
            ),
            # Secondary action (View in YouTube) if we have data
            (
                A(
                    UkIcon("external-link", cls="w-4 h-4 mr-2"),
                    "View on YouTube",
                    href=playlist_url,
                    target="_blank",
                    cls=(
                        "flex items-center justify-center w-full mt-3 py-2 px-4 "
                        "text-sm font-medium text-gray-700 bg-white border border-gray-300 "
                        "rounded-lg hover:bg-gray-50 transition-colors"
                    ),
                )
                if show_refresh
                else None
            ),
            cls="space-y-2",
        ),
        # Loading and results section
        Div(
            Loading(id="loading-bar", cls=(LoadingT.bars, LoadingT.lg)),
            Div(id="results-box", cls="mt-4"),
        ),
        header=CardTitle(
            Span(
                UkIcon("list", cls="mr-2 inline-block"),
                "Playlist Preview",
                cls="flex items-center",
            ),
            cls="text-xl font-bold text-gray-900",
        ),
        cls="max-w-md mx-auto rounded-2xl shadow-lg bg-white overflow-hidden",
        style="border: 2px solid #f3f4f6;",
    )


def MetricCard(
    title: str, value: str, subtitle: str, icon: str, color: str = "red"
) -> Card:
    """Create a clean metric card with icon, value, and context."""
    return Card(
        Div(
            # Icon in top-left
            UkIcon(icon, cls=f"text-{color}-500 mb-3", height=24, width=24),
            # Main value - big and bold
            H3(value, cls="text-2xl font-bold text-gray-900 mb-1"),
            # Subtitle with context
            P(subtitle, cls="text-sm text-gray-600"),
            cls="space-y-1",
        ),
        # Card title
        header=H4(
            title, cls="text-sm font-medium text-gray-500 uppercase tracking-wide"
        ),
        # Styling
        cls="hover:shadow-md transition-all duration-200 border border-gray-200",
    )


def PlaylistMetricsOverview(df: pl.DataFrame, summary: Dict) -> Div:
    """Create a row of 4 key metric cards that give immediate insights."""

    # Calculate key metrics from your data
    actual_playlist_count = summary.get("actual_playlist_count", 0)
    processed_count = summary.get(
        "processed_video_count", len(df) if df is not None else 0
    )
    total_views = summary.get("total_views", 0)
    total_videos = len(df) if df is not None else summary.get("video_count", 0)
    avg_engagement = summary.get("avg_engagement", 0)

    if df is not None and len(df) > 0:
        try:
            top_video_views = df.select(pl.col("View Count Raw").max()).item() or 0
        except Exception:
            top_video_views = 0
        avg_views_per_video = total_views / total_videos if total_videos > 0 else 0
    else:
        top_video_views = summary.get("max_views", 0)
        avg_views_per_video = total_views / total_videos if total_videos > 0 else 0

    metrics = [
        MetricCard(
            title="Total Reach",
            value=format_number(total_views),
            subtitle=f"Across {total_videos} videos",
            icon="eye",
            color="blue",
        ),
        MetricCard(
            title="Engagement Rate",
            value=f"{avg_engagement:.1f}%",
            subtitle="Average likes + comments",
            icon="heart",
            color="red",
        ),
        MetricCard(
            title="Top Performer",
            value=format_number(top_video_views),
            subtitle="Most viewed video",
            icon="trending-up",
            color="green",
        ),
        MetricCard(
            title="Average Performance",
            value=format_number(int(avg_views_per_video)),
            subtitle="Views per video",
            icon="bar-chart",
            color="purple",
        ),
    ]

    return Div(
        # Section header
        Div(
            H2("📊 Key Metrics", cls="text-xl font-semibold text-gray-800 mb-2"),
            P(
                "At a glance overview of your playlist performance",
                cls="text-gray-600 text-sm mb-6",
            ),
            cls="text-center",
        ),
        # Metrics grid - responsive
        Grid(
            *metrics,
            cols_sm=2,  # 2 columns on small screens
            cols_lg=4,  # 4 columns on large screens
            gap=4,  # Consistent spacing
            cls="mb-8",  # Space before your existing charts
        ),
        cls="mb-12",  # Extra space to separate from charts section
    )


def FooterLinkGroup(title, links):
    return DivVStacked(
        H4(title),
        *[
            A(text, href=f"#{text.lower().replace(' ', '-')}", cls=TextT.muted)
            for text in links
        ],
    )


def footer():
    company = ["About", "Blog", "Careers", "Press Kit"]
    resources = ["Documentation", "Help Center", "Status", "Contact Sales"]
    legal = ["Terms of Service", "Privacy Policy", "Cookie Settings", "Accessibility"]

    return Container(cls="uk-background-muted py-12")(
        Div(
            DivFullySpaced(
                H3("ViralVibes"),
                DivHStacked(
                    UkIcon("twitter", cls=TextT.lead),
                    UkIcon("facebook", cls=TextT.lead),
                    UkIcon("github", cls=TextT.lead),
                    UkIcon("linkedin", cls=TextT.lead),
                ),
            ),
            DividerLine(),
            DivFullySpaced(
                FooterLinkGroup("Company", company),
                FooterLinkGroup("Resources", resources),
                FooterLinkGroup("Legal", legal),
            ),
            DividerLine(),
            P("© 2025 ViralVibes. All rights reserved.", cls=TextT.lead + TextT.sm),
            cls="space-y-8 p-8",
        )
    )


def section_wrapper(content, bg_color, xtra="", flex=True):
    """Wraps a section with background color, layout, and rounded corners."""
    return Section(
        content,
        cls=f"bg-{bg_color} {section_base1} {FLEX_COL if flex else ''} -mt-8 lg:-mt-16 items-center rounded-t-3xl lg:rounded-t-[2.5rem] relative {xtra}",
    )


def section_header(mono_text, heading, subheading, max_width=32, center=True):
    pos = "items-center text-center" if center else "items-start text-start"
    return Div(
        P(mono_text, cls="mono-body text-opacity-60"),
        H2(heading, cls=f"text-white heading-2 {maxrem(max_width)}"),
        P(subheading, cls=f"l-body {maxrem(max_width)}"),
        cls=f"{maxrem(50)} mx-auto {col} {pos} gap-6",
    )


def arrow(d):
    return Button(
        Img(src=f"assets/icons/arrow-{d}.svg", alt="Arrow left"),
        cls="disabled:opacity-40 transition-opacity",
        id=f"slide{d.capitalize()}",
        aria_label=f"Slide {d}",
    )


def carousel(items, id="carousel-container", extra_classes=""):
    carousel_content = Div(
        *items,
        id=id,
        cls=f"hide-scrollbar {FLEX_COL} lg:flex-row gap-4 lg:gap-6 rounded-l-3xl xl:rounded-3xl w-full lg:overflow-hidden xl:overflow-hidden whitespace-nowrap {extra_classes}",
    )

    arrows = Div(
        Div(arrow("left"), arrow("right"), cls=f"w-[4.5rem] {FLEX_BETWEEN} ml-auto"),
        cls=f"hidden lg:flex xl:flex justify-start {maxrem(41)} py-6 pl-6 pr-20",
    )
    return Div(
        carousel_content,
        arrows,
        cls=f"max-h-fit {FLEX_COL} items-start lg:-mr-16 {maxpx(1440)} overflow-hidden",
    )


def testimonial_card(idx, comment, name, role, company, image_src):
    return Div(
        P(comment, cls="m-body text-black"),
        Div(
            Div(
                Img(src=image_src, alt=f"Picture of {name}", width="112", height="112"),
                cls="rounded-full w-11 h-11 lg:w-14 lg:h-14",
            ),
            Div(
                P(name, cls=f"m-body text-black"),
                Div(
                    P(role),
                    Img(
                        src=f"{icons}/dot.svg",
                        alt="Dot separator",
                        width="4",
                        height="4",
                    ),
                    P(company),
                    cls=f"{gap2} xs-mono-body w-full",
                ),
                cls="w-full",
            ),
            cls=f"{center} justify-start gap-2",
        ),
        id=f"testimonial-card-{idx + 1}",
        cls=f"testimonial-card {col} flex-none whitespace-normal flex justify-between h-96 rounded-3xl items-start bg-soft-pink p-4 lg:p-8 {maxrem(36)} lg:w-96",
    )


def testimonials_section():
    testimonial_cards = [
        testimonial_card(i, *args) for i, args in enumerate(testimonials)
    ]
    return section_wrapper(
        Div(
            section_header(
                "LOVE IS IN THE AIR",
                "What creators say",
                "Top YouTube creators and strategists share their love for ViralVibes.",
                max_width=21,
                center=True,
            ),
            carousel(testimonial_cards),
            cls=f"{section_base} {maxrem(90)} mx-auto lg:flex-row items-start",
        ),
        bg_color="red-100",
    )


def faq_section():
    return section_wrapper(
        Div(
            section_header(
                "FAQ",
                "Questions? Answers.",
                "Your top ViralVibes questions clarified.",
                max_width=21,
                center=False,
            ),
            Div(
                *[
                    faq_item(question, answer, i + 3)
                    for i, (question, answer) in enumerate(faqs)
                ],
                cls=f"{col} gap-4 {maxrem(32)} transition ease-out delay-[300ms]",
            ),
            cls=f"{section_base} w-full mx-auto lg:flex-row items-start max-w-7xl",
        ),
        bg_color="red-700",
        flex=False,
    )
