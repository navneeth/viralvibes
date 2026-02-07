"""Card components for the ViralVibes application."""

import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from fasthtml.common import *
from monsterui.all import *

from components.base import STEPS_CLS, DivCentered, styled_div
from components.buttons import FeaturePill, cta, paste_button, progress_meter
from constants import (
    BENEFITS,
    CARD_BASE,
    FEATURES,
    FLEX_CENTER,
    FLEX_COL,
    FORM_CARD,
    ICONS_PATH,
    KNOWN_PLAYLISTS,
    NEWSLETTER_CARD,
    PLAYLIST_STEPS_CONFIG,
    STYLES,
    THEME,
)
from db import fetch_playlists
from utils import format_number


# =============================================================================
# Helpers
# =============================================================================
def benefit(title: str, content: str) -> Div:
    """Create a benefit card with styled content."""
    return styled_div(
        H3(title, cls="text-white heading-3"),
        P(content, cls="text-gray-200 s-body mt-4"),
        cls="card-benefit",
    )


def accordion(
    id: str,
    question: str,
    answer: str,
    question_cls: str = "",
    answer_cls: str = "",
    container_cls: str = "bg-soft-blue rounded-2xl",
) -> Div:
    """Create a collapsible accordion component."""
    return Div(
        Input(
            id=f"collapsible-{id}",
            type="checkbox",
            cls=f"collapsible-checkbox peer/collapsible hidden",
        ),
        Label(
            P(question, cls=f"flex-grow {question_cls}"),
            Img(
                src=f"{ICONS_PATH}/plus-icon.svg",
                alt="Expand",
                cls=f"plus-icon w-6 h-6",
            ),
            Img(
                src=f"{ICONS_PATH}/minus-icon.svg",
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
    """Create a FAQ item card."""
    return accordion(
        id=str(id),
        question=question,
        answer=answer,
        question_cls="text-black text-sm font-medium",
        answer_cls="text-black/80 text-sm",
        container_cls=f"bg-blue-50 rounded-2xl shadow-inner",
    )


# =============================================================================
# Main Cards
# =============================================================================
def HeaderCard() -> Card:
    """Simplified, flat header card for ViralVibes — no nested components."""
    return CardTitle(
        Div(
            # Text content (left)
            Div(
                H1(
                    "Welcome to ViralVibes",
                    cls=STYLES["hero_title"] + " text-blue-700 mb-4",
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
                    cls="mt-6 " + STYLES["cta_primary"],
                ),
                cls="flex-1",
            ),
            # Image (right)
            Img(
                src="/static/thumbnail.png",
                alt="YouTube Analytics Dashboard Preview",
                cls="flex-1 w-64 md:w-80 lg:w-96 " + STYLES["card_thumbnail"],
                loading="lazy",
            ),
            # Overall container
            cls="flex flex-col md:flex-row gap-10 items-center justify-between",
        ),
        cls=THEME["card_base"],
        uk_scrollspy="cls: uk-animation-slide-bottom-small",
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
            # pending → gray
            step_cls = StepT.neutral

        steps.append(
            LiStep(title, cls=step_cls, data_content=icon, description=description)
        )

    return Steps(*steps, cls=STEPS_CLS)


def AnalysisFormCard() -> Div:
    """Single-layer Analysis Form card with integrated paste button."""
    # Get a random prefill URL from the known playlists
    prefill_url = random.choice(KNOWN_PLAYLISTS)["url"] if KNOWN_PLAYLISTS else ""

    return styled_div(
        # --- Hero image section with gradient background ---
        styled_div(
            # Background gradient
            Div(
                cls="absolute inset-0 bg-gradient-to-br from-red-50 via-white to-orange-50 rounded-t-2xl",
            ),
            # Content overlay
            styled_div(
                # Hero image - premium look
                Img(
                    src="https://images.unsplash.com/photo-1504639725590-34d0984388bd?w=400&h300&q=80",
                    alt="YouTube Analytics Dashboard",
                    cls="w-full h-48 object-cover rounded-lg shadow-lg mb-6",
                    loading="lazy",
                ),
                # Heading with better hierarchy
                H2(
                    "Analyze Your YouTube Playlist",
                    cls="text-4xl font-bold text-gray-900 text-center mb-2",
                ),
                P(
                    "Get deep insights into views, engagement, and virality patterns",
                    cls="text-gray-600 text-center text-lg mb-6 max-w-2xl mx-auto",
                ),
                # Trust indicators
                styled_div(
                    FeaturePill("bolt", "Real-time Analytics"),
                    FeaturePill("users", "Creator Insights"),
                    FeaturePill("trending-up", "Viral Patterns"),
                    cls="flex flex-wrap gap-4 justify-center mb-8",
                ),
                cls="relative z-10 text-center py-12 px-6",
            ),
            cls="relative overflow-hidden rounded-t-2xl",
        ),
        # --- Steps with better styling ---
        styled_div(
            id="steps-container",
            children=[PlaylistSteps(completed_steps=0)],
            cls="justify-center my-10 px-6",
        ),
        # Main form – clean modern input
        Form(
            # Input section with label and hint
            Label(
                "Playlist URL 🔗 ",
                cls="block text-sm font-medium text-gray-700 mb-2",
            ),
            # Input group with leading icon + trailing paste
            Div(
                # Leading YouTube icon
                Div(
                    UkIcon("youtube", cls="w-5 h-5 text-red-600"),
                    cls="pointer-events-none absolute left-4 top-1/2 -translate-y-1/2",
                ),
                # Input field – clean, minimal border
                Input(
                    type="url",
                    name="playlist_url",
                    id="playlist_url",
                    placeholder="https://www.youtube.com/playlist?list=...",
                    value=prefill_url,
                    required=True,
                    cls=(
                        "w-full pl-12 pr-12 py-4 text-gray-900 bg-white "
                        "border border-gray-300 rounded-xl "
                        "focus:outline-none focus:ring-2 focus:ring-red-500 focus:border-transparent "
                        "shadow-sm hover:shadow transition-shadow duration-200"
                    ),
                ),
                # Paste button (trailing)
                Div(
                    paste_button("playlist_url"),
                    cls="absolute right-3 top-1/2 -translate-y-1/2",
                ),
                # Status feedback
                Span(
                    "",
                    id="playlist_url_status",
                    cls="absolute -bottom-5 left-0 text-xs",
                ),
                cls="relative mb-6",
            ),
            # Helper text
            P(
                "💡 Works with any public playlist. Paste the link and click the clipboard icon.",
                cls="text-sm text-gray-500 text-center mb-8",
            ),
            # Primary CTA – modern red gradient
            Button(
                Span(UkIcon("chart-bar", cls="w-5 h-5"), "Analyze Playlist"),
                type="submit",
                cls=(
                    f"w-full {ButtonT.primary} {THEME['primary_hover']} transition-all duration-300 "
                    "py-3 text-lg font-semibold shadow-lg hover:shadow-xl transform hover:scale-[1.02] "
                    "active:scale-95 flex items-center justify-center"
                ),
            ),
            # Sample playlists (if available)
            (
                styled_div(
                    Details(
                        Summary(
                            Span(
                                UkIcon("star", cls="w-4 h-4 mr-2"),
                                "No playlist? Try a sample!",
                            ),
                            cls="text-sm font-medium text-gray-700 cursor-pointer hover:text-red-600 py-3",
                        ),
                        SamplePlaylistButtons(),
                        cls="mt-4 p-4 bg-gray-50 rounded-lg border border-gray-200",
                    ),
                    cls="mt-6",
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
            # HTMX configuration
            hx_post="/validate/url",
            hx_target="#validation-feedback",
            hx_swap="innerHTML",
            hx_indicator="#loading",
            cls="px-8 pb-8",
        ),
        # --- Feedback + Results sections  ---
        styled_div(id="validation-feedback", cls="mt-6 px-8"),
        styled_div(id="preview-box", cls="mt-6 px-8"),
        styled_div(id="result", cls="mt-8 px-8 pb-8"),
        # --- Styling (outermost container only) ---
        cls=(
            f"{THEME['card_base']} space-y-0 w-full my-12 rounded-2xl shadow-xl "
            "border border-gray-200/70 overflow-hidden"
        ),
        style=FORM_CARD,
        uk_scrollspy="cls: uk-animation-slide-bottom-small",
        id="analysis-form",
    )


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
    """Create the newsletter signup card."""
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
                pattern=r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
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
    """Create a card summarizing key statistics."""
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
        tab_content.append(Li(Div(content, cls="p-2")))

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


def PlaylistPreviewCard(
    playlist_name: str,
    channel_name: str,
    channel_thumbnail: str,
    playlist_length: Optional[int],
    playlist_url: str,
    playlist_thumbnail: Optional[str] = None,
    channel_id: Optional[str] = None,
    description: Optional[str] = None,
    privacy_status: Optional[str] = None,
    published_at: Optional[str] = None,
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

    # Build channel URL if ID available
    channel_url = (
        f"https://www.youtube.com/channel/{channel_id}" if channel_id else None
    )

    # Format published date
    published_display = None
    if published_at:
        try:
            # Assume datetime is imported outside; handle ISO formats
            if "T" in published_at:
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            else:
                dt = datetime.fromisoformat(published_at)
            published_display = dt.strftime("%B %d, %Y")
        except Exception:
            published_display = published_at

    # Privacy badge classes
    privacy_badge_cls = {
        "public": "bg-green-50 text-green-700",
        "unlisted": "bg-yellow-50 text-yellow-700",
        "private": "bg-red-50 text-red-700",
    }.get(privacy_status.lower() if privacy_status else "", "bg-gray-50 text-gray-700")

    # Truncate description
    desc_preview = (
        description[:120] + "..."
        if description and len(description) > 120
        else description
    )

    return Card(
        # Visual header with both thumbnails
        styled_div(
            # Playlist thumbnail (if available) as background accent
            (
                styled_div(
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
                href=channel_url,
                target="_blank",
                cls="relative pt-8 {THEME['flex_center']}",
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
                    progress_meter(meter_id, actual_video_count),
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
                cta(
                    "Refresh Analysis",
                    icon="refresh-cw",
                    kind="refresh",
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
                )
                if show_refresh
                # Start analysis button (for new analysis)
                else cta(
                    "Start Full Analysis",
                    icon="chart-bar",
                    kind="full",
                    hx_post="/validate/full",
                    hx_vals={
                        "playlist_url": playlist_url,
                        "meter_id": meter_id,
                        "meter_max": actual_video_count,
                    },
                    hx_target="#results-box",
                    hx_indicator="#loading-bar",
                    hx_swap="beforeend",
                )
            ),
            # Secondary action (View in YouTube) if we have data
            (
                A(
                    UkIcon("external-link", cls="w-4 h-4 mr-2"),
                    "View on YouTube",
                    href=playlist_url,
                    target="_blank",
                    cls=STYLES["cta_secondary"],
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
    title: str,
    value: str,
    subtitle: str,
    icon: str,
    color: str = "red",
) -> Card:
    """Create a clean metric card with icon, value, and context."""
    return Card(
        Div(
            UkIcon(icon, cls=f"text-{color}-500", height=28, width=28),
            H3(value, cls="text-2xl font-bold text-gray-900 mb-1"),
            P(subtitle, cls="text-sm text-gray-600"),
            cls="flex flex-col items-start space-y-1",
        ),
        header=H4(
            title, cls="text-xs font-medium text-gray-500 uppercase tracking-wider"
        ),
        cls=(
            "p-5 rounded-xl shadow-sm border border-gray-200 "
            "hover:shadow-lg transition-all duration-200 "
            "bg-white"
        ),
    )


def AnalyticsDashboardSection(
    playlist_name,
    channel_name,
    channel_thumbnail,
    summary_stats,
    dashboard_id=None,
    mode="embedded",
):
    """Dashboard header with share/export functionality."""

    # Share/Export buttons (after channel info, before stats grid)
    action_buttons = (
        Div(
            # Share button
            Button(
                UkIcon("share-2", cls="mr-2 w-4 h-4"),
                "Share",
                hx_get=f"/modal/share/{dashboard_id}",
                hx_target="#modal-container",
                hx_swap="innerHTML",
                cls="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center",
                type="button",
            ),
            # Export button
            Button(
                UkIcon("download", cls="mr-2 w-4 h-4"),
                "Export",
                hx_get=f"/modal/export/{dashboard_id}",
                hx_target="#modal-container",
                hx_swap="innerHTML",
                cls="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors flex items-center",
                type="button",
            ),
            cls="flex gap-3 mt-4",
        )
        if dashboard_id
        else None
    )

    return Section(
        # ...existing header content...
        # ✅ ADD: Action buttons after channel info
        action_buttons,
        # ...existing stats grid...
        cls=f"{THEME['section']} my-12 space-y-8",
        id="analytics-dashboard",
    )
