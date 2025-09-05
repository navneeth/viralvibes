import random
from typing import Dict, List, Optional, Tuple

import polars as pl
from fasthtml.common import *
from monsterui.all import *

from charts import (
    chart_bubble_engagement_vs_views,
    chart_controversy_score,
    chart_engagement_rate,
    chart_likes_vs_dislikes,
    chart_polarizing_videos,
    chart_scatter_likes_dislikes,
    chart_total_engagement,
    chart_treemap_views,
    chart_views_by_rank,
)
from constants import (
    BENEFITS,
    CARD_BASE,
    FEATURES,
    FLEX_BETWEEN,
    FLEX_CENTER,
    FLEX_COL,
    FORM_CARD,
    GAP_2,
    GAP_4,
    HEADER_CARD,
    KNOWN_PLAYLISTS,
    NEWSLETTER_CARD,
    PLAYLIST_STEPS_CONFIG,
    STEPS_CLS,
)
from utils import format_number
"""Define reusable UI components for the ViralVibes application."""

col = "flex flex-col"
section_base1 = "pt-8 px-4 pb-24 gap-8 lg:gap-16 lg:pt-16 lg:px-16"


def maxrem(rem):
    return f"w-full max-w-[{rem}rem]"


def benefit(title, content):
    return Div(
        H3(title, cls=f"text-white heading-3"),
        P(content, cls=f"l-body mt-6 lg:mt-6"),
        cls=
        "w-full p-6 bg-red-500 rounded-2xl xl:p-12 lg:h-[22rem] lg:w-[26rem]",
    )


def HeaderCard() -> Card:
    """Redesigned header card with image on the right side."""
    return Card(
        Div(
            Div(
                CardTitle("Welcome to ViralVibes",
                          cls="text-4xl font-bold text-white mb-4"),
                P("Decode YouTube virality. Instantly.",
                  cls="text-lg mt-2 text-white"),
                P(
                    "Analyze your YouTube playlists with creator-first insights.",
                    cls="text-sm mt-2 text-white",
                ),
                cls="flex-1",
            ),
            Div(
                Img(
                    src="/static/thumbnail.png",
                    alt="YouTube Playlist Thumbnail",
                    style=
                    "width:180px; height:auto; border-radius:1rem; box-shadow:0 4px 24px rgba(0,0,0,0.15);",
                ),
                cls="flex items-center justify-center flex-1",
            ),
            cls="flex flex-row gap-8 items-center",
        ),
        cls=HEADER_CARD,
        uk_scrollspy="cls: uk-animation-slide-bottom-small",
    )


def PlaylistSteps(completed_steps: int = 0) -> Steps:
    """Create a Steps component explaining the playlist submission process."""
    steps = []
    for i, (title, icon, description) in enumerate(PLAYLIST_STEPS_CONFIG):
        if i < completed_steps:
            step_cls = StepT.success
        elif i == completed_steps:
            step_cls = StepT.primary
        else:
            step_cls = StepT.neutral

        steps.append(
            LiStep(title,
                   cls=step_cls,
                   data_content=icon,
                   description=description))

    return Steps(*steps, cls=STEPS_CLS)


'''
def AnalysisFormCard() -> Card:
    """Create the analysis form card component."""
    prefill_url = "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC"
    return Card(
        Img(src="/static/celebration.webp",
            style=
            "width: 100%; max-width: 320px; margin: 0 auto 1rem auto; display: block;",
            alt="Celebration"),
        P("Follow these steps to analyze any YouTube playlist:",
          cls="text-lg font-semibold text-center mb-2"),
        Div(PlaylistSteps(), id="playlist-steps", cls=f"{FLEX_CENTER} w-full"),
        Form(LabelInput(
            "Playlist URL",
            type="text",
            name="playlist_url",
            placeholder="Paste YouTube Playlist URL",
            value=prefill_url,
            className=
            "px-4 py-2 w-full border rounded mb-3 focus:ring-2 focus:ring-red-500 focus:border-red-500 transition-all"
        ),
             Button(
                 "Analyze Now",
                 type="submit",
                 className=
                 f"{ButtonT.destructive} hover:scale-105 transition-transform"
             ),
             Loading(id="loading",
                     cls=(LoadingT.bars, LoadingT.lg),
                     style="margin-top:0.5rem; color:#393e6e;",
                     htmx_indicator=True),
             hx_post="/validate",
             hx_target="#playlist-steps",
             hx_indicator="#loading"),
        Div(id="result", style="margin-top:1rem;"),
        cls=FORM_CARD,
        body_cls="space-y-4")
'''


def AnalysisFormCard() -> Card:
    """Create the analysis form card component with atomic HTMX triggers."""
    # Get a random prefill URL from the known playlists
    prefill_url = random.choice(
        KNOWN_PLAYLISTS)["url"] if KNOWN_PLAYLISTS else ""

    return Card(
        # Hero section (gradient + illustration with smooth bottom transition)
        Div(
            Img(
                src="/static/celebration.webp",
                alt="YouTube Analytics Celebration",
                cls="w-28 mx-auto drop-shadow-lg",
            ),
            H2(
                "Analyze Your YouTube Playlist",
                cls="text-2xl font-bold text-white text-center mt-4",
            ),
            P(
                "Get deep insights into views, engagement, and virality patterns",
                cls="text-white/80 text-center mt-2",
            ),
            cls=("bg-gradient-to-br from-red-500 via-red-600 to-red-700 "
                 "rounded-t-xl -m-6 mb-0 py-8 shadow-lg"),
        ),

        # Form card body with slight overlap to hero for smooth transition
        # Steps section with better visual hierarchy
        Div(
            # Steps section
            Div(
                H3(
                    "How it works",
                    cls="text-lg font-semibold text-gray-800 mb-4 text-center",
                ),
                PlaylistSteps(),
                cls="bg-gray-50 rounded-xl p-6 mb-8 shadow-sm",
            ),

            # Input form
            Form(
                Div(
                    LabelInput(
                        "Playlist URL",
                        type="text",
                        name="playlist_url",
                        placeholder="Paste YouTube Playlist URL",
                        value=prefill_url,
                        className=
                        ("px-4 py-2 w-full border rounded-md "
                         "text-gray-900 placeholder-gray-400 "
                         "focus:ring-2 focus:ring-red-500 focus:border-red-500 transition"
                         ),
                        style="color: #333;"),
                    # URL validation hint
                    P("💡 Works with any public playlist",
                      cls="text-xs text-gray-500 mt-1"),
                    cls="space-y-1",
                ),
                # Action buttons with better spacing
                Div(
                    Button(
                        Span(UkIcon("search", cls="mr-2"), "Analyze Playlist"),
                        type="submit",
                        cls=
                        f"{ButtonT.primary} w-full hover:scale-105 transition-transform",
                    ),
                    # Quick action buttons for demo playlists
                    Details(
                        Summary(
                            "Try sample playlists",
                            cls=
                            "text-sm text-gray-600 cursor-pointer hover:text-gray-800"
                        ),
                        SamplePlaylistButtons(),
                        cls="mt-3",
                    ) if KNOWN_PLAYLISTS else None,
                    cls="mt-6 space-y-3",
                ),
                # Loading indicator with better positioning
                Div(
                    Loading(id="loading",
                            cls=(LoadingT.bars, LoadingT.lg),
                            htmx_indicator=True),
                    cls="flex justify-center mt-2",
                ),
                # HTMX hooks: validation triggers preview, then full analysis
                hx_post="/validate/url",
                hx_target="#validation-feedback",
                hx_swap="innerHTML",
                hx_indicator="#loading",
            ),

            # Results placeholders
            Div(id="validation-feedback", cls="mt-6 text-gray-900"),
            Div(id="preview-box", cls="mt-6 text-gray-900"),
            Div(id="result",
                cls="mt-8 min-h-[400px] border-t pt-6 text-gray-900"),
            cls=
            "bg-white rounded-b-xl -mt-6 p-10 shadow-lg text-gray-900 space-y-6",
        ),
        cls="w-full my-12",
    )


# Helper: render sample playlist quick-fill buttons


def SamplePlaylistButtons(input_name: str = "playlist_url",
                          max_items: int = 3) -> Div:
    """Render quick action buttons to prefill the playlist URL from known samples.
    Args:
        input_name: The name attribute of the input to populate.
        max_items: Number of sample playlists to show.
    """
    if not KNOWN_PLAYLISTS:
        return Div()

    buttons = []
    for pl in KNOWN_PLAYLISTS[:max_items]:
        title = pl.get("title", "Sample")
        short = f"{title[:30]}{'...' if len(title) > 30 else ''}"
        buttons.append(
            Button(
                f"📺 {short}",
                type="button",
                cls=
                ("text-left text-xs text-blue-600 hover:text-blue-800 "
                 "hover:bg-blue-50 px-2 py-1 rounded transition-colors w-full"
                 ),
                onclick=
                ("document.querySelector(\"input[name=\\\"%s\\\"]\").value = '%s'"
                 % (input_name, pl.get("url", ""))),
            ))

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
        ) for title, desc, icon in config
    ]


def create_info_card(
    title: str,
    items: List[Tuple[str, str, str]],
    img_src: Optional[str] = None,
    img_alt: Optional[str] = None,
) -> Card:
    """Helper function to create Feature and Benefit cards."""
    cards = _build_info_items(items)
    img_component = (Img(
        src=img_src,
        style="width:120px; margin: 0 auto 2rem auto; display:block;",
        alt=img_alt,
    ) if img_src else "")
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
                className=
                "px-4 py-2 w-full max-w-sm border rounded focus:ring-2 focus:ring-red-500 focus:border-red-500 transition-all invalid:border-red-500 invalid:focus:ring-red-500",
            ),
            Button(
                "Notify Me",
                type="submit",
                className=
                f"{ButtonT.primary} hover:scale-105 transition-transform",
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
        header=CardTitle("Be the first to try it",
                         cls="text-xl font-bold mb-4"),
        cls=NEWSLETTER_CARD,
        body_cls="space-y-6",
        uk_scrollspy="cls: uk-animation-slide-bottom-small",
    )


def SummaryStatsCard(summary: Dict) -> Card:
    stats = [("eye", "Total Views",
              format_number(summary.get("total_views", 0)), "text-blue-500"),
             ("heart", "Total Likes",
              format_number(summary.get("total_likes", 0)), "text-red-500"),
             ("percent", "Average Engagement",
              f"{summary.get('avg_engagement', 0):.2f}%", "text-green-500")]

    return Card(Grid(*[
        DivCentered(UkIcon(icon, height=32, cls=f"{color} mb-2"),
                    H3(value, cls="text-2xl font-bold"),
                    P(label, cls=TextPresets.muted_sm))
        for icon, label, value, color in stats
    ],
                     cols_md=3),
                cls=CardT.hover)


def create_tabs(tabs: List[Tuple[str, "Component"]], tabs_id: str) -> Div:
    """
    Creates a MonsterUI tab component.
    Args:
        tabs: A list of tuples, where each tuple is (tab_title, tab_content_component).
        tabs_id: A unique id for the tab group.
    Returns:
        A Div containing the tab structure.
    """
    tab_links = []
    tab_content = []

    for i, (title, content) in enumerate(tabs):
        link_class = "uk-active" if i == 0 else ""
        tab_links.append(Li(A(title, href="#", cls=link_class)))
        tab_content.append(Li(content))

    return Container(
        TabContainer(
            *tab_links,
            uk_switcher=f"connect: #{tabs_id}; animation: uk-animation-fade",
            alt=True,
        ),
        Ul(id=tabs_id, cls="uk-switcher")(*tab_content),
    )


def HomepageAccordion() -> Div:
    """Create an accordion section containing Features, Benefits, and Newsletter cards."""
    return Div(
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


def AnalyticsDashboardSection(df: pl.DataFrame, summary: Dict):
    return Section(
        # Header
        Div(
            H2("📊 Playlist Analytics", cls="text-3xl font-bold text-gray-900"),
            P(
                "Explore how this playlist performs across views, engagement, and audience reactions.",
                cls="text-gray-500 mt-2 mb-8 text-lg",
            ),
            cls="text-center",
        ),

        # Group 1: Reach & Views
        Div(
            H3("👀 Reach & Views",
               cls="text-2xl font-semibold text-gray-800 mb-4"),
            P("How far the playlist spreads, from rank to overall distribution.",
              cls="text-gray-500 mb-6"),
            Grid(
                chart_polarizing_videos(df),  # Bubble plot instead of line
                chart_treemap_views(df),
                cls="grid-cols-1 md:grid-cols-2 gap-10",
            ),
            cls="mb-16",
        ),

        # Group 2: Engagement
        Div(
            H3("💬 Engagement & Reactions",
               cls="text-2xl font-semibold text-gray-800 mb-4"),
            P("Do viewers interact, like, and comment? A closer look at active participation.",
              cls="text-gray-500 mb-6"),
            Grid(
                chart_engagement_rate(df),
                chart_total_engagement(summary),
                cls="grid-cols-1 md:grid-cols-2 gap-10",
            ),
            cls="mb-16",
        ),

        # Group 3: Controversy
        Div(
            H3("🔥 Controversy & Sentiment",
               cls="text-2xl font-semibold text-gray-800 mb-4"),
            P("Where opinions split — videos that polarize the audience.",
              cls="text-gray-500 mb-6"),
            Grid(
                chart_likes_vs_dislikes(df),
                chart_controversy_score(df),
                cls="grid-cols-1 md:grid-cols-2 gap-10",
            ),
            cls="mb-16",
        ),

        # Group 4: Advanced Patterns
        Div(
            H3("📈 Correlation & Advanced Patterns",
               cls="text-2xl font-semibold text-gray-800 mb-4"),
            P("Finding deeper relationships between views, likes, and engagement.",
              cls="text-gray-500 mb-6"),
            Grid(
                chart_scatter_likes_dislikes(df),
                chart_bubble_engagement_vs_views(df),
                cls="grid-cols-1 md:grid-cols-2 gap-10",
            ),
        ),
        cls="mt-20 pt-12 border-t border-gray-200 space-y-12",
    )


def PlaylistPreviewCard(
    playlist_name: str,
    channel_name: str,
    channel_thumbnail: str,
    playlist_length: Optional[int],
    playlist_url: str,
    meter_id: str = "fetch-progress-meter",
):
    return Card(
        # Thumbnail + Playlist Info
        DivCentered(Img(src=channel_thumbnail,
                        alt=f"{channel_name} thumbnail",
                        cls="w-20 h-20 rounded-full shadow-md border mb-4"),
                    H3(playlist_name,
                       cls="text-lg font-semibold text-gray-900"),
                    P(f"by {channel_name}", cls="text-sm text-gray-500"),
                    cls="text-center space-y-2"),

        # Playlist length + Progress bar
        Div(P(f"{playlist_length or 0} videos in playlist",
              cls="text-sm font-medium text-gray-700"),
            Progress(
                value=0,
                max=playlist_length or 1,
                id=meter_id,
                cls=("w-full h-2 rounded-full bg-gray-200 "
                     "[&::-webkit-progress-bar]:bg-gray-200 "
                     "[&::-webkit-progress-value]:rounded-full "
                     "[&::-webkit-progress-value]:transition-all "
                     "[&::-webkit-progress-value]:duration-300 "
                     "[&::-webkit-progress-value]:bg-blue-600 "
                     "[&::-moz-progress-bar]:bg-blue-600"),
            ),
            cls="space-y-2 mt-4"),

        # CTA button
        Button(
            "Start Full Analysis",
            hx_post="/validate/full",
            hx_vals={
                "playlist_url": playlist_url,
                "meter_id": meter_id,
                "meter_max": playlist_length or 0
            },
            hx_target="#results-box",
            hx_indicator="#loading-bar",
            hx_swap="beforeend",  # important for streaming scripts + final HTML
            cls=(
                "w-full mt-6 py-2.5 text-base font-medium rounded-xl shadow-sm "
                "bg-blue-600 hover:bg-blue-700 text-white transition"),
        ),

        # Results + Loading state
        Div(
            Loading(id="loading-bar", cls=(LoadingT.bars, LoadingT.lg)),
            Div(id="results-box", cls="mt-4"),
        ),

        # Card header
        header=CardTitle("Playlist Preview",
                         cls="text-xl font-bold text-gray-900"),
        cls="max-w-md mx-auto p-6 rounded-2xl shadow-lg bg-white space-y-4")


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
    legal = [
        "Terms of Service", "Privacy Policy", "Cookie Settings",
        "Accessibility"
    ]

    return Container(cls="uk-background-muted py-12")(Div(
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
        P("© 2025 ViralVibes. All rights reserved.",
          cls=TextT.lead + TextT.sm),
        cls="space-y-8 p-8",
    ))


def section_wrapper(content, bg_color, xtra="", flex=True):
    """
    Wraps a section with background color, layout, and rounded corners.
    """
    return Section(
        content,
        cls=
        f"bg-{bg_color} {section_base1} {FLEX_COL if flex else ''} -mt-8 lg:-mt-16 items-center rounded-t-3xl lg:rounded-t-[2.5rem] relative {xtra}",
    )


def section_header(mono_text, heading, subheading, max_width=32, center=True):
    pos = "items-center text-center" if center else "items-start text-start"
    return Div(
        P(mono_text, cls="mono-body text-opacity-60"),
        H2(heading, cls=f"text-white heading-2 {maxrem(max_width)}"),
        P(subheading, cls=f"l-body {maxrem(max_width)}"),
        cls=f"{maxrem(50)} mx-auto {col} {pos} gap-6",
    )
