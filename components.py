from typing import Dict, List, Optional, Tuple

import polars as pl
from fasthtml.common import *
from monsterui.all import *

from charts import (
    chart_bubble_engagement_vs_views,
    chart_controversy_score,
    chart_engagement_rate,
    chart_likes_vs_dislikes,
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
    NEWSLETTER_CARD,
    PLAYLIST_STEPS_CONFIG,
    STEPS_CLS,
)
from utils import format_number


def HeaderCard() -> Card:
    return Card(P("Decode YouTube virality. Instantly.",
                  cls="text-lg mt-2 text-white"),
                header=CardTitle("ViralVibes",
                                 cls="text-4xl font-bold text-white"),
                cls=HEADER_CARD,
                uk_scrollspy="cls: uk-animation-slide-bottom-small")


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
    prefill_url = "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC"

    return Card(
        Img(src="/static/celebration.webp",
            style=
            "width: 100%; max-width: 320px; margin: 0 auto 1rem auto; display: block;",
            alt="Celebration"),
        P("Follow these steps to analyze any YouTube playlist:",
          cls="text-lg font-semibold text-center mb-2"),
        Div(PlaylistSteps(), id="playlist-steps", cls=f"{FLEX_CENTER} w-full"),
        Form(
            LabelInput(
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
                f"{ButtonT.destructive} hover:scale-105 transition-transform"),
            Loading(id="loading",
                    cls=(LoadingT.bars, LoadingT.lg),
                    style="margin-top:0.5rem; color:#393e6e;",
                    htmx_indicator=True),

            # HTMX hooks: validation triggers preview, then full analysis
            hx_post="/validate/url",
            hx_target="#validation-feedback",
            hx_swap="innerHTML",
            hx_indicator="#loading"),
        Div(id="validation-feedback", cls="mt-4"),
        Div(id="preview-box", cls="mt-4"),
        Div(id="result", style="margin-top:1rem;"),
        cls=FORM_CARD,
        body_cls="space-y-4",
        uk_scrollspy="cls: uk-animation-slide-bottom-small")


def _build_icon(name: str) -> "Component":
    """Build an icon component with consistent styling."""
    return UkIcon(name, cls="text-red-500 text-3xl mb-2")


def _build_info_items(config: List[Tuple[str, str, str]]) -> List["Component"]:
    """Build a list of info item components from a configuration."""
    return [
        Div(_build_icon(icon),
            H4(title, cls="mb-2 mt-2"),
            P(desc, cls="text-gray-600 text-sm text-center"),
            cls=f"{FLEX_COL} {FLEX_CENTER}") for title, desc, icon in config
    ]


def create_info_card(title: str,
                     items: List[Tuple[str, str, str]],
                     img_src: Optional[str] = None,
                     img_alt: Optional[str] = None) -> Card:
    """Helper function to create Feature and Benefit cards."""
    cards = _build_info_items(items)
    img_component = Img(
        src=img_src,
        style="width:120px; margin: 0 auto 2rem auto; display:block;",
        alt=img_alt) if img_src else ""
    return Card(img_component,
                Grid(*cards),
                header=CardTitle(
                    title, cls="text-2xl font-semibold mb-4 text-center"),
                cls=CARD_BASE,
                body_cls="space-y-6",
                uk_scrollspy="cls: uk-animation-slide-bottom-small")


def FeaturesCard() -> Card:
    """Create the features card component."""
    return create_info_card("What is ViralVibes?", FEATURES,
                            "/static/virality.webp",
                            "Illustration of video viral insights")


def BenefitsCard() -> Card:
    """Create the benefits card component."""
    return create_info_card("Why You'll Love It", BENEFITS)


def NewsletterCard() -> Card:
    return Card(
        P("Enter your email to get early access and updates. No spam ever.",
          cls="mb-4"),
        Form(LabelInput(
            "Email",
            type="email",
            name="email",
            required=True,
            pattern="[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$",
            title="Please enter a valid email address",
            placeholder="you@example.com",
            className=
            "px-4 py-2 w-full max-w-sm border rounded focus:ring-2 focus:ring-red-500 focus:border-red-500 transition-all invalid:border-red-500 invalid:focus:ring-red-500"
        ),
             Button("Notify Me",
                    type="submit",
                    className=
                    f"{ButtonT.primary} hover:scale-105 transition-transform"),
             Loading(id="loading",
                     cls=(LoadingT.bars, LoadingT.lg),
                     style="margin-top:1rem; color:#393e6e;",
                     htmx_indicator=True),
             className=f"{FLEX_COL} {FLEX_CENTER} space-y-4",
             hx_post="/newsletter",
             hx_target="#newsletter-result",
             hx_indicator="#loading"),
        Div(id="newsletter-result", style="margin-top:1rem;"),
        header=CardTitle("Be the first to try it",
                         cls="text-xl font-bold mb-4"),
        cls=NEWSLETTER_CARD,
        body_cls="space-y-6",
        uk_scrollspy="cls: uk-animation-slide-bottom-small")


def SummaryStatsCard(summary: Dict) -> Card:
    """Create a card to display summary statistics."""
    stats = Div(Div(UkIcon("eye", cls="text-3xl text-blue-500 mb-2"),
                    H3(format_number(summary.get("total_views", 0)),
                       cls="text-2xl font-bold"),
                    P("Total Views", cls="text-gray-600"),
                    cls="text-center p-4"),
                Div(UkIcon("heart", cls="text-3xl text-red-500 mb-2"),
                    H3(format_number(summary.get("total_likes", 0)),
                       cls="text-2xl font-bold"),
                    P("Total Likes", cls="text-gray-600"),
                    cls="text-center p-4"),
                Div(UkIcon("percent", cls="text-3xl text-green-500 mb-2"),
                    H3(f"{summary.get('avg_engagement', 0):.2f}%",
                       cls="text-2xl font-bold"),
                    P("Average Engagement", cls="text-gray-600"),
                    cls="text-center p-4"),
                cls="grid grid-cols-1 md:grid-cols-3 gap-4")

    return Card(body=stats, cls=f"{CARD_BASE} w-full")


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
        link_class = 'uk-active' if i == 0 else ''
        tab_links.append(Li(A(title, href='#', cls=link_class)))
        tab_content.append(Li(content))

    return Container(
        TabContainer(
            *tab_links,
            uk_switcher=f'connect: #{tabs_id}; animation: uk-animation-fade',
            alt=True),
        Ul(id=tabs_id, cls="uk-switcher")(*tab_content))


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
        H2("📊 Playlist Analytics", cls="text-2xl font-bold mb-4"),
        P("Visual breakdown of views, engagement, controversy, and performance.",
          cls="text-gray-600 mb-10"),

        # Group 1: View-based insights
        H3("👀 Views Overview", cls="text-xl font-semibold mb-2"),
        Grid(chart_views_by_rank(df),
             chart_treemap_views(df),
             cls="grid-cols-1 md:grid-cols-2 gap-8 mb-12"),

        # Group 2: Engagement insights
        H3("💬 Engagement & Reactions", cls="text-xl font-semibold mb-2"),
        Grid(chart_engagement_rate(df),
             chart_total_engagement(summary),
             cls="grid-cols-1 md:grid-cols-2 gap-8 mb-12"),

        # Group 3: Sentiment & controversy
        H3("🔥 Controversy & Sentiment", cls="text-xl font-semibold mb-2"),
        Grid(chart_likes_vs_dislikes(df),
             chart_controversy_score(df),
             cls="grid-cols-1 md:grid-cols-2 gap-8 mb-12"),

        # Group 4: Correlation and Multivariate Relationship between likes and dislikes
        H3("📈 Correlation & Advanced Patterns",
           cls="text-xl font-semibold mb-2"),
        Grid(chart_scatter_likes_dislikes(df),
             chart_bubble_engagement_vs_views(df),
             cls="grid-cols-1 md:grid-cols-2 gap-8 mb-4"),
        cls="mt-16 pt-10 border-t border-gray-200 space-y-10")


# components.py or step_components.py


def PlaylistPreviewCard(playlist_name: str,
                        channel_name: str,
                        channel_thumbnail: str,
                        playlist_length: Optional[int],
                        playlist_url: str,
                        meter_id: str = "fetch-progress-meter"):
    return Div(Div(
        H3("Playlist Preview", cls="text-xl font-bold text-gray-800"),
        Hr(cls="my-2"),
        Div(Img(src=channel_thumbnail,
                alt=f"{channel_name} channel thumbnail",
                style="width:80px;height:80px;border-radius:50%;margin:auto;"),
            H4(playlist_name, cls="text-lg font-semibold mt-3"),
            P(f"Channel: {channel_name}", cls="text-gray-600 text-sm"),
            cls="text-center space-y-2"),
        Div(P(Span(str(playlist_length), cls="font-semibold text-blue-700"),
              " videos in playlist",
              cls="text-sm text-gray-700 text-center")
            if playlist_length else None,
            Meter(value=0,
                  min=0,
                  max=playlist_length or 1,
                  low=10,
                  high=50,
                  optimum=100,
                  id=meter_id,
                  cls="w-full h-2 bg-gray-200 rounded"),
            cls="space-y-2"),
        Button(
            "Start Full Analysis",
            hx_post="/validate/full",
            hx_vals={"playlist_url": playlist_url},
            hx_target="#results-box",
            hx_indicator="#loading-bar",
            cls=
            ("uk-button uk-button-primary w-full mt-6 py-2.5 text-base "
             "font-medium rounded-lg shadow-sm hover:bg-blue-700 transition duration-300"
             ),
            type="button"),
        Div(Loading(id="loading-bar", cls=(LoadingT.bars, LoadingT.lg)),
            id="results-box",
            cls="mt-4"),
        cls=
        f"{CARD_BASE} max-w-xl mx-auto p-6 shadow-md rounded-xl space-y-4 bg-white"
    ),
               cls="p-6 sm:p-10")
