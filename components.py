# Standard library and typing
import logging
import random
from typing import Dict, List, Optional, Tuple

# Third-party libraries
import polars as pl
from fasthtml.common import *
from monsterui.all import *

# Local modules
from charts import (
    chart_bubble_engagement_vs_views,
    chart_category_performance,
    chart_comments_engagement,
    chart_controversy_distribution,
    chart_controversy_score,
    chart_duration_impact,
    chart_duration_vs_engagement,
    chart_engagement_breakdown,
    chart_engagement_ranking,
    chart_likes_per_1k_views,
    chart_performance_heatmap,
    chart_stacked_interactions,
    chart_top_performers_radar,
    chart_treemap_reach,
    chart_treemap_views,
    chart_video_radar,
    chart_views_ranking,
    chart_views_vs_likes,
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
    STYLES,
    THEME,
    faqs,
    maxpx,
    testimonials,
)
from db import fetch_playlists, get_cached_playlist_stats
from utils import format_duration, format_number

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

# Get logger instance
logger = logging.getLogger(__name__)


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


# --- NEW: small UI helpers to centralize repeated Tailwind patterns ---
def cta(text: str, icon: Optional[str] = None, kind: str = "full", **kwargs) -> Button:
    """Create a CTA using centralized STYLES (kind='full'|'refresh'|'secondary')."""
    kind_map = {
        "full": "btn_full",
        "refresh": "btn_refresh",
        "secondary": "cta_secondary",
    }
    cls_key = kind_map.get(kind, "btn_full")
    icon_comp = UkIcon(icon, cls="mr-2") if icon else None
    return Button(
        Span(icon_comp, text) if icon_comp else text,
        cls=STYLES.get(cls_key, STYLES["btn_full"]),
        **kwargs,
    )


# --- small UI helpers (minimal, safe) ---
def small_badge(text: str, icon: Optional[str] = None, kind: str = "small") -> Span:
    """Small inline badge used for views/engagement/date."""
    cls_key = "badge_small" if kind == "small" else "badge_info"
    if icon:
        return Span(UkIcon(icon, cls="w-4 h-4 mr-1"), text, cls=STYLES[cls_key])
    return Span(text, cls=STYLES[cls_key])


def progress_meter(el_id: str, max_val: int = 1, cls: Optional[str] = None) -> Progress:
    """Return a progress element with centralized meter classes."""
    meter_cls = cls or STYLES["progress_meter"]
    return Progress(value=0, max=max_val or 1, id=el_id, cls=meter_cls)


def maxrem(rem):
    return f"w-full max-w-[{rem}rem]"


def benefit(title: str, content: str) -> Div:
    return styled_div(
        H3(title, cls="text-white text-xl font-bold"),
        P(content, cls="text-gray-200 text-base mt-4"),
        cls=f"w-full p-6 {THEME['primary_bg']} rounded-2xl lg:h-[22rem] lg:w-[26rem]",
    )


# Reusable Feature Pill – small, elegant, red-themed
def FeaturePill(icon: str, text: str):
    return Div(
        UkIcon(icon, cls="w-4 h-4 text-red-600"),
        Span(text, cls="text-xs font-semibold text-gray-700"),
        cls="flex items-center gap-1.5 px-3 py-1.5 bg-red-50/80 rounded-full border border-red-200/50 backdrop-blur-sm",
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


def hero_section():
    """
    Responsive hero that scales between mobile and desktop:
    - High-quality background image (Unsplash)
    - Gradient overlay for text readability
    - Better typography hierarchy
    - Glassmorphism effect for CTA

    """
    return Section(
        # Premium background image with gradient overlay
        styled_div(
            Img(
                src="https://images.unsplash.com/photo-1517694712202-14dd9538aa97?w=1920&q=80",
                alt="YouTube Analytics Background",
                cls="absolute inset-0 w-full h-full object-cover",
            ),
            styled_div(
                cls="absolute inset-0 bg-gradient-to-b from-black/40 via-black/50 to-black/70"
            ),
            cls="absolute inset-0 z-0",
        ),
        # Main content container
        styled_div(
            # Text content
            styled_div(
                H1(
                    "Decode YouTube Virality.",
                    cls="text-5xl md:text-6xl font-bold text-white mb-4 leading-tight",
                ),
                H1(
                    "Instantly.",
                    cls="text-5xl md:text-6xl font-bold bg-gradient-to-r from-red-400 to-pink-400 bg-clip-text text-transparent mb-6",
                ),
                P(
                    "Analyze any YouTube playlist to uncover engagement trends, viral patterns, and creator insights.",
                    cls="text-lg md:text-xl text-gray-200 max-w-2xl mb-8 leading-relaxed",
                ),
                # Glassmorphism CTA buttons
                styled_div(
                    A(
                        "🚀 Start Analyzing",
                        href="#analysis-form",
                        cls="px-8 py-4 bg-gradient-to-r from-red-600 to-red-700 text-white rounded-full font-semibold hover:shadow-lg hover:shadow-red-500/50 transition-all duration-300 transform hover:scale-105 inline-block",
                    ),
                    A(
                        "📚 Learn More",
                        href="#faq-section",
                        onclick="document.getElementById('faq-section').scrollIntoView({behavior:'smooth'}); return false;",
                        cls="px-8 py-4 bg-white/10 backdrop-blur-md text-white rounded-full font-semibold border border-white/20 hover:bg-white/20 transition-all duration-300 inline-block ml-4",
                    ),
                    cls="flex gap-4 flex-wrap justify-center lg:justify-start",
                ),
                cls="max-w-4xl text-center lg:text-left",
            ),
            cls=(
                f"{THEME['flex_col']} lg:{THEME['flex_row']} items-center justify-center "
                "px-4 lg:px-16 h-screen relative z-10 w-full"
            ),
        ),
        cls="relative overflow-hidden w-full min-h-screen bg-black",
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


def paste_button(target_id: str) -> Button:
    """Paste button with icon swap and premium styling."""
    status_id = f"{target_id}_status"
    onclick = f"""
        const btn = this, input = document.getElementById('{target_id}'), status = document.getElementById('{status_id}');
        btn.disabled = true;
        navigator.clipboard.readText()
            .then(text => {{
                input.value = text.trim();
                input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                status.textContent = '✓ Pasted';
                status.className = 'text-green-600 text-xs font-semibold';
                setTimeout(() => status.textContent = '', 1500);
            }})
            .catch(() => {{
                status.textContent = '✗ Paste failed';
                status.className = 'text-red-600 text-xs font-semibold';
                setTimeout(() => status.textContent = '', 2000);
            }})
            .finally(() => btn.disabled = false);
    """

    return Button(
        UkIcon("clipboard", cls="w-4 h-4"),  # Slightly smaller icon
        type="button",
        onclick=onclick,
        cls=(
            # ✅ SIZING
            "w-9 h-9 "  # Explicit 36×36px (slightly larger for comfort)
            "flex items-center justify-center "
            # ✅ STYLING
            "text-gray-400 hover:text-red-600 "
            "focus:outline-none focus:ring-2 focus:ring-red-500/30 "
            "disabled:opacity-50 disabled:cursor-not-allowed "
            # ✅ INTERACTIONS
            "transition-all duration-200 "
            "hover:bg-red-50 rounded-md "
            "active:scale-95 "
            # ✅ RESPONSIVE
            "flex-shrink-0"  # Never shrinks below 36×36px
        ),
        title="Paste from clipboard",
        aria_label="Paste from clipboard",
    )


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
                cls=f"{col} items-center justify-center px-6 pt-8 pb-6 relative z-10",
            ),
            cls="relative mb-6 rounded-t-2xl overflow-hidden",
        ),
        # --- Steps with better styling ---
        styled_div(
            id="steps-container",
            children=[PlaylistSteps(completed_steps=0)],
            cls=(
                "flex justify-center mb-8 transition-opacity duration-300 "
                "min-h-[120px] opacity-80"
            ),
        ),
        # --- Playlist Form with premium styling ---
        Form(
            # Input section with label and hint
            styled_div(
                Label(
                    Span(
                        "🔗 Playlist URL",
                        cls="block text-sm font-semibold text-gray-900 mb-2",
                    ),
                    Span(
                        "Paste your YouTube playlist link below",
                        cls="block text-xs text-gray-500 mb-3",
                    ),
                    cls="block",
                ),
                # Premium input group
                Div(
                    # Leading play icon
                    Span(
                        UkIcon("play", cls="w-5 h-5 text-red-500 font-bold"),
                        cls="absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none "
                        "bg-red-50 p-1 rounded-md",
                    ),
                    # Input field with enhanced styling
                    Input(
                        type="text",
                        name="playlist_url",
                        id="playlist_url",
                        placeholder="https://youtube.com/playlist?list=...",
                        value=prefill_url,
                        className=(
                            "w-full pl-12 pr-12 py-3 border-2 border-gray-200 rounded-lg "
                            "text-gray-900 placeholder-gray-400 focus:border-red-500 focus:ring-2 "
                            "focus:ring-red-500/20 focus:outline-none transition-all duration-200 "
                            "font-medium bg-white"
                        ),
                        style="color: #333;",
                    ),
                    # Trailing paste button
                    Div(
                        paste_button("playlist_url"),
                        cls="absolute right-2.5 top-1/2 -translate-y-1/2",
                    ),
                    cls="relative",
                ),
                # Status indicator (hidden by default)
                Span(
                    "", id="playlist_url_status", cls="text-xs mt-2", aria_live="polite"
                ),
                # Helper text
                P(
                    "💡 Works with any public YouTube playlist. Copy the link and click the clipboard icon above.",
                    cls="text-xs text-gray-500 mt-2 italic leading-relaxed",
                ),
                cls="mb-6",
            ),
            # Primary action button with enhanced styling
            Button(
                Span(
                    UkIcon("chart-bar", cls="w-5 h-5 flex-shrink-0"),
                    "Analyze Playlist",
                    cls="flex items-center justify-center gap-2",
                ),
                type="submit",
                cls=(
                    f"w-full {ButtonT.primary} {THEME['primary_hover']} transition-all duration-300 "
                    "py-3 text-lg font-semibold shadow-lg hover:shadow-xl transform hover:scale-[1.02] "
                    "active:scale-95 flex items-center justify-center"
                ),
            ),
            # Quick action section with better styling
            (
                styled_div(
                    Details(
                        Summary(
                            Span(
                                UkIcon("star", cls="w-4 h-4 mr-2 inline"),
                                "No playlist? Try a sample!",
                            ),
                            cls="text-sm font-medium text-gray-700 cursor-pointer hover:text-red-600 transition-colors py-2 px-3 rounded-lg hover:bg-gray-100",
                        ),
                        SamplePlaylistButtons(),
                        cls="mt-2 max-h-40 overflow-y-auto rounded-lg border border-gray-200 bg-gray-50",
                    ),
                    cls="mb-6",
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
            cls="space-y-4 mt-6 px-6 pb-6",
        ),
        # --- Feedback + Results sections with better styling ---
        styled_div(
            id="validation-feedback",
            cls="mt-6 text-gray-900 px-6",
        ),
        styled_div(
            id="preview-box",
            cls="mt-6 text-gray-900 px-6",
        ),
        styled_div(
            id="result",
            cls=(
                "mt-8 min-h-[400px] border-t pt-6 text-gray-900 "
                f"{THEME['neutral_bg']} rounded-b-2xl px-6 py-6"
            ),
        ),
        # --- Styling (outermost container only) ---
        cls=(
            f"{THEME['card_base']} space-y-0 w-full my-12 rounded-2xl shadow-xl "
            "border border-gray-200/70 overflow-hidden"
        ),
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
    # Derive total / processed counts with a clear precedence to avoid empty/missing keys
    # precedence: explicit summary keys -> common aliases -> df height -> 0
    actual_playlist_count = (
        summary.get("actual_playlist_count")
        or summary.get("video_count")
        or summary.get("total_count")
        or (df.height if (df is not None and hasattr(df, "height")) else 0)
    )

    processed_count = (
        summary.get("processed_video_count")
        if summary.get("processed_video_count") is not None
        else (df.height if (df is not None and hasattr(df, "height")) else 0)
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
            cls="text-center mb-12",
        ),
        # =====================================================================
        # BLOCK 1: REACH & PERFORMANCE
        # =====================================================================
        Div(
            H3(
                "👀 Reach & Performance",
                cls="text-2xl font-semibold text-gray-800 mb-4",
            ),
            P(
                "How your videos are reaching your audience - which ones drive the most views?",
                cls="text-gray-500 mb-8",
            ),
            Grid(
                chart_views_ranking(df, "views-ranking"),
                chart_treemap_reach(df, "treemap-reach"),
                cols="1 md:2",  # 1 col on mobile, 2 on desktop)
                gap="6 md:8",  # RESPONSIVE GAP
                cls="w-full",
            ),
            cls="pb-16 mb-16 border-b-2 border-gray-100",
        ),
        # =====================================================================
        # BLOCK 2: AUDIENCE ENGAGEMENT QUALITY
        # =====================================================================
        Div(
            H3(
                "💬 Engagement Quality", cls="text-2xl font-semibold text-gray-800 mb-4"
            ),
            P(
                "Beyond views - how engaged is your audience? Likes per view & comment rates.",
                cls="text-gray-500 mb-6",
            ),
            Grid(
                chart_engagement_ranking(df, "engagement-ranking"),
                chart_engagement_breakdown(df, "engagement-breakdown"),
                cols="1 md:2",  # 1 col on mobile, 2 on desktop)
                gap="6 md:8",  # RESPONSIVE GAP
                cls="w-full",
            ),
            Div(
                H4(
                    "📐 How We Measure Engagement",
                    cls="text-sm font-semibold text-gray-700 mt-8 mb-2",
                ),
                P(
                    "Engagement = (Likes + Comments) ÷ Views × 100",
                    cls="text-xs text-gray-600 bg-gray-50 p-4 rounded-lg border border-gray-200 leading-relaxed",
                ),
                cls="mt-8 bg-gray-50/50 rounded-xl p-6 shadow-inner",
            ),
            cls="mb-16 pb-12 border-b border-gray-200",
        ),
        # =====================================================================
        # BLOCK 3: CONTENT INTERACTION
        # =====================================================================
        Div(
            H3(
                "🎯 Content Interaction",
                cls="text-2xl font-semibold text-gray-800 mb-4",
            ),
            P(
                "How views convert to likes and comments - understand audience behavior.",
                cls="text-gray-500 mb-6",
            ),
            Grid(
                chart_views_vs_likes(df, "views-vs-likes"),
                chart_comments_engagement(df, "comments-engagement"),
                cols="1 md:2",  # 1 col on mobile, 2 on desktop)
                gap="6 md:10",  # RESPONSIVE GAP
                cls="w-full",
            ),
            cls="mb-16 pb-12 border-b border-gray-200",
        ),
        # =====================================================================
        # BLOCK 4: PERFORMANCE QUADRANTS
        # =====================================================================
        # Div(
        #     H3(
        #         "🎯 Strategic Positioning",
        #         cls="text-2xl font-semibold text-gray-800 mb-2",
        #     ),
        #     P(
        #         "Where does your content stand? Identify high-performers and improvement opportunities.",
        #         cls="text-gray-500 mb-8 text-sm",
        #     ),
        #     Grid(
        #         # chart_performance_heatmap(df, "performance-heatmap"),
        #         chart_bubble_engagement_vs_views(df, "bubble-engagement"),
        #         cols="1 md:2",
        #         gap="6 md:10",
        #         cls="w-full",
        #     ),
        #     cls="mb-16 pb-12 border-b border-gray-200",
        # ),
        # =====================================================================
        # BLOCK 5: CONTENT FACTORS
        # =====================================================================
        # Div(
        #     H3(
        #         "⏱️ Content Optimization",
        #         cls="text-2xl font-semibold text-gray-800 mb-4",
        #     ),
        #     P(
        #         "Does video length affect engagement? What's the optimal duration for your audience?",
        #         cls="text-gray-500 mb-6",
        #     ),
        #     Grid(
        #         chart_duration_impact(df, "duration-impact"),
        #         chart_duration_vs_engagement(df, "duration-engagement"),
        #         cols="1 md:2",
        #         gap="6 md:10",
        #         cls="w-full",
        #     ),
        #     cls="mb-16 pb-12 border-b border-gray-200",
        # ),
        # # =====================================================================
        # # BLOCK 6: CORRELATION & PATTERNS
        # # =====================================================================
        # Div(
        #     H3(
        #         "📈 Advanced Patterns",
        #         cls="text-2xl font-semibold text-gray-800 mb-2",
        #     ),
        #     P(
        #         "Uncover relationships: How do likes, dislikes, and views correlate? Multi-dimensional analysis.",
        #         cls="text-gray-500 mb-8 text-sm",
        #     ),
        #     Grid(
        #         # chart_scatter_likes_dislikes(df, "scatter-likes"),
        #         chart_video_radar(df, "video-radar"),
        #         cols="1 md:2",
        #         gap="6 md:8",
        #         cls="w-full",
        #     ),
        #     cls="pb-16 mb-16 border-b-2 border-gray-100",
        # ),
        # =====================================================================
        # BLOCK 7: CATEGORY ANALYSIS (if applicable)
        # =====================================================================
        # Div(
        #     H3(
        #         "📁 Category Performance",
        #         cls="text-2xl font-semibold text-gray-800 mb-2",
        #     ),
        #     P(
        #         "How do different content categories perform? Identify your strongest niches.",
        #         cls="text-gray-500 mb-8 text-sm",
        #     ),
        #     Grid(
        #         chart_category_performance(df, "category-performance"),
        #         cols="1",
        #         gap="8",
        #         cls="w-full",
        #     ),
        #     cls="pb-16 mb-16 border-b-2 border-gray-100",
        # ),
        # =====================================================================
        # BLOCK 8: SENTIMENT & CONTROVERSY
        # =====================================================================
        # Div(
        #     H3(
        #         "🔥 Audience Sentiment", cls="text-2xl font-semibold text-gray-800 mb-4"
        #     ),
        #     P(
        #         "Which videos create the strongest reactions? Controversy & polarization.",
        #         cls="text-gray-500 mb-6",
        #     ),
        #     Grid(
        #         chart_controversy_distribution(df, "controversy-dist"),
        #         # chart_controversy_score(df, "controversy-score"),
        #         cols="1 md:2",
        #         gap="6 md:8",
        #         cls="w-full",
        #     ),
        #     cls="mb-16 pb-12 border-b border-gray-200",
        # ),
        # Group 4: Advanced Insights & Patterns
        # Div(
        #     H3(
        #         "📈 Advanced Insights & Patterns",
        #         cls="text-2xl font-semibold text-gray-800 mb-4",
        #     ),
        #     P(
        #         "Uncover deeper relationships between viewership, engagement, and controversy across your content.",
        #         cls="text-gray-500 mb-6",
        #     ),
        #     Grid(
        #         chart_scatter_likes_dislikes(
        #             df, "scatter-likes"
        #         ),  # Correlation analysis
        #         chart_bubble_engagement_vs_views(
        #             df, "bubble-engagement"
        #         ),  # Multi-dimensional analysis
        #         # chart_duration_vs_engagement(df, "duration-engagement"),
        #         chart_video_radar(df, "video-radar"),
        #         cls="grid-cols-1 md:grid-cols-2 gap-10",
        #     ),
        #     cls="mb-16",
        # ),
        # Group 5: Content Strategy Insights (if we have controversy data)
        # Div(
        #     H3(
        #         "🎯 Content Strategy Insights",
        #         cls="text-2xl font-semibold text-gray-800 mb-4",
        #     ),
        #     P(
        #         "Strategic insights to help optimize your content mix and audience targeting.",
        #         cls="text-gray-500 mb-6",
        #     ),
        #     # Single chart that gives strategic insight
        #     chart_controversy_score(df, "controversy-score"),
        #     cls="mb-16",
        # ),
        # =====================================================================
        # BLOCK 6: TOP PERFORMERS COMPARISON
        # =====================================================================
        # Div(
        #     H3("🏆 Top Performers", cls="text-2xl font-semibold text-gray-800 mb-4"),
        #     P(
        #         "Compare your best videos across all key metrics.",
        #         cls="text-gray-500 mb-6",
        #     ),
        #     Grid(
        #         chart_top_performers_radar(df, "top-radar", top_n=5),
        #         cls="grid-cols-1 gap-10",
        #     ),
        #     cls="mb-16",
        # ),
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

    # reuse centralized style
    base = STYLES.get("btn_full")
    cls = f"{width_class} {base}"
    return Button(
        Span(UkIcon(icon, cls="mr-2"), text), type=button_type, cls=cls, **kwargs
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
        missing = total_videos - processed_videos
        video_info = f"{processed_videos} of {total_videos} analyzed"
        # Show why some are missing
        missing_note = f"({missing} unavailable - deleted, private, or restricted)"
    else:
        missing_note = None

    eng_display = None
    if engagement_rate is not None:
        # Normalize to 0-100 if needed
        eng_val = engagement_rate * 100 if engagement_rate <= 1 else engagement_rate
        eng_display = f"{eng_val:.1f}"

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
                        # Missing note (if any)
                        (
                            P(
                                missing_note,
                                cls="text-xs text-orange-600 mt-1 italic",
                            )
                            if missing_note
                            else None
                        ),
                        # Quick stats row
                        (
                            Div(
                                # Views badge
                                (
                                    small_badge(format_number(total_views), icon="eye")
                                    if total_views
                                    else None
                                ),
                                # Engagement badge
                                (
                                    small_badge(
                                        f"{eng_display}% engagement",
                                        icon="heart",
                                        kind="info",
                                    )
                                    if engagement_rate
                                    else None
                                ),
                                # Date badge
                                (
                                    small_badge(
                                        f"Analyzed {processed_date}", icon="calendar"
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
            # Icon with explicit color
            UkIcon(icon, cls=f"text-{color}-500", height=28, width=28),
            # Main value
            H3(value, cls="text-2xl font-bold text-gray-900 mb-1"),
            # Subtitle
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


# ----------------------------------------------------------------------
# Helper: safe numeric aggregation
# ----------------------------------------------------------------------
def _safe_agg(df: pl.DataFrame, col: str, agg: str) -> int | float:
    """
    Return 0 (or 0.0) if the column does not exist or the aggregation fails.
    `agg` can be "sum", "mean", "max", "min".
    """
    if col not in df.columns:
        return 0
    try:
        expr = getattr(pl.col(col), agg)()
        return df.select(expr).item()
    except Exception:
        return 0


# ----------------------------------------------------------------------
# Robust PlaylistMetricsOverview
# ----------------------------------------------------------------------
def PlaylistMetricsOverview(df: pl.DataFrame, summary: Dict) -> Div:
    """
    Four (or six) instantly-useful metric cards.
    Works with the exact schema you posted **and** with the enriched
    columns that `youtube_transforms._enrich_dataframe` adds.
    """

    # ------------------------------------------------------------------
    # a) Basic counts from the summary dict (always present)
    # ------------------------------------------------------------------
    total_videos = summary.get("actual_playlist_count", 0) or df.height
    processed_videos = summary.get("processed_video_count", 0) or df.height
    total_views = summary.get("total_views", 0)

    # ------------------------------------------------------------------
    # b) Pull enriched stats if they exist – otherwise calculate on-the-fly
    # ------------------------------------------------------------------
    avg_engagement = summary.get("avg_engagement", 0.0)
    avg_controversy = summary.get("avg_controversy", 0.0)

    # If the enriched columns are missing, compute them ourselves
    if "Engagement Rate Raw" not in df.columns and total_videos:
        # (likes + dislikes + comments) / views
        likes = _safe_agg(df, "Likes", "sum")
        comments = _safe_agg(df, "Comments", "sum")
        views = _safe_agg(df, "Views", "sum") or 1
        avg_engagement = (likes + comments) / views

    if "Controversy" not in df.columns and total_videos:
        likes = _safe_agg(df, "Likes", "sum")
        dislikes = _safe_agg(df, "Dislikes", "sum")
        total = likes + dislikes or 1
        avg_controversy = 1 - abs(likes - dislikes) / total

    # ------------------------------------------------------------------
    # c) Top-performer & average view count
    # ------------------------------------------------------------------
    top_views = _safe_agg(df, "Views", "max")
    avg_views = total_views / total_videos if total_videos else 0

    # ------------------------------------------------------------------
    # d) Bonus signals (HD, captions) – only if the columns exist
    # ------------------------------------------------------------------
    hd_ratio = 0.0
    caption_ratio = 0.0
    if "Definition" in df.columns and total_videos:
        hd = df.filter(pl.col("Definition") == "hd").height
        hd_ratio = hd / total_videos
    if "Caption" in df.columns and total_videos:
        caps = df.filter(pl.col("Caption") is True).height
        caption_ratio = caps / total_videos

    # ------------------------------------------------------------------
    # e) Build the four (or six) cards
    # ------------------------------------------------------------------
    cards = [
        MetricCard(
            title="Total Reach",
            value=format_number(total_views),
            subtitle=f"Across {total_videos:,} videos",
            icon="eye",
            color="blue",
        ),
        MetricCard(
            title="Engagement Rate",
            value=f"{avg_engagement:.1%}",
            subtitle="Likes + comments ÷ views",
            icon="heart",
            color="red",
        ),
        MetricCard(
            title="Top Performer",
            value=format_number(top_views),
            subtitle="Most-viewed video",
            icon="trending-up",
            color="green",
        ),
        MetricCard(
            title="Avg. Views per Video",
            value=format_number(int(avg_views)),
            subtitle="Playlist-wide average",
            icon="bar-chart",
            color="purple",
        ),
    ]

    # Optional extra cards – they disappear automatically if data is missing
    if hd_ratio > 0:
        cards.append(
            MetricCard(
                title="HD Content",
                value=f"{hd_ratio:.0%}",
                subtitle="Videos in 720p+",
                icon="film",
                color="indigo",
            )
        )
    if caption_ratio > 0:
        cards.append(
            MetricCard(
                title="Captioned",
                value=f"{caption_ratio:.0%}",
                subtitle="Videos with subtitles",
                icon="closed-caption",
                color="orange",
            )
        )

    # ------------------------------------------------------------------
    # f) Layout
    # ------------------------------------------------------------------
    return Div(
        # Section header
        Div(
            H2("Key Metrics", cls="text-xl font-semibold text-gray-800 mb-2"),
            P(
                "At-a-glance performance of your playlist",
                cls="text-gray-600 text-sm mb-6",
            ),
            cls="text-center",
        ),
        # Responsive grid
        Grid(
            *cards,
            cols_sm=2,  # 2 columns on phones
            cols_md=3,  # 3 columns on tablets
            cols_lg=len(cards),  # full row on desktop (4-6 cards)
            gap=4,
            cls="mb-8",
        ),
        cls="mb-12",
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
        styled_div(
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


def ExploreGridSection() -> Div:
    """Use MonsterUI Grid + Card to present Explore cards in 1→3 columns responsively."""
    cards = [FeaturesCard(), BenefitsCard(), NewsletterCard()]
    return Section(
        Container(
            H2("Explore ViralVibes", cls="text-3xl font-bold text-center mb-8"),
            Grid(
                *[Card(c, cls="h-full p-6") for c in cards],
                cols_md=3,
                gap=6,
                cls="max-w-7xl mx-auto",
            ),
            cls="px-4 sm:px-6 md:px-8",
        ),
        cls="py-12",
    )


def SectionDivider() -> Div:
    """Thin gradient divider that creates breathing room and rhythm (Tailwind-only)."""
    return Div(
        cls="w-full h-1 rounded-full bg-gradient-to-r from-[#00A3FF] via-[#FF4500] to-[#00A3FF] my-4 shadow-sm"
    )


def thumbnail_cell(url, vid, title=None):
    # clickable thumbnail that opens youtube in new tab
    if not url:
        # small placeholder
        return Div(
            A(
                Div("No thumbnail", cls="text-xs text-gray-400"),
                href=f"https://youtube.com/watch?v={vid}" if vid else "#",
                target="_blank",
                cls="inline-block px-2 py-1 bg-gray-50 rounded",
            ),
            cls="text-center",
        )
    return A(
        Img(
            src=url,
            cls="h-14 w-28 object-cover rounded-lg shadow-sm hover:opacity-90 transition",
        ),
        href=f"https://youtube.com/watch?v={vid}",
        target="_blank",
        title=title or "",
        cls="inline-block",
    )


def title_cell(row):
    title = row.get("Title", "Untitled")
    vid = row.get("id", "")
    uploader = row.get("Uploader", "")
    tags = row.get("Tags") or []
    tag_nodes = []
    for t in tags[:2] if isinstance(tags, (list, tuple)) else []:
        # ✅ Use Span with Tailwind classes instead of Badge
        tag_nodes.append(
            Span(
                t,
                cls="inline-block px-2 py-1 mr-1 text-xs bg-blue-100 text-blue-700 rounded-full font-medium",
            )
        )
    meta = Div(
        Div(
            A(
                title,
                href=f"https://youtube.com/watch?v={vid}",
                target="_blank",
                cls="text-blue-700 hover:underline font-semibold",
            ),
            cls="truncate max-w-md",
        ),
        Div(*tag_nodes, cls="mt-1 flex flex-wrap gap-1"),  # Added flex-wrap
        cls="space-y-1",
    )
    # uploader avatar / name small inline
    uploader_part = Div(
        # Avatar(src=row.get("Thumbnail") or "", size="xs", cls="mr-2"),
        DiceBearAvatar(uploader, h=20, w=20),
        A(uploader or "Unknown", href="#", cls="text-sm text-gray-600"),
        cls="flex items-center mt-1 space-x-2",
    )
    return Div(meta, uploader_part, cls="flex flex-col")


def number_cell(val):
    # Handle None and ensure it's numeric
    if val is None:
        return Div("", cls="text-right font-medium")
    try:
        numeric_val = float(val) if isinstance(val, str) else val
        return Div(format_number(numeric_val), cls="text-right font-medium")
    except (ValueError, TypeError):
        return Div(str(val), cls="text-right font-medium")


def VideoExtremesSection(df: pl.DataFrame) -> Div:
    """
    Display 4 card extremes: most/least viewed, longest/shortest videos.
    Optimized: uses arg_max/arg_min instead of sorting 4 times.
    """
    if df is None or df.is_empty():
        return Div(P("No videos found in playlist.", cls="text-gray-500"))

    try:
        # Compute extremes using Polars (safe with error handling)
        # ✅ EFFICIENT: Get indices directly without sorting
        most_viewed_idx = df.select(pl.col("Views").arg_max()).item()
        least_viewed_idx = df.select(pl.col("Views").arg_min()).item()
        longest_idx = df.select(pl.col("Duration").arg_max()).item()
        shortest_idx = df.select(pl.col("Duration").arg_min()).item()

        # Fetch rows by index
        most_viewed = df.row(most_viewed_idx, named=True)
        least_viewed = df.row(least_viewed_idx, named=True)
        longest = df.row(longest_idx, named=True)
        shortest = df.row(shortest_idx, named=True)

        extremes = [
            (
                "trending-up",
                "Most Viewed",
                most_viewed,
                f"{format_number(most_viewed.get('Views', 0))} views",
            ),
            (
                "trending-down",
                "Least Viewed",
                least_viewed,
                f"{format_number(least_viewed.get('Views', 0))} views",
            ),
            (
                "clock",
                "Longest Video",
                longest,
                f"{format_duration(longest.get('Duration', 0))}",
            ),
            (
                "zap",
                "Shortest Video",
                shortest,
                f"{format_duration(shortest.get('Duration', 0))}",
            ),
        ]

        cards = []
        for icon_name, title, row, metric in extremes:
            video_id = row.get("id") or row.get("ID") or ""
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            thumbnail = (
                row.get("Thumbnail") or row.get("thumbnail") or "/static/favicon.jpeg"
            )

            cards.append(
                Card(
                    Div(
                        UkIcon(
                            icon_name,
                            height=28,
                            width=28,
                            cls="text-red-600 flex-shrink-0",
                        ),
                        H4(title, cls="font-semibold text-gray-900"),
                        cls="flex items-center gap-3 mb-4",
                    ),
                    Img(
                        src=thumbnail,
                        alt=row.get("Title", "Video"),
                        cls="w-full h-40 object-cover rounded-lg shadow-sm",
                        loading="lazy",
                        onerror="this.src='/static/favicon.jpeg'",
                    ),
                    Div(
                        P(
                            row.get("Title", "Untitled"),
                            cls="font-medium line-clamp-2 mt-3 text-gray-800",
                        ),
                        P(metric, cls="text-sm text-red-600 font-semibold mt-1"),
                        P(
                            row.get("Uploader", "Unknown"),
                            cls="text-xs text-gray-500 mt-1",
                        ),
                        cls="px-1",
                    ),
                    footer=Div(
                        A(
                            Button(
                                UkIcon("play", width=16, height=16, cls="mr-1"),
                                "Watch",
                                href=video_url,
                                target="_blank",
                                rel="noopener noreferrer",
                                cls=(ButtonT.ghost, "w-full text-center"),
                            ),
                            href=video_url,
                            target="_blank",
                            rel="noopener noreferrer",
                            cls="no-underline",
                        ),
                        cls="mt-4",
                    ),
                    cls="shadow-lg hover:shadow-xl hover:-translate-y-1 transition-all duration-300 bg-white",
                )
            )

        return Div(
            Div(
                H2("🎯 Video Extremes", cls="text-2xl font-bold text-gray-900"),
                P(
                    "Identify your viral peaks and quiet spots",
                    cls="text-gray-600 text-sm mt-1",
                ),
                cls="mb-8",
            ),
            Grid(
                *cards,
                cols="1 sm:2 lg:4",
                gap="4 md:6",
                cls="w-full",
            ),
            cls="mt-12 pb-12 border-b border-gray-200",
        )

    except Exception as e:
        logger.exception(f"Error rendering VideoExtremesSection: {e}")
        return Div(
            P("Unable to load video extremes.", cls="text-gray-500"),
            cls="mt-8 p-4 bg-gray-50 rounded-lg",
        )
