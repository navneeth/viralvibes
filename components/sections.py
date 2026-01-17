# routes/homepage.py
from fasthtml.common import *
from monsterui.all import *

from components.base import DivFullySpaced, DivHStacked, maxpx, maxrem, styled_div
from components.cards import accordion, benefit
from constants import (
    FLEX_BETWEEN,
    FLEX_CENTER,
    FLEX_COL,
    GAP_2,
    ICONS_PATH,
    SECTION_BASE,
    THEME,
    faqs,
    testimonials,
)


# =============================================================================
# Section Helpers
# =============================================================================
def section_wrapper(content, bg_color, xtra="", flex=True) -> Section:
    """Wraps a section with background color, layout, and rounded corners."""
    return Section(
        content,
        cls=f"bg-{bg_color} {SECTION_BASE} {FLEX_COL if flex else ''} -mt-8 lg:-mt-16 items-center rounded-t-3xl lg:rounded-t-[2.5rem] relative {xtra}",
    )


def section_header(mono_text, heading, subheading, max_width=32, center=True) -> Div:
    """Reusable section header with mono label."""
    pos = "items-center text-center" if center else "items-start text-start"
    return Div(
        P(mono_text, cls="mono-body text-opacity-60"),
        H2(heading, cls=f"text-white heading-2 {maxrem(max_width)}"),
        P(subheading, cls=f"l-body {maxrem(max_width)}"),
        cls=f"{maxrem(50)} mx-auto {FLEX_COL} {pos} gap-6",
    )


# =============================================================================
# Main Sections
# =============================================================================


def hero_section() -> Section:
    """
    Analytics Dashboard Hero - Two Column Layout
    Left: Text + CTA
    Right: Live analytics dashboard mockup with metrics

    Uses: Tailwind animations, custom dashboard cards, no external images needed

    - Gradient overlay for text readability
    - Better typography hierarchy
    - Glassmorphism effect for CTA
    """
    # Chart bar heights (0-100 representing engagement)
    ENGAGEMENT_HEIGHTS = [35, 50, 42, 65, 78, 72, 88, 95]

    return Section(
        # Subtle animated background glow elements
        styled_div(
            Div(
                cls="absolute top-20 left-10 w-72 h-72 bg-red-500 rounded-full mix-blend-multiply filter blur-3xl opacity-10"
            ),
            Div(
                cls="absolute top-40 right-10 w-72 h-72 bg-blue-500 rounded-full mix-blend-multiply filter blur-3xl opacity-10"
            ),
            cls="absolute inset-0 opacity-20",
        ),
        # Main Content Container
        Div(
            # LEFT COLUMN: Text Content
            Div(
                # Headline with gradient
                styled_div(
                    H1(
                        "Decode YouTube Virality. ",
                        cls="text-5xl md:text-6xl lg:text-7xl font-bold leading-tight text-white mb-3",
                    ),
                    H1(
                        "Instantly.",
                        cls="text-5xl md:text-6xl lg:text-7xl font-bold leading-tight bg-gradient-to-r from-red-500 to-pink-500 bg-clip-text text-transparent",
                    ),
                    cls="space-y-0 animate-in fade-in slide-in-from-top-8 duration-700",
                ),
                # Subheading
                P(
                    "See what's trending before it peaks. Analyze playlists, spot viral patterns, "
                    "and understand your audience with real-time YouTube data.",
                    cls="text-lg md:text-xl text-gray-300 max-w-2xl leading-relaxed mt-8 animate-in fade-in slide-in-from-top-10 duration-700 delay-100",
                ),
                # CTA Buttons
                Div(
                    A(
                        Span(
                            "🚀 Start Analyzing", cls="inline-flex items-center gap-2"
                        ),
                        href="#analysis-form",
                        cls=(
                            "px-8 py-4 md:px-10 md:py-5 "
                            "bg-red-600 text-white font-bold text-lg rounded-full "
                            "hover:bg-red-700 hover:shadow-2xl hover:shadow-red-500/50 "
                            "transition-all duration-300 transform hover:scale-105 "
                            "inline-block"
                        ),
                    ),
                    A(
                        Span("📚 Learn More", cls="inline-flex items-center gap-2"),
                        href="#faq-section",
                        onclick=(
                            "document.getElementById('faq-section')"
                            ".scrollIntoView({behavior:'smooth'}); return false;"
                        ),
                        cls=(
                            "px-8 py-4 md:px-10 md:py-5 "
                            "border-2 border-gray-500 text-gray-300 font-semibold rounded-full "
                            "hover:border-white hover:text-white hover:bg-white/5 "
                            "transition-all duration-300 "
                            "inline-block"
                        ),
                    ),
                    cls="flex flex-col sm:flex-row gap-4 mt-10 animate-in fade-in slide-in-from-top-12 duration-700 delay-200",
                ),
                # Trust Badge
                Div(
                    Div(
                        Div(
                            cls="w-8 h-8 rounded-full bg-gradient-to-br from-red-500 to-pink-500 -mr-2"
                        ),
                        Div(
                            cls="w-8 h-8 rounded-full bg-gradient-to-br from-blue-500 to-cyan-500 -mr-2"
                        ),
                        Div(
                            cls="w-8 h-8 rounded-full bg-gradient-to-br from-green-500 to-emerald-500"
                        ),
                        cls="flex",
                    ),
                    Span(
                        "Join 500+ creators analyzing playlists",
                        cls="text-gray-400 text-sm",
                    ),
                    cls="flex items-center gap-3 mt-10 text-gray-400 text-sm animate-in fade-in delay-300",
                ),
                cls=f"{FLEX_COL} flex-1 space-y-6",
            ),
            # RIGHT COLUMN: Analytics Dashboard Mockup
            Div(
                # Main Dashboard Card
                Div(
                    # Header with Live Indicator
                    Div(
                        H3("Viral Analysis", cls="text-white font-bold text-lg"),
                        Div(cls="w-3 h-3 bg-red-500 rounded-full animate-pulse"),
                        cls="flex items-center justify-between mb-6",
                    ),
                    # Top Stats Grid (3 columns)
                    Div(
                        # Viral Score Card
                        Div(
                            P(
                                "Viral Score",
                                cls="text-gray-400 text-xs font-medium mb-2",
                            ),
                            P("87%", cls="text-red-500 text-3xl font-bold"),
                            P(
                                "↑ +12%",
                                cls="text-green-500 text-xs font-semibold mt-2",
                            ),
                            cls="bg-gray-900/50 border border-gray-700 rounded-xl p-4 hover:border-red-500/50 transition-colors",
                        ),
                        # Views Card
                        Div(
                            P("Views", cls="text-gray-400 text-xs font-medium mb-2"),
                            P("2.4M", cls="text-blue-500 text-3xl font-bold"),
                            P("↑ +8%", cls="text-green-500 text-xs font-semibold mt-2"),
                            cls="bg-gray-900/50 border border-gray-700 rounded-xl p-4 hover:border-blue-500/50 transition-colors",
                        ),
                        # Engagement Card
                        Div(
                            P(
                                "Engagement",
                                cls="text-gray-400 text-xs font-medium mb-2",
                            ),
                            P("12.3%", cls="text-green-500 text-3xl font-bold"),
                            P("↑ +4%", cls="text-green-500 text-xs font-semibold mt-2"),
                            cls="bg-gray-900/50 border border-gray-700 rounded-xl p-4 hover:border-green-500/50 transition-colors",
                        ),
                        cls="grid grid-cols-3 gap-3 mb-8",
                    ),
                    # Chart Section
                    Div(
                        # Chart Header
                        Div(
                            H4(
                                "Engagement Trend",
                                cls="text-gray-300 font-semibold text-sm",
                            ),
                            Div(
                                Span("📈", cls="mr-1"),
                                Span("+23%", cls="text-green-500 text-xs font-bold"),
                                cls="flex items-center gap-1",
                            ),
                            cls="flex items-center justify-between mb-4",
                        ),
                        # Animated Bar Chart (8 bars representing 8 days)
                        Div(
                            *[
                                Div(
                                    cls=f"flex-1 rounded-t-lg transition-all duration-500 ease-out animate-in slide-in-from-bottom-0 fill-mode-both",
                                    style=f"animation-delay: {idx * 50}ms; "
                                    f"height: {h}%; "
                                    f"background: linear-gradient(to top, "
                                    f"{'rgb(239, 68, 68)' if idx > 5 else 'rgb(248, 113, 113)' if idx > 3 else 'rgb(254, 165, 165)'}, "
                                    f"{'rgb(220, 38, 38)' if idx > 5 else 'rgb(239, 68, 68)' if idx > 3 else 'rgb(248, 113, 113)'})",
                                )
                                for idx, h in enumerate(ENGAGEMENT_HEIGHTS)
                            ],
                            cls="flex items-end justify-between h-32 gap-2",
                            id="engagement-chart",
                        ),
                        cls="bg-gray-900/50 border border-gray-700 rounded-xl p-5 mb-6",
                    ),
                    # Video Stats
                    Div(
                        # Top Performer
                        Div(
                            Div(
                                Span("❤️", cls="mr-2"),
                                Span(
                                    "Top Performer",
                                    cls="text-gray-300 text-sm font-medium",
                                ),
                                cls="flex items-center",
                            ),
                            Span("4.2K likes", cls="text-white font-bold"),
                            cls="flex items-center justify-between bg-gray-900/50 border border-gray-700 rounded-lg p-3",
                        ),
                        # Comments
                        Div(
                            Div(
                                Span("💬", cls="mr-2"),
                                Span(
                                    "Comments", cls="text-gray-300 text-sm font-medium"
                                ),
                                cls="flex items-center",
                            ),
                            Span("847", cls="text-white font-bold"),
                            cls="flex items-center justify-between bg-gray-900/50 border border-gray-700 rounded-lg p-3 mt-2",
                        ),
                        cls="space-y-2",
                    ),
                    # Footer Action Button
                    Button(
                        Span(
                            "▶️ View Full Analysis",
                            cls="flex items-center gap-2 justify-center",
                        ),
                        cls=(
                            "w-full mt-6 py-3 "
                            "bg-red-600/20 border border-red-600/50 text-red-500 "
                            "font-semibold rounded-lg "
                            "hover:bg-red-600/30 transition-colors"
                        ),
                        type="button",
                    ),
                    cls=(
                        "bg-gradient-to-br from-gray-800 to-gray-900 "
                        "border border-gray-700 rounded-2xl p-6 md:p-8 "
                        "shadow-2xl transform hover:scale-105 transition-transform duration-300 "
                        "animate-in fade-in slide-in-from-right-8 duration-700 delay-200"
                    ),
                ),
                # Floating "LIVE" Badge
                Div(
                    "🔥 LIVE",
                    aria_label="Live analytics preview",
                    cls=(
                        "absolute -top-4 -right-4 "
                        "bg-gradient-to-br from-red-600 to-pink-600 "
                        "text-white px-4 py-2 rounded-full font-bold shadow-lg text-sm"
                    ),
                ),
                cls="flex-1 flex items-center justify-center w-full relative max-w-md",
            ),
            cls="relative z-10 max-w-7xl w-full mx-auto flex flex-col md:flex-row gap-12 md:gap-16 items-center justify-between px-4 md:px-8",
        ),
        # Scroll Indicator
        Div(
            P("Scroll to explore", cls="text-gray-400 text-sm mb-2 text-center"),
            Div(
                Div(cls="w-1 h-2 bg-gray-600 rounded-full animate-pulse"),
                cls="w-6 h-10 border-2 border-gray-600 rounded-full flex items-start justify-center p-2 mx-auto",
            ),
            cls="absolute bottom-8 left-1/2 transform -translate-x-1/2 animate-bounce",
        ),
        cls=(
            "relative min-h-screen flex items-center justify-center "
            "bg-gradient-to-br from-gray-900 via-black to-gray-900 "
            "overflow-hidden py-20"
        ),
        id="hero-section",
    )


def how_it_works_section() -> Section:
    """Step-by-step workflow section"""
    steps_msg = [
        (
            "1️⃣ Submit Playlist URL",
            "Paste your YouTube playlist link into the analysis form.",
        ),
        (
            "2️⃣ Preview",
            "See the playlist title, channel name, and thumbnail instantly.",
        ),
        (
            "3️⃣ Deep Analysis",
            "We crunch video stats—views, likes, dislikes, comments, engagement, and controversy.",
        ),
        (
            "4️⃣  Results Dashboard",
            "Get a detailed table and dashboard with trends and viral signals.",
        ),
    ]
    return section_wrapper(
        (
            Div(
                section_header(
                    "HOW IT WORKS",
                    "Analyze any YouTube playlist in seconds.",
                    "ViralVibes guides you through a simple, step-by-step workflow to decode YouTube trends and performance.",
                ),
                cls="max-w-3xl w-full mx-auto flex-col items-center text-center gap-6 mb-8 lg:mb-8",
            ),
            Div(
                *[benefit(title, content) for title, content in steps_msg],
                cls=f"{FLEX_COL} w-full lg:flex-row gap-4 items-center lg:gap-8 max-w-7xl mx-auto justify-center",
            ),
        ),
        bg_color="red-700",
        flex=False,
    )


def features_section():
    """Modern features grid using MonsterUI Card patterns."""
    feature_items = [
        ("bolt", "Real-Time Analytics", "Live data processing—no waiting for reports."),
        (
            "chart-bar",
            "Viral Pattern Detection",
            "Spot engagement spikes and controversy signals.",
        ),
        ("users", "Creator Insights", "Understand audience behavior beyond raw views."),
        ("zap", "Instant Results", "Cached analysis for lightning-fast reloads."),
        ("lock", "Privacy First", "No data stored—your playlists stay private."),
        ("download", "CSV/JSON Exports", "Download full data for offline analysis."),
    ]

    cards = [
        Card(
            Div(
                UkIcon(icon, cls="w-10 h-10 text-red-600 mb-4"),
                H4(title, cls="text-lg font-semibold text-gray-900 mb-2"),
                P(desc, cls="text-sm text-gray-600"),
                cls="flex flex-col items-center text-center h-full",
            ),
            cls=(CardT.hover, "p-6 transition-all duration-300"),
        )
        for icon, title, desc in feature_items
    ]

    return Section(
        Container(
            H2("Key Features", cls="text-3xl font-bold text-center mb-4"),
            P(
                "Everything you need to decode YouTube virality",
                cls="text-center text-gray-600 mb-12",
            ),
            Grid(
                *cards,
                cols="1 md:2 lg:3",
                gap=8,
                cls="max-w-7xl mx-auto",
            ),
        ),
        cls="py-16 bg-gray-50",
        id="features-section",
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
            cls=f"{SECTION_BASE} {maxrem(90)} mx-auto lg:flex-row items-start",
        ),
        bg_color="red-100",
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
                        src=f"{ICONS_PATH}/dot.svg",
                        alt="Dot separator",
                        width="4",
                        height="4",
                    ),
                    P(company),
                    cls=f"{GAP_2} xs-mono-body w-full",
                ),
                cls="w-full",
            ),
            cls=f"{FLEX_CENTER} justify-start gap-2",
        ),
        id=f"testimonial-card-{idx + 1}",
        cls=f"testimonial-card {FLEX_COL} flex-none whitespace-normal flex justify-between h-96 rounded-3xl items-start bg-soft-pink p-4 lg:p-8 {maxrem(36)} lg:w-96",
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
                cls=f"{FLEX_COL} gap-4 {maxrem(32)} transition ease-out delay-[300ms]",
            ),
            cls=f"{SECTION_BASE} w-full mx-auto lg:flex-row items-start max-w-7xl",
        ),
        bg_color="red-700",
        flex=False,
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


def arrow(d):
    return Button(
        Img(src=f"{ICONS_PATH}/arrow-{d}.svg", alt="Arrow left"),
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
