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


def section_wrapper(content, bg_color, xtra="", flex=True):
    """Wraps a section with background color, layout, and rounded corners."""
    return Section(
        content,
        cls=f"bg-{bg_color} {SECTION_BASE} {FLEX_COL if flex else ''} -mt-8 lg:-mt-16 items-center rounded-t-3xl lg:rounded-t-[2.5rem] relative {xtra}",
    )


def section_header(mono_text, heading, subheading, max_width=32, center=True):
    pos = "items-center text-center" if center else "items-start text-start"
    return Div(
        P(mono_text, cls="mono-body text-opacity-60"),
        H2(heading, cls=f"text-white heading-2 {maxrem(max_width)}"),
        P(subheading, cls=f"l-body {maxrem(max_width)}"),
        cls=f"{maxrem(50)} mx-auto {FLEX_COL} {pos} gap-6",
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


def how_it_works_section():
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
