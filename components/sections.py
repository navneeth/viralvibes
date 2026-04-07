# components/sections.py
"""
Reusable section components for ViralVibes homepage and pages.

Contains:
- Section wrappers (background + layout)
- Hero section (with animated background)
- Engagement slider (carousel)
- Feature sections
- How-it-works steps
- FAQ section
- Footer
"""

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
    SOCIALS,
)


# =============================================================================
# Section Helpers
# =============================================================================
def section_wrapper(content, bg_color, xtra="", flex=True) -> Section:
    """Wraps a section with background color, layout, and rounded corners.

    Args:
        content: Section content
        bg_color: Tailwind bg color (e.g., 'bg-red-500')
        xtra: Extra CSS classes
        flex: Whether to use flex layout

    Returns:
        Section element with styled wrapper
    """
    return Section(
        content,
        cls=f"bg-{bg_color} {SECTION_BASE} {FLEX_COL if flex else ''} -mt-8 lg:-mt-16 items-center rounded-t-3xl lg:rounded-t-[2.5rem] relative {xtra}",
    )


def section_header(mono_text, heading, subheading, max_width=32, center=True) -> Div:
    """Reusable section header with mono label.

    Args:
        mono_text: Small label text (e.g., "SECTION 01")
        heading: Main H2 heading
        subheading: Description text
        max_width: Max width for container (in rem)
        center: Whether to center text

    Returns:
        Div with styled header
    """
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
    Full-screen hero wrapping the vv-header-card design system.

    Uses existing vv-* CSS classes and HeaderCard() sub-components so
    there is zero duplication between this section and the card library.
    The right column is a 3-tab product switcher (Lists / Creator / Analyze)
    powered by UIkit's switcher and a small auto-advance script.
    """
    from components.cards import HeaderCard  # local import avoids circular dep

    return Section(
        HeaderCard(),
        cls="relative w-full",
        id="hero-section",
    )


def _hero_section_unused_placeholder() -> None:  # kept for diff readability
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
                        Span("🚀 Start Analyzing", cls="inline-flex items-center gap-2"),
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
                                Span("Comments", cls="text-gray-300 text-sm font-medium"),
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
        id="_hero_section_old",  # orphaned — no longer rendered
    )


def how_it_works_section() -> Section:
    """Brand discovery workflow — 4 steps from research to campaign launch."""
    steps_msg = [
        (
            "1. Browse creator lists",
            "Filter 1M+ ranked creators by niche, country, and engagement rate. "
            "What used to take a researcher a week takes minutes.",
        ),
        (
            "2. Review creator profiles",
            "See growth velocity, engagement history, and ROAS indicators for any creator "
            "— without needing channel access or their permission.",
        ),
        (
            "3. Analyze their content",
            "Paste any public playlist URL for instant analysis — views, engagement patterns, "
            "controversy signals, and viral scoring in under 2 seconds.",
        ),
        (
            "4. Build your shortlist",
            "Export ranked creator data as CSV for campaign planning. "
            "Share with clients. Skip the spreadsheet hell.",
        ),
    ]
    return section_wrapper(
        (
            Div(
                section_header(
                    "HOW IT WORKS",
                    "From research to shortlist in minutes.",
                    "Stop spending hours on manual creator research. ViralVibes gives you "
                    "the data to find, evaluate, and shortlist creators fast.",
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
    """Six-card reasons grid — brand/ROAS focused."""
    feature_items = [
        (
            "list",
            "Curated creator lists",
            "Pre-ranked creators by niche and country, filtered by engagement quality — not subscriber count.",
        ),
        (
            "bar-chart-2",
            "Real engagement signals",
            "View-to-like ratios, comment density, and consistency across recent uploads. Spot the real audience.",
        ),
        (
            "trending-up",
            "Growth velocity",
            "Who is rising now vs. plateauing. Reach out before their rates double — or before they disappear.",
        ),
        (
            "dollar-sign",
            "ROAS-correlated metrics",
            "Engagement patterns that correlate with purchase intent, not just impressions. Filter out the noise.",
        ),
        (
            "globe",
            "Any public channel",
            "No channel ownership required. Analyze any creator's public data across 150+ countries without their permission.",
        ),
        (
            "download",
            "Export for your workflow",
            "CSV and JSON exports for agency reporting, CRM imports, and campaign planning. No spreadsheet hell.",
        ),
    ]

    cards = [
        Card(
            Div(
                UkIcon(icon, cls="w-10 h-10 text-red-600 mb-4"),
                H4(title, cls="text-lg font-semibold text-foreground mb-2"),
                P(desc, cls="text-sm text-muted-foreground leading-relaxed"),
                cls="flex flex-col items-center text-center h-full",
            ),
            cls=(CardT.hover, "p-6 transition-all duration-300"),
        )
        for icon, title, desc in feature_items
    ]

    return Section(
        Container(
            P(
                "WHY BRANDS CHOOSE VIRALVIBES",
                cls="text-sm font-semibold text-red-600 uppercase tracking-wider text-center mb-4",
            ),
            H2(
                "Built for campaigns that have to perform",
                cls="text-4xl font-bold text-center text-foreground mb-4",
            ),
            P(
                "Not another dashboard full of vanity metrics. Six tools that move the needle on ROAS.",
                cls="text-center text-muted-foreground mb-12 max-w-2xl mx-auto",
            ),
            Grid(
                *cards,
                cols_sm=2,
                cols_lg=3,
                gap=6,
                cls="max-w-7xl mx-auto",
            ),
        ),
        cls="py-16 bg-muted",
        id="features-section",
    )


def testimonials_section():
    testimonial_cards = [testimonial_card(i, *args) for i, args in enumerate(testimonials)]
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
                *[faq_item(question, answer, i + 3) for i, (question, answer) in enumerate(faqs)],
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
        *[A(text, href=f"#{text.lower().replace(' ', '-')}", cls=TextT.muted) for text in links],
    )


def footer():
    product = ["Creators", "Lists", "Analyze", "Pricing"]
    company = ["About", "Blog", "Contact", "Press Kit"]
    legal = ["Terms of Service", "Privacy Policy", "Cookie Settings"]

    return Container(cls="uk-background-muted py-12")(
        styled_div(
            DivFullySpaced(
                Div(
                    H3("ViralVibes", cls="mb-1"),
                    P(
                        "Find YouTube creators who actually drive results.",
                        cls=TextT.muted + " text-sm max-w-xs",
                    ),
                ),
                DivHStacked(
                    A(
                        UkIcon("x", cls=TextT.lead),
                        href=SOCIALS["x"],
                        target="_blank",
                        rel="noopener noreferrer",
                        aria_label="ViralVibes on X (Twitter)",
                    ),
                    A(
                        UkIcon("linkedin", cls=TextT.lead),
                        href=SOCIALS["linkedin"],
                        target="_blank",
                        rel="noopener noreferrer",
                        aria_label="ViralVibes on LinkedIn",
                    ),
                ),
            ),
            DividerLine(),
            DivFullySpaced(
                FooterLinkGroup("Product", product),
                FooterLinkGroup("Company", company),
                FooterLinkGroup("Legal", legal),
            ),
            DividerLine(),
            P("© 2026 ViralVibes. All rights reserved.", cls=TextT.lead + TextT.sm),
            cls="space-y-8 p-8",
        )
    )


def BottomCTASection() -> Section:
    """
    Dark full-bleed closing CTA — Clay.com inspired.
    Left: headline + dual CTAs + trust line.
    Right: app window placeholder (replace with a 680×440 screenshot of
           the /creators ranked list page — dark-themed, showing creator
           rows with engagement bars; ideally a browser-framed PNG/WebP).

    Best image: A cropped screenshot of the ViralVibes /creators or /lists
    page showing 5-6 creator rows with rank numbers, engagement bars, and
    category badges. Crop to ~680×440px, save as
    /static/cta-preview.webp. The dark panel will frame it automatically.
    """
    # ── Placeholder image slot ──────────────────────────────────────────
    # Replace Div below with: Img(src="/static/cta-preview.webp", ...)
    # when real screenshot is ready.
    placeholder = Div(
        Div(
            UkIcon("monitor", cls="w-10 h-10 text-white/20 mb-3"),
            P(
                "App screenshot goes here",
                cls="text-white/30 text-sm font-medium text-center",
            ),
            P(
                "680 × 440 px · /creators list view · dark theme",
                cls="text-white/15 text-xs text-center font-mono mt-1",
            ),
            cls="flex flex-col items-center justify-center h-full",
        ),
        cls="w-full h-full min-h-[220px] border-2 border-dashed border-white/10 rounded-xl",
    )

    # ── Fake browser chrome wrapping the placeholder ────────────────────
    app_window = Div(
        # Traffic lights + URL bar
        Div(
            Div(
                Span(style="width:10px;height:10px;border-radius:50%;background:#ff5f57;"),
                Span(style="width:10px;height:10px;border-radius:50%;background:#febc2e;"),
                Span(style="width:10px;height:10px;border-radius:50%;background:#28c840;"),
                style="display:flex;gap:6px;align-items:center;",
            ),
            Div(
                "viralvibes.app/creators",
                style=(
                    "flex:1;text-align:center;"
                    "font-family:'Geist Mono',monospace;"
                    "font-size:0.65rem;color:rgba(255,255,255,0.25);"
                ),
            ),
            style=(
                "display:flex;align-items:center;gap:0.5rem;"
                "padding:0.6rem 1rem;"
                "background:rgba(255,255,255,0.04);"
                "border-bottom:1px solid rgba(255,255,255,0.07);"
                "border-radius:0.9rem 0.9rem 0 0;"
            ),
        ),
        # Image / placeholder
        Div(
            placeholder,
            style="padding:1rem;",
        ),
        style=(
            "background:rgba(255,255,255,0.03);"
            "border:1px solid rgba(255,255,255,0.08);"
            "border-radius:0.9rem;"
            "overflow:hidden;"
            "flex:1;"
            "min-width:280px;"
        ),
    )

    return Section(
        # Decorative gradient blob
        Div(
            style=(
                "position:absolute;top:-80px;right:-80px;"
                "width:480px;height:480px;"
                "background:radial-gradient(circle,rgba(239,68,68,0.12) 0%,transparent 70%);"
                "border-radius:50%;pointer-events:none;"
            )
        ),
        Div(
            # ── Left: copy + CTAs ──────────────────────────────────────
            Div(
                P(
                    "CLOSE MORE CAMPAIGNS",
                    cls="text-xs font-semibold text-red-500 uppercase tracking-widest mb-4",
                ),
                H2(
                    "Find the right creator before your competitor does.",
                    cls="text-4xl lg:text-5xl font-bold text-white leading-tight mb-6",
                ),
                P(
                    "Every day you spend on manual research is a day a better-fit creator "
                    "signs with someone else. ViralVibes gives you the shortlist in minutes.",
                    cls="text-gray-400 text-lg leading-relaxed mb-10 max-w-lg",
                ),
                # Dual CTA
                Div(
                    A(
                        UkIcon("users", cls="w-5 h-5"),
                        Span("Explore Creators"),
                        href="/creators",
                        cls="inline-flex items-center gap-2 px-6 py-3 rounded-lg bg-red-500 hover:bg-red-600 text-white font-semibold transition-colors",
                    ),
                    A(
                        UkIcon("play-circle", cls="w-5 h-5"),
                        Span("Analyze a Playlist"),
                        href="/analysis",
                        cls="inline-flex items-center gap-2 px-6 py-3 rounded-lg border border-white/20 text-white hover:bg-white/5 font-semibold transition-colors",
                    ),
                    cls="flex flex-wrap gap-4",
                ),
                # Trust line
                P(
                    UkIcon("check", cls="w-4 h-4 text-red-500"),
                    Span(
                        " No credit card required  ·  Free to start  ·  Updated daily",
                        cls="text-gray-500 text-sm",
                    ),
                    cls="flex items-center gap-1 mt-6",
                ),
                cls="flex flex-col justify-center",
            ),
            # ── Right: app window ───────────────────────────────────────
            app_window,
            cls="flex flex-col lg:flex-row gap-12 lg:gap-16 items-center max-w-7xl mx-auto px-6 lg:px-16 py-24",
        ),
        style="background:#0d0d0d;position:relative;overflow:hidden;",
        id="cta-section",
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


def engagement_slider_section() -> Section:
    """
    Five-card capability slider — brand/ROAS focused.
    Swipeable on mobile, nav on desktop.
    """

    slide_data = [
        {
            "icon": "📋",
            "title": "Pre-ranked creator lists",
            "desc": "Browse 1M+ creators organized by niche and country. No cold research from scratch.",
            "accent": "from-red-500 to-rose-600",
            "accent_light": "from-red-500/30 to-rose-600/30",
            "image": "https://images.unsplash.com/photo-1552664730-d307ca884978?w=400&q=80",
            "alt": "Creator ranked lists",
            "link_color": "text-red-400",
        },
        {
            "icon": "📊",
            "title": "Engagement quality over follower count",
            "desc": "500K subscribers at 8% engagement beats 5M at 0.3%. We surface the difference.",
            "accent": "from-blue-500 to-indigo-600",
            "accent_light": "from-blue-500/30 to-indigo-600/30",
            "image": "https://images.unsplash.com/photo-1551288049-bebda4e38f71?w=400&q=80",
            "alt": "Engagement analytics data",
            "link_color": "text-blue-400",
        },
        {
            "icon": "⚡",
            "title": "Catch rising stars early",
            "desc": "30-day momentum signals show who's growing fast. Reach out before their rates double.",
            "accent": "from-yellow-400 to-orange-500",
            "accent_light": "from-yellow-400/30 to-orange-500/30",
            "image": "https://images.unsplash.com/photo-1504384308090-c894fdcc538d?w=400&q=80",
            "alt": "Rising creator growth signal",
            "link_color": "text-yellow-400",
        },
        {
            "icon": "🎯",
            "title": "ROAS-correlated signals",
            "desc": "We surface the engagement patterns that correlate with conversion — not just impressions.",
            "accent": "from-green-500 to-emerald-600",
            "accent_light": "from-green-500/30 to-emerald-600/30",
            "image": "https://images.unsplash.com/photo-1517694712202-14dd9538aa97?w=400&q=80",
            "alt": "ROAS and conversion signals",
            "link_color": "text-green-400",
        },
        {
            "icon": "🔍",
            "title": "Analyze any playlist instantly",
            "desc": "Paste a public YouTube URL. Get engagement, viral scores, and content patterns in under 2 seconds.",
            "accent": "from-purple-500 to-fuchsia-600",
            "accent_light": "from-purple-500/30 to-fuchsia-600/30",
            "image": "https://images.unsplash.com/photo-1611532736579-6b16e2b50449?w=400&q=80",
            "alt": "Playlist analysis",
            "link_color": "text-purple-400",
        },
    ]

    return Section(
        # Header
        Div(
            H2(
                "Built for campaigns that have to perform",
                cls="text-3xl md:text-4xl font-bold text-white mb-3",
            ),
            P(
                "Five reasons teams use ViralVibes instead of guessing",
                cls="text-gray-400 text-lg",
            ),
            cls="text-center mb-12 px-4",
        ),
        # MonsterUI Slider Component
        Slider(
            *[
                slider_card(
                    icon=card["icon"],
                    title=card["title"],
                    desc=card["desc"],
                    accent=card["accent"],
                    accent_light=card["accent_light"],
                    image=card["image"],
                    alt=card["alt"],
                    link_color=card["link_color"],
                )
                for card in slide_data
            ],
            # Slider container styling
            cls="bg-gradient-to-b from-gray-900 via-black to-gray-900",
            # Items spacing
            items_cls="gap-6 px-4 md:px-8",
            # Custom nav styling
            nav=True,
            nav_cls="uk-position-small uk-flex uk-flex-center uk-hidden-hover",
            # UK Slider options
            uk_slider="finite: true; center: false; autoplay: false;",
        ),
        # Hint text (mobile)
        P(
            "← Swipe to explore →",
            cls="text-gray-500 text-sm text-center mt-8 md:hidden px-4",
        ),
        # Stats Row (Social Proof)
        Div(
            Div(
                P("1M+", cls="text-2xl font-bold text-white"),
                P("Creators Tracked", cls="text-gray-400 text-sm"),
                cls="text-center",
            ),
            Div(
                P("150+", cls="text-2xl font-bold text-white"),
                P("Countries", cls="text-gray-400 text-sm"),
                cls="text-center",
            ),
            Div(
                P("25+", cls="text-2xl font-bold text-white"),
                P("Niches Ranked", cls="text-gray-400 text-sm"),
                cls="text-center",
            ),
            cls="grid grid-cols-3 gap-8 mt-16 px-4 max-w-2xl mx-auto",
        ),
        cls="relative py-20 md:py-32 overflow-hidden",
        id="engagement-slider",
    )


def slider_card(
    icon: str,
    title: str,
    desc: str,
    accent: str,
    accent_light: str,
    image: str,
    alt: str,
    link_color: str,
) -> Card:
    """
    Slider card using MonsterUI Card component
    - Image on top with icon badge
    - Content below with gradient text link
    """

    return Card(
        # Image container with overlay
        Div(
            # Background image
            Img(
                src=image,
                alt=alt,
                cls="w-full h-48 object-cover",
                loading="lazy",
            ),
            # Gradient overlay
            Div(
                cls="absolute inset-0 bg-gradient-to-b from-transparent via-transparent to-black/80"
            ),
            # Icon badge (floats over image)
            Div(
                Span(icon, cls="text-4xl"),
                cls=(
                    f"absolute top-4 right-4 "
                    f"w-14 h-14 rounded-full flex items-center justify-center "
                    f"bg-gradient-to-br {accent} "
                    f"shadow-xl border-2 border-white/30 "
                    f"backdrop-blur-md"
                ),
            ),
            cls="relative h-48 rounded-t-2xl overflow-hidden",
        ),
        # Title and description
        H3(title, cls="text-white font-bold text-lg mb-2 line-clamp-2"),
        P(desc, cls="text-gray-300 text-sm leading-relaxed line-clamp-3"),
        # Gradient text CTA link
        A(
            "Learn more →",
            href="#",
            cls=f"inline-block mt-4 text-sm font-semibold {link_color} hover:opacity-80 transition-opacity",
        ),
        # Card styling
        cls=(
            "min-w-[280px] md:min-w-[320px] "
            "bg-white/5 backdrop-blur-xl border border-white/10 rounded-2xl "
            "shadow-xl hover:shadow-2xl "
            "hover:border-white/20 hover:bg-white/10 "
            "transition-all duration-300 "
            "overflow-hidden "
            "flex flex-col p-0"
        ),
        body_cls="p-5 md:p-6 flex flex-col flex-1",
    )
