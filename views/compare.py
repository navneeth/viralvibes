"""
Creator comparison page — side-by-side deep analysis.

Route: GET /compare?a=<creator_id>&b=<creator_id>
"""

from __future__ import annotations

from fasthtml.common import *
from monsterui.all import *

from utils import format_number, safe_get_value, slugify
from utils.creator_metrics import (
    calculate_avg_views_per_video,
    calculate_growth_rate,
    calculate_views_per_subscriber,
    get_country_flag,
    get_grade_info,
    get_language_emoji,
    get_language_name,
)
from views.creators import get_topic_category_emoji

logger = __import__("logging").getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

_GRADE_ORDER = {"A+": 5, "A": 4, "B+": 3, "B": 2, "C": 1}
_GRADE_CLS = {
    "A+": "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300",
    "A": "bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300",
    "B+": "bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300",
    "B": "bg-sky-100 text-sky-700 dark:bg-sky-900/40 dark:text-sky-300",
    "C": "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/40 dark:text-yellow-300",
}


def _pct(part: float, total: float) -> float:
    """Fraction of total as 0–100, clamped."""
    if not total:
        return 50.0
    return max(4.0, min(96.0, 100.0 * part / total))


def _trophy(winner: bool) -> Span | None:
    if winner:
        return Span("🏆", cls="text-sm ml-1", title="Winner in this metric")
    return None


def _ratio_bar(
    val_a: float, val_b: float, colour_a: str = "bg-blue-500", colour_b: str = "bg-violet-500"
):
    """
    Single proportional bar: A fills from left, B from right.
    Width is proportional to share of (A + B).
    """
    total = val_a + val_b
    pct_a = _pct(val_a, total)
    pct_b = 100.0 - pct_a
    return Div(
        Div(cls=f"h-2.5 {colour_a} rounded-l-full", style=f"width:{pct_a:.1f}%"),
        Div(cls=f"h-2.5 {colour_b} rounded-r-full", style=f"width:{pct_b:.1f}%"),
        cls="flex w-full h-2.5 bg-border rounded-full overflow-hidden my-2",
    )


def _dot_meter(score: float, max_score: float = 10.0, filled_cls: str = "bg-blue-500") -> Div:
    """Row of 10 dots, filled proportionally."""
    filled = round((score / max_score) * 10) if max_score else 0
    dots = [
        Span(
            cls=(
                f"w-2 h-2 rounded-full {filled_cls}"
                if i < filled
                else "w-2 h-2 rounded-full bg-border"
            )
        )
        for i in range(10)
    ]
    return Div(*dots, cls="flex gap-1 items-center")


def _insight_callout(*text_parts: str) -> Div:
    """Highlighted insight box with lightbulb."""
    return Div(
        Span("💡", cls="text-base shrink-0 mt-0.5"),
        P(" ".join(text_parts), cls="text-sm text-foreground leading-snug"),
        cls="flex gap-2 items-start p-3 rounded-xl bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 mt-3",
    )


def _section_card(*children, title: str, icon: str) -> Div:
    return Card(
        Div(
            UkIcon(icon, cls="w-4 h-4 text-muted-foreground"),
            H2(title, cls="text-base font-bold text-foreground"),
            cls="flex items-center gap-2 mb-4",
        ),
        *children,
        body_cls="p-5",
        cls="mb-4",
    )


def _metric_row(
    label: str,
    val_a,
    val_b,
    *,
    higher_is_better: bool = True,
    unit: str = "",
    val_a_raw: float = 0.0,
    val_b_raw: float = 0.0,
    show_bar: bool = False,
    colour_a: str = "bg-blue-500",
    colour_b: str = "bg-violet-500",
):
    """
    A single comparison row:
      label  |  val_a  🏆?  |  bar (optional)  |  🏆?  val_b
    """
    raw_a = val_a_raw or (
        float(str(val_a).replace(",", "").replace("%", "") or 0) if val_a else 0.0
    )
    raw_b = val_b_raw or (
        float(str(val_b).replace(",", "").replace("%", "") or 0) if val_b else 0.0
    )

    if raw_a == raw_b:
        win_a = win_b = False
    elif higher_is_better:
        win_a, win_b = raw_a > raw_b, raw_b > raw_a
    else:
        win_a, win_b = raw_a < raw_b, raw_b < raw_a

    trophy_a = _trophy(win_a)
    trophy_b = _trophy(win_b)

    disp_a = Div(
        Span(
            str(val_a) + unit if val_a is not None else "—",
            cls="text-sm font-semibold text-foreground",
        ),
        trophy_a,
        cls="flex items-center gap-1 justify-end",
    )
    disp_b = Div(
        trophy_b,
        Span(
            str(val_b) + unit if val_b is not None else "—",
            cls="text-sm font-semibold text-foreground",
        ),
        cls="flex items-center gap-1",
    )

    bar = _ratio_bar(raw_a or 0.001, raw_b or 0.001, colour_a, colour_b) if show_bar else None

    return Div(
        # 3-column: left value | bar | right value
        # On mobile the bar collapses to a line between the two values
        Div(
            Span(label, cls="text-xs text-muted-foreground col-span-3 sm:hidden block mb-1"),
            Div(disp_a, cls="flex-1 text-right"),
            Div(
                Span(label, cls="text-xs text-muted-foreground text-center hidden sm:block"),
                bar,
                cls="w-32 sm:w-40 flex flex-col items-center justify-center px-2 shrink-0",
            ),
            Div(disp_b, cls="flex-1"),
            cls="flex items-center gap-2",
        ),
        cls="py-2 border-b border-border last:border-0",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public render function
# ─────────────────────────────────────────────────────────────────────────────


def render_compare_page(
    a: dict,
    b: dict,
    ranks_a: dict | None = None,
    ranks_b: dict | None = None,
    is_fav_a: bool = False,
    is_fav_b: bool = False,
) -> Div:
    """
    Full compare page for two creators.

    Args:
        a, b:         Creator dicts from get_creator_stats().
        ranks_a/b:    Optional {country_rank, language_rank, category_rank} from
                      _get_context_ranks() — used in verdict section.
        is_fav_a/b:   Whether the current user has already favourited each creator.
    """
    ranks_a = ranks_a or {}
    ranks_b = ranks_b or {}

    # ── Extract fields ────────────────────────────────────────────────────────
    def _ex(c: dict, key: str, default=None):
        return safe_get_value(c, key, default)

    id_a = _ex(a, "id", "")
    id_b = _ex(b, "id", "")

    name_a = _ex(a, "channel_name", "Creator A")
    name_b = _ex(b, "channel_name", "Creator B")

    thumb_a = _ex(a, "channel_thumbnail_url") or ""
    thumb_b = _ex(b, "channel_thumbnail_url") or ""

    url_a = _ex(a, "channel_url") or f"https://www.youtube.com/channel/{_ex(a, 'channel_id', '')}"
    url_b = _ex(b, "channel_url") or f"https://www.youtube.com/channel/{_ex(b, 'channel_id', '')}"

    country_a = (_ex(a, "country_code") or "").upper()
    country_b = (_ex(b, "country_code") or "").upper()
    flag_a = get_country_flag(country_a) if country_a else ""
    flag_b = get_country_flag(country_b) if country_b else ""

    lang_a = _ex(a, "default_language", "")
    lang_b = _ex(b, "default_language", "")
    lang_name_a = get_language_name(lang_a) if lang_a else ""
    lang_name_b = get_language_name(lang_b) if lang_b else ""
    lang_emoji_a = get_language_emoji(lang_a) if lang_a else ""
    lang_emoji_b = get_language_emoji(lang_b) if lang_b else ""

    cat_a = _ex(a, "primary_category", "")
    cat_b = _ex(b, "primary_category", "")
    cat_emoji_a = get_topic_category_emoji(cat_a) if cat_a else ""
    cat_emoji_b = get_topic_category_emoji(cat_b) if cat_b else ""

    grade_a = _ex(a, "quality_grade", "C")
    grade_b = _ex(b, "quality_grade", "C")
    _, grade_label_a, grade_bg_a = get_grade_info(grade_a)
    _, grade_label_b, grade_bg_b = get_grade_info(grade_b)
    grade_cls_a = _GRADE_CLS.get(grade_a, "bg-accent text-muted-foreground")
    grade_cls_b = _GRADE_CLS.get(grade_b, "bg-accent text-muted-foreground")

    # Numerics
    subs_a = int(_ex(a, "current_subscribers", 0) or 0)
    subs_b = int(_ex(b, "current_subscribers", 0) or 0)
    views_a = int(_ex(a, "current_view_count", 0) or 0)
    views_b = int(_ex(b, "current_view_count", 0) or 0)
    videos_a = int(_ex(a, "current_video_count", 0) or 0)
    videos_b = int(_ex(b, "current_video_count", 0) or 0)
    eng_a = float(_ex(a, "engagement_score", 0) or 0)
    eng_b = float(_ex(b, "engagement_score", 0) or 0)
    uploads_a = float(_ex(a, "monthly_uploads", 0) or 0)
    uploads_b = float(_ex(b, "monthly_uploads", 0) or 0)
    age_a = int(_ex(a, "channel_age_days", 0) or 0)
    age_b = int(_ex(b, "channel_age_days", 0) or 0)
    hidden_a = bool(_ex(a, "hidden_subscriber_count", False))
    hidden_b = bool(_ex(b, "hidden_subscriber_count", False))

    delta_a_raw = _ex(a, "subscribers_change_30d", None)
    delta_b_raw = _ex(b, "subscribers_change_30d", None)
    delta_a = int(delta_a_raw) if delta_a_raw is not None else None
    delta_b = int(delta_b_raw) if delta_b_raw is not None else None

    growth_rate_a = calculate_growth_rate(delta_a, subs_a)
    growth_rate_b = calculate_growth_rate(delta_b, subs_b)

    avg_views_a = calculate_avg_views_per_video(views_a, videos_a)
    avg_views_b = calculate_avg_views_per_video(views_b, videos_b)
    vps_a = calculate_views_per_subscriber(views_a, subs_a)
    vps_b = calculate_views_per_subscriber(views_b, subs_b)

    # ── Colour palette (A=blue, B=violet) ────────────────────────────────────
    CL_A = "bg-blue-500"
    CL_B = "bg-violet-500"
    TEXT_A = "text-blue-600 dark:text-blue-400"
    TEXT_B = "text-violet-600 dark:text-violet-400"
    RING_A = "ring-2 ring-blue-400"
    RING_B = "ring-2 ring-violet-400"

    # ─────────────────────────────────────────────────────────────────────────
    # HEADER — dual avatar identity strip
    # ─────────────────────────────────────────────────────────────────────────
    def _avatar(thumb, name, ring_cls):
        return (
            Img(
                src=thumb,
                alt=name,
                cls=f"w-16 h-16 sm:w-20 sm:h-20 rounded-2xl object-cover {ring_cls}",
            )
            if thumb
            else Div(
                Span(name[:1].upper(), cls="text-2xl font-bold text-muted-foreground"),
                cls=f"w-16 h-16 sm:w-20 sm:h-20 rounded-2xl bg-accent flex items-center justify-center {ring_cls}",
            )
        )

    def _creator_header_col(
        cid,
        name,
        thumb,
        ring_cls,
        text_cls,
        channel_url,
        country,
        flag,
        cat,
        cat_emoji,
        grade,
        grade_cls,
        is_fav,
        align,
    ):
        fav_icon = "heart" if not is_fav else "heart"
        fav_label = "Save" if not is_fav else "Saved"
        fav_btn_cls = (
            "inline-flex items-center gap-1 px-3 py-1.5 rounded-lg text-xs font-semibold border "
            + (
                "bg-red-50 border-red-300 text-red-600"
                if is_fav
                else "bg-accent border-border text-muted-foreground hover:bg-red-50 hover:border-red-300 hover:text-red-600"
            )
            + " transition-colors no-underline"
        )
        text_align = "text-right" if align == "right" else "text-left"
        items_align = "items-end" if align == "right" else "items-start"

        return Div(
            _avatar(thumb, name, ring_cls),
            Div(
                H2(
                    name,
                    cls=f"text-lg sm:text-xl font-bold text-foreground leading-tight {text_align}",
                ),
                Div(
                    Span(grade, cls=f"text-xs font-bold px-2 py-0.5 rounded-full {grade_cls}"),
                    *(
                        [Span(f"{flag} {country}", cls="text-xs text-muted-foreground")]
                        if country
                        else []
                    ),
                    *(
                        [Span(f"{cat_emoji} {cat}", cls="text-xs text-muted-foreground")]
                        if cat
                        else []
                    ),
                    cls=f"flex flex-wrap gap-1.5 mt-1 {items_align}",
                ),
                Div(
                    A(
                        UkIcon("youtube", cls="w-3.5 h-3.5 mr-1"),
                        "YouTube",
                        href=channel_url,
                        target="_blank",
                        rel="noopener noreferrer",
                        cls="inline-flex items-center px-3 py-1.5 rounded-lg bg-red-600 hover:bg-red-700 text-white text-xs font-semibold no-underline transition-colors",
                    ),
                    A(
                        UkIcon(fav_icon, cls="w-3.5 h-3.5 mr-1"),
                        fav_label,
                        href=f"/creator/{cid}/favourite",
                        cls=fav_btn_cls,
                    ),
                    A(
                        "Profile →",
                        href=f"/creator/{cid}",
                        cls="inline-flex items-center px-3 py-1.5 rounded-lg bg-accent hover:bg-accent/80 text-foreground text-xs font-semibold no-underline transition-colors",
                    ),
                    cls=f"flex flex-wrap gap-2 mt-3 {items_align}",
                ),
                cls=f"flex flex-col {items_align} mt-3 sm:mt-0",
            ),
            cls=f"flex flex-col sm:flex-row gap-3 {items_align}",
        )

    header = Card(
        Div(
            # Creator A (left)
            _creator_header_col(
                id_a,
                name_a,
                thumb_a,
                RING_A,
                TEXT_A,
                url_a,
                country_a,
                flag_a,
                cat_a,
                cat_emoji_a,
                grade_a,
                grade_cls_a,
                is_fav_a,
                align="left",
            ),
            # VS divider
            Div(
                Div(cls="w-px h-12 bg-border hidden sm:block"),
                Span("⇄", cls="text-2xl text-muted-foreground font-bold"),
                Div(cls="w-px h-12 bg-border hidden sm:block"),
                cls="flex flex-col items-center justify-center gap-1 px-4 shrink-0",
            ),
            # Creator B (right)
            _creator_header_col(
                id_b,
                name_b,
                thumb_b,
                RING_B,
                TEXT_B,
                url_b,
                country_b,
                flag_b,
                cat_b,
                cat_emoji_b,
                grade_b,
                grade_cls_b,
                is_fav_b,
                align="right",
            ),
            cls="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-6",
        ),
        body_cls="p-5",
        cls="mb-6",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Shared column header labels (reused across sections)
    # ─────────────────────────────────────────────────────────────────────────
    def _col_labels():
        return Div(
            Span(name_a, cls=f"text-xs font-bold {TEXT_A} flex-1 text-right truncate"),
            Div(cls="w-32 sm:w-40 shrink-0"),
            Span(name_b, cls=f"text-xs font-bold {TEXT_B} flex-1 truncate"),
            cls="flex items-center gap-2 mb-2 pb-2 border-b border-border",
        )

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 1 — Audience Scale
    # ─────────────────────────────────────────────────────────────────────────
    subs_ratio = subs_a / subs_b if subs_b else None
    views_ratio = views_a / views_b if views_b else None

    def _ratio_label(ratio: float | None, name_bigger: str) -> str:
        if ratio is None:
            return ""
        r = ratio if ratio >= 1 else 1 / ratio
        n = name_bigger if ratio >= 1 else (name_b if name_bigger == name_a else name_a)
        return f"{n} is {r:.1f}× larger"

    scale_insight = None
    if subs_ratio is not None:
        bigger = name_a if subs_a >= subs_b else name_b
        r = max(subs_a, subs_b) / max(min(subs_a, subs_b), 1)
        if r > 2:
            scale_insight = _insight_callout(
                f"{bigger} has {r:.1f}× more subscribers —",
                "a meaningful scale advantage for reach-based brand deals.",
            )

    scale_section = _section_card(
        _col_labels(),
        _metric_row(
            "Subscribers",
            format_number(subs_a) if not hidden_a else "Hidden",
            format_number(subs_b) if not hidden_b else "Hidden",
            val_a_raw=float(subs_a),
            val_b_raw=float(subs_b),
            show_bar=True,
            colour_a=CL_A,
            colour_b=CL_B,
        ),
        _metric_row(
            "Total Views",
            format_number(views_a),
            format_number(views_b),
            val_a_raw=float(views_a),
            val_b_raw=float(views_b),
            show_bar=True,
            colour_a=CL_A,
            colour_b=CL_B,
        ),
        _metric_row(
            "Videos Published",
            format_number(videos_a),
            format_number(videos_b),
            val_a_raw=float(videos_a),
            val_b_raw=float(videos_b),
            show_bar=True,
            colour_a=CL_A,
            colour_b=CL_B,
        ),
        *(([scale_insight]) if scale_insight else []),
        title="Audience Scale",
        icon="bar-chart-2",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 2 — Growth Momentum
    # ─────────────────────────────────────────────────────────────────────────
    def _delta_display(delta: int | None) -> str:
        if delta is None:
            return "—"
        sign = "+" if delta > 0 else ""
        return f"{sign}{format_number(delta)}"

    def _pct_display(rate: float) -> str:
        sign = "+" if rate > 0 else ""
        return f"{sign}{rate:.2f}%"

    growth_insight = None
    if delta_a is not None and delta_b is not None and subs_a > 0 and subs_b > 0:
        if growth_rate_b > growth_rate_a * 1.5 and growth_rate_b > 0:
            ratio = growth_rate_b / growth_rate_a if growth_rate_a > 0 else None
            if ratio:
                growth_insight = _insight_callout(
                    f"{name_b} is growing {ratio:.1f}× faster as a % of audience",
                    (
                        f"— despite {subs_a // max(subs_b, 1):.0f}× fewer subscribers."
                        if subs_a > subs_b
                        else "."
                    ),
                )
        elif growth_rate_a > growth_rate_b * 1.5 and growth_rate_a > 0:
            ratio = growth_rate_a / growth_rate_b if growth_rate_b > 0 else None
            if ratio:
                growth_insight = _insight_callout(
                    f"{name_a} is growing {ratio:.1f}× faster as a % of audience."
                )

    growth_section = _section_card(
        _col_labels(),
        _metric_row(
            "Net change (30d)",
            _delta_display(delta_a),
            _delta_display(delta_b),
            val_a_raw=float(delta_a or 0),
            val_b_raw=float(delta_b or 0),
            show_bar=(delta_a is not None and delta_b is not None),
            colour_a=CL_A,
            colour_b=CL_B,
        ),
        _metric_row(
            "Growth rate (30d)",
            _pct_display(growth_rate_a),
            _pct_display(growth_rate_b),
            val_a_raw=growth_rate_a,
            val_b_raw=growth_rate_b,
        ),
        *(([growth_insight]) if growth_insight else []),
        title="30-Day Growth Momentum",
        icon="trending-up",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 3 — Content Quality
    # ─────────────────────────────────────────────────────────────────────────
    grade_ord_a = _GRADE_ORDER.get(grade_a, 0)
    grade_ord_b = _GRADE_ORDER.get(grade_b, 0)

    quality_insight = None
    if vps_a > 0 and vps_b > 0:
        bigger_vps = name_a if vps_a >= vps_b else name_b
        smaller_vps = name_b if vps_a >= vps_b else name_a
        r = max(vps_a, vps_b) / max(min(vps_a, vps_b), 0.01)
        if r > 1.5:
            quality_insight = _insight_callout(
                f"{bigger_vps} generates {r:.1f}× more views per subscriber —",
                "a strong signal of loyal, returning viewership.",
            )

    quality_section = _section_card(
        _col_labels(),
        # Grade — render pill + dot meter for engagement
        Div(
            Span(
                "Quality Grade", cls="text-xs text-muted-foreground col-span-3 sm:hidden block mb-1"
            ),
            Div(
                Span(grade_a, cls=f"text-xs font-bold px-2 py-0.5 rounded-full {grade_cls_a}"),
                _trophy(grade_ord_a > grade_ord_b),
                cls="flex items-center gap-1 flex-1 justify-end",
            ),
            Div(
                Span(
                    "Quality Grade", cls="text-xs text-muted-foreground text-center hidden sm:block"
                ),
                cls="w-32 sm:w-40 flex flex-col items-center justify-center px-2 shrink-0",
            ),
            Div(
                _trophy(grade_ord_b > grade_ord_a),
                Span(grade_b, cls=f"text-xs font-bold px-2 py-0.5 rounded-full {grade_cls_b}"),
                cls="flex items-center gap-1 flex-1",
            ),
            cls="flex items-center gap-2 py-2 border-b border-border",
        ),
        # Engagement dot meters
        Div(
            Span("Engagement", cls="text-xs text-muted-foreground col-span-3 sm:hidden block mb-1"),
            Div(
                Span(f"{eng_a:.1f}/10", cls="text-sm font-semibold text-foreground mr-2"),
                _dot_meter(eng_a, filled_cls="bg-blue-500"),
                _trophy(eng_a > eng_b),
                cls="flex items-center gap-1 flex-1 justify-end",
            ),
            Div(
                Span("Engagement", cls="text-xs text-muted-foreground text-center hidden sm:block"),
                cls="w-32 sm:w-40 flex flex-col items-center justify-center px-2 shrink-0",
            ),
            Div(
                _trophy(eng_b > eng_a),
                _dot_meter(eng_b, filled_cls="bg-violet-500"),
                Span(f"{eng_b:.1f}/10", cls="text-sm font-semibold text-foreground ml-2"),
                cls="flex items-center gap-1 flex-1",
            ),
            cls="flex items-center gap-2 py-2 border-b border-border",
        ),
        _metric_row(
            "Views / Subscriber",
            f"{vps_a:.1f}×",
            f"{vps_b:.1f}×",
            val_a_raw=vps_a,
            val_b_raw=vps_b,
        ),
        *(([quality_insight]) if quality_insight else []),
        title="Content Quality",
        icon="star",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 4 — Output & Consistency
    # ─────────────────────────────────────────────────────────────────────────
    output_insight = None
    if uploads_a > 0 and uploads_b > 0 and avg_views_a > 0 and avg_views_b > 0:
        more_uploads = name_a if uploads_a >= uploads_b else name_b
        better_per = name_a if avg_views_a >= avg_views_b else name_b
        if more_uploads != better_per:
            output_insight = _insight_callout(
                f"{more_uploads} uploads more often, but {better_per} earns more views per video —",
                "quality over quantity.",
            )

    age_str_a = f"{age_a // 365}y {(age_a % 365) // 30}mo" if age_a else "—"
    age_str_b = f"{age_b // 365}y {(age_b % 365) // 30}mo" if age_b else "—"

    output_section = _section_card(
        _col_labels(),
        _metric_row(
            "Upload rate",
            f"{uploads_a:.1f}/mo",
            f"{uploads_b:.1f}/mo",
            val_a_raw=uploads_a,
            val_b_raw=uploads_b,
            show_bar=True,
            colour_a=CL_A,
            colour_b=CL_B,
        ),
        _metric_row(
            "Avg views / video",
            format_number(int(avg_views_a)),
            format_number(int(avg_views_b)),
            val_a_raw=avg_views_a,
            val_b_raw=avg_views_b,
            show_bar=True,
            colour_a=CL_A,
            colour_b=CL_B,
        ),
        _metric_row(
            "Channel age",
            age_str_a,
            age_str_b,
            val_a_raw=float(age_a),
            val_b_raw=float(age_b),
        ),
        *(([output_insight]) if output_insight else []),
        title="Output & Consistency",
        icon="video",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 5 — Verdict + Actions
    # ─────────────────────────────────────────────────────────────────────────
    def _score_creator(name, subs, growth_rate, eng, vps, uploads, grade):
        """Simple weighted score — who 'wins' overall."""
        score = 0
        score += 2 if subs > 0 else 0  # presence
        score += 3 if growth_rate > 1 else (1 if growth_rate > 0 else 0)
        score += 3 if eng > 7 else (1 if eng > 4 else 0)
        score += 2 if vps > 200 else (1 if vps > 50 else 0)
        score += 1 if uploads > 4 else 0
        score += {"A+": 3, "A": 2, "B+": 1}.get(grade, 0)
        return score

    score_a = _score_creator(name_a, subs_a, growth_rate_a, eng_a, vps_a, uploads_a, grade_a)
    score_b = _score_creator(name_b, subs_b, growth_rate_b, eng_b, vps_b, uploads_b, grade_b)

    def _wins(name, wins_list):
        return Div(
            Span(f"🏆 {name}", cls="text-sm font-bold text-foreground"),
            *[Span(f"• {w}", cls="text-xs text-muted-foreground") for w in wins_list],
            cls="flex flex-col gap-0.5",
        )

    wins_a = []
    wins_b = []
    if subs_a > subs_b:
        wins_a.append("Reach & brand potential")
    else:
        wins_b.append("Reach & brand potential")
    if growth_rate_a > growth_rate_b:
        wins_a.append("Growth momentum")
    else:
        wins_b.append("Growth momentum")
    if eng_a > eng_b:
        wins_a.append("Engagement")
    else:
        wins_b.append("Engagement")
    if vps_a > vps_b:
        wins_a.append("Loyal viewership (views/sub)")
    else:
        wins_b.append("Loyal viewership (views/sub)")
    if uploads_a > uploads_b:
        wins_a.append("Upload consistency")
    else:
        wins_b.append("Upload consistency")
    if avg_views_a > avg_views_b:
        wins_a.append("Avg video performance")
    else:
        wins_b.append("Avg video performance")

    # Contextual use-case copy
    def _use_case(name, subs, eng, growth_rate, cat) -> str:
        parts = []
        if subs > 1_000_000:
            parts.append("mass-reach campaigns")
        if eng > 7:
            parts.append("high-CPM niches")
        if growth_rate > 2:
            parts.append("early-stage partnerships (while growing)")
        if cat:
            parts.append(f"targeting {cat} audiences")
        return f"Best for: {', '.join(parts)}." if parts else ""

    use_a = _use_case(name_a, subs_a, eng_a, growth_rate_a, cat_a)
    use_b = _use_case(name_b, subs_b, eng_b, growth_rate_b, cat_b)

    verdict_section = _section_card(
        Grid(
            # Win tallies
            Div(
                (
                    _wins(name_a, wins_a)
                    if score_a >= score_b
                    else Div(
                        _wins(name_a, wins_a),
                        cls="opacity-80",
                    )
                ),
                P(use_a, cls="text-xs text-muted-foreground mt-2 leading-snug") if use_a else None,
                cls="p-3 rounded-xl bg-blue-50 dark:bg-blue-950/30 border border-blue-200 dark:border-blue-800",
            ),
            Div(
                (
                    _wins(name_b, wins_b)
                    if score_b >= score_a
                    else Div(
                        _wins(name_b, wins_b),
                        cls="opacity-80",
                    )
                ),
                P(use_b, cls="text-xs text-muted-foreground mt-2 leading-snug") if use_b else None,
                cls="p-3 rounded-xl bg-violet-50 dark:bg-violet-950/30 border border-violet-200 dark:border-violet-800",
            ),
            cols=2,
            cls="mb-4",
        ),
        # Action buttons
        Div(
            A(
                UkIcon("heart", cls="w-3.5 h-3.5 mr-1.5"),
                f"Save {name_a}",
                href=f"/creator/{id_a}/favourite",
                cls="inline-flex items-center px-4 py-2 rounded-xl bg-blue-600 hover:bg-blue-700 text-white text-sm font-semibold no-underline transition-colors",
            ),
            A(
                UkIcon("heart", cls="w-3.5 h-3.5 mr-1.5"),
                f"Save {name_b}",
                href=f"/creator/{id_b}/favourite",
                cls="inline-flex items-center px-4 py-2 rounded-xl bg-violet-600 hover:bg-violet-700 text-white text-sm font-semibold no-underline transition-colors",
            ),
            *(
                [
                    A(
                        f"Browse {cat_emoji_a} {cat_a} →",
                        href=f"/lists/category/{slugify(cat_a)}",
                        cls="inline-flex items-center px-4 py-2 rounded-xl bg-accent hover:bg-accent/80 text-foreground text-sm font-semibold no-underline transition-colors",
                    )
                ]
                if cat_a
                else []
            ),
            *(
                [
                    A(
                        f"Browse {cat_emoji_b} {cat_b} →",
                        href=f"/lists/category/{slugify(cat_b)}",
                        cls="inline-flex items-center px-4 py-2 rounded-xl bg-accent hover:bg-accent/80 text-foreground text-sm font-semibold no-underline transition-colors",
                    )
                ]
                if cat_b and cat_b != cat_a
                else []
            ),
            A(
                f"Similar to {name_a} →",
                href=f"/creator/{id_a}",
                cls="inline-flex items-center px-4 py-2 rounded-xl border border-border hover:bg-accent text-foreground text-sm no-underline transition-colors",
            ),
            A(
                f"Similar to {name_b} →",
                href=f"/creator/{id_b}",
                cls="inline-flex items-center px-4 py-2 rounded-xl border border-border hover:bg-accent text-foreground text-sm no-underline transition-colors",
            ),
            cls="flex flex-wrap gap-2",
        ),
        title="Verdict & Actions",
        icon="zap",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # ASSEMBLE
    # ─────────────────────────────────────────────────────────────────────────
    return Div(
        # Back link
        Div(
            A(
                UkIcon("arrow-left", cls="w-4 h-4 mr-1"),
                "Back to creators",
                href="/creators",
                cls="inline-flex items-center text-sm font-medium text-muted-foreground hover:text-foreground no-underline transition-colors",
            ),
            cls="mb-4",
        ),
        header,
        scale_section,
        growth_section,
        quality_section,
        output_section,
        verdict_section,
        cls="max-w-4xl mx-auto px-4 pb-16 pt-6",
    )
