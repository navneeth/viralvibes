# components/category_stats.py
"""
Category comparison box plots for creator profile pages.

Renders a 2x2 grid of box plots (subscribers, views, engagement,
monthly_uploads) showing where a creator sits within their category.

Uses MonsterUI's ApexChart component (wraps ApexCharts JS).
Requires apex_charts=True in your app headers:
    app, rt = fast_app(hdrs=Theme.blue.headers(apex_charts=True))

Data flow:
  routes/creators.py
    -> get_cached_category_box_stats(category)   [PK lookup, <1ms]
    -> render_creator_profile_page(..., category_stats=stats)
  views/creators.py
    -> render_category_box_plots(creator, category_stats)
  components/category_stats.py                   <- this file
    -> _box_chart(...)                           [ApexChart FT component]

Graceful degradation: every public function returns a FastHTML Div.
If stats are missing the section shows a muted placeholder - callers
never need to guard for None.
"""

from __future__ import annotations

import logging

from fasthtml.common import Div, H3, P, Span
from monsterui.all import ApexChart

logger = logging.getLogger(__name__)

# Metric config: (stats_key, display_label, js_formatter_suffix)
_METRICS: list[tuple[str, str, str]] = [
    ("subscribers", "Subscribers", ""),
    ("views", "Total Views", ""),
    ("engagement", "Engagement %", "%"),
    ("monthly_uploads", "Monthly Uploads", "/mo"),
]

_BOX_FILL = "#f1f5f9"  # slate-100
_BOX_STROKE = "#94a3b8"  # slate-400
_STAR_COLOR = "#ef4444"  # red-500


# Public API


def render_category_box_plots(
    creator: dict,
    category_stats: dict | None,
) -> Div:
    """
    Top-level entry point called from views/creators.py.

    Returns a self-contained Div - either the 2x2 ApexChart grid or a
    muted placeholder when stats are not available yet.
    """
    category = creator.get("primary_category", "")

    if not category_stats or not category:
        return _placeholder(category)

    creator_values = {
        "subscribers": _safe_float(creator.get("current_subscribers")),
        "views": _safe_float(creator.get("current_view_count")),
        "engagement": _safe_float(creator.get("engagement_score")),
        "monthly_uploads": _safe_float(creator.get("monthly_uploads")),
    }

    peer_count = category_stats.get("count", 0)

    return Div(
        Div(
            H3(
                f"How {creator.get('channel_name', 'This Creator')} compares",
                cls="text-lg font-semibold text-gray-900",
            ),
            P(
                f"vs {peer_count:,} creators in {category}",
                cls="text-sm text-gray-500 mt-0.5",
            ),
            cls="mb-4",
        ),
        Div(
            *[
                _box_chart(
                    label=label,
                    stats=category_stats.get(key, {}),
                    creator_value=creator_values.get(key),
                    category=category,
                )
                for key, label, _ in _METRICS
                if category_stats.get(key)
            ],
            cls="grid grid-cols-1 sm:grid-cols-2 gap-4",
        ),
        cls="mt-8 bg-white rounded-2xl border border-gray-200 shadow-sm p-6",
        id="category-box-plots",
    )


# Chart builder

_REQUIRED_STAT_KEYS = ("min", "p25", "median", "p75", "max")


def _box_chart(
    label: str,
    stats: dict,
    creator_value: float | None,
    category: str,
) -> Div:
    """
    Single ApexChart box plot with optional scatter overlay for the creator.

    ApexCharts boxPlot series expects y as [min, q1, median, q3, max].
    The scatter series overlays the creator's value as a coloured marker.

    Returns an empty Div if the stats dict is missing any required key —
    guards against partially populated cache rows without scattering .get()
    calls across the series construction.
    """
    missing = [k for k in _REQUIRED_STAT_KEYS if k not in stats]
    if missing:
        logger.warning(
            "_box_chart: skipping '%s' — missing keys in cache row: %s",
            label,
            missing,
        )
        return Div()

    box_series = {
        "name": category,
        "type": "boxPlot",
        "data": [
            {
                "x": category,
                "y": [
                    stats["min"],
                    stats["p25"],
                    stats["median"],
                    stats["p75"],
                    stats["max"],
                ],
            }
        ],
    }

    series = [box_series]

    if creator_value is not None:
        series.append(
            {
                "name": "This creator",
                "type": "scatter",
                "data": [{"x": category, "y": creator_value}],
            }
        )

    opts = {
        "chart": {
            "type": "boxPlot",
            "height": 220,
            "toolbar": {"show": False},
            "zoom": {"enabled": False},
            "animations": {"enabled": False},
        },
        "title": {
            "text": label,
            "align": "center",
            "style": {"fontSize": "13px", "fontWeight": 600, "color": "#374151"},
        },
        "series": series,
        "plotOptions": {
            "boxPlot": {
                "colors": {"upper": _BOX_FILL, "lower": _BOX_FILL},
            },
        },
        "markers": {
            # index 0 = boxPlot (no dots), index 1 = scatter (star marker)
            "size": [0, 8],
            "colors": [_BOX_STROKE, _STAR_COLOR],
            "strokeColors": ["#fff", "#fff"],
            "strokeWidth": 2,
            "shape": ["circle", "star"],
        },
        "tooltip": {"shared": False, "intersect": True},
        "xaxis": {"labels": {"show": False}, "axisTicks": {"show": False}},
        "yaxis": {"labels": {"show": True}},
        "legend": {"show": False},
        "grid": {"borderColor": "#f3f4f6", "strokeDashArray": 4},
    }

    return ApexChart(opts=opts, cls="w-full")


# Placeholder


def _placeholder(category: str) -> Div:
    reason = (
        "Category stats are being computed - check back after the next worker run."
        if category
        else "No category set for this creator."
    )
    return Div(
        Div(
            Span("📊", cls="text-2xl"),
            P("Category comparison", cls="text-sm font-medium text-gray-500 mt-1"),
            P(reason, cls="text-xs text-gray-400 mt-1 max-w-xs text-center"),
            cls="flex flex-col items-center justify-center py-10",
        ),
        cls=(
            "mt-8 bg-gray-50 rounded-2xl border border-dashed border-gray-200 text-center"
        ),
        id="category-box-plots",
    )


# Helpers


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None
