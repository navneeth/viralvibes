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

# Metric config: (stats_key, display_label, creator_field, js_y_formatter)
# js_y_formatter: JS function string passed to ApexCharts yaxis.labels.formatter
# and tooltip.y.formatter so every number is human-readable.
_METRICS: list[tuple[str, str, str, str]] = [
    (
        "subscribers",
        "Subscribers",
        "current_subscribers",
        "function(val){if(val>=1e9)return(val/1e9).toFixed(1)+'B';"
        "if(val>=1e6)return(val/1e6).toFixed(1)+'M';"
        "if(val>=1e3)return(val/1e3).toFixed(1)+'K';return String(Math.round(val));}",
    ),
    (
        "views",
        "Total Views",
        "current_view_count",
        "function(val){if(val>=1e9)return(val/1e9).toFixed(1)+'B';"
        "if(val>=1e6)return(val/1e6).toFixed(1)+'M';"
        "if(val>=1e3)return(val/1e3).toFixed(1)+'K';return String(Math.round(val));}",
    ),
    (
        "engagement",
        "Engagement %",
        "engagement_score",
        "function(val){return val.toFixed(2)+'%';}",
    ),
    (
        "monthly_uploads",
        "Monthly Uploads",
        "monthly_uploads",
        "function(val){return val.toFixed(1)+'/mo';}",
    ),
]

_BOX_FILL = "#f1f5f9"  # slate-100
_BOX_STROKE = "#94a3b8"  # slate-400
_STAR_COLOR = "#ef4444"  # red-500
_PEER_COLOR = "#6366f1"  # indigo-500 — muted reference dots for peer creators
_PEER_MAX = 20  # max peer samples per chart to avoid clutter


# Public API


def render_category_box_plots(
    creator: dict,
    category_stats: dict | None,
    peers: list[dict] | None = None,
) -> Div:
    """
    Top-level entry point called from views/creators.py.

    Returns a self-contained Div - either the 2x2 ApexChart grid or a
    muted placeholder when stats are not available yet.

    Args:
        creator:        Creator dict (must contain at least primary_category).
        category_stats: Pre-aggregated box plot stats row from the DB cache.
        peers:          Optional combined list of "you may also like" +
                        "similar creators" dicts.  Up to _PEER_MAX samples
                        are plotted as indigo scatter points so the viewer
                        can see where their peer group sits relative to the
                        category distribution.
    """
    category = creator.get("primary_category", "")

    if not category_stats or not category:
        return _placeholder(category)

    # Map stats_key → creator metric value using the field name from _METRICS.
    creator_values: dict[str, float | None] = {
        key: _safe_float(creator.get(field)) for key, _, field, _ in _METRICS
    }

    # Deduplicate and cap peer samples (exclude the current creator itself).
    creator_id = creator.get("id")
    seen_ids: set = {creator_id} if creator_id else set()
    sampled_peers: list[dict] = []
    for p in peers or []:
        pid = p.get("id")
        if pid and pid not in seen_ids:
            seen_ids.add(pid)
            sampled_peers.append(p)
            if len(sampled_peers) >= _PEER_MAX:
                break

    # Per-metric peer value lists (preserves _METRICS order, filters None).
    peer_values_by_metric: dict[str, list[float]] = {
        key: [v for p in sampled_peers if (v := _safe_float(p.get(field))) is not None]
        for key, _, field, _ in _METRICS
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
                    formatter=fmt,
                    peer_values=peer_values_by_metric.get(key, []),
                )
                for key, label, _, fmt in _METRICS
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
    formatter: str = "",
    peer_values: list[float] | None = None,
) -> Div:
    """
    Single ApexChart box plot with optional scatter overlays.

    - The category distribution is the box (min/p25/median/p75/max).
    - ``creator_value`` is plotted as a red star so the viewer can see
      where this creator sits inside the distribution.
    - ``peer_values`` (you-may-also-like + similar creators) are plotted
      as indigo circles behind the creator star, giving a peer reference.
    - ``formatter`` is a JS function string applied to both yaxis labels
      and tooltip values (e.g. "function(val){return (val/1e6).toFixed(1)+'M'}").

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

    # Build series list: boxPlot → peers (behind) → creator (on top).
    series: list[dict] = [box_series]

    has_peers = bool(peer_values)
    if has_peers:
        series.append(
            {
                "name": "Peers",
                "type": "scatter",
                "data": [{"x": category, "y": v} for v in peer_values],
            }
        )

    if creator_value is not None:
        series.append(
            {
                "name": "This creator",
                "type": "scatter",
                "data": [{"x": category, "y": creator_value}],
            }
        )

    # Marker config arrays are indexed by series position.
    marker_sizes: list[int] = [0]  # boxPlot: no dots
    marker_colors: list[str] = [_BOX_STROKE]
    marker_shapes: list[str] = ["circle"]

    if has_peers:
        marker_sizes.append(5)
        marker_colors.append(_PEER_COLOR)
        marker_shapes.append("circle")

    if creator_value is not None:
        marker_sizes.append(8)
        marker_colors.append(_STAR_COLOR)
        marker_shapes.append("star")

    # Tooltip and yaxis both use the same formatter when provided.
    tooltip: dict = {"shared": False, "intersect": True}
    if formatter:
        tooltip["y"] = {"formatter": formatter}

    yaxis_labels: dict = {"show": True}
    if formatter:
        yaxis_labels["formatter"] = formatter

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
            "size": marker_sizes,
            "colors": marker_colors,
            "strokeColors": ["#fff"] * len(marker_sizes),
            "strokeWidth": 2,
            "shape": marker_shapes,
        },
        "tooltip": tooltip,
        "xaxis": {"labels": {"show": False}, "axisTicks": {"show": False}},
        "yaxis": {"labels": yaxis_labels},
        "legend": {"show": has_peers},
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
        cls=("mt-8 bg-gray-50 rounded-2xl border border-dashed border-gray-200 text-center"),
        id="category-box-plots",
    )


# Helpers


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None
