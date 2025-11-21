import logging
from typing import Dict

import polars as pl
from monsterui.all import ApexChart

logger = logging.getLogger("charts")

SCALE_MILLIONS = 1_000_000
SCALE_THOUSANDS = 1_000


def _safe_sort(df: pl.DataFrame, col: str, descending: bool = True) -> pl.DataFrame:
    """
    Sort DataFrame safely across Polars versions.
    Handles:
      - older/newer Polars signatures
      - missing columns
      - numeric columns stored as strings
    """
    if df is None or df.is_empty():
        return df

    if col not in df.columns:
        logger.warning(f"[charts] Column '{col}' not found; skipping sort.")
        return df

    try:
        series = df[col]
        # Auto-coerce to numeric if needed
        if series.dtype == pl.Utf8:
            logger.debug(f"[charts] Auto-casting column '{col}' from str to Float64")
            # Remove duplicate: df = df.with_columns(...)

        df_casted = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
        return df_casted.sort(by=col, descending=descending)

    except TypeError:
        try:
            # Older Polars fallback
            return df.sort(col, reverse=descending)
        except Exception as e:
            logger.warning(f"[charts] Fallback sort failed for '{col}': {e}")
            return df

    except Exception as e:
        logger.warning(f"[charts] Failed to sort by '{col}': {e}")
        return df


# --------------------------------------------------------------
# 1. DESIGN CONSTANTS
# --------------------------------------------------------------
THEME_COLORS = [
    "#3B82F6",  # blue-500
    "#EF4444",  # red-500
    "#10B981",  # emerald-500
    "#F59E0B",  # amber-500
    "#8B5CF6",  # violet-500
    "#EC4899",  # pink-500
]
CHART_HEIGHT = "h-96"  # ~384px – perfect on phone & desktop

# Constant
CHART_WRAPPER_CLS = (
    "w-full min-h-[420px] rounded-xl bg-white shadow-sm border border-gray-200 p-4"
)


# Height management - single source of truth
CHART_HEIGHTS = {
    "vertical_bar": 500,  # Views ranking, engagement
    "horizontal_bar": 480,  # For Y-axis categories
    "scatter": 500,  # More space for point visibility
    "bubble": 520,  # Large bubble radius needs room
    "radar": 520,  # 5+ axes need vertical space
    "line": 450,  # Less vertical than bar
    "treemap": 480,  # Hierarchical data
}


# Get height with fallback
def get_chart_height(chart_type: str) -> int:
    return CHART_HEIGHTS.get(chart_type, 500)


def _truncate_title(title: str, max_len: int = 50) -> str:
    """Safely truncate title with ellipsis."""
    if not title:
        return "Untitled"
    return title[:max_len] + ("..." if len(title) > max_len else "")


def _base_opts(chart_type: str, **extra) -> dict:
    """Common ApexCharts options - height now automatic."""
    height = get_chart_height(chart_type)
    return {
        "chart": {
            "type": chart_type,
            "height": height,
            "fontFamily": "Inter, system-ui, sans-serif",
            "toolbar": {"show": False},
            "zoom": {"enabled": False},
            "background": "transparent",
            **extra.get("chart", {}),
        },
        "theme": {"mode": "light", "palette": "palette1"},
        "colors": THEME_COLORS,
        "stroke": {"width": 2, "curve": "smooth"},
        "grid": {"show": True, "borderColor": "#e5e7eb", "strokeDashArray": 4},
        "dataLabels": {"enabled": False},
        "tooltip": {"theme": "light", "marker": {"show": True}},
        **extra,
    }


def _apex_opts(chart_type: str, **kwargs) -> dict:
    """Helper to build ApexChart options."""
    height = get_chart_height(chart_type)
    opts = {
        "chart": {"type": chart_type, "height": height, **kwargs.pop("chart", {})},
        "series": kwargs.pop("series", []),
        "xaxis": kwargs.pop("xaxis", {}),
        "yaxis": kwargs.pop("yaxis", {}),
    }
    opts.update(kwargs)
    return opts


def _empty_chart(chart_type: str, chart_id: str) -> ApexChart:
    """Return a placeholder empty chart when df is empty."""
    opts = _apex_opts(
        chart_type,
        series=[],
        chart={"id": chart_id},
        title={"text": "No data available", "align": "center"},
        subtitle={
            "text": "This chart will display when data is loaded",
            "align": "center",
        },
    )
    return ApexChart(opts=opts, cls=CHART_WRAPPER_CLS)


TOOLTIP_TRUNC = r"""
function({ series, seriesIndex, dataPointIndex, w }) {
    const pt = w.config.series?.[seriesIndex]?.data?.[dataPointIndex];
    if (!pt) {
        return '<div style="padding:8px;">No data</div>';
    }

    // Title truncation
    const rawTitle = String(pt.title || "");
    const title = rawTitle.length > 60 ? rawTitle.substring(0, 60) + '…' : rawTitle;

    // Build lines dynamically from pt object
    let lines = [];

    for (const [key, value] of Object.entries(pt)) {
        if (key === "title") continue;   // already printed
        if (value === null || value === undefined) continue;

        // Format large numbers using locale
        let formatted = value;

        if (typeof value === "number") {
            if (Math.abs(value) >= 1000) {
                formatted = value.toLocaleString();
            }
        }

        lines.push(`<b>${key}:</b> ${formatted}`);
    }

    return `
        <div style="padding:8px; max-width:260px; word-break:break-word;">
            <div><b>${title}</b></div>
            <div style="margin-top:6px;">
                ${lines.join("<br>")}
            </div>
        </div>
    `;
}
"""


# ----------------------
# Charts
# ----------------------


def chart_views_ranking(df: pl.DataFrame, chart_id: str = "views-ranking") -> ApexChart:
    """
    Line chart of views per video, sorted by view count descending.
    Gives instant insight into top and bottom performers.
    """
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    sorted_df = _safe_sort(df, "Views", descending=True)
    views_m = (sorted_df["Views"].fill_null(0) / 1_000_000).round(0).to_list()
    labels = sorted_df["Title"].to_list()

    opts = _base_opts(
        "bar",
        series=[{"name": "Views (Millions)", "data": views_m}],
        xaxis={"categories": labels, "labels": {"rotate": -45, "maxHeight": 80}},
        yaxis={"title": {"text": "Views (Millions)"}},
        title={"text": "🏆 Top Videos by Views", "align": "left"},
        plotOptions={"bar": {"borderRadius": 4, "columnWidth": "55%"}},
    )
    return ApexChart(
        opts=opts,
        cls=CHART_WRAPPER_CLS,
    )


def chart_engagement_ranking(
    df: pl.DataFrame, chart_id: str = "engagement-ranking"
) -> ApexChart:
    """Horizontal bar: Videos ranked by engagement rate."""
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    df = _safe_sort(df, "Engagement Rate Raw", descending=True)
    rates = (df["Engagement Rate Raw"] * 100).fill_null(0).round(2).to_list()
    avg = sum(rates) / len(rates) if rates else 0.0

    opts = _base_opts(
        "bar",
        series=[{"name": "Engagement %", "data": rates}],
        plotOptions={"bar": {"horizontal": True, "barHeight": "60%"}},
        xaxis={"title": {"text": "Engagement %"}},
        yaxis={"categories": df["Title"].to_list()},
        annotations={
            "xaxis": [
                {
                    "x": avg,
                    "borderColor": "#EF4444",
                    "label": {"text": f"Avg {avg:.1f}%"},
                }
            ]
        },
        title={"text": "Engagement Leaderboard", "align": "left"},
    )
    return ApexChart(
        opts=opts,
        cls=CHART_WRAPPER_CLS,
    )


def chart_engagement_breakdown(
    df: pl.DataFrame, chart_id: str = "engagement-breakdown"
) -> ApexChart:
    """Stacked bar: Show composition of engagement (likes, comments) per video."""
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    top_videos = _safe_sort(df, "Views", descending=True).head(10)

    data_likes = (top_videos["Likes"] / 1000).fill_null(0).to_list()
    data_comments = (top_videos["Comments"] / 100).fill_null(0).to_list()

    return ApexChart(
        opts=_apex_opts(
            "bar",
            series=[
                {"name": "👍 Likes (K)", "data": data_likes},
                {"name": "💬 Comments (×100)", "data": data_comments},
            ],
            xaxis={
                "categories": top_videos["Title"].to_list(),
                "labels": {"rotate": -45, "maxHeight": 60},
            },
            yaxis={"title": {"text": "Count"}},
            plotOptions={"bar": {"horizontal": False, "stacked": True}},
            title={"text": "📊 Engagement Composition (Top 10)", "align": "left"},
            colors=["#3B82F6", "#10B981"],
        ),
        cls=CHART_WRAPPER_CLS,
    )


def chart_likes_per_1k_views(
    df: pl.DataFrame, chart_id: str = "likes-per-1k"
) -> ApexChart:
    """Scatter: Likes per 1K views (engagement quality metric)."""
    if df is None or df.is_empty():
        return _empty_chart("scatter", chart_id)

    df_calc = df.with_columns(
        (pl.col("Likes") / (pl.col("Views") / 1000)).alias("Likes_per_1K")
    )

    data = [
        {
            "x": round((row["Views"] or 0) / 1_000_000, 1),
            "y": float(row["Likes_per_1K"] or 0),
            "title": _truncate_title(row["Title"]),
        }
        for row in df_calc.iter_rows(named=True)
    ]

    return ApexChart(
        opts={
            "chart": {"type": "scatter", "id": chart_id, "zoom": {"enabled": True}},
            "series": [{"name": "Videos", "data": data}],
            "xaxis": {"title": {"text": "Views (Millions)"}},
            "yaxis": {"title": {"text": "Likes per 1K Views"}},
            "title": {
                "text": "📊 Audience Quality: Likes per 1K Views",
                "align": "left",
            },
            "tooltip": {"custom": TOOLTIP_TRUNC},
        },
        cls=CHART_WRAPPER_CLS,
    )


def chart_performance_heatmap(
    df: pl.DataFrame, chart_id: str = "performance-heatmap"
) -> ApexChart:
    """Heatmap: Views (rows) vs Engagement (cols) - see where videos cluster."""
    if df is None or df.is_empty():
        return _empty_chart("heatmap", chart_id)

    # Bin videos into quadrants
    view_median = df["Views"].median()
    eng_median = df["Engagement Rate Raw"].median()

    quadrants = {
        "🔥 High Views\nHigh Engage": len(
            df.filter(
                (df["Views"] > view_median) & (df["Engagement Rate Raw"] > eng_median)
            )
        ),
        "👀 High Views\nLow Engage": len(
            df.filter(
                (df["Views"] > view_median) & (df["Engagement Rate Raw"] <= eng_median)
            )
        ),
        "💪 Low Views\nHigh Engage": len(
            df.filter(
                (df["Views"] <= view_median) & (df["Engagement Rate Raw"] > eng_median)
            )
        ),
        "📊 Low Views\nLow Engage": len(
            df.filter(
                (df["Views"] <= view_median) & (df["Engagement Rate Raw"] <= eng_median)
            )
        ),
    }

    return ApexChart(
        opts=_apex_opts(
            "bar",
            series=[{"name": "Videos", "data": list(quadrants.values())}],
            xaxis={"categories": list(quadrants.keys())},
            title={"text": "🎯 Performance Quadrants", "align": "left"},
            colors=["#8B5CF6"],
            plotOptions={"bar": {"columnWidth": "70%"}},
        ),
        cls=CHART_WRAPPER_CLS,
    )


def chart_controversy_score(
    df: pl.DataFrame, chart_id: str = "controversy"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    data = (df["Controversy Raw"].fill_null(0) * 100).to_list()
    opts = _apex_opts(
        "bar",
        series=[{"name": "Controversy", "data": data}],
        xaxis={"title": {"text": "Controversy (%)"}},
        yaxis={"categories": df["Title"].to_list()},
        plotOptions={"bar": {"horizontal": True}},
        chart={"id": chart_id},
        tooltip={"enabled": True},
    )
    return ApexChart(opts=opts, cls=CHART_WRAPPER_CLS)


def chart_treemap_views(df: pl.DataFrame, chart_id: str = "treemap-views") -> ApexChart:
    """
    Treemap of video views, showing contribution to overall reach.
    """
    if df is None or df.is_empty():
        return _empty_chart("treemap", chart_id)

    data = [
        {"x": row["Title"], "y": row["Views"] or 0} for row in df.iter_rows(named=True)
    ]
    opts = _apex_opts(
        "treemap",
        series=[{"data": data}],
        legend={"show": False},
        title={"text": "Views Distribution by Video", "align": "center"},
        dataLabels={"enabled": True, "style": {"fontSize": "11px"}},
        chart={"id": chart_id},
        tooltip={"enabled": True},
    )
    return ApexChart(opts=opts, cls=CHART_WRAPPER_CLS)


def chart_scatter_likes_dislikes(
    df: pl.DataFrame, chart_id: str = "scatter-likes"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("scatter", chart_id)

    # Use dicts with x, y, and custom field "title"
    data = [
        {
            "x": int(row["Likes"] or 0),
            "y": int(row["Dislikes"] or 0),
            "title": _truncate_title(row["Title"]),
        }
        for row in df.iter_rows(named=True)
    ]

    opts = {
        "chart": {"id": chart_id, "type": "scatter", "zoom": {"enabled": True}},
        "series": [{"name": "Videos", "data": data}],
        "xaxis": {"title": {"text": "👍 Like Count"}},
        "yaxis": {"title": {"text": "👎 Dislike Count"}},
        "title": {"text": "Likes vs Dislikes Correlation", "align": "center"},
        "tooltip": {"custom": TOOLTIP_TRUNC},
    }

    return ApexChart(opts=opts, cls=CHART_WRAPPER_CLS)


def chart_bubble_engagement_vs_views(
    df: pl.DataFrame, chart_id: str = "bubble-engagement"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("bubble", chart_id)

    # Build data as required: array of [x, y, z] with extra "title" kept separately
    data = [
        {
            "x": round((row["Views"] or 0) / 1_000_000, 2),
            "y": float(row["Engagement Rate Raw"] or 0.0) * 100,
            "z": round(float(row["Controversy"] or 0.0), 1),
            "title": row["Title"],
        }
        for row in df.iter_rows(named=True)
    ]

    opts = _apex_opts(
        "bubble",
        series=[{"name": "Videos", "data": data}],
        xaxis={
            "title": {"text": "View Count (Millions)"},
            "labels": {"formatter": "function(val){ return val + 'M'; }"},
        },
        yaxis={"title": {"text": "Engagement Rate (%)"}},
        tooltip={"custom": TOOLTIP_TRUNC},
        plotOptions={"bubble": {"minBubbleRadius": 5, "maxBubbleRadius": 30}},
        chart={"id": chart_id, "toolbar": {"show": True}, "zoom": {"enabled": True}},
        title={"text": "Engagement vs Views Analysis", "align": "center"},
    )
    return ApexChart(opts=opts, cls=CHART_WRAPPER_CLS)


def chart_duration_vs_engagement(
    df: pl.DataFrame, chart_id: str = "duration-engagement"
) -> ApexChart:
    """Bubble: Duration (X) vs Engagement (Y), bubble size = Views."""
    if df is None or df.is_empty():
        return _empty_chart("bubble", chart_id)

    df_prep = df.with_columns((pl.col("Duration") / 60).alias("Duration_min"))

    data = [
        {
            "x": float(row["Duration_min"] or 0),
            "y": float(row["Engagement Rate Raw"] or 0) * 100,
            "z": round((row["Views"] or 0) / 1_000_000, 1),
            "title": (row["Title"] or "Untitled")[:40],
        }
        for row in df_prep.iter_rows(named=True)
    ]

    return ApexChart(
        opts=_apex_opts(
            "bubble",
            series=[{"name": "Videos", "data": data}],
            xaxis={
                "title": {"text": "Duration (minutes)"},
                "labels": {"formatter": "function(val){ return val.toFixed(1); }"},
            },
            yaxis={"title": {"text": "Engagement Rate (%)"}},
            tooltip={
                "custom": (
                    "function({series, seriesIndex, dataPointIndex, w}) {"
                    "var pt = w.config.series[seriesIndex].data[dataPointIndex];"
                    "return `<div style='padding:6px;'><b>${pt.title}</b><br>"
                    "⏱️ Duration: ${pt.x.toFixed(1)}m<br>"
                    "✨ Engagement: ${pt.y.toFixed(1)}%<br>"
                    "👀 Views: ${pt.z}M</div>`;"
                    "}"
                )
            },
            plotOptions={"bubble": {"minBubbleRadius": 5, "maxBubbleRadius": 35}},
            title={"text": "⏱️ Length vs Engagement (bubble = views)", "align": "left"},
        ),
        cls=CHART_WRAPPER_CLS,
    )


def chart_video_radar(
    df: pl.DataFrame, chart_id: str = "video-radar", top_n: int = 5
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("radar", chart_id)

    # Pick top videos by view count
    sorted_df = _safe_sort(df, "Views", descending=True).head(top_n)

    # Columns we want to normalize
    numeric_cols = [
        "Views",
        "Likes",
        "Comments",
        "Engagement Rate Raw",
    ]

    # Cast numeric columns explicitly to Float64 for safe arithmetic
    cast_df = sorted_df.with_columns(
        [pl.col(c).cast(pl.Float64).fill_null(0) for c in numeric_cols]
    )

    # Normalize per metric to 0–100 scale
    norm_df = cast_df.with_columns(
        [
            (
                (
                    pl.col(c)
                    / pl.when(pl.col(c).max() > 0).then(pl.col(c).max()).otherwise(1)
                )
                * 100
            ).alias(f"{c}_norm")
            for c in numeric_cols
        ]
    )

    # Build series for radar chart
    series = []
    for row in norm_df.iter_rows(named=True):
        series.append(
            {
                "name": _truncate_title(row["Title"]),  # Truncate long titles
                "data": [row[f"{c}_norm"] for c in numeric_cols],
            }
        )

    opts = _apex_opts(
        "radar",
        series=series,
        labels=numeric_cols,
        chart={"id": chart_id},
        title={"text": "Top Videos: Multi-Metric Comparison", "align": "center"},
    )

    return ApexChart(opts=opts, cls=CHART_WRAPPER_CLS)


def chart_comments_engagement(
    df: pl.DataFrame, chart_id: str = "comments-engagement"
) -> ApexChart:
    """Scatter: Comments vs Engagement Rate."""
    if df is None or df.is_empty():
        return _empty_chart("scatter", chart_id)

    data = [
        {
            "x": int(row["Comments"] or 0),
            "y": round(float(row["Engagement Rate Raw"] or 0) * 100, 2),
            "title": _truncate_title(row["Title"]),
        }
        for row in df.iter_rows(named=True)
    ]

    return ApexChart(
        opts={
            "chart": {"type": "scatter", "id": chart_id, "zoom": {"enabled": True}},
            "series": [{"name": "Videos", "data": data}],
            "xaxis": {"title": {"text": "💬 Comments"}},
            "yaxis": {"title": {"text": "Engagement Rate (%)"}},
            "title": {"text": "💬 Comments vs Engagement", "align": "left"},
        },
        cls=CHART_WRAPPER_CLS,
    )


def chart_views_vs_likes(
    df: pl.DataFrame, chart_id: str = "views-vs-likes"
) -> ApexChart:
    """Bubble: Views (X) vs Likes (Y), size = Comments."""
    if df is None or df.is_empty():
        return _empty_chart("bubble", chart_id)

    data = [
        {
            "x": round((row["Views"] or 0) / 1_000_000, 1),
            "y": round((row["Likes"] or 0) / 1000, 1),
            "z": int((row["Comments"] or 0) / 100),
            "title": _truncate_title(row["Title"]),
        }
        for row in df.iter_rows(named=True)
    ]

    return ApexChart(
        opts={
            "chart": {"type": "bubble", "id": chart_id, "zoom": {"enabled": True}},
            "series": [{"name": "Videos", "data": data}],
            "xaxis": {"title": {"text": "👀 Views (Millions)"}},
            "yaxis": {"title": {"text": "👍 Likes (Thousands)"}},
            "title": {
                "text": "🎯 Views vs Likes (bubble size = comments)",
                "align": "left",
            },
            "tooltip": {"custom": TOOLTIP_TRUNC},
            "plotOptions": {"bubble": {"minBubbleRadius": 4, "maxBubbleRadius": 25}},
        },
        cls=CHART_WRAPPER_CLS,
    )


def chart_duration_impact(
    df: pl.DataFrame, chart_id: str = "duration-impact"
) -> ApexChart:
    """Line: Engagement rate by video duration (sorted)."""
    if df is None or df.is_empty():
        return _empty_chart("line", chart_id)

    sorted_df = _safe_sort(df, "Duration", descending=False)
    durations = (
        sorted_df["Duration"].cast(pl.Float64) / 60
    ).to_list()  # Convert to minutes
    eng_rates = (sorted_df["Engagement Rate Raw"]).to_list()

    return ApexChart(
        opts={
            "chart": {"type": "line", "id": chart_id},
            "series": [{"name": "Engagement Rate (%)", "data": eng_rates}],
            "xaxis": {
                "title": {"text": "Video Duration (minutes)"},
                "categories": [f"{d:.1f}m" for d in durations],
            },
            "yaxis": {"title": {"text": "Engagement Rate (%)"}},
            "title": {"text": "⏱️ Does Duration Affect Engagement?", "align": "left"},
            "stroke": {"curve": "smooth"},
        },
        cls=CHART_WRAPPER_CLS,
    )


def chart_category_performance(
    df: pl.DataFrame, chart_id: str = "category-performance"
) -> ApexChart:
    """Bar: Average engagement by category."""
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    by_cat = df.group_by("CategoryName").agg(
        pl.col("Views").mean().alias("Avg_Views"),
        pl.col("Engagement Rate Raw").mean().alias("Avg_Engagement"),
        pl.col("id").count().alias("Count"),
    )

    categories = by_cat["CategoryName"].to_list()
    avg_eng = (by_cat["Avg_Engagement"] * 100).fill_null(0).to_list()

    return ApexChart(
        opts={
            "chart": {"type": "bar", "id": chart_id},
            "series": [{"name": "Avg Engagement (%)", "data": avg_eng}],
            "xaxis": {"categories": categories, "labels": {"rotate": -45}},
            "colors": ["#10B981"],
            "title": {"text": "📁 Average Engagement by Category", "align": "left"},
        },
        cls=CHART_WRAPPER_CLS,
    )


def chart_controversy_distribution(
    df: pl.DataFrame, chart_id: str = "controversy-dist"
) -> ApexChart:
    """Horizontal bar: Controversy score by video."""
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    sorted_df = _safe_sort(df, "Controversy", descending=True)
    titles = sorted_df["Title"].to_list()
    controversy = (sorted_df["Controversy"] * 100).fill_null(0).to_list()

    return ApexChart(
        opts={
            "chart": {"type": "bar", "id": chart_id},
            "series": [{"name": "Controversy (%)", "data": controversy}],
            "plotOptions": {"bar": {"horizontal": True}},
            "xaxis": {"title": {"text": "Controversy %"}},
            "yaxis": {"categories": titles},
            "colors": ["#FF6B6B"],
            "title": {
                "text": "🔥 Most Polarizing Videos (0=Unanimous, 100=Split)",
                "align": "left",
            },
        },
        cls=CHART_WRAPPER_CLS,
    )


def chart_treemap_reach(df: pl.DataFrame, chart_id: str = "treemap-reach") -> ApexChart:
    """Treemap: Video contribution to total views."""
    if df is None or df.is_empty():
        return _empty_chart("treemap", chart_id)

    data = [
        {"x": row["Title"][:50], "y": row["Views"] or 0}
        for row in df.iter_rows(named=True)
    ]

    return ApexChart(
        opts={
            "chart": {"type": "treemap", "id": chart_id},
            "series": [{"data": data}],
            "title": {
                "text": "📊 Reach Distribution (larger = more views)",
                "align": "left",
            },
            "dataLabels": {"enabled": True},
        },
        cls=CHART_WRAPPER_CLS,
    )


def chart_top_performers_radar(
    df: pl.DataFrame, chart_id: str = "top-radar", top_n: int = 5
) -> ApexChart:
    """Radar: Top N videos across 4 metrics."""
    if df is None or df.is_empty():
        return _empty_chart("radar", chart_id)

    sorted_df = _safe_sort(df, "Views", descending=True).head(top_n)
    metrics = ["Views", "Likes", "Comments", "Engagement Rate Raw"]

    cast_df = sorted_df.with_columns(
        [pl.col(c).cast(pl.Float64).fill_null(0) for c in metrics]
    )

    norm_df = cast_df.with_columns(
        [(pl.col(c) / pl.col(c).max() * 100).alias(f"{c}_norm") for c in metrics]
    )

    series = []
    for row in norm_df.iter_rows(named=True):
        series.append(
            {
                "name": row["Title"][:25],
                "data": [row[f"{c}_norm"] for c in metrics],
            }
        )

    return ApexChart(
        opts={
            "chart": {"type": "radar", "id": chart_id},
            "series": series,
            "labels": ["Views", "Likes", "Comments", "Engagement %"],
            "title": {
                "text": f"🌟 Top {top_n} Videos: Multi-Metric Radar",
                "align": "left",
            },
        },
        cls=CHART_WRAPPER_CLS,
    )
