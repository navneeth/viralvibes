import logging
from typing import Dict

import polars as pl
from monsterui.all import ApexChart

logger = logging.getLogger("charts")


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
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

        # Newer Polars signature
        return df.sort(by=col, descending=descending)

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


def _apex_opts(chart_type: str, height: int = 350, **kwargs) -> dict:
    """Helper to build ApexChart options."""
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
        350,
        series=[],
        chart={"id": chart_id},
        title={"text": "No data available", "align": "center"},
        subtitle={
            "text": "This chart will display when data is loaded",
            "align": "center",
        },
    )
    return ApexChart(opts=opts, cls="w-full h-[22rem]")


# ----------------------
# Charts
# ----------------------


def chart_views_ranking(df: pl.DataFrame, chart_id: str = "views-ranking") -> ApexChart:
    """
    Line chart of views per video, sorted by view count descending.
    Gives instant insight into top and bottom performers.
    """
    if df is None or df.is_empty():
        return _empty_chart("line", chart_id)

    # Sort videos by view count descending
    sorted_df = _safe_sort(df, "Views", descending=True)

    views_m = (sorted_df["Views"].fill_null(0) / 1_000_000).round(1).to_list()
    titles = sorted_df["Title"].to_list()

    opts = _apex_opts(
        "bar",
        300,
        series=[{"name": "Views (in Millions)", "data": views_m}],
        xaxis={
            "categories": titles,
            "title": {"text": "Video Title"},
            "labels": {"rotate": -45, "style": {"fontSize": "10px"}},
        },
        yaxis={"title": {"text": "Views (Millions)"}},
        chart={"id": chart_id, "zoom": {"enabled": False}, "toolbar": {"show": False}},
        tooltip={"enabled": True},
        title={"text": "🏆 Top Videos by Views", "align": "left"},
    )
    return ApexChart(opts=opts, cls="w-full h-80")


def chart_polarizing_videos(
    df: pl.DataFrame, chart_id: str = "polarizing-videos"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("bubble", chart_id)

    # Vectorized extraction
    mask = (df["Likes"] > 0) & (df["Dislikes"] > 0) & (df["Views"] > 0)
    filtered = df.filter(mask)

    if len(filtered) == 0:
        # Handle empty data case
        data = []
    else:
        data = [
            {
                "x": int(row["Likes"]),
                "y": int(row["Dislikes"]),
                "z": round(row["Views"] / 1_000_000, 2),
                "name": row["Title"][:40],
            }
            for row in filtered.iter_rows(named=True)
        ]

    opts = _apex_opts(
        "bubble",
        350,
        series=[{"name": "Videos", "data": data}],
        chart={
            "id": chart_id,  # ✅ give chart a stable unique id
            "toolbar": {"show": True},
            "zoom": {"enabled": True},
        },
        dataLabels={"enabled": False},
        xaxis={"title": {"text": "👍 Likes"}},
        yaxis={"title": {"text": "👎 Dislikes"}},
        # tooltip={
        #     "shared": False,
        #     "intersect": True,
        #     "custom": """
        #         function({series, seriesIndex, dataPointIndex, w}) {
        #             var pt = w.config.series[seriesIndex].data[dataPointIndex];
        #             return  '<div style="padding:6px;"><b>' + pt.name + '</b><br>' +
        #                     'Likes: ' + pt.x.toLocaleString() + '<br>' +
        #                     'Dislikes: ' + pt.y.toLocaleString() + '<br>' +
        #                     'Views: ' + pt.z + 'M</div>';
        #         }
        #     """,
        # },
        tooltip={
            "enabled": True,
            "shared": False,
            "intersect": True,
            "followCursor": True,
            "marker": {"show": True},
        },
        plotOptions={"bubble": {"minBubbleRadius": 5, "maxBubbleRadius": 30}},
        title={"text": "Video Polarization Analysis", "align": "center"},
    )
    return ApexChart(opts=opts, cls="w-full h-96")


def chart_engagement_ranking(
    df: pl.DataFrame, chart_id: str = "engagement-ranking"
) -> ApexChart:
    """Horizontal bar: Videos ranked by engagement rate."""
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    sorted_df = _safe_sort(df, "Engagement Rate Raw", descending=True)
    titles = sorted_df["Title"].to_list()
    eng_rates = (sorted_df["Engagement Rate Raw"] * 100).fill_null(0).to_list()
    avg_eng = float(sorted_df["Engagement Rate Raw"].mean() or 0) * 100

    return ApexChart(
        opts={
            "chart": {"type": "bar", "id": chart_id},
            "series": [{"name": "Engagement Rate (%)", "data": eng_rates}],
            "plotOptions": {"bar": {"horizontal": True}},
            "xaxis": {"title": {"text": "Engagement Rate (%)"}},
            "yaxis": {"categories": titles},
            "colors": ["#F59E0B"],
            "title": {"text": "💬 Videos Ranked by Engagement", "align": "left"},
            "annotations": {
                "xaxis": [
                    {
                        "x": avg_eng,
                        "borderColor": "#EF4444",
                        "label": {"text": f"Avg: {avg_eng:.1f}%"},
                    }
                ]
            },
        },
        cls="w-full h-96",
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
            "title": row["Title"][:40],
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
            "tooltip": {
                "custom": (
                    "function({series, seriesIndex, dataPointIndex, w}) {"
                    "var pt = w.config.series[seriesIndex].data[dataPointIndex];"
                    "return `<div style='padding:6px;'><b>${pt.title}</b><br>"
                    "👀 Views: ${pt.x}M<br>"
                    "👍 Likes per 1K: ${pt.y.toFixed(2)}</div>`;"
                    "}"
                )
            },
        },
        cls="w-full h-80",
    )


def chart_likes_vs_dislikes(
    df: pl.DataFrame, chart_id: str = "likes-dislikes"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    opts = _apex_opts(
        "bar",
        300,
        series=[
            {"name": "👍 Likes", "data": df["Likes"].fill_null(0).to_list()},
            {
                "name": "👎 Dislikes",
                "data": df["Dislikes"].fill_null(0).to_list(),
            },
        ],
        xaxis={
            "categories": df["Title"].to_list(),
            "labels": {"rotate": -45, "style": {"fontSize": "10px"}},
        },
        plotOptions={"bar": {"horizontal": False, "columnWidth": "45%"}},
        chart={
            "id": chart_id,  # ✅ give chart a stable unique id
            "stacked": False,
        },
        tooltip={"enabled": True},
        colors=["#22C55E", "#EF4444"],  # Green for likes, red for dislikes
    )
    return ApexChart(opts=opts, cls="w-full h-80")


def chart_controversy_score(
    df: pl.DataFrame, chart_id: str = "controversy"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    data = (df["Controversy Raw"].fill_null(0) * 100).to_list()
    opts = _apex_opts(
        "bar",
        300,
        series=[{"name": "Controversy", "data": data}],
        xaxis={"title": {"text": "Controversy (%)"}},
        yaxis={"categories": df["Title"].to_list()},
        plotOptions={"bar": {"horizontal": True}},
        chart={"id": chart_id},
        tooltip={"enabled": True},
    )
    return ApexChart(opts=opts, cls="w-full h-80")


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
        350,
        series=[{"data": data}],
        legend={"show": False},
        title={"text": "Views Distribution by Video", "align": "center"},
        dataLabels={"enabled": True, "style": {"fontSize": "11px"}},
        chart={"id": chart_id},
        tooltip={"enabled": True},
    )
    return ApexChart(opts=opts, cls="w-full h-[22rem]")


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
            "title": row["Title"][:40],  # truncate to avoid overflow
        }
        for row in df.iter_rows(named=True)
    ]

    opts = {
        "chart": {"id": chart_id, "type": "scatter", "zoom": {"enabled": True}},
        "series": [{"name": "Videos", "data": data}],
        "xaxis": {"title": {"text": "👍 Like Count"}},
        "yaxis": {"title": {"text": "👎 Dislike Count"}},
        "title": {"text": "Likes vs Dislikes Correlation", "align": "center"},
        "tooltip": {
            "custom": (
                "function({series, seriesIndex, dataPointIndex, w}) {"
                "  var pt = w.config.series[seriesIndex].data[dataPointIndex];"
                "  return `<div style='padding:6px; max-width:250px;'>"
                "    <strong>${pt.title}</strong><br>"
                "    👍 Likes: ${pt.x.toLocaleString()}<br>"
                "    👎 Dislikes: ${pt.y.toLocaleString()}"
                "  </div>`;"
                "}"
            )
        },
    }

    return ApexChart(opts=opts, cls="w-full h-[22rem]")


def chart_bubble_engagement_vs_views(
    df: pl.DataFrame, chart_id: str = "bubble-engagement"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("bubble", chart_id)

    # Build data as required: array of [x, y, z] with extra "title" kept separately
    data = [
        {
            "x": round((row["Views"] or 0) / 1_000_000, 2),
            "y": float(row["Engagement Rate Raw"] or 0.0),
            "z": round(float(row["Controversy"] or 0.0) * 100, 1),
            "title": row["Title"][:60],  # truncate long titles
        }
        for row in df.iter_rows(named=True)
    ]

    opts = _apex_opts(
        "bubble",
        350,
        series=[{"name": "Videos", "data": data}],
        xaxis={
            "title": {"text": "View Count (Millions)"},
            "labels": {"formatter": "function(val){ return val + 'M'; }"},
        },
        yaxis={"title": {"text": "Engagement Rate (%)"}},
        tooltip={
            "custom": (
                "function({series, seriesIndex, dataPointIndex, w}) {"
                "var pt = w.config.series[seriesIndex].data[dataPointIndex];"
                "return `<div style='padding:6px; max-width:250px;'>"
                "<strong>${pt.title}</strong><br>"
                "📺 Views: ${pt.x}M<br>"
                "✨ Engagement: ${pt.y}%<br>"
                "⚡ Controversy: ${pt.z}%</div>`;"
                "}"
            )
        },
        plotOptions={"bubble": {"minBubbleRadius": 5, "maxBubbleRadius": 30}},
        chart={"id": chart_id, "toolbar": {"show": True}, "zoom": {"enabled": True}},
        title={"text": "Engagement vs Views Analysis", "align": "center"},
    )
    return ApexChart(opts=opts, cls="w-full h-[22rem]")


def chart_duration_vs_engagement(
    df: pl.DataFrame, chart_id: str = "duration-engagement"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("scatter", chart_id)

    # Convert duration from seconds to minutes
    df = df.with_columns((pl.col("Duration") / 60).alias("Duration (min)"))

    # Build scatter points with custom tooltips
    data = [
        {
            "x": row["Duration (min)"] or 0,
            "y": row["Engagement Rate (%)"] or 0,
            "title": row["Title"] or "Untitled",
        }
        for row in df.iter_rows(named=True)
    ]

    opts = _apex_opts(
        "scatter",
        350,
        series=[{"name": "Videos", "data": data}],
        xaxis={
            "title": {"text": "Duration (minutes)"},
            "labels": {"formatter": "function(val) { return val.toFixed(1); }"},
        },
        yaxis={"title": {"text": "Engagement Rate (%)"}},
        tooltip={
            "enabled": True,
            "custom": """
                function({series, seriesIndex, dataPointIndex, w}) {
                    const d = w.globals.initialSeries[seriesIndex].data[dataPointIndex];
                    return `<b>${d.title}</b><br/>Duration: ${d.x} min<br/>Engagement: ${d.y.toFixed(2)}%`;
                }
            """,
        },
        chart={"id": chart_id},
        title={"text": "Does Video Length Affect Engagement?", "align": "center"},
    )
    return ApexChart(opts=opts, cls="w-full h-80")


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
        [(pl.col(c) / pl.col(c).max() * 100).alias(f"{c}_norm") for c in numeric_cols]
    )

    # Build series for radar chart
    series = []
    for row in norm_df.iter_rows(named=True):
        series.append(
            {
                "name": row["Title"][:20],  # truncate long titles
                "data": [row[f"{c}_norm"] for c in numeric_cols],
            }
        )

    opts = _apex_opts(
        "radar",
        350,
        series=series,
        labels=numeric_cols,
        chart={"id": chart_id},
        title={"text": "Top Videos: Multi-Metric Comparison", "align": "center"},
    )

    return ApexChart(opts=opts, cls="w-full h-96")


def chart_comments_engagement(
    df: pl.DataFrame, chart_id: str = "comments-engagement"
) -> ApexChart:
    """Scatter: Comments vs Engagement Rate."""
    if df is None or df.is_empty():
        return _empty_chart("scatter", chart_id)

    data = [
        {
            "x": row["Comments"] or 0,
            "y": float(row["Engagement Rate Raw"] or 0) * 100,
            "title": row["Title"][:40],
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
        cls="w-full h-80",
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
            "title": row["Title"][:40],
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
            "tooltip": {
                "custom": (
                    "function({series, seriesIndex, dataPointIndex, w}) {"
                    "var pt = w.config.series[seriesIndex].data[dataPointIndex];"
                    "return `<div style='padding:6px;'><b>${pt.title}</b><br>"
                    "👀 Views: ${pt.x}M<br>"
                    "👍 Likes: ${pt.y}K<br>"
                    "💬 Comments: ~${(pt.z*100).toLocaleString()}</div>`;"
                    "}"
                )
            },
            "plotOptions": {"bubble": {"minBubbleRadius": 4, "maxBubbleRadius": 25}},
        },
        cls="w-full h-96",
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
    eng_rates = (sorted_df["Engagement Rate Raw"] * 100).to_list()

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
        cls="w-full h-80",
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
        cls="w-full h-80",
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
        cls="w-full h-96",
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
        cls="w-full h-80",
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
        cls="w-full h-80",
    )
