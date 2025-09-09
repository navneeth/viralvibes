from typing import Dict

import polars as pl
from monsterui.all import ApexChart


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


def chart_views_by_rank(df: pl.DataFrame, chart_id: str = "views-rank") -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("line", chart_id)

    data = (df["View Count Raw"].fill_null(0) / 1_000_000).round(1).to_list()
    opts = _apex_opts(
        "line",
        300,
        series=[{"name": "Views (in Millions)", "data": data}],
        xaxis={
            "categories": df["Rank"].cast(pl.Int64).to_list(),
            "title": {"text": "Video Rank"},
        },
        yaxis={"title": {"text": "Views (Millions)"}},
        chart={"id": chart_id, "zoom": {"enabled": False}, "toolbar": {"show": False}},
        tooltip={"enabled": True},  # Use built-in tooltip
    )
    return ApexChart(opts=opts, cls="w-full h-80")


def chart_polarizing_videos(
    df: pl.DataFrame, chart_id: str = "polarizing-videos"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("bubble", chart_id)

    # Vectorized extraction
    mask = (
        (df["Like Count Raw"] > 0)
        & (df["Dislike Count Raw"] > 0)
        & (df["View Count Raw"] > 0)
    )
    filtered = df.filter(mask)

    if len(filtered) == 0:
        # Handle empty data case
        data = []
    else:
        data = [
            {
                "x": int(row["Like Count Raw"]),
                "y": int(row["Dislike Count Raw"]),
                "z": round(row["View Count Raw"] / 1_000_000, 2),
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


def chart_engagement_rate(
    df: pl.DataFrame, chart_id: str = "engagement-rate"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    data = df["Engagement Rate (%)"].cast(pl.Float64).fill_null(0).to_list()
    opts = _apex_opts(
        "bar",
        300,
        series=[{"name": "Engagement Rate (%)", "data": data}],
        xaxis={
            "categories": df["Title"].to_list(),
            "labels": {"rotate": -45, "style": {"fontSize": "10px"}},
        },
        chart={"id": chart_id},
        tooltip={"enabled": True},
    )
    return ApexChart(opts=opts, cls="w-full h-80")


def chart_likes_vs_dislikes(
    df: pl.DataFrame, chart_id: str = "likes-dislikes"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    opts = _apex_opts(
        "bar",
        300,
        series=[
            {"name": "👍 Likes", "data": df["Like Count Raw"].fill_null(0).to_list()},
            {
                "name": "👎 Dislikes",
                "data": df["Dislike Count Raw"].fill_null(0).to_list(),
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


def chart_total_engagement(
    summary: Dict, chart_id: str = "total-engagement"
) -> ApexChart:
    if not summary or "total_likes" not in summary:
        return _empty_chart("donut", chart_id)

    estimated_dislikes = int(summary.get("total_views", 0) * 0.02)
    opts = _apex_opts(
        "donut",
        300,
        labels=["Total Likes", "Est. Dislikes"],
        series=[summary["total_likes"], estimated_dislikes],
        chart={"id": chart_id},
        tooltip={"enabled": True},
        colors=["#22C55E", "#EF4444"],
    )
    return ApexChart(opts=opts, cls="w-full h-80")


def chart_treemap_views(df: pl.DataFrame, chart_id: str = "treemap-views") -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("treemap", chart_id)

    data = [
        {"x": row["Title"], "y": row["View Count Raw"] or 0}
        for row in df.iter_rows(named=True)
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

    data = [
        [row["Like Count Raw"] or 0, row["Dislike Count Raw"] or 0]
        for row in df.iter_rows(named=True)
    ]
    opts = _apex_opts(
        "scatter",
        350,
        series=[{"name": "Videos", "data": data}],
        xaxis={
            "title": {"text": "👍 Like Count"},
            "labels": {
                "formatter": "function(val){ return val >= 1e6 ? (val/1e6).toFixed(1) + 'M' : val >= 1e3 ? (val/1e3).toFixed(0) + 'K' : val; }"
            },
        },
        yaxis={
            "title": {"text": "👎 Dislike Count"},
            "labels": {
                "formatter": "function(val){ return val >= 1e6 ? (val/1e6).toFixed(1) + 'M' : val >= 1e3 ? (val/1e3).toFixed(0) + 'K' : val; }"
            },
        },
        # tooltip={
        #     "custom": (
        #         "function({series, seriesIndex, dataPointIndex, w}) {"
        #         "const val = w.globals.initialSeries[0].data[dataPointIndex];"
        #         "return Likes: ${val[0]}<br>Dislikes: ${val[1]};"
        #         "}"
        #     )
        # },
        tooltip={"enabled": True},
        chart={"id": chart_id, "zoom": {"enabled": True}},
        title={"text": "Likes vs Dislikes Correlation", "align": "center"},
    )
    return ApexChart(opts=opts, cls="w-full h-[22rem]")


def chart_bubble_engagement_vs_views(
    df: pl.DataFrame, chart_id: str = "bubble-engagement"
) -> ApexChart:
    if df is None or df.is_empty():
        return _empty_chart("bubble", chart_id)

    data = [
        {
            "x": round(row["View Count Raw"] / 1_000_000, 2)
            if row["View Count Raw"] is not None
            else 0,
            "y": float(row["Engagement Rate (%)"])
            if row["Engagement Rate (%)"] is not None
            else 0.0,
            "z": round(float(row["Controversy Raw"]) * 100, 1)
            if row["Controversy Raw"] is not None
            else 0.0,
            "name": row["Title"],
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
        # tooltip={
        #     "custom": (
        #         "function({ series, seriesIndex, dataPointIndex, w }) {"
        #         "const pt = w.globals.initialSeries[seriesIndex].data[dataPointIndex];"
        #         "return `<div style='padding:6px; max-width:250px;'>"
        #         "<strong>${pt.name}</strong><br>"
        #         "📺 Views: ${pt.x}M<br>"
        #         "✨ Engagement: ${pt.y}%<br>"
        #         "⚡ Controversy: ${pt.z}%</div>`;"
        #         "}"
        #     )
        # },
        tooltip={"enabled": True, "shared": False, "followCursor": True},
        plotOptions={"bubble": {"minBubbleRadius": 5, "maxBubbleRadius": 30}},
        chart={"id": chart_id},
        title={"text": "Engagement vs Views Analysis", "align": "center"},
    )
    return ApexChart(opts=opts, cls="w-full h-[22rem]")
