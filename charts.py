from typing import Dict

import polars as pl
from monsterui.all import ApexChart


def _apex_opts(chart_type: str, height: int = 300, **kwargs) -> dict:
    """Helper to build ApexChart options."""
    opts = {
        "chart": {"type": chart_type, "height": height, **kwargs.pop("chart", {})},
        "series": kwargs.pop("series", []),
        "xaxis": kwargs.pop("xaxis", {}),
        "yaxis": kwargs.pop("yaxis", {}),
    }
    opts.update(kwargs)
    return opts


def chart_views_by_rank(df: pl.DataFrame) -> ApexChart:
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
        chart={"zoom": {"enabled": False}, "toolbar": {"show": False}},
    )
    return ApexChart(opts=opts, cls="w-full h-80")


def chart_polarizing_videos(df: pl.DataFrame) -> ApexChart:
    # Vectorized extraction
    likes = df["Like Count Raw"].fill_null(0).cast(pl.Int64)
    dislikes = df["Dislike Count Raw"].fill_null(0).cast(pl.Int64)
    views = df["View Count Raw"].fill_null(0).cast(pl.Int64)
    titles = df["Title"].fill_null("").str.slice(0, 40)
    mask = (likes > 0) & (dislikes > 0) & (views > 0)
    filtered = df.filter(mask)
    likes = filtered["Like Count Raw"]
    dislikes = filtered["Dislike Count Raw"]
    views = filtered["View Count Raw"]
    titles = filtered["Title"].fill_null("").str.slice(0, 40)
    data = [
        {"x": int(l), "y": int(d), "z": round(v / 1_000_000, 2), "name": t}
        for l, d, v, t in zip(likes, dislikes, views, titles)
    ]
    opts = _apex_opts(
        "bubble",
        350,
        series=[{"name": "Videos", "data": data}],
        chart={"toolbar": {"show": True}, "zoom": {"enabled": True}},
        dataLabels={"enabled": False},
        xaxis={"title": {"text": "Likes"}},
        yaxis={"title": {"text": "Dislikes"}},
        tooltip={
            "shared": False,
            "intersect": True,
            "custom": (
                "function({series, seriesIndex, dataPointIndex, w}) {"
                "var pt = w.config.series[seriesIndex].data[dataPointIndex];"
                "return <b>${pt.name}</b><br>Likes: ${pt.x.toLocaleString()}<br>"
                "Dislikes: ${pt.y.toLocaleString()}<br>Views: ${pt.z.toLocaleString()};"
                "}"
            ),
        },
        plotOptions={"bubble": {"minBubbleRadius": 5, "maxBubbleRadius": 30}},
    )
    return ApexChart(opts=opts, cls="w-full h-96")


def chart_engagement_rate(df: pl.DataFrame) -> ApexChart:
    data = df["Engagement Rate (%)"].cast(pl.Float64).fill_null(0).to_list()
    opts = _apex_opts(
        "bar",
        300,
        series=[{"name": "Engagement Rate (%)", "data": data}],
        xaxis={
            "categories": df["Title"].to_list(),
            "labels": {"rotate": -45, "style": {"fontSize": "10px"}},
        },
    )
    return ApexChart(opts=opts, cls="w-full h-80")


def chart_likes_vs_dislikes(df: pl.DataFrame) -> ApexChart:
    opts = _apex_opts(
        "bar",
        300,
        series=[
            {"name": "Likes", "data": df["Like Count Raw"].fill_null(0).to_list()},
            {
                "name": "Dislikes",
                "data": df["Dislike Count Raw"].fill_null(0).to_list(),
            },
        ],
        xaxis={
            "categories": df["Title"].to_list(),
            "labels": {"rotate": -45, "style": {"fontSize": "10px"}},
        },
        plotOptions={"bar": {"horizontal": False, "columnWidth": "45%"}},
        chart={"stacked": False},
    )
    return ApexChart(opts=opts, cls="w-full h-80")


def chart_controversy_score(df: pl.DataFrame) -> ApexChart:
    data = (df["Controversy Raw"].fill_null(0) * 100).to_list()
    opts = _apex_opts(
        "bar",
        300,
        series=[{"name": "Controversy", "data": data}],
        xaxis={"title": {"text": "Controversy (%)"}},
        yaxis={"categories": df["Title"].to_list()},
        plotOptions={"bar": {"horizontal": True}},
    )
    return ApexChart(opts=opts, cls="w-full h-80")


def chart_total_engagement(summary: Dict) -> ApexChart:
    opts = _apex_opts(
        "donut",
        300,
        labels=["Total Likes", "Est. Dislikes"],
        series=[summary["total_likes"], int(summary["total_views"] * 0.02)],
    )
    return ApexChart(opts=opts, cls="w-full h-80")


def chart_treemap_views(df: pl.DataFrame) -> ApexChart:
    data = [
        {"x": row["Title"], "y": row["View Count Raw"] or 0}
        for row in df.iter_rows(named=True)
    ]
    opts = _apex_opts(
        "treemap",
        350,
        series=[{"data": data}],
        legend={"show": False},
        title={"text": "Views Contribution by Video", "align": "center"},
        dataLabels={"enabled": True, "style": {"fontSize": "11px"}},
    )
    return ApexChart(opts=opts, cls="w-full h-[22rem]")


def chart_scatter_likes_dislikes(df: pl.DataFrame) -> ApexChart:
    data = [
        [row["Like Count Raw"] or 0, row["Dislike Count Raw"] or 0]
        for row in df.iter_rows(named=True)
    ]
    opts = _apex_opts(
        "scatter",
        350,
        series=[{"name": "Videos", "data": data}],
        xaxis={
            "title": {"text": "Like Count"},
            "labels": {
                "formatter": "function(val){ return Math.round(val/1e6) + 'M'; }"
            },
        },
        yaxis={
            "title": {"text": "Dislike Count"},
            "labels": {
                "formatter": "function(val){ return Math.round(val/1e6) + 'M'; }"
            },
        },
        tooltip={
            "custom": (
                "function({series, seriesIndex, dataPointIndex, w}) {"
                "const val = w.globals.initialSeries[0].data[dataPointIndex];"
                "return Likes: ${val[0]}<br>Dislikes: ${val[1]};"
                "}"
            )
        },
        chart={"zoom": {"enabled": True}},
    )
    return ApexChart(opts=opts, cls="w-full h-[22rem]")


def chart_bubble_engagement_vs_views(df: pl.DataFrame) -> ApexChart:
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
        tooltip={
            "custom": (
                "function({ series, seriesIndex, dataPointIndex, w }) {"
                "const pt = w.globals.initialSeries[seriesIndex].data[dataPointIndex];"
                "return `<div style='padding:6px; max-width:250px;'>"
                "<strong>${pt.name}</strong><br>"
                "📺 Views: ${pt.x}M<br>"
                "✨ Engagement: ${pt.y}%<br>"
                "⚡ Controversy: ${pt.z}%</div>`;"
                "}"
            )
        },
        plotOptions={"bubble": {"minBubbleRadius": 5, "maxBubbleRadius": 30}},
    )
    return ApexChart(opts=opts, cls="w-full h-[22rem]")
