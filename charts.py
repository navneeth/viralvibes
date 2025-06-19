from typing import Dict

import polars as pl
from monsterui.all import ApexChart


def chart_views_by_rank(df: pl.DataFrame):
    return ApexChart(opts={
        "chart": {
            "type": "line",
            "zoom": {
                "enabled": False
            },
            "toolbar": {
                "show": False
            },
            "height": 300
        },
        "series": [{
            "name":
            "Views (in Millions)",
            "data": [round(vc / 1_000_000, 1) for vc in df["View Count Raw"]]
        }],
        "xaxis": {
            "categories": df["Rank"].cast(pl.Int64).to_list(),
            "title": {
                "text": "Video Rank"
            }
        },
        "yaxis": {
            "title": {
                "text": "Views (Millions)"
            }
        }
    },
                     cls="w-full h-80")


def chart_engagement_rate(df: pl.DataFrame):
    return ApexChart(opts={
        "chart": {
            "type": "bar",
            "height": 300
        },
        "series": [{
            "name":
            "Engagement Rate (%)",
            "data": [
                float(rate)
                for rate in df["Engagement Rate (%)"].cast(pl.Float64)
            ]
        }],
        "xaxis": {
            "categories": df["Title"].to_list(),
            "labels": {
                "rotate": -45,
                "style": {
                    "fontSize": "10px"
                }
            }
        },
    },
                     cls="w-full h-80")


def chart_likes_vs_dislikes(df: pl.DataFrame):
    return ApexChart(opts={
        "chart": {
            "type": "bar",
            "stacked": False,
            "height": 300
        },
        "series": [
            {
                "name": "Likes",
                "data": df["Like Count Raw"].to_list()
            },
            {
                "name": "Dislikes",
                "data": df["Dislike Count Raw"].to_list()
            },
        ],
        "xaxis": {
            "categories": df["Title"].to_list(),
            "labels": {
                "rotate": -45,
                "style": {
                    "fontSize": "10px"
                }
            }
        },
        "plotOptions": {
            "bar": {
                "horizontal": False,
                "columnWidth": "45%"
            }
        },
    },
                     cls="w-full h-80")


def chart_controversy_score(df: pl.DataFrame):
    return ApexChart(opts={
        "chart": {
            "type": "bar",
            "height": 300
        },
        "plotOptions": {
            "bar": {
                "horizontal": True
            }
        },
        "series": [{
            "name":
            "Controversy",
            "data": [float(score * 100) for score in df["Controversy Raw"]]
        }],
        "xaxis": {
            "title": {
                "text": "Controversy (%)"
            }
        },
        "yaxis": {
            "categories": df["Title"].to_list()
        }
    },
                     cls="w-full h-80")


def chart_total_engagement(summary: Dict):
    return ApexChart(opts={
        "chart": {
            "type": "donut",
            "height": 300
        },
        "labels": ["Total Likes", "Est. Dislikes"],
        "series": [summary["total_likes"],
                   int(summary["total_views"] * 0.02)],
    },
                     cls="w-full h-80")
