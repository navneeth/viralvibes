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
            "data": [
                round(vc / 1_000_000, 1)
                for vc in df["View Count Raw"].fill_null(0)
            ]
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


def chart_polarizing_videos(df: pl.DataFrame):
    # Ensure all values are numeric and fallback to 0
    data = []
    for row in df.iter_rows(named=True):
        likes = int(row.get("Like Count Raw") or 0)
        dislikes = int(row.get("Dislike Count Raw") or 0)
        views = int(row.get("View Count Raw") or 0)
        title = row.get("Title", "")[:40]
        # Only include videos that have some engagement
        if likes + dislikes > 0:
            data.append({"x": likes, "y": dislikes, "z": views, "name": title})

    return ApexChart(
        opts={
            "chart": {
                "type": "bubble",
                "height": 350,
                "toolbar": {
                    "show": True
                },
                "zoom": {
                    "enabled": True
                },
            },
            "series": [{
                "name": "Videos",
                "data": data
            }],
            "dataLabels": {
                "enabled": False
            },
            "xaxis": {
                "title": {
                    "text": "Likes"
                }
            },
            "yaxis": {
                "title": {
                    "text": "Dislikes"
                }
            },
            "tooltip": {
                "shared":
                False,
                "intersect":
                True,
                "custom":
                """
                    function({series, seriesIndex, dataPointIndex, w}) {
                        var pt = w.config.series[seriesIndex].data[dataPointIndex];
                        return '<b>' + pt.name + '</b><br>'
                               + 'Likes: ' + pt.x.toLocaleString() + '<br>'
                               + 'Dislikes: ' + pt.y.toLocaleString() + '<br>'
                               + 'Views: ' + pt.z.toLocaleString();
                    }
                """,
            },
            "plotOptions": {
                "bubble": {
                    "minBubbleRadius": 5,
                    "maxBubbleRadius": 30
                }
            },
        },
        cls="w-full h-96",
    )


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
                float(rate) if rate is not None else 0.0 for rate in
                df["Engagement Rate (%)"].cast(pl.Float64).fill_null(0)
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
                "data": df["Like Count Raw"].fill_null(0).to_list()
            },
            {
                "name": "Dislikes",
                "data": df["Dislike Count Raw"].fill_null(0).to_list()
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
            "data": [
                float(score * 100) if score is not None else 0.0
                for score in df["Controversy Raw"].fill_null(0)
            ]
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


def chart_treemap_views(df: pl.DataFrame):
    return ApexChart(opts={
        "chart": {
            "type": "treemap",
            "height": 350
        },
        "series": [{
            "data": [{
                "x":
                row["Title"],
                "y":
                row["View Count Raw"]
                if row["View Count Raw"] is not None else 0
            } for row in df.iter_rows(named=True)]
        }],
        "legend": {
            "show": False
        },
        "title": {
            "text": "Views Contribution by Video",
            "align": "center"
        },
        "dataLabels": {
            "enabled": True,
            "style": {
                "fontSize": "11px"
            }
        }
    },
                     cls="w-full h-[22rem]")


def chart_scatter_likes_dislikes(df: pl.DataFrame):
    return ApexChart(opts={
        "chart": {
            "type": "scatter",
            "zoom": {
                "enabled": True
            },
            "height": 350
        },
        "xaxis": {
            "title": {
                "text": "Like Count"
            },
            "labels": {
                "formatter":
                "function(val){ return Math.round(val/1e6) + 'M'; }"
            }
        },
        "yaxis": {
            "title": {
                "text": "Dislike Count"
            },
            "labels": {
                "formatter":
                "function(val){ return Math.round(val/1e6) + 'M'; }"
            }
        },
        "series": [{
            "name":
            "Videos",
            "data": [[
                row["Like Count Raw"] if row["Like Count Raw"] is not None else
                0, row["Dislike Count Raw"]
                if row["Dislike Count Raw"] is not None else 0
            ] for row in df.iter_rows(named=True)]
        }],
        "tooltip": {
            "custom":
            """
                    function({series, seriesIndex, dataPointIndex, w}) {
                        const val = w.globals.initialSeries[0].data[dataPointIndex];
                        return `Likes: ${val[0]}<br>Dislikes: ${val[1]}`;
                    }
                """
        }
    },
                     cls="w-full h-[22rem]")


def chart_bubble_engagement_vs_views(df: pl.DataFrame):
    return ApexChart(opts={
        "chart": {
            "type": "bubble",
            "height": 350,
            "toolbar": {
                "show": True
            }
        },
        "xaxis": {
            "title": {
                "text": "View Count (Millions)"
            },
            "labels": {
                "formatter": "function(val){ return val + 'M'; }"
            }
        },
        "yaxis": {
            "title": {
                "text": "Engagement Rate (%)"
            }
        },
        "series": [{
            "name":
            "Videos",
            "data": [{
                "x":
                round(row["View Count Raw"] / 1_000_000, 2)
                if row["View Count Raw"] is not None else 0,
                "y":
                float(row["Engagement Rate (%)"])
                if row["Engagement Rate (%)"] is not None else 0.0,
                "z":
                round(float(row["Controversy Raw"]) *
                      100, 1) if row["Controversy Raw"] is not None else 0.0,
                "name":
                row["Title"]
            } for row in df.iter_rows(named=True)]
        }],
        "tooltip": {
            "custom":
            """
                function({ series, seriesIndex, dataPointIndex, w }) {
                    const pt = w.globals.initialSeries[seriesIndex].data[dataPointIndex];
                    return `
                        <strong>${pt.name}</strong><br>
                        Views: ${pt.x}M<br>
                        Engagement: ${pt.y}%<br>
                        Controversy: ${pt.z}%
                    `;
                }
            """
        },
        "plotOptions": {
            "bubble": {
                "minBubbleRadius": 5,
                "maxBubbleRadius": 30
            }
        }
    },
                     cls="w-full h-[22rem]")
