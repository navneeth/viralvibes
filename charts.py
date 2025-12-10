import logging

import polars as pl
from monsterui.all import ApexChart

logger = logging.getLogger("charts")

SCALE_MILLIONS = 1_000_000
SCALE_THOUSANDS = 1_000


# --------------------------------------------------------------
# 1. DESIGN CONSTANTS
# --------------------------------------------------------------
THEME_COLORS = [
    "#3B82F6",  # blue-500 (Primary)
    "#EF4444",  # red-500
    "#10B981",  # emerald-500
    "#F59E0B",  # amber-500
    "#8B5CF6",  # violet-500
    "#EC4899",  # pink-500
]


# Constant
CHART_WRAPPER_CLS = (
    "w-full min-h-[420px] rounded-xl bg-white shadow-sm border border-gray-200 p-4"
)


def chart_wrapper_class(chart_type: str) -> str:
    """
    Return wrapper classes tuned per chart type. Uses min-height so Apex gets enough room,
    and avoids overflow-hidden which causes clipping.
    """
    # fallback heights (same mapping as your CHART_HEIGHTS)
    h = get_chart_height(chart_type)
    return (
        f"w-full min-h-[{h}px] rounded-lg bg-white shadow-sm border border-gray-200 "
        "transition-shadow hover:shadow-lg"
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


# ─────────────────────────────────────────────────────────────
# Core Utilities and Abstraction
# ─────────────────────────────────────────────────────────────


def _base_chart_options(chart_type: str, chart_id: str, **extra) -> dict:
    """
    Consolidated function for base ApexCharts options.
    Sets theme, font, colors, grid, and the custom tooltip.
    """
    height = get_chart_height(chart_type)

    # Chart settingss
    chart_settings = {
        "type": chart_type,
        "id": chart_id,
        "height": height,
        "fontFamily": "Inter, system-ui, sans-serif",
        "toolbar": {"show": False},
        "zoom": {"enabled": False},
        "background": "transparent",
        "parentHeightOffset": 0,
        "redrawOnParentResize": True,
        **extra.get("chart", {}),
    }

    opts = {
        "chart": chart_settings,
        "theme": {"mode": "light", "palette": "palette1"},
        "colors": THEME_COLORS,
        "stroke": {"width": 2, "curve": "smooth"},
        "grid": {"show": True, "borderColor": "#e5e7eb", "strokeDashArray": 4},
        "dataLabels": {"enabled": False},
        "tooltip": {
            "theme": "light",
            "marker": {"show": True},
            "custom": CLICKABLE_TOOLTIP,
            "intersect": False,
            "shared": True,  # For multi-series charts
        },
        **extra,
    }

    return opts


def _apex_opts(chart_type: str, **kwargs) -> dict:
    """
    Build Apex options with safe defaults that avoid clipping.
    chart_type: "treemap", "bar", ...
    """
    height = get_chart_height(chart_type)
    opts = {
        "chart": {
            "type": chart_type,
            "height": height,  # numeric px height — matches wrapper min-h
            "id": kwargs.pop("id", None),
            "toolbar": {"show": False},
            # parentHeightOffset ensures Apex respects parent bounding box properly
            "parentHeightOffset": 0,
            # allow redrawing when parent resizes (good for responsive grids)
            "redrawOnParentResize": True,
        },
        # base defaults
        "colors": THEME_COLORS,
        "dataLabels": {"enabled": False},
        "tooltip": {"theme": "light"},
    }
    # merge extra passed kwargs in
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


# ─────────────────────────────────────────────────────────────
# Reusable Tooltip Templates
# ─────────────────────────────────────────────────────────────
# Reusable tooltip generator for bubble charts
def _bubble_tooltip(x_label: str, y_label: str, z_label: str) -> str:
    """
    Generate consistent bubble chart tooltip.

    Args:
        x_label: Label for X-axis (e.g., "Views (M)")
        y_label: Label for Y-axis (e.g., "Engagement %")
        z_label: Label for bubble size (e.g., "Comments")

    Returns:
        JavaScript function string for ApexCharts custom tooltip
    """
    # Build the JavaScript string without f-string complications
    tooltip_js = (
        """
    function({ series, seriesIndex, dataPointIndex, w }) {
        const pt = w.config.series?.[seriesIndex]?.data?.[dataPointIndex];
        if (!pt || !pt.title) {
            return '<div style="padding:8px;">No data</div>';
        }

        const title = pt.title.length > 50 ? pt.title.substring(0, 50) + '…' : pt.title;

        // Format numbers with locale awareness
        const fmt = (v) => {
            if (typeof v !== 'number') return v;
            return Math.abs(v) >= 1000 ? v.toLocaleString() : (v || 0).toFixed(1);
        };

        return `
            <div style="padding:10px; max-width:280px; border-radius:6px;
                        background:#fff; box-shadow:0 2px 8px rgba(0,0,0,0.15);
                        font-family: Inter, sans-serif;">
                <div style="font-weight:700; color:#1f2937; margin-bottom:6px; font-size:13px;">${title}</div>
                <div style="font-size:12px; color:#4b5563; line-height:1.8;">
                    <div>📊 """
        + x_label
        + """: <b>${fmt(pt.x)}</b></div>
                    <div>📈 """
        + y_label
        + """: <b>${fmt(pt.y)}</b></div>
                    <div>🔵 """
        + z_label
        + """: <b>${fmt(pt.z)}</b></div>
                </div>
            </div>
        `;
    }
    """
    )
    return tooltip_js


# ⚠️ DEPRECATED: Old tooltip pattern - being replaced with _scatter_tooltip()
# Keep until all 3 scatter charts are migrated to new pattern
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
# ─────────────────────────────────────────────────────────────
# THE ONE TOOLTIP TO RULE THEM ALL – clickable + beautiful
# ─────────────────────────────────────────────────────────────
CLICKABLE_TOOLTIP = """
function({seriesIndex, dataPointIndex, w}) {
    const series = w.config.series[seriesIndex];
    const point = series?.data?.[dataPointIndex];

    if (!point) return '<div class="p-3">No data</div>';

    const title = point.title?.length > 55 ? point.title.slice(0,52) + '...' : point.title || 'Untitled Video';
    const videoId = point.id || point.video_id || 'dQw4w9WgXcQ';
    const url = `https://youtu.be/${videoId}`;

    // Format numbers nicely (M, K suffixes)
    const fmt = (v, isRate = false) => {
        if (v === null || v === undefined) return '—';
        if (isRate) return (v * 100).toFixed(2) + '%';
        if (typeof v === 'number') {
            if (Math.abs(v) >= 1_000_000) return (v/1_000_000).toFixed(1) + 'M';
            if (Math.abs(v) >= 1_000) return (v/1_000).toFixed(1) + 'K';
            return v.toLocaleString();
        }
        return v;
    };

    // --- Dynamic Content Generation ---
    let statsHtml = '';

    // 1. Primary X/Y/Z for Scatter/Bubble charts (with generic labels)
    if (point.x !== undefined) {
        const xLabel = w.config.xaxis?.title?.text || 'X Value';
        statsHtml += `<div>${xLabel}: <b>${fmt(point.x)}</b></div>`;
    }
    if (point.y !== undefined) {
        const yLabel = w.config.yaxis?.[0]?.title?.text || series.name || 'Y Value';
        statsHtml += `<div>${yLabel}: <b>${fmt(point.y)}</b></div>`;
    }
    if (point.z !== undefined) {
        statsHtml += `<div>Bubble Size: <b>${fmt(point.z)}</b></div>`;
    }

    // 2. Secondary/Detail metadata (Likes, Comments, etc.)
    // These keys are often passed in by the Polars data prep
    if (point.Likes) statsHtml += `<div>👍 Likes: <b>${fmt(point.Likes)}</b></div>`;
    if (point.Comments) statsHtml += `<div>💬 Comments: <b>${fmt(point.Comments)}</b></div>`;
    if (point.Duration) statsHtml += `<div>⏱ Duration: <b>${point.Duration}</b></div>`;
    if (point.EngagementRateRaw) statsHtml += `<div>📈 Engagement: <b>${fmt(point.EngagementRateRaw, true)}</b></div>`;


    return `
        <div class="p-4 bg-white rounded-lg shadow-2xl border border-gray-200 font-sans text-sm max-w-xs">
            <div class="font-bold text-gray-900 mb-2 leading-tight">${title}</div>
            <div class="space-y-1 text-gray-600 text-xs">
                ${statsHtml}
            </div>
            <div class="mt-3 pt-3 border-t border-gray-200">
                <a href="${url}" target="_blank" rel="noopener"
                   class="inline-flex items-center gap-1 text-blue-600 hover:text-blue-800 font-medium text-xs">
                   Watch on YouTube
                </a>
            </div>
        </div>
    `;
}
"""


def _safe_bubble_data(df: pl.DataFrame, max_points: int = 150) -> pl.DataFrame:
    """
    Limit bubble chart data for performance on mobile.

    Keeps top performers + random sample of others.

    Args:
        df: Input DataFrame
        max_points: Maximum number of points to render

    Returns:
        Sampled DataFrame
    """
    if df is None or df.is_empty():
        return df

    if len(df) <= max_points:
        return df

    logger.debug(
        f"[charts] Sampling {len(df)} rows down to {max_points} for bubble chart"
    )

    try:
        # Keep top 10 by views, sample the rest
        top_views = df.top_k(10, by="Views")
        remaining = df.anti_join(top_views, on="id" if "id" in df.columns else "ID")

        sample_size = max_points - len(top_views)
        sampled_rest = remaining.sample(
            n=min(sample_size, len(remaining)), seed=42, shuffle=False
        )

        return pl.concat([top_views, sampled_rest])
    except Exception as e:
        logger.warning(f"[charts] Sampling failed: {e}. Returning original df")
        return df


def _bubble_opts_mobile_safe(
    chart_type: str = "bubble",
    x_label: str = "X Axis",
    y_label: str = "Y Axis",
    z_label: str = "Bubble Size",
    disable_zoom: bool = True,
    chart_id: str = None,
    **kwargs,
) -> dict:
    """
    Build bubble chart options optimized for mobile.

    Features:
    - No zoom (prevents scroll conflicts)
    - Hidden toolbar (cleaner mobile UI)
    - Consistent tooltips
    - Touch-friendly bubble sizes
    - Hover feedback

    Args:
        chart_type: "bubble", "scatter", etc.
        x_label, y_label, z_label: Axis/size labels for tooltip
        disable_zoom: Disable zoom on mobile (default: True)
        chart_id: Chart ID
        **kwargs: Additional ApexCharts options to merge

    Returns:
        Complete options dict for ApexChart()
    """
    height = get_chart_height(chart_type)

    base_opts = {
        "chart": {
            "type": chart_type,
            "id": chart_id,
            "height": height,
            "toolbar": {"show": False},
            "zoom": {"enabled": not disable_zoom},  # ✅ True disables zoom
            "parentHeightOffset": 0,
            "redrawOnParentResize": True,
            "sparkline": {"enabled": False},
            "fontFamily": "Inter, -apple-system, BlinkMacSystemFont, sans-serif",
        },
        "colors": THEME_COLORS,
        "dataLabels": {"enabled": False},
        "tooltip": {
            "theme": "light",
            "custom": CLICKABLE_TOOLTIP,
            "intersect": False,
        },
        "plotOptions": {
            "bubble": {
                "minBubbleRadius": 6,  # ✅ Touch-friendly
                "maxBubbleRadius": 30,
                "dataLabels": {"enabled": False},
            }
        },
        "states": {
            "hover": {"filter": {"type": "darken", "value": 0.15}},
            "active": {"filter": {"type": "darken", "value": 0.25}},
        },
    }

    # Merge with any passed kwargs (allows override)
    base_opts.update(kwargs)
    return base_opts


def _scatter_opts_mobile_safe(
    chart_type: str = "scatter",
    x_label: str = "X Axis",
    y_label: str = "Y Axis",
    disable_zoom: bool = True,
    chart_id: str = None,
    **kwargs,
) -> dict:
    """
    Build scatter chart options optimized for mobile.

    Features:
    - No zoom (prevents scroll conflicts)
    - Hidden toolbar (cleaner mobile UI)
    - Consistent tooltips
    - Touch-friendly interaction

    Args:
        chart_type: "scatter" (usually)
        x_label: Label for X-axis (e.g., "Views (M)")
        y_label: Label for Y-axis (e.g., "Engagement %")
        disable_zoom: Disable zoom on mobile (default: True)
        chart_id: Chart ID
        **kwargs: Additional ApexCharts options to merge

    Returns:
        Complete options dict for ApexChart()
    """
    height = get_chart_height(chart_type)

    base_opts = {
        "chart": {
            "type": chart_type,
            "id": chart_id,
            "height": height,
            "toolbar": {"show": False},
            "zoom": {"enabled": not disable_zoom},  # ✅ True disables zoom
            "parentHeightOffset": 0,
            "redrawOnParentResize": True,
            "sparkline": {"enabled": False},
            "fontFamily": "Inter, -apple-system, BlinkMacSystemFont, sans-serif",
        },
        "colors": THEME_COLORS,
        "dataLabels": {"enabled": False},
        "tooltip": {
            "theme": "light",
            "custom": CLICKABLE_TOOLTIP,
            "intersect": False,
            "shared": True,
        },
        "states": {
            "hover": {"filter": {"type": "darken", "value": 0.15}},
            "active": {"filter": {"type": "darken", "value": 0.25}},
        },
    }

    base_opts.update(kwargs)
    return base_opts


# ----------------------------------------------------------------------
# 3. Data Preparation Helpers (Optimization & Robustness)
# ----------------------------------------------------------------------


def _safe_sort(df: pl.DataFrame, col: str, descending: bool = True) -> pl.DataFrame:
    """
    Sort DataFrame safely across Polars versions.
    Handles:
      - older/newer Polars signatures
      - missing columns
      - numeric columns stored as strings
    """
    if df is None or df.is_empty() or col not in df.columns:
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


def _scatter_data_vectorized(
    df: pl.DataFrame,
    x_col: str,
    y_col: str,
    z_col: str = None,
    x_scale: float = 1.0,
    y_scale: float = 1.0,
    x_round: int = 1,
    y_round: int = 2,
) -> list:
    """
    Build scatter data using vectorized Polars operations (faster than iter_rows).
    Includes video metadata for the rich tooltip.

    Args:
        df: Input DataFrame
        x_col: X-axis column name
        y_col: Y-axis column name
        x_scale: Divisor for X values (e.g., 1_000_000 for millions)
        y_scale: Divisor for Y values
        x_round: Decimal places to round X
        y_round: Decimal places to round Y

    Returns:
        List of dicts with x, y, title keys (ready for ApexCharts)
    """
    if df is None or df.is_empty():
        return []

    try:
        # Columns required for all scatter/bubble charts
        select_exprs = [
            (pl.col(x_col) / x_scale).round(x_round).alias("x"),
            (pl.col(y_col) / y_scale).round(y_round).alias("y"),
            pl.col("Title").alias("title"),
            pl.col("id").alias("id"),
            # Include detail metadata for the rich tooltip
            pl.col("Likes").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("Comments").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("Duration Formatted").alias("Duration"),
            pl.col("Engagement Rate Raw").alias("EngagementRateRaw"),
        ]

        # Add Z column only for bubble charts
        if z_col and z_col in df.columns:
            select_exprs.append((pl.col(z_col) / z_scale).round(1).alias("z"))

        data = df.select(select_exprs).to_dicts()
        return data

    except Exception as e:
        logger.warning(f"[charts] Scatter data extraction failed: {e}")
        return []


# ----------------------
# Charts
# ----------------------


def chart_views_ranking(df: pl.DataFrame, chart_id: str = "views-ranking") -> ApexChart:
    """
    Bar chart of views per video, sorted by view count descending.
    Dynamic scaling (K/M) to keep differences visible.
    """
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    sorted_df = _safe_sort(df, "Views", descending=True)

    # Get labels
    labels = (
        sorted_df["Title"]
        .cast(pl.Utf8, strict=False)
        .fill_null("Untitled")
        .str.slice(0, 50)
        .to_list()
        if "Title" in sorted_df.columns
        else []
    )

    # Get views
    views = sorted_df["Views"].cast(pl.Float64, strict=False).fill_null(0)
    max_views = float(views.max() or 0)

    # Dynamic scaling based on max value
    use_thousands = max_views < 2_000_000

    if use_thousands:
        data = (views / SCALE_THOUSANDS).round(1).to_list()
        y_title = "Views (Thousands)"
    else:
        data = (views / SCALE_MILLIONS).round(1).to_list()
        y_title = "Views (Millions)"

    # Ensure categories length matches data
    if len(labels) != len(data):
        labels = [f"Video {i+1}" for i in range(len(data))]

    # ✅ Use _apex_opts instead of _base_chart_options (for scalar data)
    opts = _apex_opts(
        "bar",
        id=chart_id,
        series=[{"name": y_title, "data": data}],
        title={"text": "🏆 Top Videos by Views", "align": "left"},
        xaxis={"categories": labels, "labels": {"rotate": -45, "maxHeight": 80}},
        yaxis={"title": {"text": y_title}},
        # ✅ Format numbers in tooltip, not yaxis labels
        tooltip={
            "theme": "light",
            "y": {
                "formatter": f"function(val) {{ return val.toFixed(1) + '{'K' if use_thousands else 'M'}'; }}"
            },
        },
        plotOptions={
            "bar": {
                "borderRadius": 4,
                "columnWidth": "55%",
                "distributed": True,  # Different color per bar
            }
        },
        colors=THEME_COLORS,
        legend={"show": False},
    )

    return ApexChart(
        opts=opts,
        cls=chart_wrapper_class("vertical_bar"),
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

    opts = _base_chart_options(
        "bar",
        chart_id,
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
        cls=chart_wrapper_class("horizontal_bar"),
    )


def chart_engagement_breakdown(
    df: pl.DataFrame, chart_id: str = "engagement-breakdown"
) -> ApexChart:
    """
    Stacked bar/column chart: Likes and Comments per video in the same bar.

    - Top 10 videos by Views
    - Likes shown in thousands (K)
    - Comments shown in hundreds (×100) to keep scales closer
    - Truncated titles for mobile
    """
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    # Top 10 by views
    top_videos = _safe_sort(df, "Views", descending=True).head(10)

    # Prepare series (scaled for readability)
    data_likes_k = (
        (top_videos["Likes"].fill_null(0) / SCALE_THOUSANDS).round(1).to_list()
    )
    data_comments_x100 = (top_videos["Comments"].fill_null(0) / 100).round(1).to_list()

    categories = (
        top_videos["Title"].str.slice(0, 40).to_list()
        if "Title" in top_videos.columns
        else []
    )

    return ApexChart(
        opts=_base_chart_options(
            "bar",
            series=[
                {"name": "👍 Likes (K)", "data": data_likes_k},
                {"name": "💬 Comments (×100)", "data": data_comments_x100},
            ],
            xaxis={
                "categories": categories,
                "labels": {"rotate": -45, "maxHeight": 64, "trim": True},
            },
            yaxis={"title": {"text": "Count (scaled)"}},
            plotOptions={
                "bar": {
                    "horizontal": False,  # column layout for mobile
                    "stacked": True,  # stack both metrics into the same bar
                    "borderRadius": 4,
                    "columnWidth": "55%",
                    "dataLabels": {"position": "top"},
                }
            },
            dataLabels={"enabled": False},
            title={"text": "📊 Engagement Breakdown (Top 10)", "align": "left"},
            colors=["#3B82F6", "#10B981"],
            tooltip={
                "shared": True,
                "intersect": False,
                "y": {
                    "formatter": (
                        "function(val, {series, seriesIndex}) {"
                        "  if (seriesIndex === 0) return val.toFixed(1) + 'K';"
                        "  if (seriesIndex === 1) return val.toFixed(1) + ' ×100';"
                        "  return val;"
                        "}"
                    )
                },
            },
            responsive=[
                {
                    "breakpoint": 640,
                    "options": {
                        "chart": {"height": 420},
                        "xaxis": {"labels": {"rotate": -45, "maxHeight": 52}},
                        "plotOptions": {"bar": {"columnWidth": "65%"}},
                        "legend": {"position": "bottom"},
                    },
                }
            ],
        ),
        cls=chart_wrapper_class("vertical_bar"),
    )


def chart_likes_per_1k_views(
    df: pl.DataFrame, chart_id: str = "likes-per-1k"
) -> ApexChart:
    """Scatter: Likes per 1K views (engagement quality metric)."""
    if df is None or df.is_empty():
        return _empty_chart("scatter", chart_id)

    # Calculate likes per 1K views
    df_calc = df.with_columns(
        (pl.col("Likes") / (pl.col("Views") / 1000)).alias("Likes_per_1K")
    )

    # Vectorized data extraction
    try:
        data = df_calc.select(
            (pl.col("Views") / SCALE_MILLIONS).round(1).alias("x"),
            pl.col("Likes_per_1K").round(1).alias("y"),
            (pl.col("Title").str.slice(0, 40)).alias("title"),
        ).to_dicts()
    except Exception as e:
        logger.warning(f"[charts] Data extraction failed: {e}")
        return _empty_chart("scatter", chart_id)

    return ApexChart(
        opts=_scatter_opts_mobile_safe(
            chart_type="scatter",
            x_label="Views (Millions)",
            y_label="Likes per 1K Views",
            chart_id=chart_id,
            disable_zoom=True,
            series=[{"name": "Videos", "data": data}],
            xaxis={"title": {"text": "Views (Millions)"}},
            yaxis={"title": {"text": "Likes per 1K Views"}},
            title={"text": "📊 Audience Quality: Likes per 1K Views", "align": "left"},
        ),
        cls=chart_wrapper_class("scatter"),
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
        cls=chart_wrapper_class("heatmap"),
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
    return ApexChart(opts=opts, cls=chart_wrapper_class("horizontal_bar"))


def chart_treemap_views(df: pl.DataFrame, chart_id: str = "treemap-views") -> ApexChart:
    """
    Treemap of video views, showing contribution to overall reach.
    ✅ FIXED: Each tile has its own color
    """
    if df is None or df.is_empty():
        return _empty_chart("treemap", chart_id)

    # Build raw data
    raw = [
        {"x": row["Title"], "y": row["Views"] or 0} for row in df.iter_rows(named=True)
    ]

    # ✅ NEW: Generate distinct color per tile
    palette = _distributed_palette(len(raw))

    # ✅ NEW: Add fillColor to each data point
    data = [{**d, "fillColor": palette[i % len(palette)]} for i, d in enumerate(raw)]

    opts = _apex_opts(
        "treemap",
        id=chart_id,
        series=[{"data": data}],
        legend={"show": False},
        title={"text": "Views Distribution by Video", "align": "center"},
        dataLabels={"enabled": True, "style": {"fontSize": "11px"}},
        tooltip={"enabled": True},
        # ✅ IMPORTANT: Tell ApexCharts NOT to override with series colors
        plotOptions={
            "treemap": {
                "distributed": True,  # ✅ Enable per-point coloring
                "enableShades": False,  # ✅ Don't auto-shade
            }
        },
        # ✅ Optional: Still pass palette for consistency
        colors=palette,
    )
    return ApexChart(opts=opts, cls=chart_wrapper_class("treemap"))


def chart_bubble_engagement_vs_views(
    df: pl.DataFrame, chart_id: str = "bubble-engagement"
) -> ApexChart:
    """
    Bubble: Views (X) vs Engagement (Y), bubble size = Controversy.
    Mobile-safe, consistent tooltips, faster rendering.
    """
    if df is None or df.is_empty():
        return _empty_chart("bubble", chart_id)

    # Sample data for performance
    df_safe = _safe_bubble_data(df, max_points=150)

    # Build data as required: array of [x, y, z] with extra "title" kept separately
    # Vectorized data extraction (faster than iter_rows)
    try:
        data = df_safe.select(
            (pl.col("Views") / SCALE_MILLIONS).round(2).alias("x"),
            (pl.col("Engagement Rate Raw") * 100).round(1).alias("y"),
            pl.col("Controversy").round(2).alias("z"),
            (pl.col("Title").str.slice(0, 45)).alias("title"),
            (pl.col("id").cast(pl.Utf8)).alias("id"),
        ).to_dicts()
    except Exception as e:
        logger.warning(f"[charts] Data extraction failed: {e}")
        return _empty_chart("bubble", chart_id)

    return ApexChart(
        opts=_bubble_opts_mobile_safe(
            chart_type="bubble",
            x_label="View Count (Millions)",
            y_label="Engagement Rate (%)",
            z_label="Controversy Score",
            chart_id=chart_id,
            series=[{"name": "Videos", "data": data}],
            xaxis={
                "title": {"text": "View Count (Millions)"},
                "labels": {"formatter": "function(val){ return val + 'M'; }"},
            },
            yaxis={"title": {"text": "Engagement Rate (%)"}},
            title={"text": "Engagement vs Views Analysis", "align": "center"},
            plotOptions={"bubble": {"minBubbleRadius": 5, "maxBubbleRadius": 30}},
        ),
        cls=chart_wrapper_class("bubble"),
    )


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
            "y": round(float(row["Engagement Rate Raw"] or 0) * 100, 2),
            "z": round((row["Views"] or 0) / SCALE_MILLIONS, 1),
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
        cls=chart_wrapper_class("bubble"),
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

    return ApexChart(opts=opts, cls=chart_wrapper_class("radar"))


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
        cls=chart_wrapper_class("scatter"),
    )


def chart_views_vs_likes(
    df: pl.DataFrame, chart_id: str = "views-vs-likes"
) -> ApexChart:
    """Bubble: Views (X) vs Likes (Y), size = Comments."""
    if df is None or df.is_empty():
        return _empty_chart("bubble", chart_id)

    df_safe = _safe_bubble_data(df, max_points=150)

    try:
        data = df_safe.select(
            (pl.col("Views") / SCALE_MILLIONS).round(1).alias("x"),
            (pl.col("Likes") / SCALE_THOUSANDS).round(1).alias("y"),
            ((pl.col("Comments") / 100).cast(pl.Int32)).alias("z"),
            (pl.col("Title").str.slice(0, 40)).alias("title"),
            (pl.col("id").cast(pl.Utf8)).alias("id"),
        ).to_dicts()
    except Exception as e:
        logger.warning(f"[charts] Data extraction failed: {e}")
        return _empty_chart("bubble", chart_id)

    return ApexChart(
        opts=_bubble_opts_mobile_safe(
            chart_type="bubble",
            x_label="👀 Views (Millions)",
            y_label="👍 Likes (Thousands)",
            z_label="Comments (×100)",
            chart_id=chart_id,
            series=[{"name": "Videos", "data": data}],
            xaxis={"title": {"text": "👀 Views (Millions)"}},
            yaxis={"title": {"text": "👍 Likes (Thousands)"}},
            title={
                "text": "🎯 Views vs Likes (bubble size = comments)",
                "align": "left",
            },
            tooltip={"custom": TOOLTIP_TRUNC},
            plotOptions={"bubble": {"minBubbleRadius": 4, "maxBubbleRadius": 25}},
        ),
        cls=chart_wrapper_class("bubble"),
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

    eng_rates = (sorted_df["Engagement Rate Raw"] * 100).round(2).to_list()

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
        cls=chart_wrapper_class("line"),
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
        cls=chart_wrapper_class("vertical_bar"),
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
        cls=chart_wrapper_class("horizontal_bar"),
    )


def chart_treemap_reach(df: pl.DataFrame, chart_id: str = "treemap-reach") -> ApexChart:
    """Treemap: Video contribution to total views."""
    if df is None or df.is_empty():
        return _empty_chart("treemap", chart_id)

    # Build raw data
    raw = [
        {"x": row["Title"][:50], "y": row["Views"] or 0}
        for row in df.iter_rows(named=True)
    ]

    # Generate distinct color per tile
    palette = _distributed_palette(len(raw))
    data = [{**d, "fillColor": palette[i % len(palette)]} for i, d in enumerate(raw)]

    return ApexChart(
        opts={
            "chart": {"type": "treemap", "id": chart_id},
            "series": [{"data": data}],
            "title": {
                "text": "📊 Reach Distribution (larger = more views)",
                "align": "left",
            },
            "dataLabels": {"enabled": True, "style": {"fontSize": "11px"}},
            "tooltip": {"theme": "light"},
            "legend": {"show": False},
            # Ensure per-point coloring
            "plotOptions": {"treemap": {"distributed": True, "enableShades": False}},
            # Pass palette for consistency; will not override fillColor
            "colors": palette,
        },
        cls=chart_wrapper_class("treemap"),
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
        cls=chart_wrapper_class("radar"),
    )


# Palette helper: generate n distinct HSL colors
def _distributed_palette(n: int) -> list[str]:
    if n <= 0:
        return THEME_COLORS
    # Evenly spaced hues; soft saturation/lightness for readability
    return [f"hsl({int(360*i/n)}, 70%, 55%)" for i in range(n)]
