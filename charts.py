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


# Tufte + Economist Minimal Style: Restrained colors & typography
# ─────────────────────────────────────────────────────────────────────────

TUFTE_FONT_FAMILY = "'Helvetica Neue', Helvetica, Arial, 'Inter', system-ui, sans-serif"
TUFTE_TEXT_COLOR = "#2c3e50"  # Slightly softer than pure black (#1a1a1a)

# Minimal grid: very light dashed (Economist style)
GRID_MINIMAL = {
    "show": True,
    "borderColor": "#ecf0f1",  # Lighter than default #e2e8f0
    "strokeDashArray": 2,  # Single-pixel dashes vs [4, 4] — more subtle
}

# Economist restrained palette: red, blue, grays
# Use ONLY for single/dual-series charts; preserve THEME_COLORS for multi-series
PALETTE_ECONOMIST = [
    "#c0392b",  # Economist red — for highlights, outliers, controversy
    "#2980b9",  # Economist blue — for baseline data
    "#95a5a6",  # Neutral gray — for secondary/filler
    "#34495e",  # Dark gray — for annotations
    "#e74c3c",  # Bright red — for emphasis/virality
]

# Highlight colors for editorial emphasis (Tufte + YouTuber insights)
COLOR_HIGHLIGHT_RED = "#c0392b"  # Viral spikes, controversy, outliers
COLOR_HIGHLIGHT_BLUE = "#2980b9"  # Baseline, normal performance
COLOR_NEUTRAL = "#95a5a6"  # Filler, background, context


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


# Palette helper: generate n distinct HSL colors
def _distributed_palette(n: int) -> list[str]:
    if n <= 0:
        return THEME_COLORS
    # Evenly spaced hues; soft saturation/lightness for readability
    return [f"hsl({int(360 * i / n)}, 70%, 55%)" for i in range(n)]


def _apply_point_colors(data: list[dict]) -> tuple[list[dict], list[str]]:
    """
    Assign a distinct color to each point and return (data_with_colors, palette).
    Safe on empty lists and preserves existing fields.
    """
    if not data:
        return data, THEME_COLORS
    palette = _distributed_palette(len(data))
    for i, d in enumerate(data):
        # ApexCharts reads 'color' per point
        d["fillColor"] = palette[i % len(palette)]
    return data, palette


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
        "dataLabels": {"enabled": False},
        "tooltip": _tooltip_opts_for_chart(chart_type),
        # grid visibility
        "grid": {
            "show": True,
            "borderColor": "#e2e8f0",  # Light gray
            "strokeDashArray": [4, 4],  # Dashed pattern
            "xaxis": {"lines": {"show": True}},
            "yaxis": {"lines": {"show": True}},
            "padding": {"left": 10, "right": 10},
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
            #  Smooth animations
            "animations": {
                "enabled": True,
                "easing": "easeinout",
                "speed": 800,
                "animateGradually": {
                    "enabled": True,
                    "delay": 150,
                },
            },
        },
        # base defaults
        "colors": THEME_COLORS,
        "dataLabels": {"enabled": False},
        "tooltip": {"theme": "light"},
    }
    # merge extra passed kwargs in
    opts.update(kwargs)
    return opts


def apply_tufte_economist(
    opts: dict, chart_type: str = "line", single_series: bool = True
) -> dict:
    """
    Apply Tufte-Economist minimal style to chart options *safely*.

    Philosophy:
    - Maximize data-ink ratio (thin lines, no junk)
    - Restrained editorial style (light grids, muted colors)
    - Direct emphasis on outliers & patterns (red highlights, annotations)
    - YouTuber-friendly: highlights virality, retention, early traction

    Args:
        opts: ApexCharts options dict (modified in-place)
        chart_type: "line", "bar", "scatter", "bubble", etc.
        single_series: If True, use PALETTE_ECONOMIST; else keep THEME_COLORS
                      (prevents color palette breakage on multi-series charts)

    Returns:
        Modified opts dict with minimal styling

    Design Choices (rationale):
    - Thin strokes (1.2px): Tufte's data-ink ratio principle
    - Straight lines: Clarity & emphasis (not smooth curves)
    - Minimal grid: Light dashed, no padding
    - No legend (replaced by direct labels or omitted)
    - No toolbar, animations: Reduce distractions
    - Title: Smaller (13px), lighter weight (500)
    - Markers: Tiny or hidden (dots distract from lines)
    - Colors: Restrained (2 core + grays) for editorial polish

    Safe Practices:
    - Does NOT override existing "smooth" curves (respects intent)
    - Preserves bubble size encoding (doesn't shrink markers aggressively)
    - Respects existing legends/annotations (can be overridden per chart)
    - Non-destructive: returns modified copy, safe to call last
    """
    # Ensure chart dict exists
    if "chart" not in opts:
        opts["chart"] = {}

    # ─────────────────────────────────────────────────────────────────────
    # 1. TYPOGRAPHY: Tufte-approved fonts & colors
    # ─────────────────────────────────────────────────────────────────────
    opts["chart"]["fontFamily"] = TUFTE_FONT_FAMILY
    opts["chart"]["foreColor"] = TUFTE_TEXT_COLOR

    # ─────────────────────────────────────────────────────────────────────
    # 2. GRID: Ultra-light (Economist style)
    # ─────────────────────────────────────────────────────────────────────
    opts["grid"] = GRID_MINIMAL.copy()
    opts["grid"]["xaxis"] = {"lines": {"show": True}}
    opts["grid"]["yaxis"] = {"lines": {"show": True}}
    opts["grid"]["padding"] = {"left": 0, "right": 0, "top": 0, "bottom": 0}

    # ─────────────────────────────────────────────────────────────────────
    # 3. STROKES: Thin for data-ink ratio
    # ─────────────────────────────────────────────────────────────────────
    if "stroke" not in opts:
        opts["stroke"] = {}
    opts["stroke"]["width"] = 1.2  # vs default 2
    # Smart: only force straight lines if not already smooth
    # (respects chart author's intent if curve="smooth" was set)
    if opts["stroke"].get("curve") not in ["smooth"]:
        opts["stroke"]["curve"] = "straight"

    # ─────────────────────────────────────────────────────────────────────
    # 4. MARKERS: Tiny or hidden (improve line clarity)
    # ─────────────────────────────────────────────────────────────────────
    if "markers" not in opts:
        opts["markers"] = {}
    opts["markers"].setdefault("size", 0)  # No dots on lines (Tufte: reduce junk)
    opts["markers"].setdefault("strokeWidth", 0.5)  # Subtle if visible

    # ─────────────────────────────────────────────────────────────────────
    # 5. DATA LABELS & JUNK REMOVAL
    # ─────────────────────────────────────────────────────────────────────
    opts["dataLabels"] = {"enabled": False}
    # Toolbar is typically already hidden, but ensure it
    if "toolbar" not in opts.get("chart", {}):
        opts["chart"]["toolbar"] = {"show": False}
    # Default: no legend (can be overridden per chart)
    opts["legend"] = {"show": False}

    # ─────────────────────────────────────────────────────────────────────
    # 6. COLORS: Restrained palette (if single-series)
    # ─────────────────────────────────────────────────────────────────────
    if single_series:
        # Single series → use Economist red only
        opts["colors"] = [COLOR_HIGHLIGHT_RED]
    else:
        # Multi-series → preserve THEME_COLORS (prevents breakage)
        # but could be refined further per chart type
        pass  # opts["colors"] unchanged

    # ─────────────────────────────────────────────────────────────────────
    # 7. TITLE STYLING: Smaller, lighter weight (editorial restraint)
    # ─────────────────────────────────────────────────────────────────────
    if "title" in opts:
        opts["title"].setdefault("style", {})
        opts["title"]["style"].update(
            {
                "fontSize": "13px",  # vs 15px (less shouty)
                "fontWeight": 500,  # vs 600 (lighter)
                "color": TUFTE_TEXT_COLOR,
            }
        )

    # ─────────────────────────────────────────────────────────────────────
    # 8. ANIMATIONS (optional): Keep functional, remove flashy
    # ─────────────────────────────────────────────────────────────────────
    # Note: Don't disable entirely (animations help with data loading)
    # Just ensure they're subtle (already in your _apex_opts defaults)

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
def _tooltip_opts_for_chart(chart_type: str) -> dict:
    """
    Return tooltip config optimized for chart type.

    - scatter/bubble: per-point tooltips (shared=False, intersect=True)
    - bar/column: per-bar tooltips (shared=False, intersect=True)
    - line/area: shared tooltips across series (shared=True, intersect=False)
    - default: per-point behavior

    Args:
        chart_type: "scatter", "bubble", "bar", "line", etc.

    Returns:
        Tooltip config dict
    """
    # Charts that need per-point/per-bar tooltips
    per_point_charts = {"scatter", "bubble", "bar", "column", "horizontal_bar"}

    if chart_type in per_point_charts:
        return {
            "enabled": True,
            "custom": CLICKABLE_TOOLTIP,
            "shared": False,  # Per-point tooltip
            "intersect": True,  # Show on hover
            "theme": "light",
        }
    else:  # line, area, radar, etc.
        return {
            "enabled": True,
            "custom": CLICKABLE_TOOLTIP,
            "shared": True,  # Show all series at once
            "intersect": False,
            "theme": "light",
        }


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
        "tooltip": _tooltip_opts_for_chart(chart_type),
        "plotOptions": {
            "bubble": {
                "minBubbleRadius": 6,  # ✅ Touch-friendly
                "maxBubbleRadius": 30,
                "dataLabels": {"enabled": False},
                # Use different color per bubble
                "colorByPoint": True,
            }
        },
        "states": {
            "hover": {"filter": {"type": "darken", "value": 0.15}},
            "active": {"filter": {"type": "darken", "value": 0.25}},
        },
        # ✅ Add responsive breakpoints
        "responsive": [
            {
                "breakpoint": 1024,  # Tablets
                "options": {
                    "chart": {"height": 450},
                    "plotOptions": {
                        "bubble": {
                            "minBubbleRadius": 5,
                            "maxBubbleRadius": 25,
                        }
                    },
                },
            },
            {
                "breakpoint": 640,  # Mobile
                "options": {
                    "chart": {"height": 380},
                    "plotOptions": {
                        "bubble": {
                            "minBubbleRadius": 4,
                            "maxBubbleRadius": 20,
                        }
                    },
                    "legend": {"position": "bottom"},
                },
            },
        ],
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
        "tooltip": _tooltip_opts_for_chart(chart_type),
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
    z_scale: float = 1.0,
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
            pl.col("Duration Formatted").fill_null("—").alias("Duration"),
            pl.col("Engagement Rate Raw").fill_null(0).alias("EngagementRateRaw"),
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
        labels = [f"Video {i + 1}" for i in range(len(data))]

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


def chart_engagement_ranking(
    df: pl.DataFrame, chart_id: str = "engagement-ranking"
) -> ApexChart:
    """Horizontal bar: Videos ranked by engagement rate with rich tooltip."""
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    # Sort by engagement descending
    MAX_BARS = 12
    df_sorted = _safe_sort(df, "Engagement Rate Raw", descending=True).head(MAX_BARS)

    # Build per-point objects so the global CLICKABLE_TOOLTIP can show video info
    # Apex bar supports data points as { x, y, ... }
    points = []
    max_engagement = 0.0

    for i, row in enumerate(df_sorted.iter_rows(named=True)):
        er = float(row.get("Engagement Rate Raw") or 0)
        er_pct = round(er * 100, 2)
        max_engagement = max(max_engagement, er_pct)

        # 🎨 rank-based color (top 3 highlighted)
        color = (
            "#F59E0B"
            if i == 0
            # 🥇 gold
            else (
                "#9CA3AF"
                if i < 3
                # 🥈🥉 muted
                else "#3B82F6"
            )  # rest: blue
        )
        points.append(
            {
                "x": _truncate_title(row.get("Title") or "Untitled"),
                "y": er_pct,
                "fillColor": color,
                "title": _truncate_title(row.get("Title") or "Untitled"),
                "id": str(row.get("id") or row.get("ID") or ""),
                # Optional extras for the tooltip
                "Likes": int(row.get("Likes") or 0),
                "Comments": int(row.get("Comments") or 0),
                "Duration": row.get("Duration Formatted") or "",
                "EngagementRateRaw": er,
            }
        )

    # Distributed colors: one color per bar
    opts = _base_chart_options(
        "bar",
        chart_id,
        series=[{"name": "Engagement %", "data": points}],
        plotOptions={
            "bar": {
                "horizontal": True,
                "barHeight": "60%",
                "borderRadius": 4,
                "distributed": False,  # distinct color per bar
            }
        },
        # x-axis shows Engagement %, y-axis shows video titles via point.x
        xaxis={"title": {"text": "Engagement %"}},
        yaxis={"labels": {"maxWidth": 260}},
        title={"text": "Engagement Leaderboard", "align": "left"},
        legend={"show": False},
        colors=THEME_COLORS,
        tooltip={
            # Use the global rich, clickable tooltip
            "custom": CLICKABLE_TOOLTIP,
            "shared": False,
            "intersect": True,
            # Format numeric value (y) to show %
            "y": {"formatter": "function(val){ return (val||0).toFixed(2) + '%'; }"},
        },
        responsive=[
            {
                "breakpoint": 640,
                "options": {
                    "plotOptions": {"bar": {"barHeight": "70%"}},
                    "yaxis": {"labels": {"maxWidth": 200}},
                    "legend": {"position": "bottom"},
                },
            }
        ],
    )
    return ApexChart(opts=opts, cls=chart_wrapper_class("horizontal_bar"))


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
            chart_id=chart_id,
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
        # Add title/id and per-point color
        raw = df_calc.select(
            (pl.col("Views") / SCALE_MILLIONS).round(1).alias("x"),
            pl.col("Likes_per_1K").round(1).alias("y"),
            (pl.col("Title").cast(pl.Utf8, strict=False).str.slice(0, 55)).alias(
                "title"
            ),
            (pl.col("id").cast(pl.Utf8, strict=False)).alias("id"),
            pl.col("Likes").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("Comments").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("Duration Formatted").alias("Duration"),
            pl.col("Engagement Rate Raw").alias("EngagementRateRaw"),
        ).to_dicts()
        data, palette = _apply_point_colors(raw)
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
            markers={"size": 4, "hover": {"size": 6}},  # improve hover target
            colors=palette,
        ),
        cls=chart_wrapper_class("scatter"),
    )


def chart_stacked_interactions(
    df: pl.DataFrame,
    chart_id: str = "stacked-interactions",
    top_n: int = 12,
) -> ApexChart:
    """
    Stacked column chart: Top videos by total interactions (Views + Likes + Comments).
    Each column represents one video.
    Stack layers (bottom to top): Views → Likes → Comments.
    Fully compatible with raw YouTube DataFrame – no enrichment required.
    """
    if df is None or df.is_empty():
        return _empty_chart("bar", chart_id)

    # ------------------------------------------------------------------
    # 1. Calculate total interactions and select top N videos
    # ------------------------------------------------------------------
    df_with_total = df.with_columns(
        (pl.col("Views") + pl.col("Likes") + pl.col("Comments")).alias(
            "TotalInteraction"
        )
    )

    df_top = (
        df_with_total.sort("TotalInteraction", descending=True).head(top_n)
        # Final sort by Views descending for consistent visual order
        .sort("Views", descending=True)
    )

    # ------------------------------------------------------------------
    # 2. Prepare categories (truncated titles)
    # ------------------------------------------------------------------
    categories = (
        df_top["Title"]
        .fill_null("Untitled")
        .str.slice(0, 40)  # Slightly longer for vertical bars
        .to_list()
    )

    # ------------------------------------------------------------------
    # 3. Prepare series data
    # ------------------------------------------------------------------
    series = [
        {
            "name": "👀 Views",
            "data": df_top["Views"].fill_null(0).to_list(),
        },
        {
            "name": "👍 Likes",
            "data": df_top["Likes"].fill_null(0).to_list(),
        },
        {
            "name": "💬 Comments",
            "data": df_top["Comments"].fill_null(0).to_list(),
        },
    ]

    # ------------------------------------------------------------------
    # 4. Build ApexCharts options (stacked column – best per docs)
    # ------------------------------------------------------------------
    opts = _apex_opts(
        "bar",
        id=chart_id,
        series=series,
        chart={
            "type": "bar",
            "stacked": True,
            "toolbar": {"show": False},
        },
        plotOptions={
            "bar": {
                "horizontal": False,
                "borderRadius": 6,
                "columnWidth": "55%",
                "dataLabels": {"enabled": False},
            }
        },
        xaxis={
            "categories": categories,
            "title": {"text": "Video"},
            "labels": {
                "rotate": -45,
                "maxHeight": 100,
                "trim": True,
            },
        },
        yaxis={
            "title": {"text": "Interactions"},
            "labels": {
                "formatter": """
                function(val) {
                    if (val >= 1000000) return (val / 1000000).toFixed(1) + 'M';
                    if (val >= 1000) return (val / 1000).toFixed(0) + 'K';
                    return val.toLocaleString();
                }
                """
            },
        },
        title={
            "text": f"📊 Interaction Breakdown – Top {top_n} Videos",
            "align": "left",
            "style": {"fontSize": "16px", "fontWeight": 600},
        },
        colors=["#94a3b8", "#3b82f6", "#10b981"],  # Neutral → Blue → Emerald
        tooltip={
            "shared": True,
            "intersect": False,
            "y": {
                "formatter": """
                function(val, { seriesIndex }) {
                    const names = ['Views', 'Likes', 'Comments'];
                    return val.toLocaleString() + ' ' + names[seriesIndex];
                }
                """
            },
        },
        legend={"position": "top", "horizontalAlign": "center"},
        responsive=[
            {
                "breakpoint": 768,
                "options": {
                    "plotOptions": {"bar": {"columnWidth": "70%"}},
                    "xaxis": {"labels": {"rotate": -60}},
                },
            },
            {
                "breakpoint": 640,
                "options": {
                    "xaxis": {"labels": {"rotate": -90, "maxHeight": 80}},
                },
            },
        ],
    )

    return ApexChart(
        opts=opts,
        cls=chart_wrapper_class("vertical_bar"),
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
            # pl.col("Controversy").round(2).alias("z"),
            (pl.col("Title").str.slice(0, 45)).alias("title"),
            (pl.col("id").cast(pl.Utf8)).alias("id"),
            # Required fields for CLICKABLE_TOOLTIP
            pl.col("Likes").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("Comments").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("Duration Formatted").alias("Duration"),
            pl.col("Engagement Rate Raw").alias("EngagementRateRaw"),
        ).to_dicts()
    except Exception as e:
        logger.warning(f"[charts] Data extraction failed: {e}")
        return _empty_chart("bubble", chart_id)

    return ApexChart(
        opts=_bubble_opts_mobile_safe(
            chart_type="bubble",
            x_label="View Count (Millions)",
            y_label="Engagement Rate (%)",
            # z_label="Controversy Score",
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
                    "return '<div style=\"padding:6px;\"><b>' + pt.title + '</b><br>"
                    "⏱️ Duration: ' + pt.x.toFixed(1) + 'm<br>"
                    "✨ Engagement: ' + pt.y.toFixed(1) + '%<br>"
                    "👀 Views: ' + pt.z + 'M</div>';"
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

    try:
        raw = df.select(
            (pl.col("Comments").cast(pl.Float64, strict=False).fill_null(0)).alias("x"),
            (pl.col("Engagement Rate Raw") * 100).round(2).alias("y"),
            (pl.col("Title").cast(pl.Utf8, strict=False).str.slice(0, 55)).alias(
                "title"
            ),
            (pl.col("id").cast(pl.Utf8, strict=False)).alias("id"),
            # Required fields for CLICKABLE_TOOLTIP
            pl.col("Likes").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("Comments").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("Duration Formatted").alias("Duration"),
            pl.col("Engagement Rate Raw").alias("EngagementRateRaw"),
        ).to_dicts()
        data, palette = _apply_point_colors(raw)
    except Exception as e:
        logger.warning(f"[charts] Data extraction failed: {e}")
        return _empty_chart("scatter", chart_id)

    return ApexChart(
        opts=_scatter_opts_mobile_safe(
            chart_type="scatter",
            x_label="💬 Comments",
            y_label="Engagement Rate (%)",
            chart_id=chart_id,
            disable_zoom=True,
            series=[{"name": "Videos", "data": data}],
            xaxis={"title": {"text": "💬 Comments"}},
            yaxis={"title": {"text": "Engagement Rate (%)"}},
            title={"text": "💬 Comments vs Engagement", "align": "left"},
            colors=palette,
        ),
        cls=chart_wrapper_class("scatter"),
    )


def chart_comments_engagement(
    df: pl.DataFrame, chart_id: str = "comments-engagement"
) -> ApexChart:
    """
    DEPRECATED: Use chart_engagement_correlation(df, "Likes", "Comments", None) instead.
    Scatter: Likes (X) vs Comments (Y).
    """
    return chart_engagement_correlation(
        df,
        x_axis="Likes",
        y_axis="Comments",
        size_by=None,
        chart_id=chart_id,
        minimal=False,
    )


def chart_engagement_correlation(
    df: pl.DataFrame,
    x_axis: str = "Views",
    y_axis: str = "Engagement Rate Raw",
    size_by: str = "Comments",
    chart_id: str = "correlation",
    minimal: bool = False,
) -> ApexChart:
    """
    Universal correlation chart (bubble/scatter).

    Consolidates: views_vs_likes, comments_engagement, duration_vs_engagement, etc.

    Args:
        x_axis: Column name for X-axis
        y_axis: Column name for Y-axis
        size_by: Column for bubble size (None for scatter)
        minimal: If True, use log scales + Tufte styling
    """
    if df is None or df.is_empty():
        return _empty_chart("bubble" if size_by else "scatter", chart_id)

    # Validate columns exist
    required = [x_axis, y_axis]
    if size_by:
        required.append(size_by)
    if not all(col in df.columns for col in required):
        logger.warning(f"[charts] Missing columns {required}")
        return _empty_chart("bubble" if size_by else "scatter", chart_id)

    # Extract data
    x_data = df[x_axis].cast(pl.Float64, strict=False).fill_null(0)
    y_data = df[y_axis].cast(pl.Float64, strict=False).fill_null(0)
    size_data = None
    if size_by:
        size_data = df[size_by].cast(pl.Float64, strict=False).fill_null(0)

    # Build series
    data_points = []
    for i, row in enumerate(df.iter_rows(named=True)):
        point = {
            "x": float(x_data[i]),
            "y": float(y_data[i]),
            "title": _truncate_title(row.get("Title", f"Item {i + 1}"), 60),
            "id": row.get("id", str(i)),
        }
        if size_by:
            point["z"] = float(size_data[i]) if size_data else 0
        data_points.append(point)

    # Apply styling
    chart_type = "bubble" if size_by else "scatter"
    opts = _apex_opts(
        chart_type, id=chart_id, series=[{"name": "Data", "data": data_points}]
    )

    if minimal:
        opts = apply_tufte_economist(opts, chart_type, single_series=True)
        # Log scales for minimal mode
        opts["xaxis"] = {"type": "logarithmic", "title": {"text": x_axis}}
        opts["yaxis"] = {"type": "logarithmic", "title": {"text": y_axis}}

    return ApexChart(opts=opts, cls=chart_wrapper_class(chart_type))


# def chart_views_vs_likes(
#     df: pl.DataFrame, chart_id: str = "views-vs-likes"
# ) -> ApexChart:
#     """Bubble: Views (X) vs Likes (Y), size = Comments."""
#     if df is None or df.is_empty():
#         return _empty_chart("bubble", chart_id)

#     df_safe = _safe_bubble_data(df, max_points=150)

#     try:
#         raw = df_safe.select(
#             (pl.col("Views") / SCALE_MILLIONS).round(1).alias("x"),
#             (pl.col("Likes") / SCALE_THOUSANDS).round(1).alias("y"),
#             ((pl.col("Comments") / 100).cast(pl.Int32)).alias("z"),
#             (pl.col("Title").cast(pl.Utf8, strict=False).str.slice(0, 55)).alias(
#                 "title"
#             ),
#             (pl.col("id").cast(pl.Utf8, strict=False)).alias("id"),
#             # Required fields for CLICKABLE_TOOLTIP
#             pl.col("Likes").cast(pl.Int64, strict=False).fill_null(0),
#             pl.col("Comments").cast(pl.Int64, strict=False).fill_null(0),
#             pl.col("Duration Formatted").alias("Duration"),
#             pl.col("Engagement Rate Raw").alias("EngagementRateRaw"),
#         ).to_dicts()
#         data, palette = _apply_point_colors(raw)
#     except Exception as e:
#         logger.warning(f"[charts] Data extraction failed: {e}")
#         return _empty_chart("bubble", chart_id)

#     return ApexChart(
#         opts=_bubble_opts_mobile_safe(
#             chart_type="bubble",
#             x_label="👀 Views (Millions)",
#             y_label="👍 Likes (Thousands)",
#             z_label="Comments (×100)",
#             chart_id=chart_id,
#             series=[{"name": "Videos", "data": data}],
#             xaxis={"title": {"text": "👀 Views (Millions)"}},
#             yaxis={"title": {"text": "👍 Likes (Thousands)"}},
#             title={
#                 "text": "🎯 Views vs Likes (bubble size = comments)",
#                 "align": "left",
#             },
#             tooltip={
#                 "theme": "light",
#                 "y": {"formatter": "function(val){ return val.toFixed(2) + '%'; }"},
#             },
#             plotOptions={
#                 "bubble": {
#                     "minBubbleRadius": 4,
#                     "maxBubbleRadius": 25,
#                     # "colorByPoint": True,
#                 }
#             },
#             colors=palette,
#         ),
#         cls=chart_wrapper_class("bubble"),
#     )


def chart_views_vs_likes(
    df: pl.DataFrame, chart_id: str = "views-vs-likes"
) -> ApexChart:
    """DEPRECATED: Use chart_engagement_correlation(df, "Views", "Likes") instead."""
    return chart_engagement_correlation(df, "Views", "Likes", "Comments", chart_id)


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


def chart_engagement_sparkline(df: pl.DataFrame) -> ApexChart:
    """
    Compact sparkline: Engagement trend over time.
    Perfect for dashboard cards or inline metrics.
    """
    if df is None or df.is_empty():
        return Span("—", cls="text-muted")

    # Get engagement over time (sorted by rank/index)
    data = (df["Engagement Rate Raw"] * 100).fill_null(0).to_list()

    return ApexChart(
        opts={
            "chart": {
                "type": "area",
                "height": 60,  # Compact height
                "sparkline": {"enabled": True},  # ✅ Removes axes, labels
            },
            "series": [{"name": "Engagement %", "data": data}],
            "stroke": {"curve": "smooth", "width": 2},
            "colors": ["#3B82F6"],
            "fill": {
                "type": "gradient",
                "gradient": {
                    "opacityFrom": 0.4,
                    "opacityTo": 0.1,
                },
            },
            "tooltip": {
                "fixed": {"enabled": False},
                "y": {"formatter": "function(val){ return val.toFixed(1) + '%'; }"},
            },
        }
    )


# Usage in dashboard cards:
def SummaryCard(title: str, value: str, sparkline_df: pl.DataFrame):
    return Card(
        H3(title, cls="text-sm text-muted"),
        Div(
            Span(value, cls="text-3xl font-bold"),
            chart_engagement_sparkline(sparkline_df),  # ✅ Inline chart
            cls="flex items-center justify-between",
        ),
        cls="p-4",
    )


# ============================================================================
# STEP 5: UPDATE EXISTING — chart_views_vs_likes (Add minimal parameter)
# ============================================================================


def chart_views_vs_likes_enhanced(
    df: pl.DataFrame, chart_id: str = "views-vs-likes", minimal: bool = False
) -> ApexChart:
    """
    [ENHANCED version of existing chart_views_vs_likes]

    Bubble: Views (X) vs Likes (Y), size = Comments.

    Args:
        df: Data
        chart_id: Chart ID
        minimal: If True, apply Tufte-Economist styling + log scales + outlier highlighting

    Enhancements (when minimal=True):
    - Log scales on both axes (compress skewed distribution, reveal patterns)
    - Highlight high-engagement videos in red (virality signal)
    - Baseline videos in neutral gray (normal performance)
    - Median reference lines (editorial context)
    - Light grid & thin strokes (Tufte)

    YouTuber Insight:
    - Log scales handle 1K to 10M views in the same visual space
    - Red outliers = videos to analyze for CTR/thumbnail/title patterns
    - Median lines show "typical" performance for content gap analysis
    """
    if df is None or df.is_empty():
        return _empty_chart("bubble", chart_id)

    df_safe = _safe_bubble_data(df, max_points=150)

    try:
        raw = df_safe.select(
            (pl.col("Views") / SCALE_MILLIONS).round(1).alias("x"),
            (pl.col("Likes") / SCALE_THOUSANDS).round(1).alias("y"),
            ((pl.col("Comments") / 100).cast(pl.Int32)).alias("z"),
            (pl.col("Title").cast(pl.Utf8, strict=False).str.slice(0, 55)).alias(
                "title"
            ),
            (pl.col("id").cast(pl.Utf8, strict=False)).alias("id"),
            pl.col("Likes").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("Comments").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("Duration Formatted").alias("Duration"),
            pl.col("Engagement Rate Raw").alias("EngagementRateRaw"),
        ).to_dicts()

        data, palette = _apply_point_colors(raw)

        # If minimal mode: highlight outliers in red
        if minimal:
            eng_threshold = df_safe["Engagement Rate Raw"].quantile(0.85)
            for p in data:
                if p.get("EngagementRateRaw", 0) > eng_threshold:
                    p["fillColor"] = COLOR_HIGHLIGHT_RED  # Viral outliers pop
                else:
                    p["fillColor"] = COLOR_NEUTRAL  # Baseline gray
    except Exception as e:
        logger.warning(f"[charts] Views vs Likes data extraction failed: {e}")
        return _empty_chart("bubble", chart_id)

    # Build options
    opts = _bubble_opts_mobile_safe(
        chart_type="bubble",
        x_label="👀 Views (Millions)",
        y_label="👍 Likes (Thousands)",
        z_label="Comments (×100)",
        chart_id=chart_id,
        series=[{"name": "Videos", "data": data}],
        xaxis={
            "type": "logarithmic" if minimal else "numeric",
            "title": {
                "text": "👀 Views (Millions)" + (" — log scale" if minimal else "")
            },
        },
        yaxis={
            "type": "logarithmic" if minimal else "numeric",
            "title": {
                "text": "👍 Likes (Thousands)" + (" — log scale" if minimal else "")
            },
        },
        title={
            "text": "🎯 Views vs Likes (bubble size = comments)",
            "align": "left",
        },
        tooltip={
            "theme": "light",
            "y": {"formatter": "function(val){ return val.toFixed(2) + ' K'; }"},
        },
        plotOptions={
            "bubble": {
                "minBubbleRadius": 4,
                "maxBubbleRadius": 25,
            }
        },
    )

    # Apply minimal styling
    if minimal:
        opts = apply_tufte_economist(opts, chart_type="bubble", single_series=False)
        # For dense scatter, enable legend (can't use direct labels on 150 points)
        opts["legend"] = {"show": True, "position": "bottom", "fontSize": "11px"}

        # Add median reference lines (Economist style)
        med_v = df_safe["Views"].median() / SCALE_MILLIONS
        med_l = df_safe["Likes"].median() / SCALE_THOUSANDS

        if "annotations" not in opts:
            opts["annotations"] = {}
        opts["annotations"]["xaxis"] = [
            {
                "x": med_v,
                "strokeDashArray": 2,
                "borderColor": COLOR_NEUTRAL,
                "label": {
                    "text": f"Median Views",
                    "style": {
                        "color": COLOR_NEUTRAL,
                        "fontSize": "10px",
                        "background": {"color": "transparent"},
                    },
                },
            }
        ]
        opts["annotations"]["yaxis"] = [
            {
                "y": med_l,
                "strokeDashArray": 2,
                "borderColor": COLOR_NEUTRAL,
                "label": {
                    "text": f"Median Likes",
                    "style": {
                        "color": COLOR_NEUTRAL,
                        "fontSize": "10px",
                        "background": {"color": "transparent"},
                    },
                },
            }
        ]

    return ApexChart(opts=opts, cls=chart_wrapper_class("bubble"))


# ============================================================================
#  Engagement Decay (Tufte + YouTuber Insight)
# ============================================================================


def chart_engagement_decay_minimal(
    df: pl.DataFrame, chart_id: str = "engagement-decay-minimal"
) -> ApexChart:
    """
    Line chart: Engagement Rate (%) vs Days Since Publish.

    Tufte-Minimal Styling:
    - Straight lines (clarity)
    - Outlier labels (direct emphasis — no legend hunting)
    - Light grid, thin strokes
    - No junk (toolbar, unnecessary legends)

    YouTuber Insight:
    - Early-hour traction predicts long-term success
    - Videos that hold engagement over time = quality signal
    - Spike patterns reveal algorithm behavior & audience preferences

    Use Case:
    - "Which of my videos sustained engagement?" (retention predictor)
    - "When do viewers typically abandon?" (editing/pacing insight)
    - "Do controversies create spikes?" (combined with sentiment)

    Args:
        df: DataFrame with columns: PublishedAt, Title, Engagement Rate Raw, Likes, Comments, id
        chart_id: Unique chart ID for Apex

    Returns:
        ApexChart instance with Tufte styling
    """
    if df is None or df.is_empty():
        return _empty_chart("line", chart_id)

    try:
        # Calculate days since publish
        df_decay = (
            df.with_columns(
                pl.col("PublishedAt")
                .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
                .alias("dt")
            )
            .with_columns(
                ((pl.col("dt").max() - pl.col("dt")).dt.total_days()).alias("DaysSince")
            )
            .sort("DaysSince")
        )

        # Build data points with metadata for tooltip
        data_points = []
        for row in df_decay.iter_rows(named=True):
            data_points.append(
                {
                    "x": int(row["DaysSince"]) if row["DaysSince"] else 0,
                    "y": (row["Engagement Rate Raw"] or 0) * 100,
                    "title": _truncate_title(row["Title"], 60),
                    "id": row["id"],
                    "Likes": int(row.get("Likes") or 0),
                    "Comments": int(row.get("Comments") or 0),
                }
            )

        if not data_points:
            return _empty_chart("line", chart_id)

        # Identify top 5 outliers for direct labels (Tufte principle)
        outliers = sorted(data_points, key=lambda p: p.get("y", 0), reverse=True)[:5]

        # Build chart options
        max_eng = max([p["y"] for p in data_points] + [1])
        opts = _apex_opts(
            "line",
            id=chart_id,
            series=[{"name": "Engagement Rate (%)", "data": data_points}],
            xaxis={
                "type": "numeric",
                "title": {"text": "Days Since Publish"},
                "tickAmount": 6,
                "labels": {
                    "formatter": "function(val){ return Math.round(val) + 'd'; }"
                },
            },
            yaxis={
                "title": {"text": "Engagement Rate (%)"},
                "min": 0,
                "max": max_eng * 1.15,  # 15% headroom for labels
                "labels": {
                    "formatter": "function(val){ return val.toFixed(1) + '%'; }"
                },
            },
            stroke={"curve": "straight", "width": 1.2},  # Clarity, thin
            markers={"size": 0},  # Pure line (no dots)
            annotations={
                "points": [
                    {
                        "x": p["x"],
                        "y": p["y"],
                        "label": {
                            "text": p["title"][:18]
                            + ("..." if len(p["title"]) > 18 else ""),
                            "style": {
                                "fontSize": "10px",
                                "color": COLOR_HIGHLIGHT_RED,
                                "background": {"color": "transparent"},
                            },
                        },
                        "marker": {
                            "size": 5,
                            "fillColor": COLOR_HIGHLIGHT_RED,
                            "strokeColor": "#fff",
                            "strokeWidth": 1,
                        },
                    }
                    for p in outliers
                ]
            },
        )

        # Apply Tufte-Economist minimal style
        opts = apply_tufte_economist(opts, chart_type="line", single_series=True)

        # CRITICAL: Restore custom tooltip (YouTuber value: clickable YouTube links)
        opts["tooltip"] = _tooltip_opts_for_chart("line")

        # Add descriptive subtitle
        opts["subtitle"] = {
            "text": "Hover for video details • Red points = strongest sustained engagement",
            "style": {"fontSize": "11px", "color": "#7f8c8d"},
        }

    except Exception as e:
        logger.warning(f"[charts] Engagement decay chart failed: {e}")
        return _empty_chart("line", chart_id)

    return ApexChart(opts=opts, cls=chart_wrapper_class("line"))


def chart_duration_impact_tufte(
    df: pl.DataFrame, chart_id: str = "duration-impact"
) -> ApexChart:
    """
    [REPLACEMENT for existing chart_duration_impact]

    Line: Engagement rate by video duration (sorted).

    Tufte Update:
    - Straight lines (vs smooth curves) for clarity
    - Minimal grid & typography
    - No legend (single series)

    Original author's function signature preserved for drop-in replacement.
    """
    if df is None or df.is_empty():
        return _empty_chart("line", chart_id)

    sorted_df = _safe_sort(df, "Duration", descending=False)
    durations = (
        sorted_df["Duration"].cast(pl.Float64) / 60
    ).to_list()  # Convert to minutes

    eng_rates = (sorted_df["Engagement Rate Raw"] * 100).round(2).to_list()

    # Build with _apex_opts for consistency
    opts = _apex_opts(
        "line",
        id=chart_id,
        series=[{"name": "Engagement Rate (%)", "data": eng_rates}],
        xaxis={
            "title": {"text": "Video Duration (minutes)"},
            "categories": [f"{d:.1f}m" for d in durations],
        },
        yaxis={
            "title": {"text": "Engagement Rate (%)"},
            "labels": {"formatter": "function(val){ return val.toFixed(1) + '%'; }"},
        },
        stroke={"curve": "straight", "width": 1.2},  # Tufte: straight, thin
        title={"text": "⏱️ Does Duration Affect Engagement?", "align": "left"},
    )

    # Apply minimal style
    opts = apply_tufte_economist(opts, chart_type="line", single_series=True)

    return ApexChart(opts=opts, cls=chart_wrapper_class("line"))
