# views/table.py
from urllib.parse import quote_plus

from fasthtml.common import *

from components import (
    category_emoji_cell,
    number_cell,
    thumbnail_cell,
    title_cell,
)
from utils import (
    format_duration,
    format_number,
    format_percentage,
    get_columns,
    get_row_count,
    get_unique_count,
    has_column,
)

# Manually define the headers here instead of importing them
COLUMNS = {
    "Rank": ("Rank", "Rank"),
    "Title": ("Title", "Title"),
    "Thumbnail": ("Thumbnail", "Thumbnail"),
    "Views": ("Views", "Views Formatted"),
    "Likes": ("Likes", "Likes Formatted"),
    "Comments": ("Comments", "Comments Formatted"),
    "Duration": ("Duration", "Duration Formatted"),
    "Engagement Rate": ("Engagement Rate Raw", "Engagement Rate (%)"),
    "Category": ("Category Emoji", "Category Emoji"),
}
DISPLAY_HEADERS = list(COLUMNS.keys())


def get_sort_col(header: str) -> str:
    """Get raw column for sorting."""
    return COLUMNS[header][0]


def get_render_col(header: str) -> str:
    """Get formatted column for display."""
    return COLUMNS[header][1]


def build_table_footer(summary_stats, svc_headers):
    """
    Build table footer dynamically from DISPLAY_HEADERS.

    Maps each header to its corresponding summary stat and formats it appropriately.
    Ensures footer structure stays in sync with header structure.

    Args:
        summary_stats: Dict with keys like 'total_views', 'total_likes', 'avg_engagement', etc.
        svc_headers: List of headers in display order (from DISPLAY_HEADERS)

    Returns:
        Tfoot component with properly aligned summary cells
    """
    footer_cells = []
    label_added = False

    for header in svc_headers:
        if header == "Rank":
            # Empty cell for rank column
            footer_cells.append(Td("", cls="px-4 py-3 font-bold text-left"))

        elif header == "Title":
            # Add "Total / Avg" label in first numeric column (Title)
            if not label_added:
                footer_cells.append(
                    Td("Total / Avg", cls="px-4 py-3 font-bold text-left")
                )
                label_added = True

        elif header == "Thumbnail":
            # Empty cell for thumbnail
            footer_cells.append(Td("", cls="px-4 py-3 text-center"))

        elif header == "Views":
            footer_cells.append(
                Td(
                    format_number(summary_stats.get("total_views", 0)),
                    cls="px-4 py-3 text-right font-bold",
                )
            )

        elif header == "Likes":
            footer_cells.append(
                Td(
                    format_number(summary_stats.get("total_likes", 0)),
                    cls="px-4 py-3 text-right font-bold",
                )
            )

        elif header == "Comments":
            footer_cells.append(
                Td(
                    format_number(summary_stats.get("total_comments", 0)),
                    cls="px-4 py-3 text-right font-bold",
                )
            )

        elif header == "Duration":
            # Average duration in seconds
            avg_duration = summary_stats.get("avg_duration", 0)
            formatted_duration = (
                format_duration(int(avg_duration)) if avg_duration is not None else "—"
            )
            footer_cells.append(
                Td(
                    formatted_duration,
                    cls="px-4 py-3 text-center font-bold",
                )
            )

        elif header == "Engagement Rate":
            footer_cells.append(
                Td(
                    f"{summary_stats.get('avg_engagement', 0):.2%}",
                    cls="px-4 py-3 text-center font-bold text-green-600",
                )
            )

        elif header == "Category":
            footer_cells.append(
                Td(
                    (
                        f"{summary_stats.get('category_count', 0)} categories"
                        if summary_stats.get("category_count", 0) > 0
                        else "—"
                    ),
                    cls="px-4 py-3 text-center font-semibold text-blue-600",
                )
            )

        else:
            # Fallback for any unknown headers
            footer_cells.append(Td("", cls="px-4 py-3"))

    return Tfoot(
        Tr(*footer_cells, cls="bg-gray-50"),
        cls="border-t-2 border-gray-300",
    )


def render_playlist_table(
    df,
    summary_stats,
    playlist_url: str,
    valid_sort: str,
    valid_order: str,
    next_order,
    user_id: str | None = None,
):
    """Pure extraction of existing table rendering. No behavior changes."""
    # Use yt_service display headers, but map them to the actual df columns used in table
    svc_headers = (
        # yt_service.get_display_headers()
        DISPLAY_HEADERS  # e.g., ["Rank","Title","Views","Likes",...]
    )
    # Map display header → actual column in DF (raw for sorting, formatted for display)

    # Build sortable_map: header → raw numeric column
    df_columns = get_columns(df)
    sortable_map = {
        h: get_sort_col(h) for h in DISPLAY_HEADERS if get_sort_col(h) in df_columns
    }

    # --- THEAD ---
    thead = Thead(
        Tr(
            *[
                (
                    Th(
                        A(
                            h
                            + (
                                " ▲"
                                if h == valid_sort and valid_order == "asc"
                                else (
                                    " ▼"
                                    if h == valid_sort and valid_order == "desc"
                                    else ""
                                )
                            ),
                            href="#",
                            hx_get=f"/validate/full?playlist_url={quote_plus(playlist_url)}&sort_by={quote_plus(h)}&order={next_order(h)}",
                            hx_target="#playlist-table-container",
                            hx_swap="outerHTML",
                            cls="text-white font-semibold hover:underline",
                        ),
                        cls="px-4 py-2 text-sm text-left",
                    )
                    if h in sortable_map
                    else Th(
                        h,
                        cls="px-4 py-2 text-sm text-white font-semibold text-left",
                    )
                )
                for h in svc_headers
            ],
            cls="bg-gradient-to-r from-blue-600 to-blue-700 text-white",
        )
    )

    # --- TBODY ---
    # --- Build tbody with CORRECT display columns ---
    rows = []
    for row in df:
        cells = []
        for h in svc_headers:
            if h == "Thumbnail":
                cell = thumbnail_cell(
                    row.get("Thumbnail") or row.get("thumbnail") or "",
                    row.get("id"),
                    row.get("Title"),
                )
                td_cls = "px-4 py-3 text-center"

            elif h == "Title":
                cell = title_cell(row)
                td_cls = "px-4 py-3"

            elif h in ("Views", "Likes", "Comments"):
                cell = number_cell(row.get(get_render_col(h), row.get(h)))
                td_cls = "px-4 py-3 text-right"

            elif h == "Duration":
                cell = Div(format_duration(row.get("Duration")), cls="text-center")
                td_cls = "px-4 py-3 text-center"

            elif h == "Engagement Rate":
                raw = row.get(get_render_col(h), row.get("Engagement Rate Raw"))
                cell = Div(
                    format_percentage(raw),
                    cls="text-center font-semibold text-green-600",
                )
                td_cls = "px-4 py-3 text-center"

            elif h == "Category":  # ✅ Category with Emoji
                cell = category_emoji_cell(row)
                td_cls = "px-4 py-3 text-center"

            else:
                # default fallback
                val = row.get(get_render_col(h), row.get(h, ""))
                cell = Div(val)
                td_cls = "px-4 py-3"

            cells.append(Td(cell, cls=td_cls))

        rows.append(
            Tr(
                *cells,
                cls="bg-white hover:bg-gray-50 transition-shadow border-b border-gray-100",
            )
        )

    tbody = Tbody(*rows, cls="divide-y divide-gray-100")

    # --- TFOOT ---
    # Build footer dynamically from DISPLAY_HEADERS to ensure sync with thead
    # Create a copy of summary_stats to avoid mutating the passed dict
    footer_stats = {**summary_stats}
    if has_column(df, "CategoryName") and get_row_count(df) > 0:
        footer_stats["category_count"] = get_unique_count(df, "CategoryName")
    else:
        footer_stats["category_count"] = 0

    tfoot = build_table_footer(footer_stats, svc_headers)

    # --- Final: table with wrapper ---
    return Div(
        Table(
            thead,
            tbody,
            tfoot,
            id="playlist-table",
            cls="w-full text-sm text-gray-700 border border-gray-200 rounded-lg shadow-sm overflow-hidden",
        ),
        id="playlist-table-container",
        cls="overflow-x-auto mb-8",
    )
