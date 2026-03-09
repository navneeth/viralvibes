"""
Lists route — curated creator list pages.
"""

import logging

from db_lists import (
    get_most_active_creators,
    get_rising_creators,
    get_top_categories_with_counts,
    get_top_countries_with_counts,
    get_top_rated_creators,
    get_veteran_creators,
    get_creators_by_country,
    get_creators_by_category,
)
from views.lists import render_lists_page

logger = logging.getLogger(__name__)


def lists_route(request):
    """GET /lists - Curated creator lists page."""
    active_tab = request.query_params.get("tab", "top-rated")

    # Fetch data based on active tab to minimize database queries
    tab_data = {}

    if active_tab == "top-rated":
        tab_data["top_rated"] = get_top_rated_creators(limit=20)
    elif active_tab == "most-active":
        tab_data["most_active"] = get_most_active_creators(limit=20)
    elif active_tab == "by-country":
        # Get top 8 countries and fetch top 5 creators from each
        top_countries = get_top_countries_with_counts(limit=8)
        tab_data["country_rankings"] = [
            {
                "country_code": country_code,
                "count": count,
                "creators": get_creators_by_country(country_code, limit=5),
            }
            for country_code, count in top_countries
        ]
    elif active_tab == "by-category":
        # Get top 8 categories and fetch top 5 creators from each
        top_categories = get_top_categories_with_counts(limit=8)
        tab_data["category_rankings"] = [
            {
                "category": category,
                "count": count,
                "creators": get_creators_by_category(category, limit=5),
            }
            for category, count in top_categories
        ]
    elif active_tab == "rising":
        tab_data["rising"] = get_rising_creators(limit=20)
    elif active_tab == "veterans":
        tab_data["veterans"] = get_veteran_creators(limit=20)

    return render_lists_page(active_tab=active_tab, tab_data=tab_data)
