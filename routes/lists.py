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
    """
    GET /lists - Curated creator lists page.

    Loads all tab data upfront to support UIkit's client-side tab switcher.
    This prevents showing placeholders when users click different tabs.
    """
    active_tab = request.query_params.get("tab", "top-rated")

    # Load data for ALL tabs upfront (UIkit switcher renders all panels at once)
    # This ensures client-side tab switching shows real data, not placeholders
    tab_data = {}

    # Top Rated: Quality-sorted creators
    tab_data["top_rated"] = get_top_rated_creators(limit=20)

    # Most Active: Upload frequency leaders
    tab_data["most_active"] = get_most_active_creators(limit=20)

    # By Country: Grouped rankings by country
    top_countries = get_top_countries_with_counts(limit=8)
    tab_data["country_rankings"] = [
        {
            "country_code": country_code,
            "count": count,
            "creators": get_creators_by_country(country_code, limit=5),
        }
        for country_code, count in top_countries
    ]

    # By Category: Grouped rankings by topic category
    top_categories = get_top_categories_with_counts(limit=8)
    tab_data["category_rankings"] = [
        {
            "category": category,
            "count": count,
            "creators": get_creators_by_category(category, limit=5),
        }
        for category, count in top_categories
    ]

    # Rising Stars: Fastest growth rate
    tab_data["rising"] = get_rising_creators(limit=20)

    # Veterans: 10+ year channels
    tab_data["veterans"] = get_veteran_creators(limit=20)

    return render_lists_page(active_tab=active_tab, tab_data=tab_data)
