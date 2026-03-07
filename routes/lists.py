"""
Lists route — curated creator list pages.
"""

import logging

from views.lists import render_lists_page

logger = logging.getLogger(__name__)


def lists_route(request):
    """GET /lists - Curated creator lists page."""
    active_tab = request.query_params.get("tab", "top-rated")
    return render_lists_page(active_tab=active_tab)
