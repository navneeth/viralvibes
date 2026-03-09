"""
Database queries for creator lists/rankings.
Specialized functions for the /lists page tab content.
"""

import logging
from typing import Optional

from db import supabase_client, safe_get_value

logger = logging.getLogger(__name__)


def get_top_rated_creators(limit: int = 20) -> list[dict]:
    """
    Get top-rated creators sorted by quality grade and subscribers.

    Grade hierarchy: A+ > A > B+ > B > C
    Within each grade, sort by subscribers descending.

    Args:
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts with stats
    """
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        # Define grade order for sorting
        grade_order = {"A+": 1, "A": 2, "B+": 3, "B": 4, "C": 5}

        response = (
            supabase_client.table("creators")
            .select("*")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .order("current_subscribers", desc=True)
            .limit(limit * 3)  # Fetch extra to sort by grade
            .execute()
        )

        creators = response.data if response.data else []

        # Sort by grade first, then subscribers
        def sort_key(c):
            grade = safe_get_value(c, "quality_grade", "C")
            subs = safe_get_value(c, "current_subscribers", 0)
            return (grade_order.get(grade, 99), -subs)

        creators.sort(key=sort_key)
        return creators[:limit]

    except Exception as e:
        logger.exception(f"Error fetching top rated creators: {e}")
        return []


def get_most_active_creators(limit: int = 20) -> list[dict]:
    """
    Get most active creators sorted by monthly upload frequency.

    Args:
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by monthly_uploads descending
    """
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        response = (
            supabase_client.table("creators")
            .select("*")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .not_.is_("monthly_uploads", "null")
            .order("monthly_uploads", desc=True)
            .limit(limit)
            .execute()
        )

        return response.data if response.data else []

    except Exception as e:
        logger.exception(f"Error fetching most active creators: {e}")
        return []


def get_creators_by_country(country_code: str, limit: int = 10) -> list[dict]:
    """
    Get top creators from a specific country.

    Args:
        country_code: Two-letter country code (e.g., "US", "JP")
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by subscribers
    """
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        response = (
            supabase_client.table("creators")
            .select("*")
            .eq("country_code", country_code)
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .order("current_subscribers", desc=True)
            .limit(limit)
            .execute()
        )

        return response.data if response.data else []

    except Exception as e:
        logger.exception(f"Error fetching creators for country {country_code}: {e}")
        return []


def get_creators_by_category(category: str, limit: int = 10) -> list[dict]:
    """
    Get top creators from a specific topic category.

    Args:
        category: Topic category name (e.g., "Music", "Gaming")
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by subscribers
    """
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        # Use ILIKE for case-insensitive partial match
        # topic_categories can contain multiple comma-separated values
        response = (
            supabase_client.table("creators")
            .select("*")
            .ilike("topic_categories", f"%{category}%")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .order("current_subscribers", desc=True)
            .limit(limit)
            .execute()
        )

        return response.data if response.data else []

    except Exception as e:
        logger.exception(f"Error fetching creators for category {category}: {e}")
        return []


def get_rising_creators(limit: int = 20) -> list[dict]:
    """
    Get fastest-growing creators by 30-day growth rate.

    Filters for creators with:
    - Positive subscriber growth
    - At least 1000 subscribers (avoid noise from tiny channels)

    Args:
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by growth rate descending
    """
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        response = (
            supabase_client.table("creators")
            .select("*")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 1000)
            .not_.is_("subscribers_change_30d", "null")
            .gt("subscribers_change_30d", 0)
            .order("subscribers_change_30d", desc=True)
            .limit(limit)
            .execute()
        )

        return response.data if response.data else []

    except Exception as e:
        logger.exception(f"Error fetching rising creators: {e}")
        return []


def get_veteran_creators(limit: int = 20) -> list[dict]:
    """
    Get veteran creators with channels 10+ years old.

    Filters for:
    - channel_age_days >= 3650 (10 years)
    - Sorted by subscribers to show most successful veterans

    Args:
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by subscribers descending
    """
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        response = (
            supabase_client.table("creators")
            .select("*")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .gte("channel_age_days", 3650)  # 10 years
            .order("current_subscribers", desc=True)
            .limit(limit)
            .execute()
        )

        return response.data if response.data else []

    except Exception as e:
        logger.exception(f"Error fetching veteran creators: {e}")
        return []


def get_top_countries_with_counts(limit: int = 10) -> list[tuple[str, int]]:
    """
    Get top countries by creator count.

    Returns list of (country_code, creator_count) tuples.
    Used for "By Country" tab to show which countries to display.

    Args:
        limit: Maximum number of countries to return

    Returns:
        List of (country_code, count) tuples sorted by count descending
    """
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        # Aggregate count by country
        response = (
            supabase_client.table("creators")
            .select("country_code")
            .not_.is_("channel_name", "null")
            .not_.is_("country_code", "null")
            .gt("current_subscribers", 0)
            .execute()
        )

        creators = response.data if response.data else []

        # Count by country
        country_counts = {}
        for c in creators:
            country = c.get("country_code")
            if country:
                country_counts[country] = country_counts.get(country, 0) + 1

        # Sort by count descending
        sorted_countries = sorted(
            country_counts.items(), key=lambda x: x[1], reverse=True
        )

        return sorted_countries[:limit]

    except Exception as e:
        logger.exception(f"Error fetching top countries: {e}")
        return []


def get_top_categories_with_counts(limit: int = 10) -> list[tuple[str, int]]:
    """
    Get top topic categories by creator count.

    Returns list of (category_name, creator_count) tuples.
    Used for "By Category" tab to show which categories to display.

    Args:
        limit: Maximum number of categories to return

    Returns:
        List of (category, count) tuples sorted by count descending
    """
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        # Fetch all creators with topic categories
        response = (
            supabase_client.table("creators")
            .select("topic_categories")
            .not_.is_("channel_name", "null")
            .not_.is_("topic_categories", "null")
            .gt("current_subscribers", 0)
            .execute()
        )

        creators = response.data if response.data else []

        # Count categories (topic_categories can be comma-separated)
        category_counts = {}
        for c in creators:
            categories_str = c.get("topic_categories", "")
            if categories_str:
                # Split on comma and clean up
                categories = [cat.strip() for cat in categories_str.split(",")]
                for cat in categories:
                    if cat:
                        category_counts[cat] = category_counts.get(cat, 0) + 1

        # Sort by count descending
        sorted_categories = sorted(
            category_counts.items(), key=lambda x: x[1], reverse=True
        )

        return sorted_categories[:limit]

    except Exception as e:
        logger.exception(f"Error fetching top categories: {e}")
        return []
