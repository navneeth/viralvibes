"""
Database queries for creator lists/rankings.
Specialized functions for the /lists page tab content.
"""

import json
import logging
from typing import Optional

from db import supabase_client, safe_get_value
from utils import normalize_category_name

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


def get_top_creators_by_countries(
    country_codes: list[str], limit_per_country: int = 5
) -> dict[str, list[dict]]:
    """
    Batch-fetch top creators for a list of countries in a single DB query.

    Replaces N separate get_creators_by_country() calls when many country
    groups need to be rendered (e.g. the /lists page).

    Args:
        country_codes: ISO 3166-1 alpha-2 codes, e.g. ["US", "GB", "JP"]
        limit_per_country: Max creators to return per country

    Returns:
        Dict of {country_code: [creators sorted by subscribers desc]}
    """
    if not supabase_client or not country_codes:
        return {}

    try:
        fetch_limit = min(len(country_codes) * limit_per_country * 3, 2000)

        response = (
            supabase_client.table("creators")
            .select("*")
            .in_("country_code", country_codes)
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .order("current_subscribers", desc=True)
            .limit(fetch_limit)
            .execute()
        )

        creators = response.data if response.data else []

        # Group by country_code, keep top N per country
        result: dict[str, list[dict]] = {code: [] for code in country_codes}
        for creator in creators:
            code = creator.get("country_code", "")
            if code in result and len(result[code]) < limit_per_country:
                result[code].append(creator)

        return result

    except Exception as e:
        logger.exception(f"Error batch-fetching creators for countries: {e}")
        return {}


def get_top_creators_by_categories(
    categories: list[str], limit_per_category: int = 5
) -> dict[str, list[dict]]:
    """
    Batch-fetch top creators for a list of normalized category names in one query.

    Fetches a large result set ordered by subscribers, then groups client-side
    by normalized category name.  One query replaces N ILIKE queries.

    Args:
        categories: Normalized category names (output of normalize_category_name)
        limit_per_category: Max creators to return per category

    Returns:
        Dict of {category: [creators sorted by subscribers desc]}
    """
    if not supabase_client or not categories:
        return {}

    try:
        fetch_limit = min(len(categories) * limit_per_category * 5, 3000)

        response = (
            supabase_client.table("creators")
            .select("*")
            .not_.is_("channel_name", "null")
            .not_.is_("topic_categories", "null")
            .gt("current_subscribers", 0)
            .order("current_subscribers", desc=True)
            .limit(fetch_limit)
            .execute()
        )

        creators = response.data if response.data else []
        cat_set = set(categories)
        result: dict[str, list[dict]] = {cat: [] for cat in categories}

        for creator in creators:
            raw = creator.get("topic_categories")
            if not raw:
                continue

            # Parse topic_categories — JSON array string, Python list, or CSV
            if isinstance(raw, list):
                creator_cats = raw
            elif isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                    creator_cats = parsed if isinstance(parsed, list) else []
                except (json.JSONDecodeError, ValueError):
                    creator_cats = [c.strip() for c in raw.split(",")]
            else:
                continue

            # Match against each requested category
            for raw_cat in creator_cats:
                normalized = normalize_category_name(str(raw_cat))
                if (
                    normalized in cat_set
                    and len(result[normalized]) < limit_per_category
                ):
                    result[normalized].append(creator)
                    # Don't break — a creator can belong to multiple categories

        return result

    except Exception as e:
        logger.exception(f"Error batch-fetching creators for categories: {e}")
        return {}


def get_rising_creators(limit: int = 20) -> list[dict]:
    """
    Get fastest-growing creators by 30-day growth rate (percentage).

    Growth rate = (subscribers_change_30d / current_subscribers) * 100

    This favors channels with explosive percentage growth regardless of size,
    so a 10k channel doubling (+10k, 100% growth) ranks higher than a 10M
    channel gaining 50k (+50k, 0.5% growth).

    Filters for creators with:
    - Positive subscriber growth
    - At least 1000 subscribers (avoid noise from tiny channels)

    Args:
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by growth rate (%) descending
    """
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        # Fetch extra creators to ensure we have enough after calculating rates
        response = (
            supabase_client.table("creators")
            .select("*")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 1000)
            .not_.is_("subscribers_change_30d", "null")
            .gt("subscribers_change_30d", 0)
            .limit(limit * 3)  # Fetch 3x to allow for rate calculation and sorting
            .execute()
        )

        creators = response.data if response.data else []

        # Calculate growth rate for each creator and attach it
        for creator in creators:
            subs_change = creator.get("subscribers_change_30d", 0)
            current_subs = creator.get(
                "current_subscribers", 1
            )  # Avoid division by zero

            if current_subs > 0:
                # Growth rate as percentage
                growth_rate = (subs_change / current_subs) * 100
                creator["_growth_rate"] = growth_rate
            else:
                creator["_growth_rate"] = 0

        # Sort by growth rate descending
        creators.sort(key=lambda c: c.get("_growth_rate", 0), reverse=True)

        return creators[:limit]

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

    Performance note: This performs client-side aggregation. For production scale
    (10k+ creators), consider creating a database RPC or materialized view:

        CREATE FUNCTION get_top_countries_with_counts(p_limit INT)
        RETURNS TABLE (country_code TEXT, creator_count BIGINT)
        AS $$
            SELECT country_code, COUNT(*) as creator_count
            FROM creators
            WHERE country_code IS NOT NULL AND current_subscribers > 0
            GROUP BY country_code
            ORDER BY creator_count DESC
            LIMIT p_limit;
        $$ LANGUAGE SQL;

    Args:
        limit: Maximum number of countries to return

    Returns:
        List of (country_code, count) tuples sorted by count descending
    """
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        # Fetch with reasonable limit to avoid loading entire table
        # Limit to 50k creators (enough for aggregation, not entire DB)
        MAX_FETCH = 50000

        response = (
            supabase_client.table("creators")
            .select("country_code")
            .not_.is_("channel_name", "null")
            .not_.is_("country_code", "null")
            .gt("current_subscribers", 0)
            .limit(MAX_FETCH)
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


def get_lists_meta() -> dict:
    """
    Return aggregate stats used to drive dynamic tab badges and load-more limits.

    Returns a dict with:
        total_countries  – number of distinct countries represented
        total_categories – number of distinct normalised topic categories
        total_creators   – total creators with valid channel_name + subscribers

    This is a single function so the route only pays the aggregation cost once.
    The heavy lifting is the same scan used by get_top_countries_with_counts /
    get_top_categories_with_counts, but we do it in ONE combined pass.
    """
    if not supabase_client:
        return {"total_countries": 0, "total_categories": 0, "total_creators": 0}

    try:
        MAX_FETCH = 50000

        # Fetch the columns we need for both aggregations in a single call
        response = (
            supabase_client.table("creators")
            .select("country_code, topic_categories")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .limit(MAX_FETCH)
            .execute()
        )

        rows = response.data if response.data else []

        countries: set[str] = set()
        categories: set[str] = set()

        for row in rows:
            cc = row.get("country_code")
            if cc:
                countries.add(cc)

            cats_raw = row.get("topic_categories")
            if cats_raw:
                if isinstance(cats_raw, list):
                    cat_list = cats_raw
                elif isinstance(cats_raw, str):
                    try:
                        cat_list = json.loads(cats_raw)
                        if not isinstance(cat_list, list):
                            cat_list = []
                    except (json.JSONDecodeError, ValueError):
                        cat_list = [c.strip() for c in cats_raw.split(",")]
                else:
                    cat_list = []

                for cat in cat_list:
                    clean = normalize_category_name(str(cat))
                    if clean:
                        categories.add(clean)

        return {
            "total_countries": len(countries),
            "total_categories": len(categories),
            "total_creators": len(rows),
        }

    except Exception as e:
        logger.exception(f"Error fetching lists meta: {e}")
        return {"total_countries": 0, "total_categories": 0, "total_creators": 0}


def get_country_groups(
    offset: int = 0, limit: int = 8, creators_per_group: int = 5
) -> list[dict]:
    """
    Return paginated country groups, each with their top creators.

    Uses a single batch DB query for the creators rather than one per country.
    Each item: { country_code, count, creators }

    Args:
        offset: How many top countries to skip (for load-more pagination)
        limit: How many country groups to return
        creators_per_group: How many creators per country

    Returns:
        List of country group dicts
    """
    top_countries = get_top_countries_with_counts(limit=offset + limit)
    page = top_countries[offset : offset + limit]

    if not page:
        return []

    # Batch-fetch creators for all countries in this page — 1 query instead of N
    country_codes = [cc for cc, _ in page]
    creators_by_country = get_top_creators_by_countries(
        country_codes, limit_per_country=creators_per_group
    )

    return [
        {
            "country_code": country_code,
            "count": count,
            "creators": creators_by_country.get(country_code, []),
        }
        for country_code, count in page
    ]


def get_category_groups(
    offset: int = 0, limit: int = 8, creators_per_group: int = 5
) -> list[dict]:
    """
    Return paginated category groups, each with their top creators.

    Uses a single batch DB query for the creators rather than one per category.
    Each item: { category, count, creators }

    Args:
        offset: How many top categories to skip (for load-more pagination)
        limit: How many category groups to return
        creators_per_group: How many creators per category

    Returns:
        List of category group dicts
    """
    top_categories = get_top_categories_with_counts(limit=offset + limit)
    page = top_categories[offset : offset + limit]

    if not page:
        return []

    # Batch-fetch creators for all categories in this page — 1 query instead of N
    category_names = [cat for cat, _ in page]
    creators_by_category = get_top_creators_by_categories(
        category_names, limit_per_category=creators_per_group
    )

    return [
        {
            "category": category,
            "count": count,
            "creators": creators_by_category.get(category, []),
        }
        for category, count in page
    ]


def get_top_categories_with_counts(limit: int = 10) -> list[tuple[str, int]]:
    """
    Get top topic categories by creator count.

    Returns list of (category_name, creator_count) tuples.
    Used for "By Category" tab to show which categories to display.

    Performance note: This performs client-side aggregation with comma-separated
    category parsing. For production scale, consider creating a database RPC that
    handles the aggregation server-side after normalizing categories to a separate
    table or JSONB array.

    Args:
        limit: Maximum number of categories to return

    Returns:
        List of (category, count) tuples sorted by count descending
    """
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        # Fetch with reasonable limit to avoid loading entire table
        # Limit to 50k creators (enough for aggregation, not entire DB)
        MAX_FETCH = 50000

        response = (
            supabase_client.table("creators")
            .select("topic_categories")
            .not_.is_("channel_name", "null")
            .not_.is_("topic_categories", "null")
            .gt("current_subscribers", 0)
            .limit(MAX_FETCH)
            .execute()
        )

        creators = response.data if response.data else []

        # Count categories (topic_categories stored as JSON array or PostgreSQL array)
        category_counts = {}
        for c in creators:
            categories_raw = c.get("topic_categories")
            if not categories_raw:
                continue

            # Parse categories - can be JSON array string, Python list, or PostgreSQL array
            categories = []
            if isinstance(categories_raw, list):
                # Already a list (from PostgreSQL array type)
                categories = categories_raw
            elif isinstance(categories_raw, str):
                # JSON array string - parse it
                try:
                    categories = json.loads(categories_raw)
                    if not isinstance(categories, list):
                        categories = []
                except (json.JSONDecodeError, ValueError):
                    # Fallback: treat as comma-separated for old data
                    categories = [cat.strip() for cat in categories_raw.split(",")]

            # Count each category, cleaning up names
            for cat in categories:
                if cat:
                    # Clean up category name using shared helper
                    clean_cat = normalize_category_name(str(cat))
                    if clean_cat:
                        category_counts[clean_cat] = (
                            category_counts.get(clean_cat, 0) + 1
                        )

        # Sort by count descending
        sorted_categories = sorted(
            category_counts.items(), key=lambda x: x[1], reverse=True
        )

        return sorted_categories[:limit]

    except Exception as e:
        logger.exception(f"Error fetching top categories: {e}")
        return []
