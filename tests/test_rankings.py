from services.rankings import (
    country_slug,
    ranking_category_slug,
    ranking_path,
    resolve_country_slug,
    resolve_ranking_category_slug,
)


def test_resolve_country_slug_handles_search_friendly_names():
    assert resolve_country_slug("united-states") == "US"
    assert resolve_country_slug("uk") == "GB"
    assert resolve_country_slug("south-korea") == "KR"


def test_ranking_path_uses_canonical_slugs():
    assert ranking_path("Video game culture", "US") == "/rankings/gaming/united-states"
    assert country_slug("GB") == "united-kingdom"


def test_ranking_category_aliases_use_search_friendly_terms():
    assert resolve_ranking_category_slug("gaming") == "Video game culture"
    assert resolve_ranking_category_slug("fitness") == "Physical fitness"
    assert ranking_category_slug("Lifestyle (sociology)") == "lifestyle"
