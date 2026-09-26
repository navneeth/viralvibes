from services.creator_search_intent import (
    SearchIntentKind,
    classify_creator_search,
    normalize_handle,
)


def test_handle_looks_like_exact_handle():
    intent = classify_creator_search("@MrBeast")
    assert intent.kind is SearchIntentKind.EXACT_HANDLE
    assert intent.normalized == "mrbeast"
    assert intent.display == "@mrbeast"


def test_bare_handle_matches_at_prefixed_behavior():
    intent = classify_creator_search("mrbeast")
    assert intent.kind is SearchIntentKind.EXACT_HANDLE
    assert intent.normalized == "mrbeast"
    assert intent.display == "@mrbeast"


def test_channel_id_is_strict_and_not_prefixed():
    intent = classify_creator_search("UCX6OQ3DkcsbYNE6H8uQQuVA")
    assert intent.kind is SearchIntentKind.CHANNEL_ID
    assert intent.normalized == "UCX6OQ3DkcsbYNE6H8uQQuVA"
    assert intent.display == "UCX6OQ3DkcsbYNE6H8uQQuVA"


def test_mixed_tokens_are_text_search_not_handle_search():
    intent = classify_creator_search("@mrbeast from Germany")
    assert intent.kind is SearchIntentKind.TEXT_SEARCH


def test_invalid_handle_is_flagged_for_validation():
    intent = classify_creator_search("@")
    assert intent.kind is SearchIntentKind.INVALID_HANDLE
    assert intent.error is not None


def test_normalize_handle_strips_only_leading_at_symbols():
    assert normalize_handle("@@MrBeast") == "mrbeast"
    assert normalize_handle("mrbeast@") == "mrbeast@"


def test_youtube_url_extracts_to_handle():
    intent = classify_creator_search("https://youtube.com/@MrBeast")
    assert intent.kind is SearchIntentKind.EXACT_HANDLE
    assert intent.normalized == "mrbeast"
    assert intent.display == "@mrbeast"
