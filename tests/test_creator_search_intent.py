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


def test_channel_id_url_extracts_to_channel_id():
    intent = classify_creator_search("https://youtube.com/channel/UCX6OQ3DkcsbYNE6H8uQQuVA")
    assert intent.kind is SearchIntentKind.CHANNEL_ID
    assert intent.normalized == "UCX6OQ3DkcsbYNE6H8uQQuVA"
    assert intent.display == "UCX6OQ3DkcsbYNE6H8uQQuVA"


def test_legacy_custom_url_path_is_text_search_not_invalid():
    """A pasted legacy /c/ URL isn't a stable identifier — it should fall
    through to a normal text search, not be rejected as an invalid handle."""
    intent = classify_creator_search("https://youtube.com/c/OldCustomName")
    assert intent.kind is SearchIntentKind.TEXT_SEARCH


def test_legacy_user_url_path_is_text_search_not_invalid():
    intent = classify_creator_search("https://youtube.com/user/somebody")
    assert intent.kind is SearchIntentKind.TEXT_SEARCH


def test_lowercased_channel_id_is_text_search_not_handle():
    """Channel ids are case-sensitive. A lowercased channel id shouldn't be
    silently treated as a valid @handle — that would offer to add a creator
    whose "handle" is an obviously garbled channel id."""
    intent = classify_creator_search("ucx6oq3dkcsbyne6h8uqquva")
    assert intent.kind is SearchIntentKind.TEXT_SEARCH


def test_handle_with_slash_is_invalid():
    intent = classify_creator_search("@handle/with/slash")
    assert intent.kind is SearchIntentKind.INVALID_HANDLE


def test_handle_length_bound_matches_production_not_youtube_real_rule():
    """HANDLE_BODY_RE intentionally matches db._HANDLE_RE's existing bound
    (1-100 chars), not YouTube's real handle rule (3-30), until a length
    audit of real stored custom_url values confirms tightening is safe."""
    assert classify_creator_search("@ab").kind is SearchIntentKind.EXACT_HANDLE
    assert classify_creator_search("@" + "a" * 50).kind is SearchIntentKind.EXACT_HANDLE
    assert classify_creator_search("@" + "a" * 101).kind is SearchIntentKind.INVALID_HANDLE
