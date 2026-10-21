import json

import pytest

from services.creator_topics import creator_topic_export_fields, parse_creator_topics


def test_legacy_topics_do_not_infer_source_or_computation_date():
    profile = parse_creator_topics(
        json.dumps([{"kw": "mechanical keyboards", "score": 0.7}]),
        uploaded_at="2026-10-20T10:00:00Z",
    )
    assert profile.topics == ("mechanical keyboards",)
    assert profile.source_label == "Source unavailable"
    assert profile.computed_at == ""
    assert profile.uploaded_at == "2026-10-20T10:00:00+00:00"


@pytest.mark.parametrize(
    "payload",
    [None, "broken", "{}", {}, 1, True, "x" * 65_537],
    ids=["null", "invalid-json", "json-object", "object", "number", "boolean", "oversized"],
)
def test_invalid_payload_is_empty(payload):
    assert parse_creator_topics(payload).topics == ()


def test_topics_are_bounded_normalized_and_deduplicated():
    payload = [None, {"kw": 4}, {"kw": "  "}, {"kw": "x" * 121}]
    payload += [{"kw": " Custom   switches "}, {"kw": "custom switches"}]
    payload += [{"kw": f"topic {index}"} for index in range(30)]
    profile = parse_creator_topics(payload)
    assert profile.topics[0] == "Custom switches"
    assert len(profile.topics) == 15


@pytest.mark.parametrize(
    "source,transcripts,descriptions,label",
    [
        ("transcript", 2, 0, "Transcripts"),
        ("description", 0, 2, "Video descriptions"),
        ("mixed", 1, 1, "Mixed sources"),
        ("transcript", 0, 2, "Source unavailable"),
        ("transcript", True, 0, "Source unavailable"),
        ("transcript", 5, 0, "Source unavailable"),
        ([], 2, 0, "Source unavailable"),
    ],
)
def test_source_requires_versioned_consistent_counts(source, transcripts, descriptions, label):
    profile = parse_creator_topics(
        [{"kw": "keyboards"}],
        {
            "schema_version": 1,
            "source": source,
            "sampled_video_count": 2,
            "transcript_video_count": transcripts,
            "description_video_count": descriptions,
        },
    )
    assert profile.source_label == label


def test_export_keeps_computed_and_uploaded_dates_separate_and_neutralizes_formulas():
    fields = creator_topic_export_fields(
        {
            "transcript_keywords": [{"kw": "=HYPERLINK(bad)"}],
            "transcript_keywords_metadata": {
                "schema_version": 1,
                "source": "description",
                "sampled_video_count": 2,
                "transcript_video_count": 0,
                "description_video_count": 2,
                "computed_at": "2026-10-19T10:00:00Z",
                "extractor_version": "@unsafe",
            },
            "transcript_keywords_updated_at": "2026-10-20T10:00:00Z",
            "private_context": "must not leak",
        }
    )
    assert fields["Content Topics"].startswith("'=")
    assert fields["Extractor Version"] == "'@unsafe"
    assert fields["Keyword Source"] == "Video descriptions"
    assert fields["Topics Computed At"].startswith("2026-10-19")
    assert fields["Topics Uploaded At"].startswith("2026-10-20")
    assert "private_context" not in fields


@pytest.mark.parametrize(
    "metadata",
    [None, "bad", [], {"schema_version": 2}, {"schema_version": 1, "computed_at": "not a date"}],
)
def test_invalid_metadata_does_not_break_legacy_topics(metadata):
    profile = parse_creator_topics([{"kw": "keyboards"}], metadata)
    assert profile.topics == ("keyboards",)
    assert profile.source_label == "Source unavailable"
    assert profile.computed_at == ""
