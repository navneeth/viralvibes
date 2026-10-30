from scripts.lists_video_gen import LISTS, _format_metric


def test_most_active_video_metric_matches_recent_cadence_ranking():
    spec = LISTS["most-active"]

    assert spec.col_header == "AVG GAP"
    assert spec.metric_key == "avg_days_between_uploads"
    assert _format_metric(spec, {"avg_days_between_uploads": 0.02})[0] == "~29m"


def test_most_active_video_hides_invalid_rounded_interval():
    spec = LISTS["most-active"]

    assert _format_metric(spec, {"avg_days_between_uploads": 0.01})[0] == "—"
