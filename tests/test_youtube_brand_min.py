"""Regression tests for the YouTube branding-min icon size contract.

Google's YouTube API branding guidelines require a 20 px minimum on all
digital media renderings of the play-button icon:
https://developers.google.com/youtube/terms/developer-policies#f.-user-experience

This test suite pins the two failure modes we hit in production:

1. `YtIcon(size=...)` must produce an SVG whose rendered `width`/`height`
   attributes are AT LEAST 20, even if a caller asks for less.
2. The SVG must carry an inline `style` with `flex-shrink:0` and
   `min-width`/`min-height` so a flex parent cannot squish it below the
   floor (the failure mode observed on creator cards).
"""

import re

import pytest

from components.buttons import _YT_BRAND_MIN_PX, YtIcon


BRAND_MIN = 20


def _svg_size(html: str) -> tuple[int, int]:
    """Return (width, height) parsed from the SVG's inline attributes."""
    w_match = re.search(r'width="(\d+)"', html)
    h_match = re.search(r'height="(\d+)"', html)
    assert w_match, f"no width attribute found in {html!r}"
    assert h_match, f"no height attribute found in {html!r}"
    return int(w_match.group(1)), int(h_match.group(1))


def _svg_style(html: str) -> str:
    """Return the inline style string."""
    m = re.search(r'style="([^"]+)"', html)
    assert m, f"no style attribute found in {html!r}"
    return m.group(1)


def test_default_size_is_at_or_above_brand_min():
    html = str(YtIcon())
    w, h = _svg_size(html)
    assert w >= BRAND_MIN
    assert h >= BRAND_MIN
    assert w == h == _YT_BRAND_MIN_PX


@pytest.mark.parametrize("requested", [24, 32, 48])
def test_larger_sizes_pass_through(requested):
    html = str(YtIcon(size=requested))
    w, h = _svg_size(html)
    assert w == requested
    assert h == requested


@pytest.mark.parametrize("requested", [0, 8, 13, 16, 19])
def test_below_brand_min_is_clamped(requested):
    """A caller asking for less than 20 px must NOT get less than 20 px."""
    html = str(YtIcon(size=requested))
    w, h = _svg_size(html)
    assert w >= BRAND_MIN, f"YtIcon rendered {w}px width, below {BRAND_MIN}px brand minimum"
    assert h >= BRAND_MIN, f"YtIcon rendered {h}px height, below {BRAND_MIN}px brand minimum"


def test_inline_style_prevents_flex_squish():
    html = str(YtIcon())
    style = _svg_style(html)
    # flex-shrink:0 blocks a flex parent from shrinking us below intrinsic size.
    assert "flex-shrink:0" in style
    # min-width / min-height back up the width/height attributes against any
    # CSS override that would otherwise take precedence.
    assert "min-width:" in style
    assert "min-height:" in style


@pytest.mark.parametrize("variant", ["branded", "mono"])
def test_both_variants_respect_brand_min(variant):
    html = str(YtIcon(variant=variant, size=8))
    w, h = _svg_size(html)
    assert w >= BRAND_MIN
    assert h >= BRAND_MIN
