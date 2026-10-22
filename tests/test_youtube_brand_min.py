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
from html.parser import HTMLParser
from pathlib import Path
from xml.etree import ElementTree

import pytest

from fasthtml.common import to_xml

from components.buttons import _YT_BRAND_MIN_PX, YouTubeBrandIcon, YtIcon


BRAND_MIN = 20


class _Markup(HTMLParser):
    def __init__(self, html: str):
        super().__init__()
        self.elements = []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        self.elements.append((tag, dict(attrs)))


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


def test_official_icon_preserves_artwork_proportions():
    html = to_xml(YouTubeBrandIcon())
    assert 'src="/assets/icons/youtube-official-red.svg"' in html
    assert "height:auto" in html
    assert "object-fit:contain" in html
    assert "flex-shrink:0" in html
    assert 'aria-hidden="true"' in html


@pytest.mark.parametrize("width", [26, 52, 104])
def test_official_icon_width_scales_proportionally(width):
    markup = _Markup(to_xml(YouTubeBrandIcon(width=width)))
    image = next(attrs for tag, attrs in markup.elements if tag == "img")
    assert float(image["width"]) == width
    assert float(image["height"]) == pytest.approx(width * 515.868 / 602.187, abs=0.001)
    assert f"width:{width}px" in image["style"]


@pytest.mark.parametrize("width", [0, -1])
def test_official_icon_rejects_nonpositive_width(width):
    with pytest.raises(ValueError, match="width must be positive"):
        YouTubeBrandIcon(width=width)


def test_official_asset_contains_vector_paths_not_a_bitmap():
    asset = Path(__file__).resolve().parents[1] / "assets/icons/youtube-official-red.svg"
    root = ElementTree.parse(asset).getroot()
    namespace = {"svg": "http://www.w3.org/2000/svg"}
    assert root.attrib["viewBox"] == "0 0 602.187 515.868"
    assert not root.findall(".//svg:image", namespace)
    paths = root.findall(".//svg:g/svg:path", namespace)
    assert len(paths) == 2
    assert {path.attrib["fill"] for path in paths} == {"#ff0033", "#ffffff"}


def test_footer_uses_official_icon_in_accessible_channel_link():
    from components.sections import footer

    html = to_xml(footer())
    elements = _Markup(html).elements
    assert any(
        tag == "img" and attrs.get("src") == "/assets/icons/youtube-official-red.svg"
        for tag, attrs in elements
    )
    assert not any(tag == "uk-icon" and attrs.get("icon") == "youtube" for tag, attrs in elements)
    link = next(
        attrs
        for tag, attrs in elements
        if tag == "a" and attrs.get("aria-label") == "ViralVibes on YouTube"
    )
    assert link["target"] == "_blank"
    assert set(link["rel"].split()) == {"noopener", "noreferrer"}
    assert "focus-visible:outline" in link["class"]
    assert "hover:scale-110" not in link["class"]


def test_social_link_preserves_generic_and_custom_artwork_contracts():
    from components.sections import _SocialIcon

    generic = _Markup(to_xml(_SocialIcon("x", "https://example.com", "Social link")))
    custom = _Markup(
        to_xml(
            _SocialIcon(
                "youtube", "https://example.com", "Social link", artwork=YouTubeBrandIcon(width=104)
            )
        )
    )
    for markup in (generic, custom):
        link = next(attrs for tag, attrs in markup.elements if tag == "a")
        assert link["href"] == "https://example.com"
        assert link["aria-label"] == link["title"] == "Social link"
        assert link["target"] == "_blank"
        assert set(link["rel"].split()) == {"noopener", "noreferrer"}
        assert "focus-visible:outline" in link["class"]
    assert any(tag == "uk-icon" and attrs.get("icon") == "x" for tag, attrs in generic.elements)
    assert not any(tag == "uk-icon" for tag, attrs in custom.elements)
    image = next(attrs for tag, attrs in custom.elements if tag == "img")
    assert image["width"] == "104"


def test_compact_form_uses_official_icon_without_random_prefill(monkeypatch):
    from components.cards import AnalysisFormCard

    def unexpected_sample_fetch(*args, **kwargs):
        pytest.fail("Compact form must not fetch duplicate samples")

    monkeypatch.setattr("components.cards.fetch_playlists", unexpected_sample_fetch)
    html = to_xml(AnalysisFormCard(compact=True))
    assert "/assets/icons/youtube-official-red.svg" in html
    assert 'value=""' in html
    assert 'for="playlist_url"' in html
    assert '<label for="playlist_url" class="block' in html
    assert 'hx-post="/validate/url"' in html
    assert 'hx-target="#validation-feedback"' in html
    assert 'id="preview-box"' in html
    assert 'id="result"' in html
    assert 'aria-live="polite"' in html
    assert "max-w-420px" not in html
    assert "steps-container" not in html
    assert "No playlist?" not in html
