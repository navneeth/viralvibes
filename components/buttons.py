"""Button components for the ViralVibes application."""

from typing import Optional

from fasthtml.common import *
from monsterui.all import *

from constants import STYLES


def cta(text: str, icon: Optional[str] = None, kind: str = "full", **kwargs) -> Button:
    """Create a CTA using centralized CSS classes
    from main.css (kind='full'|'refresh'|'secondary')."""
    kind_map = {
        "full": "btn_full",
        "refresh": "btn_refresh",
        "secondary": "cta_secondary",
    }
    cls_key = kind_map.get(kind, "btn_full")
    base_cls = STYLES.get(cls_key, STYLES.get("btn_full", ""))

    icon_comp = UkIcon(icon, cls="mr-2") if icon else None
    content = Span(icon_comp, text) if icon_comp else text

    return Button(
        content,
        cls=base_cls,
        **kwargs,
    )


def small_badge(text: str, icon: Optional[str] = None, kind: str = "small") -> Span:
    """Small inline badge used for views/engagement/date."""
    cls_key = "badge_small" if kind == "small" else "badge_info"
    base_cls = STYLES.get(cls_key, "")

    if icon:
        return Span(UkIcon(icon, cls="w-4 h-4 mr-1"), text, cls=base_cls)
    return Span(text, cls=base_cls)


def progress_meter(el_id: str, max_val: int = 1, cls: Optional[str] = None) -> Progress:
    """Return a progress element with centralized meter classes."""
    meter_cls = cls or STYLES["progress_meter"]
    return Progress(value=0, max=max_val or 1, id=el_id, cls=meter_cls)


# Reusable Feature Pill – small, elegant, red-themed
def FeaturePill(icon: str, text: str):
    """Reusable Feature Pill – small, elegant, red-themed."""
    return Div(
        UkIcon(icon, cls="w-4 h-4 text-red-600"),
        Span(text, cls="text-xs font-semibold text-gray-700"),
        cls="flex items-center gap-1.5 px-3 py-1.5 bg-red-50/80 rounded-full border border-red-200/50 backdrop-blur-sm",
    )


def paste_button(target_id: str) -> Button:
    """Paste button with icon swap and premium styling."""
    status_id = f"{target_id}_status"
    onclick = f"""
        const btn = this, input = document.getElementById('{target_id}'), status = document.getElementById('{status_id}');
        btn.disabled = true;
        navigator.clipboard.readText()
            .then(text => {{
                input.value = text.trim();
                input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                status.textContent = '✓ Pasted';
                status.className = 'text-green-600 text-xs font-semibold';
                setTimeout(() => status.textContent = '', 1500);
            }})
            .catch(() => {{
                status.textContent = '✗ Paste failed';
                status.className = 'text-red-600 text-xs font-semibold';
                setTimeout(() => status.textContent = '', 2000);
            }})
            .finally(() => btn.disabled = false);
    """

    return Button(
        UkIcon("clipboard", cls="w-4 h-4"),  # Slightly smaller icon
        type="button",
        onclick=onclick,
        cls=(
            # ✅ SIZING
            "w-9 h-9 "  # Explicit 36×36px (slightly larger for comfort)
            "flex items-center justify-center "
            # ✅ STYLING
            "text-gray-400 hover:text-red-600 "
            "focus:outline-none focus:ring-2 focus:ring-red-500/30 "
            "disabled:opacity-50 disabled:cursor-not-allowed "
            # ✅ INTERACTIONS
            "transition-all duration-200 "
            "hover:bg-red-50 rounded-md "
            "active:scale-95 "
            # ✅ RESPONSIVE
            "flex-shrink-0"  # Never shrinks below 36×36px
        ),
        title="Paste from clipboard",
        aria_label="Paste from clipboard",
    )


def ViralVibesButton(
    text: str,
    icon: str = "chart-bar",
    button_type: str = "button",
    full_width: bool = False,
    **kwargs,
) -> Button:
    """Create a consistently styled ViralVibes button."""
    width_class = "w-full" if full_width else ""
    # reuse centralized style
    base_cls = STYLES.get("btn_full", "")
    final_cls = f"{width_class} {base_cls}"

    icon_comp = UkIcon(icon, cls="mr-2 w-5 h-5") if icon else None
    content = Span(icon_comp, text) if icon_comp else text

    return Button(content, type=button_type, cls=final_cls, **kwargs)


# ---------------------------------------------------------------------------
# Metric provenance badges
#
# Two visual tiers, deliberately distinct colours:
#   fx   (violet) — already in components/cards.py — calculated from YouTube API data
#   est. (amber)  — ViralVibes model/estimate; not a figure YouTube provides
#
# Both follow the same accessibility pattern: title + tabindex + aria_label.
# ---------------------------------------------------------------------------

_EST_REVENUE_DETAIL = (
    "ViralVibes estimates this from public view, subscriber, and upload data combined "
    "with country-level CPM benchmarks and category multipliers. "
    "YouTube doesn\u2019t expose revenue, CPM, or monetisation data for channels you "
    "don\u2019t own \u2014 this is our own model, not a number from YouTube\u2019s API."
)
_EST_MOMENTUM_DETAIL = (
    "ViralVibes-computed score derived from 30-day subscriber and view velocity. "
    "YouTube doesn\u2019t provide a momentum or growth-velocity metric via its API."
)


def EstimatedBadge(detail: str = "") -> Span:
    """Amber \u2018est.\u2019 badge for ViralVibes-estimated metrics not sourced from YouTube\u2019s API."""
    tip = detail or _EST_REVENUE_DETAIL
    return Span(
        "est.",
        title=tip,
        tabindex="0",
        aria_label=f"ViralVibes estimate \u2014 {tip}",
        cls=(
            "text-[9px] font-mono font-bold tracking-wide "
            "px-1.5 py-0.5 rounded "
            "bg-amber-50 text-amber-600 border border-amber-200 "
            "cursor-default select-none "
            "focus:outline-none focus:ring-1 focus:ring-amber-300"
        ),
    )


def YtSourceBadge() -> Span:
    """Sky-blue 'yt' badge for metrics sourced directly from the YouTube API."""
    tip = "Sourced directly from the YouTube API."
    return Span(
        "yt",
        title=tip,
        cls=(
            "text-[9px] font-mono font-bold tracking-wide "
            "px-1.5 py-0.5 rounded "
            "bg-sky-50 text-sky-600 border border-sky-200 "
            "cursor-default select-none"
        ),
    )


# ---------------------------------------------------------------------------
# YouTube channel button — compliant with YouTube API branding guidelines.
#
# Lucide's mono "youtube" icon renders the rectangle and play triangle in the
# same color, so the triangle is invisible against the rectangle.  These
# helpers use a two-path inline SVG: red rounded-rect + white play triangle
# (branded) or a single evenodd path that punches the triangle through a solid
# shape (mono, for use inside a red button).  Min icon size = 20 px per the
# YouTube API branding guidelines for digital media.
# ---------------------------------------------------------------------------

# Official YouTube icon SVG paths (24×24 viewBox)
_YT_RECT = (
    "M23.495 6.205a3.007 3.007 0 0 0-2.088-2.088"
    "c-1.87-.501-9.396-.501-9.396-.501"
    "s-7.507-.01-9.396.501A3.007 3.007 0 0 0 .527 6.205"
    "a31.247 31.247 0 0 0-.522 5.805"
    "a31.247 31.247 0 0 0 .522 5.783"
    "a3.007 3.007 0 0 0 2.088 2.088"
    "c1.868.502 9.396.502 9.396.502"
    "s7.506 0 9.396-.502"
    "a3.007 3.007 0 0 0 2.088-2.088"
    "a31.247 31.247 0 0 0 .5-5.783"
    "a31.247 31.247 0 0 0-.5-5.805z"
)
_YT_TRIANGLE = "M9.609 15.601V8.408l6.264 3.602z"
# Triangle reversed for fill-rule evenodd punch-out (monochrome on solid bg)
_YT_PUNCHOUT = _YT_RECT + " M15.873 12.010L9.609 8.408V15.601z"


def YtIcon(variant: str = "branded", size: int = 20) -> NotStr:
    """Inline SVG YouTube play-button icon.

    variant="branded" — red rectangle, white triangle (for light/transparent bg).
    variant="mono"    — white evenodd punch-out shape (for use inside a red button).
    Both meet the 20 px minimum size requirement from the YouTube API branding guidelines.
    """
    if variant == "mono":
        return NotStr(
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"'
            f' width="{size}" height="{size}" aria-hidden="true" focusable="false">'
            f'<path fill="#fff" fill-rule="evenodd" d="{_YT_PUNCHOUT}"/>'
            f"</svg>"
        )
    # branded (default)
    return NotStr(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"'
        f' width="{size}" height="{size}" aria-hidden="true" focusable="false">'
        f'<path fill="#FF0000" d="{_YT_RECT}"/>'
        f'<path fill="#fff" d="{_YT_TRIANGLE}"/>'
        f"</svg>"
    )


def YoutubeChannelButton(
    channel_url: str,
    *,
    variant: str = "solid",
    as_button: bool = False,
    onclick_js: str = "",
    size: str = "sm",
    extra_cls: str = "",
) -> ...:
    """Branded YouTube channel button — compliant with YouTube API branding guidelines.

    variant="solid"  — red (#FF0000) pill with white icon + text.  Used on
                       profile pages and compare cards where the button is a
                       primary action standing alone.
    variant="ghost"  — transparent background with branded (red+white) icon
                       and red label.  Used inside card footers that are
                       themselves wrapped in an <a>, so nested <a> is invalid.
                       Pass as_button=True to render a <button> with onclick.

    size="sm"  → py-1.5 px-3 text-xs  (card footer, compact contexts)
    size="md"  → py-2   px-4 text-sm  (profile page, compare card)

    as_button=True renders a <button> element (for contexts inside an existing
    <a>) instead of an <a>.  Pair with onclick_js for the click handler.
    """
    pad = "px-3 py-1.5 text-xs" if size == "sm" else "px-4 py-2 text-sm"
    icon_size = 20  # always ≥20px per YouTube branding guidelines

    if variant == "ghost":
        icon = YtIcon("branded", size=icon_size)
        cls = (
            f"inline-flex items-center gap-1.5 {pad} rounded-lg font-semibold "
            f"text-red-600 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300 "
            f"bg-transparent border-0 cursor-pointer transition-colors {extra_cls}"
        )
        content = (icon, Span("YouTube"))
        if as_button:
            return Button(
                *content,
                type="button",
                onclick=onclick_js,
                cls=cls,
                aria_label="Open YouTube channel",
            )
        return A(
            *content,
            href=channel_url,
            target="_blank",
            rel="noopener noreferrer",
            cls=cls,
            aria_label="Open YouTube channel",
        )

    # solid (default)
    icon = YtIcon("mono", size=icon_size)
    cls = (
        f"inline-flex items-center gap-1.5 {pad} rounded-lg font-semibold "
        f"text-white no-underline transition-colors {extra_cls}"
    )
    # YouTube brand red — do not substitute with Tailwind red-600 (#DC2626)
    style = "background:#FF0000;"
    hover_script = (
        "this.style.background='#CC0000'"
        ",this.onmouseleave=function(){this.style.background='#FF0000'}"
    )
    if as_button:
        return Button(
            icon,
            Span("YouTube"),
            type="button",
            onclick=onclick_js,
            cls=cls,
            style=style,
            onmouseenter=hover_script,
            aria_label="Open YouTube channel",
        )
    return A(
        icon,
        Span("YouTube"),
        href=channel_url,
        target="_blank",
        rel="noopener noreferrer",
        cls=cls,
        style=style,
        onmouseenter=hover_script,
        aria_label="Open YouTube channel",
    )


def SignUpNudge(
    feature: str = "this feature",
    benefit: str = "sign in to unlock full access",
    return_url: str = "/",
) -> Alert:
    """Inline soft sign-up prompt — shown in place of a hard auth wall.

    Stays on the current page; never redirects. Converts better than a
    wall because users have already seen value before being asked to sign in.

    Built on MonsterUI's Alert + DivFullySpaced primitives so it inherits
    theme tokens and DaisyUI's alert accessibility role automatically.

    Args:
        feature:    Short label for what is locked, e.g. "full playlist analysis".
        benefit:    Value proposition line shown under the heading.
        return_url: Where to redirect after successful sign-in.
    """
    return Alert(
        DivFullySpaced(
            DivHStacked(
                Div(
                    UkIcon("lock", cls="size-5"),
                    cls="size-10 rounded-full flex items-center justify-center bg-background/60 flex-shrink-0",
                ),
                Div(
                    P(f"Sign in to unlock {feature}", cls="font-semibold text-sm"),
                    P(benefit, cls="text-xs opacity-70 mt-0.5"),
                ),
            ),
            A(
                UkIcon("log-in", cls="size-4"),
                Span("Sign in with Google"),
                href=f"/login?return_url={return_url}",
                cls=(ButtonT.primary, "inline-flex items-center gap-2 flex-shrink-0 text-sm"),
            ),
        ),
        cls=(AlertT.info, "my-6"),
    )
