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
    cls_name = kind_map.get(kind, "btn-full")

    icon_comp = UkIcon(icon, cls="mr-2") if icon else None
    content = Span(icon_comp, text) if icon_comp else text

    return Button(
        content,
        cls=cls_name,
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
