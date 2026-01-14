from fasthtml.common import *
from monsterui.all import *

from constants import (
    FLEX_CENTER,
    FLEX_COL,
    THEME,
)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def maxpx(px: int) -> str:
    """Generate max-width class in pixels."""
    return f"w-full max-w-[{px}px]"


def maxrem(rem: int) -> str:
    """Generate max-width class in rem units."""
    return f"w-full max-w-[{rem}rem]"


# Helper Functions to make the component file self-contained
def DivCentered(*args, **kwargs) -> Div:
    """A Div with flexbox for centering content."""
    # Extract existing cls if present
    existing_cls = kwargs.pop("cls", "")

    # Merge with centering classes
    merged_cls = f"{FLEX_COL} {FLEX_CENTER} {existing_cls}".strip()

    return Div(*args, **kwargs, cls=merged_cls)


def DivHStacked(*args, **kwargs):
    """A Div with horizontal flex layout (gap-4 spacing)."""
    existing_cls = kwargs.pop("cls", "")
    merged_cls = f"flex items-center gap-4 {existing_cls}".strip()
    return Div(*args, **kwargs, cls=merged_cls)


def DivFullySpaced(*args, **kwargs):
    """A Div with flex items spaced evenly (justify-between)."""
    existing_cls = kwargs.pop("cls", "")
    merged_cls = f"flex items-center justify-between {existing_cls}".strip()
    return Div(*args, **kwargs, cls=merged_cls)


def styled_div(*args, **kwargs):
    """Helper for consistent div styling."""
    # Just pass through - no default classes
    return Div(*args, **kwargs)
