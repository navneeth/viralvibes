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
    return Div(*args, **kwargs, cls=f"{FLEX_COL} {FLEX_CENTER}")


def DivHStacked(*args, **kwargs) -> Div:
    """A horizontal stack of Divs with a gap."""
    return Div(*args, **kwargs, cls=f"flex gap-4")


def DivFullySpaced(*args, **kwargs) -> Div:
    """A Div with full space between items."""
    return Div(*args, **kwargs, cls=f"flex justify-between items-center")


def styled_div(*children, cls: str = "", **kwargs) -> Div:
    """Flexible Div factory with theme integration."""
    full_cls = f"{THEME['flex_col']} {cls}" if "flex-col" in cls else cls
    return Div(*children, cls=full_cls, **kwargs)
