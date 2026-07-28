"""
Blog coming-soon page — public route linked from the footer.
"""

from fasthtml.common import *
from monsterui.all import *


# ---------------------------------------------------------------------------
# SVG Illustration — editorial / writing theme
# ---------------------------------------------------------------------------

_COMING_SOON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 260" width="320" height="260" aria-hidden="true">
  <defs>
    <linearGradient id="nb" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#3b82f6"/>
      <stop offset="100%" stop-color="#6366f1"/>
    </linearGradient>
    <linearGradient id="page" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%" stop-color="#ffffff"/>
      <stop offset="100%" stop-color="#f1f5f9"/>
    </linearGradient>
    <linearGradient id="pen" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#f59e0b"/>
      <stop offset="100%" stop-color="#ef4444"/>
    </linearGradient>
  </defs>

  <!-- Notebook body -->
  <rect x="60" y="30" width="170" height="200" rx="10" fill="url(#nb)"/>
  <!-- Spine binding -->
  <rect x="60" y="30" width="18" height="200" rx="5" fill="#2563eb"/>
  <!-- Binding rings -->
  <circle cx="69" cy="75"  r="6" fill="white" opacity="0.9"/>
  <circle cx="69" cy="115" r="6" fill="white" opacity="0.9"/>
  <circle cx="69" cy="155" r="6" fill="white" opacity="0.9"/>
  <circle cx="69" cy="195" r="6" fill="white" opacity="0.9"/>
  <!-- Page -->
  <rect x="83" y="44" width="135" height="174" rx="4" fill="url(#page)"/>

  <!-- Ruled lines on page -->
  <line x1="95" y1="78"  x2="206" y2="78"  stroke="#cbd5e1" stroke-width="1.5"/>
  <line x1="95" y1="96"  x2="206" y2="96"  stroke="#cbd5e1" stroke-width="1.5"/>
  <line x1="95" y1="114" x2="206" y2="114" stroke="#cbd5e1" stroke-width="1.5"/>
  <line x1="95" y1="132" x2="175" y2="132" stroke="#cbd5e1" stroke-width="1.5"/>
  <!-- Partial line — trailing off for "coming soon" effect -->
  <line x1="95" y1="150" x2="155" y2="150" stroke="#cbd5e1" stroke-width="1.5" stroke-dasharray="4 3"/>

  <!-- "BLOG" label on cover -->
  <text x="150" y="63" text-anchor="middle" font-family="'Inter', system-ui, sans-serif"
        font-size="11" font-weight="700" letter-spacing="3" fill="white" opacity="0.85">BLOG</text>

  <!-- Pen — rotated, resting across the notebook -->
  <g transform="rotate(-38, 220, 95)">
    <!-- Barrel -->
    <rect x="198" y="60" width="14" height="72" rx="3" fill="url(#pen)"/>
    <!-- Clip -->
    <rect x="208" y="64" width="3" height="52" rx="1.5" fill="#fbbf24" opacity="0.7"/>
    <!-- Cap -->
    <rect x="197" y="55" width="16" height="14" rx="3" fill="#1e293b"/>
    <!-- Tip taper -->
    <polygon points="198,132 212,132 205,148" fill="#d97706"/>
    <!-- Nib -->
    <polygon points="202,142 208,142 205,152" fill="#1e293b"/>
  </g>

  <!-- Sparkle top-right -->
  <g fill="#f59e0b">
    <polygon points="267,28 269,36 277,38 269,40 267,48 265,40 257,38 265,36" opacity="0.9"/>
    <polygon points="245,14 246,18 250,19 246,20 245,24 244,20 240,19 244,18" opacity="0.7"/>
  </g>

  <!-- Sparkle left -->
  <g fill="#6366f1">
    <polygon points="42,88 44,95 51,97 44,99 42,106 40,99 33,97 40,95" opacity="0.75"/>
    <polygon points="28,66 29,70 33,71 29,72 28,76 27,72 23,71 27,70" opacity="0.5"/>
  </g>

  <!-- Floating dot accents -->
  <circle cx="56"  cy="185" r="3" fill="#3b82f6" opacity="0.4"/>
  <circle cx="270" cy="170" r="4" fill="#6366f1" opacity="0.3"/>
  <circle cx="280" cy="60"  r="2.5" fill="#f59e0b" opacity="0.5"/>
</svg>"""


# ---------------------------------------------------------------------------
# Page content
# ---------------------------------------------------------------------------


def blog_page_content() -> Div:
    return Div(
        Div(
            # Illustration
            Div(
                NotStr(_COMING_SOON_SVG),
                cls="flex justify-center mb-10",
            ),
            # Eyebrow
            P(
                "Coming Soon",
                cls="text-xs font-mono uppercase tracking-[0.18em] text-blue-600 mb-4 text-center",
            ),
            # Heading
            H1(
                "Stories worth reading.",
                cls=(
                    "text-4xl md:text-5xl font-bold tracking-tight text-center mb-5 "
                    "bg-gradient-to-br from-foreground via-foreground to-foreground/60 "
                    "bg-clip-text text-transparent"
                ),
            ),
            # Subtext
            P(
                "Deep dives on creator marketing, campaign strategy, and the data behind "
                "what makes YouTube channels grow. We're writing it now — check back soon.",
                cls="text-base text-muted-foreground leading-relaxed text-center max-w-xl mx-auto mb-10",
            ),
            # CTA
            A(
                "Browse Creators in the Meantime →",
                href="/creators",
                cls=(
                    "inline-flex items-center gap-2 px-5 py-2.5 rounded-lg "
                    "bg-blue-600 text-white text-sm font-medium "
                    "hover:bg-blue-700 transition-colors"
                ),
            ),
            cls="flex flex-col items-center py-24 px-4",
        ),
        cls="max-w-2xl mx-auto",
    )
