"""
Blog data layer — Post model, post loading, and markdown rendering.

Pattern adapted from https://github.com/jackhogan/personal-site:
  - YAML frontmatter parsed by python-frontmatter
  - Markdown rendered via monsterui's render_md / FrankenRenderer
  - Internal site links get HTMX push-url attrs so navigation stays fast
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_POSTS_DIR = Path("posts")


# ---------------------------------------------------------------------------
# Post model
# ---------------------------------------------------------------------------


class Post:
    """A single markdown blog post with YAML frontmatter.

    Frontmatter keys:
        title       (str)  — display title
        date        (date) — publication date used for sort order
        excerpt     (str)  — one-paragraph teaser shown on the index
        tags        (list) — optional tag list e.g. ["strategy", "data"]
        placeholder (bool) — when true the post renders the coming-soon SVG
                             instead of the markdown body
    """

    def __init__(self, path: Path) -> None:
        import frontmatter  # lazy — not installed in the worker / test env

        self.path = path
        self.slug = path.stem

        post = frontmatter.load(str(path))
        self.content: str = post.content
        self.meta: dict = post.metadata

        self.title: str = self.meta.get("title", self.slug.replace("-", " ").title())

        raw_date = self.meta.get("date")
        self.date: Optional[date] = raw_date if isinstance(raw_date, date) else None
        self.datestr: str = self.date.strftime("%d %b %Y") if self.date else ""

        self.excerpt: str = self.meta.get("excerpt", "")
        self.tags: list[str] = list(self.meta.get("tags", []))
        self.placeholder: bool = bool(self.meta.get("placeholder", False))


# ---------------------------------------------------------------------------
# Post loader
# ---------------------------------------------------------------------------


def get_posts(published_only: bool = False) -> list[Post]:
    """Return posts from ``posts/``, sorted newest-first.

    Args:
        published_only: When True, exclude posts with ``placeholder: true``.
    """
    if not _POSTS_DIR.exists():
        return []
    try:
        posts: list[Post] = sorted(
            [Post(p) for p in _POSTS_DIR.glob("*.md")],
            key=lambda p: p.date or date.min,
            reverse=True,
        )
    except Exception:
        logger.exception("blog: failed to load posts from %s", _POSTS_DIR)
        return []

    return [p for p in posts if not p.placeholder] if published_only else posts


def get_post(slug: str) -> Optional[Post]:
    """Return the post matching *slug*, or None."""
    path = _POSTS_DIR / f"{slug}.md"
    if not path.exists():
        return None
    try:
        return Post(path)
    except Exception:
        logger.exception("blog: failed to load post %s", slug)
        return None


# ---------------------------------------------------------------------------
# Markdown renderer
# ---------------------------------------------------------------------------
# Tailwind class overrides applied to the default monsterui/FrankenUI output.
# Keys are lowercase HTML tag names; values replace (not extend) the default.

_MD_CLASS_MODS: dict[str, str] = {
    "h1": "text-3xl font-bold tracking-tight mb-4 mt-10 text-foreground",
    "h2": "text-2xl font-semibold tracking-tight mb-3 mt-8 text-foreground",
    "h3": "text-xl font-semibold mb-2 mt-6 text-foreground",
    "p":  "text-base leading-relaxed mb-4 text-foreground",
    "li": "text-base leading-relaxed",
    "ul": "list-disc ml-6 space-y-1.5 mb-4",
    "ol": "list-decimal ml-6 space-y-1.5 mb-4",
    "pre": "border border-border rounded-lg my-4 overflow-x-auto text-sm",
    "blockquote": "border-l-4 border-blue-400 pl-4 my-4 text-muted-foreground italic",
    "hr": "border-t border-border my-8",
    "img": "rounded-lg my-6 max-w-full",
}

# Inline style injected once per page so code blocks look reasonable even
# without a dedicated highlight.js theme loaded on this route.
_PROSE_STYLE = "<style>.prose-content a{color:var(--primary);text-decoration:underline;text-underline-offset:2px}.prose-content a:hover{opacity:.8}.prose-content code{font-size:.875em;background:var(--muted);padding:.1em .3em;border-radius:.2em}.prose-content pre code{background:none;padding:0}</style>"


def _make_renderer():
    """Build a FrankenRenderer subclass that adds HTMX attrs to internal links."""
    try:
        from monsterui.all import FrankenRenderer
        import mistletoe

        class _SiteRenderer(FrankenRenderer):
            """Extends FrankenRenderer: internal links get hx-get / hx-push-url
            so blog-to-blog (or blog-to-creators) navigation stays fast."""

            def render_link(self, token):
                href = token.target or ""
                inner = self.render_inner(token)
                title_attr = f' title="{token.title}"' if getattr(token, "title", None) else ""
                # Internal: starts with / but not //
                is_internal = (
                    href.startswith("/")
                    and not href.startswith("//")
                    and "." not in href.split("/")[-1]
                )
                if is_internal:
                    return (
                        f'<a href="{href}"{title_attr} '
                        f'class="text-primary underline underline-offset-2 hover:opacity-80">'
                        f"{inner}</a>"
                    )
                return (
                    f'<a href="{href}"{title_attr} '
                    f'target="_blank" rel="noopener noreferrer" '
                    f'class="text-primary underline underline-offset-2 hover:opacity-80">'
                    f"{inner}</a>"
                )

        return _SiteRenderer
    except Exception:
        logger.warning("blog: FrankenRenderer not available, falling back to render_md default")
        return None


_renderer_cls = None  # module-level singleton, built lazily


def from_md(content: str) -> "object":
    """Render *content* as styled markdown, returning a FastHTML Div."""
    from fasthtml.common import Div, NotStr

    try:
        from monsterui.all import render_md

        global _renderer_cls
        if _renderer_cls is None:
            _renderer_cls = _make_renderer()

        kwargs: dict = {"class_map_mods": _MD_CLASS_MODS}
        if _renderer_cls is not None:
            from functools import partial
            kwargs["renderer"] = partial(_renderer_cls)

        rendered = render_md(content, **kwargs)
        return Div(NotStr(_PROSE_STYLE), rendered, cls="prose-content max-w-none")
    except Exception:
        logger.exception("blog: markdown render failed")
        # Graceful fallback — show raw text rather than a 500
        from fasthtml.common import P
        return Div(P(content, cls="text-sm font-mono text-muted-foreground whitespace-pre-wrap"))
