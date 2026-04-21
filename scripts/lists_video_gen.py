#!/usr/bin/env python3
"""
tools/list_video.py — Export viralvibes.fyi/lists as an MP4.

Fetches the top-N creators from your existing `creators` table,
renders each as a styled frame with Pillow, then encodes to H.264
via FFmpeg. No Manim. No time-series. No animation complexity.

Usage:
    python tools/list_video.py --list top-rated --top-n 20 --output out/list.mp4
    python tools/list_video.py --list rising-stars --audio music.mp3
    python tools/list_video.py --list by-category --category Gaming

Reads NEXT_PUBLIC_SUPABASE_URL + SUPABASE_SERVICE_KEY from .env.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFilter, ImageFont
from supabase import Client, create_client

load_dotenv()
logger = logging.getLogger("list_video")

# ── Video constants ────────────────────────────────────────────────────────────
W, H = 1920, 1080
FPS = 30
HOLD_SECS = 5  # seconds to show the full list card
REVEAL_SECS = 0.6  # seconds to slide in each row (staggered)

# ── Palette (editorial dark theme matching viralvibes.fyi) ────────────────────
BG = (8, 8, 8)  # #080808
SURFACE = (18, 18, 22)  # card background
ACCENT = (255, 69, 0)  # #FF4500 orange
CYAN = (0, 200, 255)  # #00C8FF
CREAM = (245, 240, 232)  # text primary
MUTED = (107, 107, 107)  # text secondary
BAR_BG = (30, 30, 36)  # bar track
RANK_COLORS = [
    (255, 215, 0),  # gold   — rank 1
    (192, 192, 192),  # silver — rank 2
    (205, 127, 50),  # bronze — rank 3
]

# ── Layout ─────────────────────────────────────────────────────────────────────
MARGIN_X = 80
TOP_BAND = 120  # height of title band
ROW_H = 64
ROW_GAP = 8
AVATAR_SIZE = 48


# ─────────────────────────────────────────────────────────────────────────────
# Data
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class Creator:
    rank: int
    channel_name: str
    subscribers: int
    view_count: int
    engagement: float
    category: str
    country: str
    thumbnail_url: str


LIST_QUERIES = {
    "top-rated": {
        "label": "Top Rated Creators",
        "order": "engagement_score",
    },
    "rising-stars": {
        "label": "Rising Stars",
        "order": "subscriber_growth_30d",
    },
    "by-category": {
        "label": "Top by Category",
        "order": "current_subscribers",
    },
}


def fetch_creators(
    db: Client,
    list_name: str,
    top_n: int,
    category: Optional[str],
) -> tuple[list[Creator], str]:
    """
    Pull creators from Supabase. Returns (creators, display_label).
    """
    cfg = LIST_QUERIES.get(list_name, LIST_QUERIES["top-rated"])
    order_col = cfg["order"]
    label = cfg["label"]
    if category:
        label = f"{label} · {category}"

    q = (
        db.table("creators")
        .select(
            "channel_name, current_subscribers, current_view_count, "
            "engagement_score, primary_category, country_code, "
            "channel_thumbnail_url, subscriber_growth_30d"
        )
        .eq("sync_status", "synced")
        .gt("current_subscribers", 0)
    )
    if category:
        q = q.eq("primary_category", category)
    if order_col in ("engagement_score", "subscriber_growth_30d"):
        q = q.not_.is_(order_col, "null")

    resp = q.order(order_col, desc=True).limit(top_n).execute()

    creators = []
    for i, row in enumerate(resp.data or [], start=1):
        creators.append(
            Creator(
                rank=i,
                channel_name=row.get("channel_name") or "Unknown",
                subscribers=int(row.get("current_subscribers") or 0),
                view_count=int(row.get("current_view_count") or 0),
                engagement=float(row.get("engagement_score") or 0.0),
                category=row.get("primary_category") or "",
                country=row.get("country_code") or "",
                thumbnail_url=row.get("channel_thumbnail_url") or "",
            )
        )

    if not creators:
        raise RuntimeError(f"No creators found for list '{list_name}' category='{category}'")

    logger.info("Fetched %d creators for '%s'", len(creators), label)
    return creators, label


# ─────────────────────────────────────────────────────────────────────────────
# Assets
# ─────────────────────────────────────────────────────────────────────────────


class Avatars:
    def __init__(self, cache_dir: Path):
        self._dir = cache_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    def get(self, url: str, size: int = AVATAR_SIZE) -> Image.Image:
        if not url:
            return self._placeholder(size)
        path = self._dir / f"{hashlib.md5(url.encode()).hexdigest()}.png"
        if not path.exists():
            try:
                r = requests.get(url, timeout=6)
                r.raise_for_status()
                img = Image.open(io.BytesIO(r.content)).convert("RGBA")
                img = _circle_crop(img, size)
                img.save(path)
            except Exception as e:
                logger.debug("Avatar fetch failed %s: %s", url, e)
                return self._placeholder(size)
        return Image.open(path).convert("RGBA")

    @staticmethod
    def _placeholder(size: int) -> Image.Image:
        img = Image.new("RGBA", (size, size), (40, 40, 50, 220))
        draw = ImageDraw.Draw(img)
        draw.ellipse((1, 1, size - 2, size - 2), outline=(*MUTED, 180), width=2)
        return img


def _circle_crop(img: Image.Image, size: int) -> Image.Image:
    img = img.resize((size, size), Image.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 1, size - 1), fill=255)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    out.paste(img, mask=mask)
    return out


def _load_font(name: str, size: int) -> ImageFont.FreeTypeFont:
    """Try system fonts by name; fall back to default."""
    candidates = {
        "bold": [
            "/System/Library/Fonts/Helvetica.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ],
        "regular": [
            "/System/Library/Fonts/Helvetica.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ],
        "mono": [
            "/System/Library/Fonts/Menlo.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        ],
    }
    for path in candidates.get(name, candidates["regular"]):
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def _fmt(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.0f}K"
    return str(n)


# ─────────────────────────────────────────────────────────────────────────────
# Frame renderer
# ─────────────────────────────────────────────────────────────────────────────


class FrameRenderer:
    """
    Renders all frames as PIL Images.
    Returns a generator of (frame_index, Image) — caller pipes to FFmpeg.
    """

    def __init__(self, creators: list[Creator], label: str, avatars: Avatars):
        self.creators = creators
        self.label = label
        self.avatars = avatars

        self.font_xl = _load_font("bold", 52)
        self.font_lg = _load_font("bold", 28)
        self.font_md = _load_font("regular", 22)
        self.font_sm = _load_font("regular", 17)
        self.font_mono = _load_font("mono", 20)
        self.font_rank = _load_font("bold", 20)

        # Pre-fetch avatars
        logger.info("Downloading avatars...")
        self._avatars = {c.channel_name: avatars.get(c.thumbnail_url) for c in creators}

        # Max subscribers for bar scaling
        self._max_subs = max(c.subscribers for c in creators) or 1

    # ── Layout helpers ─────────────────────────────────────────────────────

    def _row_y(self, idx: int) -> int:
        """Y coordinate of row `idx` (0-based)."""
        return TOP_BAND + MARGIN_X // 2 + idx * (ROW_H + ROW_GAP)

    def _canvas(self) -> Image.Image:
        img = Image.new("RGB", (W, H), BG)
        # subtle grid texture
        draw = ImageDraw.Draw(img)
        for x in range(0, W, 60):
            draw.line([(x, 0), (x, H)], fill=(20, 20, 24), width=1)
        for y in range(0, H, 60):
            draw.line([(0, y), (W, y)], fill=(20, 20, 24), width=1)
        return img

    def _draw_header(self, draw: ImageDraw.Draw, img: Image.Image) -> None:
        # Orange accent bar at very top
        draw.rectangle([(0, 0), (W, 4)], fill=ACCENT)

        # Title
        draw.text((MARGIN_X, 24), "ViralVibes.fyi", font=self.font_lg, fill=ACCENT)
        title_w = draw.textlength("ViralVibes.fyi", font=self.font_lg)
        draw.text((MARGIN_X + title_w + 24, 30), "·", font=self.font_md, fill=MUTED)
        draw.text((MARGIN_X + title_w + 44, 26), self.label, font=self.font_md, fill=CREAM)

        # Column headers
        header_y = TOP_BAND - 24
        draw.text((MARGIN_X + 90, header_y), "CREATOR", font=self.font_sm, fill=(*MUTED,))
        draw.text((W - 520, header_y), "SUBSCRIBERS", font=self.font_sm, fill=(*MUTED,))
        draw.text((W - 340, header_y), "ENG RATE", font=self.font_sm, fill=(*MUTED,))
        draw.text((W - 190, header_y), "COUNTRY", font=self.font_sm, fill=(*MUTED,))

        # Separator
        draw.line(
            [(MARGIN_X, TOP_BAND - 4), (W - MARGIN_X, TOP_BAND - 4)], fill=(*MUTED, 60), width=1
        )

    def _draw_row(
        self,
        img: Image.Image,
        draw: ImageDraw.Draw,
        creator: Creator,
        row_idx: int,
        alpha: float = 1.0,  # 0→1 reveal progress
    ) -> None:
        y = self._row_y(row_idx)
        cx = MARGIN_X

        # Row highlight on even rows
        if row_idx % 2 == 0:
            draw.rectangle(
                [(cx, y - 4), (W - cx, y + ROW_H - 4)],
                fill=(25, 25, 30),
            )

        # ── Rank ────────────────────────────────────────────────────────────
        rank_color = RANK_COLORS[creator.rank - 1] if creator.rank <= 3 else MUTED
        draw.text(
            (cx, y + (ROW_H - 24) // 2),
            f"#{creator.rank:02d}",
            font=self.font_rank,
            fill=rank_color,
        )
        cx += 58

        # ── Avatar ──────────────────────────────────────────────────────────
        av = self._avatars.get(creator.channel_name)
        if av:
            if alpha < 1.0:
                # fade in
                faded = av.copy()
                faded.putalpha(int(255 * alpha))
                img.paste(faded, (cx, y + (ROW_H - AVATAR_SIZE) // 2), faded)
            else:
                img.paste(av, (cx, y + (ROW_H - AVATAR_SIZE) // 2), av)
        cx += AVATAR_SIZE + 16

        # ── Name ────────────────────────────────────────────────────────────
        name = creator.channel_name[:28]
        draw.text((cx, y + (ROW_H - 28) // 2), name, font=self.font_lg, fill=CREAM)

        # Category pill
        if creator.category:
            pill_x = cx
            pill_y = y + ROW_H - 20
            draw.text((pill_x, pill_y), creator.category[:20], font=self.font_sm, fill=(*CYAN,))

        # ── Subscriber bar + value ───────────────────────────────────────────
        bar_x = W - 540
        bar_y = y + (ROW_H - 12) // 2
        bar_total = 280
        bar_fill = int(bar_total * creator.subscribers / self._max_subs * alpha)

        draw.rectangle([(bar_x, bar_y), (bar_x + bar_total, bar_y + 12)], fill=BAR_BG)
        if bar_fill > 0:
            draw.rectangle(
                [(bar_x, bar_y), (bar_x + bar_fill, bar_y + 12)],
                fill=ACCENT if creator.rank == 1 else (*ACCENT[:2], 180),
            )

        draw.text(
            (bar_x + bar_total + 12, y + (ROW_H - 22) // 2),
            _fmt(creator.subscribers),
            font=self.font_mono,
            fill=CREAM,
        )

        # ── Engagement ──────────────────────────────────────────────────────
        eng_color = (
            (80, 220, 100)
            if creator.engagement >= 5.0
            else (255, 200, 0) if creator.engagement >= 2.0 else MUTED
        )
        draw.text(
            (W - 340, y + (ROW_H - 22) // 2),
            f"{creator.engagement:.1f}%",
            font=self.font_mono,
            fill=eng_color,
        )

        # ── Country ─────────────────────────────────────────────────────────
        draw.text(
            (W - 190, y + (ROW_H - 22) // 2),
            creator.country or "—",
            font=self.font_mono,
            fill=MUTED,
        )

    def _draw_footer(self, draw: ImageDraw.Draw) -> None:
        draw.line([(MARGIN_X, H - 52), (W - MARGIN_X, H - 52)], fill=(*MUTED, 40), width=1)
        draw.text(
            (MARGIN_X, H - 40),
            "viralvibes.fyi  ·  Creator Intelligence Platform",
            font=self.font_sm,
            fill=(*MUTED, 180),
        )
        draw.text(
            (W - MARGIN_X - 280, H - 40),
            "Data updated daily  ·  Public sources only",
            font=self.font_sm,
            fill=(*MUTED, 120),
        )
        # Bottom accent line
        draw.rectangle([(0, H - 4), (W, H)], fill=ACCENT)

    # ── Frame generation ───────────────────────────────────────────────────

    def render_frames(self) -> list[Image.Image]:
        """
        Returns the list of unique frames.
        Each frame is a full 1920×1080 PIL Image.
        FFmpeg will be told to duplicate them to fill hold time.
        """
        total_rows = len(self.creators)
        reveal_frames = int(FPS * REVEAL_SECS)
        hold_frames = int(FPS * HOLD_SECS)

        frames: list[Image.Image] = []

        # Phase 1: staggered row reveal
        # Each row slides in over `reveal_frames` frames,
        # staggered by half a reveal window per row.
        stagger = max(1, reveal_frames // 2)
        total_reveal = reveal_frames + stagger * (total_rows - 1)

        for f in range(total_reveal):
            img = self._canvas()
            draw = ImageDraw.Draw(img)
            self._draw_header(draw, img)

            for i, creator in enumerate(self.creators):
                row_start = i * stagger
                row_end = row_start + reveal_frames
                if f < row_start:
                    alpha = 0.0
                elif f >= row_end:
                    alpha = 1.0
                else:
                    alpha = (f - row_start) / reveal_frames

                if alpha > 0:
                    self._draw_row(img, draw, creator, i, alpha)

            self._draw_footer(draw)
            frames.append(img)

        # Phase 2: hold final frame
        final = frames[-1].copy()
        for _ in range(hold_frames):
            frames.append(final)

        logger.info("Generated %d frames", len(frames))
        return frames


# ─────────────────────────────────────────────────────────────────────────────
# Encode + mux
# ─────────────────────────────────────────────────────────────────────────────


def encode(frames: list[Image.Image], output: Path, audio: Optional[Path]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    # Pipe raw RGB frames → FFmpeg stdin
    # Format: rawvideo, rgb24, W×H at FPS
    video_cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "rawvideo",
        "-vcodec",
        "rawvideo",
        "-s",
        f"{W}x{H}",
        "-pix_fmt",
        "rgb24",
        "-r",
        str(FPS),
        "-i",
        "pipe:0",
    ]

    if audio and audio.exists():
        video_cmd += ["-i", str(audio), "-shortest", "-c:a", "aac", "-b:a", "192k"]

    video_cmd += [
        "-vcodec",
        "libx264",
        "-preset",
        "fast",
        "-crf",
        "18",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        str(output),
    ]

    logger.info("Encoding %d frames → %s", len(frames), output)
    proc = subprocess.Popen(video_cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)

    for img in frames:
        proc.stdin.write(img.tobytes())

    proc.stdin.close()
    _, stderr = proc.communicate()

    if proc.returncode != 0:
        raise RuntimeError(f"FFmpeg failed:\n{stderr.decode()}")

    logger.info("✅  %s  (%.1f MB)", output, output.stat().st_size / 1e6)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Export a ViralVibes creator list as an MP4.",
    )
    ap.add_argument(
        "--list",
        default="top-rated",
        choices=list(LIST_QUERIES),
        help="Which list to export (default: top-rated)",
    )
    ap.add_argument("--category", default=None, help="Filter by category (e.g. Gaming, Music)")
    ap.add_argument("--top-n", type=int, default=20, help="Number of rows (default: 20)")
    ap.add_argument("--audio", default=None, help="Background audio file to mux (optional)")
    ap.add_argument(
        "--output", default="output/list.mp4", help="Output path (default: output/list.mp4)"
    )
    ap.add_argument("--cache-dir", default=".cache/avatars", help="Avatar cache dir")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    url = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        sys.exit("Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_KEY in .env")

    db = create_client(url, key)
    creators, label = fetch_creators(db, args.list, args.top_n, args.category)

    avatars = Avatars(Path(args.cache_dir))
    renderer = FrameRenderer(creators, label, avatars)
    frames = renderer.render_frames()

    audio = Path(args.audio) if args.audio else None
    encode(frames, Path(args.output), audio)


if __name__ == "__main__":
    main()
