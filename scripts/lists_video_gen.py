#!/usr/bin/env python3
"""
scripts/lists_video_gen.py — Ranking card video (Pillow + FFmpeg, v1).

Exports any ViralVibes creator list as a 1920×1080 MP4, matching the data
shown on viralvibes.fyi/lists.  All rows appear at once; subscriber bars grow
from 0 → full over one second, then the card holds for HOLD_SECS seconds
(handled by FFmpeg tpad — no duplicate frames in Python).

Usage:
    python scripts/lists_video_gen.py                             # top-rated
    python scripts/lists_video_gen.py --list rising-stars
    python scripts/lists_video_gen.py --list most-active
    python scripts/lists_video_gen.py --list veteran
    python scripts/lists_video_gen.py --list by-category --category Gaming
    python scripts/lists_video_gen.py --list by-country  --country US
    python scripts/lists_video_gen.py --all               # all six base lists
    python scripts/lists_video_gen.py --list top-rated --audio music.mp3

Reads NEXT_PUBLIC_SUPABASE_URL + SUPABASE_SERVICE_KEY from .env.
Requires: Pillow, requests, python-dotenv, FFmpeg on PATH.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import requests
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont

load_dotenv()
logger = logging.getLogger(__name__)

# ── Video params ─────────────────────────────────────────────────────────────
W, H = 1920, 1080
FPS = 30
BAR_FRAMES = FPS  # frames to animate bars 0 → full (~1 second)
HOLD_SECS = 5  # hold card via FFmpeg tpad (no Python duplicate frames)

# ── Palette (matches viralvibes.fyi dark theme) ──────────────────────────────
BG = (8, 8, 8)
ACCENT = (255, 69, 0)
CYAN = (0, 200, 255)
CREAM = (245, 240, 232)
MUTED = (107, 107, 107)
BAR_BG = (30, 30, 36)
RANK_COLORS = [(255, 215, 0), (192, 192, 192), (205, 127, 50)]  # gold/silver/bronze

# ── Layout ───────────────────────────────────────────────────────────────────
MARGIN_X = 80
HEADER_H = 120
ROW_H = 64
ROW_GAP = 6
AVATAR_SIZE = 48
BAR_X = W - 560  # left edge of subscriber bar
BAR_W_MAX = 280  # max bar pixel width
METRIC_X = W - 250  # right-side metric column x


# ─────────────────────────────────────────────────────────────────────────────
# Per-list configuration
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ListSpec:
    name: str  # fetcher key
    label: str  # header text (overridden per-job at runtime)
    col_header: str  # right column header
    metric_key: str  # creator dict key
    metric_fmt: str  # "pct" | "count" | "age"


LISTS: dict[str, ListSpec] = {
    "top-rated": ListSpec("top-rated", "Top Rated Creators", "ENG RATE", "engagement_score", "pct"),
    "rising-stars": ListSpec(
        "rising-stars", "Rising Stars — 30-Day Growth", "GROWTH %", "_growth_rate", "pct"
    ),
    "most-active": ListSpec(
        "most-active", "Most Active Creators", "UPLOADS/MO", "monthly_uploads", "count"
    ),
    "veteran": ListSpec(
        "veteran", "Veteran Creators (10+ years)", "AGE (YRS)", "channel_age_days", "age"
    ),
    "by-category": ListSpec(
        "by-category", "Top by Category", "ENG RATE", "engagement_score", "pct"
    ),
    "by-country": ListSpec("by-country", "Top by Country", "ENG RATE", "engagement_score", "pct"),
}


# ─────────────────────────────────────────────────────────────────────────────
# Job — one video to produce
# Each job owns its filename, label override, and optional filter params.
# JOBS drives --all mode; add rows here to produce new videos automatically.
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Job:
    spec: ListSpec
    filename: str  # e.g. TopCreators_IN.mp4
    label: str  # header text shown in the video
    category: str = ""  # only for by-category jobs
    country: str = ""  # only for by-country jobs


def _country_job(code: str, name: str) -> Job:
    return Job(
        LISTS["by-country"],
        f"TopCreators_{code}.mp4",
        f"Top Creators · {name} ({code})",
        country=code,
    )


def _category_job(cat: str) -> Job:
    slug = cat.replace(" ", "")
    return Job(
        LISTS["by-category"], f"TopCreators_{slug}.mp4", f"Top Creators · {cat}", category=cat
    )


JOBS: list[Job] = [
    # ── base lists ─────────────────────────────────────────────────────────
    Job(LISTS["top-rated"], "TopCreators_TopRated.mp4", "Top Rated Creators"),
    Job(LISTS["rising-stars"], "TopCreators_RisingStars.mp4", "Rising Stars — 30-Day Growth"),
    Job(LISTS["most-active"], "TopCreators_MostActive.mp4", "Most Active Creators"),
    Job(LISTS["veteran"], "TopCreators_Veterans.mp4", "Veteran Creators (10+ years)"),
    # ── top countries ──────────────────────────────────────────────────────
    _country_job("US", "United States"),
    _country_job("IN", "India"),
    _country_job("GB", "United Kingdom"),
    _country_job("BR", "Brazil"),
    _country_job("JP", "Japan"),
    _country_job("KR", "South Korea"),
    _country_job("DE", "Germany"),
    _country_job("FR", "France"),
    _country_job("MX", "Mexico"),
    _country_job("CA", "Canada"),
    # ── top categories ─────────────────────────────────────────────────────
    _category_job("Gaming"),
    _category_job("Music"),
    _category_job("Education"),
    _category_job("Technology"),
    _category_job("Entertainment"),
]


# ─────────────────────────────────────────────────────────────────────────────
# Data layer
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class Row:
    rank: int
    channel_name: str
    subscribers: int
    metric: str  # pre-formatted display string for right column
    metric_color: tuple  # RGB colour for metric text
    category: str
    country: str
    thumbnail_url: str


def _format_metric(spec: ListSpec, raw: dict) -> tuple[str, tuple]:
    """Return (display_string, rgb_colour) for the right-side metric."""
    value = raw.get(spec.metric_key)
    if value is None:
        return "—", MUTED

    if spec.metric_fmt == "pct":
        v = float(value)
        color = (80, 220, 100) if v >= 5.0 else (255, 200, 0) if v >= 2.0 else MUTED
        return f"{v:.1f}%", color

    if spec.metric_fmt == "count":
        v = float(value)
        color = (80, 200, 255) if v >= 10 else CREAM
        return _fmt(int(v)), color

    if spec.metric_fmt == "age":
        years = int(float(value)) // 365
        color = (255, 215, 0) if years >= 15 else CREAM
        return f"{years}y", color

    return str(value), MUTED


def _to_row(rank: int, raw: dict, spec: ListSpec) -> Row:
    metric_str, metric_color = _format_metric(spec, raw)
    return Row(
        rank=rank,
        channel_name=raw.get("channel_name") or "Unknown",
        subscribers=int(raw.get("current_subscribers") or 0),
        metric=metric_str,
        metric_color=metric_color,
        category=raw.get("primary_category") or "",
        country=raw.get("country_code") or "",
        thumbnail_url=raw.get("channel_thumbnail_url") or "",
    )


def fetch_list(job: Job, top_n: int) -> list[Row]:
    """
    Pull data via db_lists (same source as the /lists page).
    Returns the rendered Row list; label is carried by the Job.
    """
    # Import after db.init_supabase() has been called in main().
    from db_lists import (
        get_creators_by_category,
        get_creators_by_country,
        get_most_active_creators,
        get_rising_creators,
        get_top_rated_creators,
        get_veteran_creators,
    )

    spec = job.spec
    fetchers: dict[str, Callable] = {
        "top-rated": lambda: get_top_rated_creators(top_n),
        "rising-stars": lambda: get_rising_creators(top_n),
        "most-active": lambda: get_most_active_creators(top_n),
        "veteran": lambda: get_veteran_creators(top_n),
        "by-category": lambda: get_creators_by_category(job.category, top_n),
        "by-country": lambda: get_creators_by_country(job.country, top_n),
    }

    raw_rows = fetchers[spec.name]()
    if not raw_rows:
        raise RuntimeError(f"No creators returned for '{job.filename}'")

    rows = [_to_row(i, r, spec) for i, r in enumerate(raw_rows, start=1)]
    logger.info("Fetched %d rows for '%s'", len(rows), job.label)
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Assets
# ─────────────────────────────────────────────────────────────────────────────


class Avatars:
    """Downloads, circle-crops, and disk-caches creator thumbnails."""

    def __init__(self, cache_dir: Path) -> None:
        self._dir = cache_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    def get(self, url: str) -> Image.Image | None:
        if not url:
            return None
        path = self._dir / f"{hashlib.md5(url.encode()).hexdigest()}.png"
        if not path.exists():
            try:
                r = requests.get(url, timeout=6)
                r.raise_for_status()
                img = Image.open(io.BytesIO(r.content)).convert("RGBA")
                _circle_crop(img, AVATAR_SIZE).save(path)
            except Exception as exc:
                logger.debug("Avatar fetch failed %s: %s", url, exc)
                return None
        try:
            return Image.open(path).convert("RGBA")
        except Exception:
            return None


def _circle_crop(img: Image.Image, size: int) -> Image.Image:
    img = img.resize((size, size), Image.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 1, size - 1), fill=255)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    out.paste(img, mask=mask)
    return out


def _load_font(weight: str, size: int) -> ImageFont.ImageFont:
    candidates = {
        "bold": [
            "C:/Windows/Fonts/arialbd.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ],
        "regular": [
            "C:/Windows/Fonts/arial.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ],
        "mono": [
            "C:/Windows/Fonts/cour.ttf",
            "/System/Library/Fonts/Menlo.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        ],
    }
    for path in candidates.get(weight, candidates["regular"]):
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def _fmt(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return str(n)


# ─────────────────────────────────────────────────────────────────────────────
# Frame renderer
# ─────────────────────────────────────────────────────────────────────────────


class FrameRenderer:
    """
    Renders a ranking card with a simple bar-grow animation.

    Phase 1 — BAR_FRAMES: all rows visible; subscriber bars grow 0 → full.
    Phase 2 — hold: delegated to FFmpeg tpad (no Python duplicate frames).
    """

    def __init__(
        self,
        rows: list[Row],
        label: str,
        col_header: str,
        avatars: Avatars,
    ) -> None:
        self.rows = rows
        self.label = label
        self.col_header = col_header

        self.f_bold = _load_font("bold", 26)
        self.f_title = _load_font("bold", 30)
        self.f_reg = _load_font("regular", 20)
        self.f_sm = _load_font("regular", 16)
        self.f_mono = _load_font("mono", 20)
        self.f_rank = _load_font("bold", 18)

        logger.info("Downloading %d avatars …", len(rows))
        self._avatars: dict[str, Image.Image | None] = {
            r.channel_name: avatars.get(r.thumbnail_url) for r in rows
        }
        self._max_subs = max((r.subscribers for r in rows), default=1) or 1

    # ── per-frame helpers ──────────────────────────────────────────────────

    def _canvas(self) -> Image.Image:
        img = Image.new("RGB", (W, H), BG)
        d = ImageDraw.Draw(img)
        for x in range(0, W, 60):
            d.line([(x, 0), (x, H)], fill=(20, 20, 24))
        for y in range(0, H, 60):
            d.line([(0, y), (W, y)], fill=(20, 20, 24))
        return img

    def _draw_header(self, d: ImageDraw.Draw) -> None:
        d.rectangle([(0, 0), (W, 4)], fill=ACCENT)

        brand = "ViralVibes.fyi"
        d.text((MARGIN_X, 22), brand, font=self.f_bold, fill=ACCENT)
        bw = int(d.textlength(brand, font=self.f_bold))
        d.text((MARGIN_X + bw + 18, 26), "·", font=self.f_reg, fill=MUTED)
        d.text((MARGIN_X + bw + 36, 22), self.label, font=self.f_reg, fill=CREAM)

        hy = HEADER_H - 24
        d.text((MARGIN_X + 116, hy), "CREATOR", font=self.f_sm, fill=MUTED)
        d.text((BAR_X, hy), "SUBSCRIBERS", font=self.f_sm, fill=MUTED)
        d.text((METRIC_X, hy), self.col_header, font=self.f_sm, fill=MUTED)
        cw = int(d.textlength("COUNTRY", font=self.f_sm))
        d.text((W - MARGIN_X - cw, hy), "COUNTRY", font=self.f_sm, fill=MUTED)

        d.line([(MARGIN_X, HEADER_H - 4), (W - MARGIN_X, HEADER_H - 4)], fill=(*MUTED, 60), width=1)

    def _draw_row(
        self,
        img: Image.Image,
        d: ImageDraw.Draw,
        row: Row,
        idx: int,
        t: float,
    ) -> None:
        """Draw one creator row. t ∈ (0, 1] drives bar fill."""
        y = HEADER_H + 10 + idx * (ROW_H + ROW_GAP)
        cx = MARGIN_X

        # Alternating row background
        if idx % 2 == 0:
            d.rectangle([(cx, y - 2), (W - cx, y + ROW_H - 2)], fill=(22, 22, 28))

        # ── rank badge ──────────────────────────────────────────────────────
        rank_color = RANK_COLORS[row.rank - 1] if row.rank <= 3 else MUTED
        d.text((cx, y + (ROW_H - 18) // 2), f"#{row.rank:02d}", font=self.f_rank, fill=rank_color)
        cx += 56

        # ── avatar ──────────────────────────────────────────────────────────
        av = self._avatars.get(row.channel_name)
        av_y = y + (ROW_H - AVATAR_SIZE) // 2
        if av:
            img.paste(av, (cx, av_y), av)
        else:
            d.ellipse(
                [(cx, av_y), (cx + AVATAR_SIZE, av_y + AVATAR_SIZE)],
                fill=(35, 35, 45),
                outline=(*MUTED, 100),
            )
        cx += AVATAR_SIZE + 14

        # ── name + category tag ─────────────────────────────────────────────
        d.text((cx, y + 10), row.channel_name[:28], font=self.f_bold, fill=CREAM)
        if row.category:
            d.text((cx, y + 38), row.category[:26], font=self.f_sm, fill=(*CYAN,))

        # ── subscriber bar (grows with t) ───────────────────────────────────
        bar_fill = int(BAR_W_MAX * row.subscribers / self._max_subs * t)
        d.rectangle([(BAR_X, y + 24), (BAR_X + BAR_W_MAX, y + 36)], fill=BAR_BG)
        if bar_fill > 0:
            bar_color = ACCENT if row.rank == 1 else (200, 55, 0)
            d.rectangle([(BAR_X, y + 24), (BAR_X + bar_fill, y + 36)], fill=bar_color)
        d.text(
            (BAR_X + BAR_W_MAX + 10, y + (ROW_H - 20) // 2),
            _fmt(row.subscribers),
            font=self.f_mono,
            fill=CREAM,
        )

        # ── right-side metric ───────────────────────────────────────────────
        d.text(
            (METRIC_X, y + (ROW_H - 20) // 2), row.metric, font=self.f_mono, fill=row.metric_color
        )

        # ── country code ────────────────────────────────────────────────────
        if row.country:
            cw = int(d.textlength(row.country, font=self.f_mono))
            d.text(
                (W - MARGIN_X - cw, y + (ROW_H - 20) // 2),
                row.country,
                font=self.f_mono,
                fill=MUTED,
            )

    def _draw_footer(self, d: ImageDraw.Draw) -> None:
        d.line([(MARGIN_X, H - 50), (W - MARGIN_X, H - 50)], fill=(*MUTED, 40))
        d.text(
            (MARGIN_X, H - 38),
            "viralvibes.fyi  ·  Creator Intelligence Platform",
            font=self.f_sm,
            fill=(*MUTED, 160),
        )
        rtext = "Data updated daily  ·  Public sources only"
        rw = int(d.textlength(rtext, font=self.f_sm))
        d.text((W - MARGIN_X - rw, H - 38), rtext, font=self.f_sm, fill=(*MUTED, 110))
        d.rectangle([(0, H - 4), (W, H)], fill=ACCENT)

    def render_frames(self):
        """Yield 1920×1080 PIL Images; bars grow 0 → full over BAR_FRAMES frames."""
        for f in range(BAR_FRAMES):
            t = (f + 1) / BAR_FRAMES  # 1/30 … 30/30  (never 0)
            img = self._canvas()
            d = ImageDraw.Draw(img)
            self._draw_header(d)
            for idx, row in enumerate(self.rows):
                self._draw_row(img, d, row, idx, t)
            self._draw_footer(d)
            yield img


# ─────────────────────────────────────────────────────────────────────────────
# Encode
# ─────────────────────────────────────────────────────────────────────────────


def encode(frames, output: Path, audio: Path | None) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    if audio and not audio.exists():
        logger.warning("Audio file not found: %s — continuing without audio", audio)
        audio = None

    cmd = [
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
    if audio:
        cmd += ["-i", str(audio), "-shortest", "-c:a", "aac", "-b:a", "320k"]

    # YouTube-optimised H.264:
    #   -profile:v high -level 4.1   → required by YouTube for 1080p
    #   -crf 18 -preset slow         → near-lossless quality
    #   -g 30                        → keyframe every second (fast seeking)
    #   -movflags +faststart         → moov atom first (instant play)
    # tpad clones the last frame for HOLD_SECS — no Python duplicate frames.
    cmd += [
        "-vf",
        f"tpad=stop_mode=clone:stop_duration={HOLD_SECS}",
        "-vcodec",
        "libx264",
        "-profile:v",
        "high",
        "-level",
        "4.1",
        "-preset",
        "slow",
        "-crf",
        "18",
        "-g",
        str(FPS),
        "-keyint_min",
        str(FPS),
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        str(output),
    ]

    logger.info("Encoding → %s  (hold %ds via tpad)", output, HOLD_SECS)
    # All args are constants/Path objects — no shell=True, no user input interpolated.
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)  # noqa: S603

    n = 0
    for img in frames:
        proc.stdin.write(img.tobytes())
        n += 1
    proc.stdin.close()
    _, stderr = proc.communicate()

    if proc.returncode != 0:
        raise RuntimeError(f"FFmpeg error:\n{stderr.decode()}")

    mb = output.stat().st_size / 1e6
    logger.info("✓  %s  (%.1f MB — %d frames + %ds hold)", output, mb, n, HOLD_SECS)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def _render_job(job: Job, top_n: int, avatars: Avatars, out_dir: Path, audio: Path | None) -> None:
    """Fetch, render, and encode one job → out_dir/job.filename."""
    output = out_dir / job.filename
    rows = fetch_list(job, top_n)
    renderer = FrameRenderer(rows, job.label, job.spec.col_header, avatars)
    encode(renderer.render_frames(), output, audio)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Render ViralVibes ranking lists as YouTube-ready MP4s.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  python scripts/lists_video_gen.py                 # all JOBS → output/
  python scripts/lists_video_gen.py --job top-rated # single job by filename prefix
  python scripts/lists_video_gen.py --list-jobs     # print every job and exit""",
    )
    ap.add_argument(
        "--job",
        default=None,
        metavar="NAME",
        help="Run only jobs whose filename contains NAME (case-insensitive)",
    )
    ap.add_argument(
        "--list-jobs",
        action="store_true",
        help="Print all configured jobs and exit",
    )
    ap.add_argument("--top-n", type=int, default=20, help="Rows per video (default: 20)")
    ap.add_argument("--audio", default=None, help="Background audio file to mux")
    ap.add_argument("--output-dir", default="output", help="Output directory (default: output/)")
    ap.add_argument("--cache-dir", default=".cache/avatars", help="Avatar thumbnail cache")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.list_jobs:
        print(f"{'FILENAME':<40} LABEL")
        print("-" * 70)
        for j in JOBS:
            print(f"{j.filename:<40} {j.label}")
        return

    url = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        sys.exit("Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_KEY in .env")

    sys.path.insert(0, str(Path(__file__).parent.parent))
    import db

    db.init_supabase()

    out_dir = Path(args.output_dir)
    audio = Path(args.audio) if args.audio else None
    avatars = Avatars(Path(args.cache_dir))

    jobs = JOBS
    if args.job:
        jobs = [j for j in JOBS if args.job.lower() in j.filename.lower()]
        if not jobs:
            sys.exit(f"No jobs match '{args.job}'. Run --list-jobs to see all.")

    ok, fail = 0, 0
    for job in jobs:
        try:
            _render_job(job, args.top_n, avatars, out_dir, audio)
            ok += 1
        except Exception as exc:
            logger.error("SKIP %s — %s", job.filename, exc)
            fail += 1

    logger.info("Done: %d rendered, %d skipped.", ok, fail)


if __name__ == "__main__":
    main()
