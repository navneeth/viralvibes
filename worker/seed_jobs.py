from datetime import datetime, timezone

from secrets_loader import load_secrets

from constants import KNOWN_PLAYLISTS
from db import init_supabase


def main() -> None:
    load_secrets()
    supabase = init_supabase()
    if not supabase:
        raise SystemExit("❌ Supabase init failed — check env vars")

    for playlist in KNOWN_PLAYLISTS:
        url = playlist.get("url")
        if not url:
            continue
        job = {
            "playlist_url": url,
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        resp = supabase.table("playlist_jobs").insert(job).execute()
        print(f"Inserted job for playlist: {url} -> {resp.data}")


if __name__ == "__main__":
    main()
