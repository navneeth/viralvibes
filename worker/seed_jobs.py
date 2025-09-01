import os
from datetime import datetime

from dotenv import load_dotenv

from constants import KNOWN_PLAYLISTS
from db import init_supabase

load_dotenv()
supabase = init_supabase()

for playlist in KNOWN_PLAYLISTS:
    url = playlist["url"]
    job = {
        "playlist_url": url,
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),  # <-- Set current UTC time
    }
    resp = supabase.table("playlist_jobs").insert(job).execute()
    print(f"Inserted job for playlist: {url} -> {resp.data}")
