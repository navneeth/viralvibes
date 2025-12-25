from fasthtml.common import *

from db import (
    get_dashboard_event_counts,
    get_supabase,
    record_dashboard_event,
)
from utils import compute_dashboard_id, load_df_from_json
from views.dashboard import render_dashboard


@rt("/d/{dashboard_id}")
def dashboard_view(dashboard_id: str):
    supabase = get_supabase()

    # 1️⃣ Find playlist by derived dashboard_id
    resp = supabase.table("playlist_stats").select("*").execute()

    playlist_row = None
    for row in resp.data or []:
        if compute_dashboard_id(row["playlist_url"]) == dashboard_id:
            playlist_row = row
            break

    if not playlist_row:
        return Div(
            H2("Dashboard not found"),
            P("This playlist dashboard does not exist."),
            cls="max-w-xl mx-auto mt-24 text-center",
        )

    # 2️⃣ Record view
    record_dashboard_event(
        supabase,
        dashboard_id=dashboard_id,
        event_type="view",
    )

    # 3️⃣ Load analytics data
    df = load_df_from_json(playlist_row["df_json"])

    # 4️⃣ Interest metrics
    interest = get_dashboard_event_counts(
        supabase,
        dashboard_id,
    )

    # 5️⃣ Render persistent dashboard
    return render_dashboard(
        df=df,
        playlist_stats=playlist_row,
        mode="persistent",
        dashboard_id=dashboard_id,
        interest=interest,
    )
