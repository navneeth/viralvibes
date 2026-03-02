"""
ViralVibes - STAGED DEPLOYMENT DEBUGGER
========================================
Deploy each stage in order. The first stage that FAILS reveals the culprit.

INSTRUCTIONS:
  Stage 1 → Deploy. Works? → Uncomment Stage 2 block, re-deploy.
  Stage 2 → Works? → Uncomment Stage 3 block, re-deploy. Etc.

HOW TO USE:
  Each stage is separated by a clearly labeled block.
  Uncomment the next stage's block, keep all previous stages uncommented.
"""

# ============================================================================
# STAGE 1 — Absolute minimum: Does FastHTML itself start?
# If this fails → wrong Python version, fasthtml not installed, or port issue.
# ============================================================================

import logging
import os

from dotenv import load_dotenv
from fasthtml.common import *
from fasthtml.common import RedirectResponse, Response

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app, rt = fast_app(title="ViralVibes - Debug")


@rt("/")
def index():
    return Titled("Stage 1 OK", P("FastHTML is running. Proceed to Stage 2."))


# ============================================================================
# STAGE 2 — Add MonsterUI + Theme headers
# If this fails → monsterui not installed, or `apex_charts` param changed.
# FIX CANDIDATE: Try Theme.red.headers() without apex_charts=True first.
# ============================================================================

# from monsterui.all import *
#
# hdrs = Theme.red.headers()
# # NOTE: If the above works but the original failed, the bug is apex_charts=True
# # Try: hdrs = Theme.red.headers(apex_charts=True)  — if this raises, that's the bug.
#
# app, rt = fast_app(
#     hdrs=hdrs,
#     title="ViralVibes - Debug Stage 2",
# )
#
# @rt("/")
# def index():
#     return Titled("Stage 2 OK", P("MonsterUI + Theme loaded. Proceed to Stage 3."))

# ============================================================================
# STAGE 3 — Add internal imports one group at a time.
# If ANY import below fails → that module has a broken dependency or syntax error.
# Uncomment ONE group at a time to isolate.
# ============================================================================

# --- 3a: Auth modules ---
# from auth.auth_service import AUTH_SKIP_ROUTE_PATTERNS, ViralVibesAuth, init_google_oauth
# from auth.token_revocation import clear_auth_session, revoke_google_token

# --- 3b: Components ---
# from components import (
#     AnalysisFormCard, AnalyticsDashboardSection, BenefitsCard, ExploreGridSection,
#     FeaturesCard, HeaderCard, HomepageAccordion, NavComponent, NewsletterCard,
#     SectionDivider, StepProgress, engagement_slider_section, faq_section,
#     features_section, footer, hero_section, how_it_works_section,
# )
# from components.modals import ExportModal, ShareModal

# --- 3c: Constants ---
# from constants import PLAYLIST_STATS_TABLE, PLAYLIST_STEPS_CONFIG, SECTION_BASE, SIGNUPS_TABLE

# --- 3d: Controllers ---
# from controllers.auth_routes import (
#     build_auth_redirect_page, build_login_page, build_logout_response,
#     build_onetap_login_page, normalize_intended_url, require_auth,
# )
# from controllers.job_progress import job_progress_controller
# from controllers.preview import preview_playlist_controller

# --- 3e: DB ---
# from db import (
#     get_cached_playlist_stats, get_estimated_stats, get_job_progress,
#     get_playlist_job_status, get_playlist_preview_info, get_user_dashboards,
#     init_supabase, resolve_playlist_url_from_dashboard_id, setup_logging,
#     submit_playlist_job, supabase_client, upsert_playlist_stats,
# )

# --- 3f: Services / Utils / Validators / Views ---
# from services.playlist_loader import load_cached_or_stub, load_dashboard_by_id
# from utils import compute_dashboard_id, get_columns, sort_dataframe
# from validators import YoutubePlaylist, YoutubePlaylistValidator
# from views.dashboard import render_full_dashboard
# from views.my_dashboards import render_my_dashboards_page
# from views.table import DISPLAY_HEADERS, get_sort_col, render_playlist_table

# @rt("/")
# def index():
#     return Titled("Stage 3 OK", P("All imports loaded. Proceed to Stage 4."))

# ============================================================================
# STAGE 4 — App init with beforeware + service initialization
# If this fails → env vars missing (SUPABASE_URL, GOOGLE_CLIENT_ID etc.)
#                or init_google_oauth / init_supabase crashes.
# ============================================================================

# _bware = Beforeware(
#     lambda req, sess: req.scope.__setitem__("auth", sess.get("auth", None)),
#     skip=AUTH_SKIP_ROUTE_PATTERNS,
# )
#
# app, rt = fast_app(
#     hdrs=hdrs,
#     before=_bware,
#     title="ViralVibes - Debug Stage 4",
# )
#
# def init_app_services(app):
#     setup_logging()
#     is_testing = os.getenv("TESTING") == "1"
#     if is_testing:
#         return {"supabase_client": None, "oauth": None, "google_client": None}
#     try:
#         client = init_supabase()
#         global supabase_client
#         if client:
#             supabase_client = client
#     except Exception as e:
#         logger.error(f"Supabase init failed: {e}")
#         supabase_client = None
#     google_client, oauth = init_google_oauth(app, supabase_client)
#     return {"supabase_client": supabase_client, "oauth": oauth, "google_client": google_client}
#
# services = init_app_services(app)
# supabase_client = services["supabase_client"]
# oauth = services["oauth"]
#
# @rt("/")
# def index(req, sess):
#     return Titled("Stage 4 OK", P("Services initialized. Proceed to Stage 5."))

# ============================================================================
# STAGE 5 — Add routes back, one suspicious group at a time.
#
# ⚠️  KNOWN BUG — HIGH PROBABILITY CULPRIT:
#     Line 733 in original:  @rt("/update-steps/<int:step>")
#     FastHTML/Starlette does NOT support Flask-style <int:step> converters.
#     The correct syntax is:  @rt("/update-steps/{step}")
#     This will cause a startup crash when routes are registered.
#     FIXED below — verify this was the issue.
# ============================================================================

# --- 5a: Safe, simple routes first ---
# @rt("/login")
# def login(req, sess):
#     normalize_intended_url(sess)
#     return build_auth_redirect_page(oauth, req, sess, return_url="/")
#
# @rt("/logout")
# def logout():
#     return build_logout_response()
#
# @rt("/")
# def index(req, sess):
#     return Titled("Stage 5a OK", P("Basic auth routes work."))

# --- 5b: The previously broken route (FIXED) ---
# @rt("/update-steps/{step}")           # ← FIXED: was "/update-steps/<int:step>"
# def update_steps_progressive(step: int):
#     """Progressively update steps to show completion"""
#     response = StepProgress(step)
#     if step < len(PLAYLIST_STEPS_CONFIG) - 1:
#         response = Div(
#             response,
#             Script(
#                 f"""
#                 setTimeout(() => {{
#                     htmx.ajax('GET', '/update-steps/{step + 1}', {{target: '#playlist-steps'}});
#                 }}, 800);
#             """
#             ),
#         )
#     return response

# --- 5c: All remaining routes (dashboard, validate, export, etc.) ---
# Paste them in from the original file once 5a + 5b are confirmed working.

# ============================================================================
# SERVE — always at the bottom
# ============================================================================
serve()
