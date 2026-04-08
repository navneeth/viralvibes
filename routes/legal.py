"""
Legal pages: Terms of Service and Privacy Policy.
"""

from fasthtml.common import *
from monsterui.all import *


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _legal_page(title: str, last_updated: str, *sections) -> Div:
    """Wraps a legal page in a consistent container with a header."""
    return Div(
        # Page header
        Div(
            H1(title, cls="text-3xl font-bold text-foreground mb-2"),
            P(f"Last updated: {last_updated}", cls="text-sm text-muted-foreground"),
            cls="mb-10 pb-8 border-b border-border",
        ),
        # Sections
        Div(*sections, cls="space-y-10"),
        cls="max-w-3xl mx-auto px-4 py-16",
    )


def _section(heading: str, *paragraphs) -> Div:
    return Div(
        H2(heading, cls="text-xl font-semibold text-foreground mb-3"),
        *[P(text, cls="text-muted-foreground leading-relaxed mb-3") for text in paragraphs],
    )


def _bullet_section(heading: str, items: list[str]) -> Div:
    return Div(
        H2(heading, cls="text-xl font-semibold text-foreground mb-3"),
        Ul(
            *[Li(item, cls="text-muted-foreground leading-relaxed mb-2") for item in items],
            cls="list-disc list-inside space-y-1",
        ),
    )


# ---------------------------------------------------------------------------
# Terms of Service
# ---------------------------------------------------------------------------

def terms_page_content() -> Div:
    return _legal_page(
        "Terms of Service",
        "April 8, 2026",
        _section(
            "1. Acceptance of Terms",
            "By accessing or using ViralVibes ("the Service"), you agree to be bound by these "
            "Terms of Service. If you do not agree to these terms, please do not use the Service.",
            "These terms apply to all visitors, users, and others who access the Service. "
            "ViralVibes reserves the right to update these terms at any time. Continued use of "
            "the Service after any changes constitutes acceptance of the new terms.",
        ),
        _section(
            "2. Description of Service",
            "ViralVibes is a creator intelligence platform that provides data-driven insights "
            "about YouTube creators and their content. The Service includes creator discovery "
            "tools, engagement analytics, playlist analysis, and curated ranking lists.",
            "All data displayed is sourced from publicly available YouTube information. "
            "ViralVibes does not require access to any YouTube channel or account in order "
            "to analyse it.",
        ),
        _section(
            "3. Permitted Use",
            "You may use the Service for lawful purposes only. You agree not to:",
        ),
        _bullet_section(
            "",
            [
                "Scrape, crawl, or programmatically extract data from the Service without prior written consent.",
                "Reproduce, redistribute, or resell Service content for commercial purposes without authorisation.",
                "Attempt to reverse-engineer or circumvent any security or access controls.",
                "Submit false or misleading information, or impersonate any person or entity.",
                "Use the Service in any way that could damage, disable, or impair its operation.",
            ],
        ),
        _section(
            "4. Accounts and Authentication",
            "Certain features of the Service require you to create an account using Google OAuth. "
            "You are responsible for maintaining the confidentiality of your account credentials "
            "and for all activity that occurs under your account.",
            "We reserve the right to suspend or terminate accounts that violate these terms "
            "or that appear to be engaged in abusive or automated behaviour.",
        ),
        _section(
            "5. Intellectual Property",
            "All content on the Service — including text, graphics, logos, and software — is the "
            "property of ViralVibes or its content suppliers and is protected by applicable "
            "intellectual property laws.",
            "YouTube data displayed through the Service remains subject to YouTube's own terms "
            "of service (https://www.youtube.com/t/terms). ViralVibes does not claim ownership "
            "of any third-party content.",
        ),
        _section(
            "6. Disclaimer of Warranties",
            "The Service is provided on an \"as is\" and \"as available\" basis without warranties "
            "of any kind, either express or implied. ViralVibes does not warrant that the Service "
            "will be uninterrupted, error-free, or free of viruses or other harmful components.",
            "Creator rankings, engagement metrics, and estimated earnings are derived from "
            "publicly available data and algorithmic analysis. They are provided for informational "
            "purposes and should not be solely relied upon for commercial decisions.",
        ),
        _section(
            "7. Limitation of Liability",
            "To the maximum extent permitted by applicable law, ViralVibes shall not be liable "
            "for any indirect, incidental, special, consequential, or punitive damages arising "
            "from your use of, or inability to use, the Service.",
        ),
        _section(
            "8. Governing Law",
            "These terms shall be governed by and construed in accordance with applicable law. "
            "Any disputes relating to these terms shall be subject to the exclusive jurisdiction "
            "of the relevant courts.",
        ),
        _section(
            "9. Contact",
            "For questions about these Terms of Service, please contact us at "
            "legal@viralvibes.app.",
        ),
    )


# ---------------------------------------------------------------------------
# Privacy Policy
# ---------------------------------------------------------------------------

def privacy_page_content() -> Div:
    return _legal_page(
        "Privacy Policy",
        "April 8, 2026",
        _section(
            "1. Introduction",
            "ViralVibes ("we", "us", or "our") is committed to protecting your personal data. "
            "This Privacy Policy explains what information we collect, how we use it, and your "
            "rights in relation to it.",
            "We process data in accordance with applicable privacy law, including the General "
            "Data Protection Regulation (GDPR) where applicable.",
        ),
        _section(
            "2. Information We Collect",
            "We collect the following categories of information:",
        ),
        _bullet_section(
            "",
            [
                "Account data: Your name and email address provided via Google OAuth when you sign in.",
                "Usage data: Pages visited, features used, and interactions within the Service — collected via Vercel Web Analytics (privacy-focused, no cookies, no cross-site tracking).",
                "User content: Playlist URLs and dashboard data you submit or save.",
                "Communications: Email addresses submitted via the newsletter signup form.",
            ],
        ),
        _section(
            "3. How We Use Your Data",
            "We use the information we collect to:",
        ),
        _bullet_section(
            "",
            [
                "Provide, operate, and improve the Service.",
                "Authenticate your account and maintain session security.",
                "Store your saved dashboards and analysis history.",
                "Send product updates or newsletters (only where you have opted in).",
                "Understand how the Service is used in aggregate to guide product decisions.",
            ],
        ),
        _section(
            "4. Data Storage and Security",
            "Account and dashboard data is stored in Supabase, a cloud database provider, "
            "with servers located in the European Union (AWS eu-west-2 / eu-central-1). "
            "Data is encrypted in transit (TLS) and at rest.",
            "We take reasonable technical and organisational measures to protect your data "
            "against unauthorised access, loss, or misuse. No method of transmission over the "
            "internet is 100% secure; we cannot guarantee absolute security.",
        ),
        _section(
            "5. Third-Party Services",
            "The Service integrates with the following third-party services, each governed by "
            "their own privacy policies:",
        ),
        _bullet_section(
            "",
            [
                "Google OAuth — for authentication. Subject to Google's Privacy Policy.",
                "YouTube Data API — for public channel and playlist data. Subject to YouTube's Terms of Service.",
                "Supabase — for database and storage. Subject to Supabase's Privacy Policy.",
                "Vercel — for hosting and analytics. Subject to Vercel's Privacy Policy.",
            ],
        ),
        _section(
            "6. Cookies",
            "ViralVibes uses only a single session cookie required for authentication. "
            "We do not use advertising cookies or third-party tracking cookies.",
            "Our analytics provider (Vercel Web Analytics) does not use cookies and does not "
            "track users across sites.",
        ),
        _section(
            "7. Your Rights",
            "Depending on your jurisdiction, you may have the right to:",
        ),
        _bullet_section(
            "",
            [
                "Access the personal data we hold about you.",
                "Request correction of inaccurate data.",
                "Request deletion of your account and associated data.",
                "Object to or restrict certain processing.",
                "Data portability — receive a copy of your data in a structured format.",
            ],
        ),
        _section(
            "",
            "To exercise any of these rights, please contact us at privacy@viralvibes.app. "
            "We will respond within 30 days.",
        ),
        _section(
            "8. Data Retention",
            "We retain your account data for as long as your account is active. "
            "Newsletter subscribers are retained until you unsubscribe. "
            "You may request deletion of your data at any time.",
        ),
        _section(
            "9. Children's Privacy",
            "The Service is not directed at children under the age of 16. We do not knowingly "
            "collect personal data from children. If you believe a child has provided us with "
            "personal data, please contact us so we can delete it.",
        ),
        _section(
            "10. Changes to This Policy",
            "We may update this Privacy Policy from time to time. Material changes will be "
            "communicated via the Service or by email. The \"last updated\" date at the top of "
            "this page reflects the most recent revision.",
        ),
        _section(
            "11. Contact",
            "For privacy-related questions or requests, please contact us at "
            "privacy@viralvibes.app.",
        ),
    )
