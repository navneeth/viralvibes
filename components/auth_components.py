"""
One-Tap Login Components
Material Design 3 inspired authentication UI components.
Designed for minimal friction and maximum trust.
"""

from fasthtml.common import *
from monsterui.all import *


# =============================================================================
# Google Brand Assets (Official SVG)
# =============================================================================
def GoogleGLogo(size: int = 24):
    """Official Google 'G' logo SVG.
    
    Source: Google Brand Resource Center
    https://developers.google.com/identity/branding-guidelines
    """
    return Svg(
        Path(
            d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z",
            fill="#4285F4",
        ),
        Path(
            d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z",
            fill="#34A853",
        ),
        Path(
            d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z",
            fill="#FBBC05",
        ),
        Path(
            d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z",
            fill="#EA4335",
        ),
        viewBox="0 0 24 24",
        width=str(size),
        height=str(size),
        style="flex-shrink: 0;",
    )


def ShieldCheckIcon(size: int = 16):
    """Trust badge icon - shield with checkmark."""
    return Svg(
        Path(
            d="M12 1L3 5v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V5l-9-4z",
            fill="none",
            stroke="currentColor",
            stroke_width="2",
        ),
        Path(
            d="M9 12l2 2 4-4",
            fill="none",
            stroke="currentColor",
            stroke_width="2",
            stroke_linecap="round",
            stroke_linejoin="round",
        ),
        viewBox="0 0 24 24",
        width=str(size),
        height=str(size),
        **{"aria-hidden": "true"},
    )


# =============================================================================
# Trust Badges & Micro-copy
# =============================================================================
def TrustBadge(icon=None, text: str = "No credit card required"):
    """Display trust signal below CTA button."""
    return Div(
        icon or ShieldCheckIcon(16),
        Span(text, cls="auth-trust-text"),
        cls="auth-trust-badge",
    )


def PrivacyDisclaimer():
    """Terms & Privacy links."""
    return P(
        "By continuing, you agree to our ",
        A("Terms of Service", href="/terms", target="_blank", cls="auth-link"),
        " and ",
        A("Privacy Policy", href="/privacy", target="_blank", cls="auth-link"),
        ".",
        cls="auth-disclaimer",
    )


# =============================================================================
# Google Sign-In Button (Official Design)
# =============================================================================
def GoogleSignInButton(
    href: str,
    text: str = "Continue with Google",
    full_width: bool = True,
):
    """Official Google Sign-In button following brand guidelines.
    
    References:
    - https://developers.google.com/identity/branding-guidelines
    - Material Design 3 elevated button specs
    """
    return A(
        GoogleGLogo(20),
        Span(text, cls="auth-btn-text"),
        href=href,
        cls=f"auth-google-btn {'auth-btn-full' if full_width else ''}",
        **{"data-test": "google-signin-btn"},
    )


# =============================================================================
# Account Chooser (Return User Flow)
# =============================================================================
def AccountChooser(email: str, avatar_url: str = None, user_id: str = None):
    """Show existing account chip for returning users.
    
    Args:
        email: User's email address
        avatar_url: User avatar (e.g., /avatar/{user_id})
        user_id: Optional user ID for analytics
    """
    return Button(
        # Avatar
        (
            Img(
                src=avatar_url,
                alt=f"{email} avatar",
                cls="auth-avatar",
            )
            if avatar_url
            else Div(
                Span(email[0].upper(), cls="auth-avatar-initial"),
                cls="auth-avatar auth-avatar-placeholder",
            )
        ),
        # Email + Switch account link
        Div(
            P(email, cls="auth-email"),
            Span("Switch account", cls="auth-switch"),
            cls="auth-account-info",
        ),
        cls="auth-account-chip",
        type="submit",
        name="account",
        value=email,
        **{"data-user-id": user_id} if user_id else {},
    )


# =============================================================================
# Main One-Tap Login Card
# =============================================================================
def OneTapLoginCard(
    oauth_login_link: str,
    site_name: str = "ViralVibes",
    logo_src: str = None,
    return_url: str = None,
    remembered_email: str = None,
    remembered_avatar: str = None,
    remembered_user_id: str = None,
):
    """One-Tap centered login card with Material Design 3 styling.
    
    Args:
        oauth_login_link: Google OAuth URL (from oauth.login_link(req))
        site_name: Your application name
        logo_src: Path to your logo image (optional)
        return_url: Where to redirect after login
        remembered_email: Last logged-in email (for account chooser)
        remembered_avatar: Avatar URL for returning user
        remembered_user_id: User ID for returning user
    
    Features:
        - Centered elevation card
        - Progressive disclosure
        - Account chooser for returning users
        - Trust badges
        - Accessibility optimized
    """
    
    # Build the card header
    card_header = Div(
        # Logo
        (
            Img(src=logo_src, alt=f"{site_name} logo", cls="auth-logo")
            if logo_src
            else H1(site_name, cls="auth-brand")
        ),
        # Headline
        H2(
            f"Sign in to {site_name}" if not remembered_email else "Welcome back",
            cls="auth-headline",
        ),
        # Subheadline
        P(
            "Analyze YouTube playlists instantly",
            cls="auth-subheadline",
        ),
        cls="auth-header",
    )
    
    # Account chooser for returning users
    account_section = None
    if remembered_email:
        account_section = Div(
            AccountChooser(
                email=remembered_email,
                avatar_url=remembered_avatar,
                user_id=remembered_user_id,
            ),
            P("or", cls="auth-divider-text"),
            cls="auth-account-section",
        )
    
    # Primary CTA
    cta_section = Div(
        GoogleSignInButton(
            href=oauth_login_link + (f"&state={return_url}" if return_url else ""),
            text="Continue with Google",
            full_width=True,
        ),
        TrustBadge(text="No credit card required • Free forever"),
        cls="auth-cta-section",
    )
    
    # Footer
    card_footer = Div(
        PrivacyDisclaimer(),
        cls="auth-footer",
    )
    
    # Assemble the card
    return Div(
        Div(
            card_header,
            account_section,
            cta_section,
            card_footer,
            cls="auth-card",
        ),
        cls="auth-container",
        **{"data-testid": "one-tap-login"},
    )


# =============================================================================
# Friction Point Login Prompt (Inline)
# =============================================================================
def LoginPrompt(
    oauth_login_link: str,
    message: str = "Sign in to continue",
    return_url: str = None,
    compact: bool = False,
):
    """Inline login prompt for friction points (e.g., before analysis).
    
    Args:
        oauth_login_link: Google OAuth URL
        message: Custom prompt message
        return_url: Where to redirect after login
        compact: Use compact layout
    """
    return Div(
        Div(
            UkIcon("lock", cls="auth-prompt-icon"),
            P(message, cls="auth-prompt-message"),
            GoogleSignInButton(
                href=oauth_login_link + (f"&state={return_url}" if return_url else ""),
                text="Sign in with Google",
                full_width=not compact,
            ),
            cls="auth-prompt-compact" if compact else "auth-prompt",
        ),
        cls="auth-prompt-container",
    )


# =============================================================================
# Loading State (OAuth Redirect)
# =============================================================================
def OAuthLoadingState(message: str = "Signing you in..."):
    """Show spinner during OAuth redirect."""
    return Div(
        Div(
            Loading(cls=(LoadingT.spinner, LoadingT.lg)),
            P(message, cls="auth-loading-text"),
            cls="auth-loading",
        ),
        cls="auth-container",
    )
