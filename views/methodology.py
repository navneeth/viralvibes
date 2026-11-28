"""Public explanation of the creator earnings estimate."""

from fasthtml.common import A, Div, H1, H2, P


def methodology_page_content() -> Div:
    return Div(
        H1("Earnings methodology", cls="text-3xl font-bold text-foreground mb-4"),
        P(
            "These figures model monthly YouTube ad revenue only (AdSense-related advertising), "
            "not creators' reported or verified earnings or AdSense payouts. They do not include "
            "sponsorships, memberships, merchandise, or other income.",
            cls="text-muted-foreground mb-8",
        ),
        Div(
            H2("How the estimate is built", cls="text-xl font-semibold mb-3"),
            P(
                "The model combines a view-based estimate with a subscriber-based estimate. "
                "The view estimate starts from lifetime channel views divided by channel age "
                "(with a three-month minimum), then may adjust that proxy using a recent "
                "30-day view change when the data is usable."
            ),
            P(
                "View-based revenue is estimated as monthly views per 1,000 multiplied by "
                "an effective RPM. The current model starts with a $1.70 US long-form RPM, "
                "multiplies it by a country-tier factor and a niche factor, applies a Shorts "
                "adjustment when a Shorts share is known, and caps effective RPM at $12. "
                "The creator data currently does not provide a reliable Shorts share, so "
                "unknown Shorts mix is treated as zero in the arithmetic and remains an "
                "important limitation."
            ),
            P(
                "Country-tier RPM factors are 1.00 (tier 1), 0.55 (tier 2), 0.25 (tier 3), "
                "and 0.12 (tier 4); an unknown country defaults to tier 2. Niche factors are "
                "4.0 finance, 3.0 business, 2.0 tech, 1.5 education and health, 1.3 knowledge "
                "and fitness, 1.0 lifestyle and other, 0.8 gaming, entertainment and comedy, "
                "0.6 sports, music and film, and 0.5 TV, news and kids. These are model "
                "assumptions, not observed rates guaranteed to a channel."
            ),
            P(
                "The niche is inferred from the channel's topic categories and primary category, "
                "using the first matching category in the model's precedence order; Made for "
                "Kids channels use the kids factor."
            ),
            P(
                "The subscriber estimate uses a fitted power-law relationship with country-tier "
                "and Shorts adjustments. The two estimates are combined as a weighted geometric "
                "mean: view data has a 60% weight below 500,000 subscribers and 15% at or above "
                "that threshold. These weights and coefficients have not been independently "
                "validated against creator-reported AdSense earnings."
            ),
            cls="space-y-3 mb-8",
        ),
        Div(
            H2("What country means here", cls="text-xl font-semibold mb-3"),
            P(
                "Country means the channel's declared YouTube channel country "
                "(the channel's snippet.country field). It is owner-set channel information, "
                "not a measurement of where the channel's viewers live. ViralVibes does not "
                "have audience-geography data for this estimate. If the channel country is "
                "missing or unrecognised, the model uses its default country tier."
            ),
            cls="mb-8",
        ),
        Div(
            H2("How to read the range and confidence", cls="text-xl font-semibold mb-3"),
            P(
                "Every public earnings figure is shown as a scenario range from one-third "
                "to three times the model's central estimate. This is a broad sensitivity "
                "band, not a statistical confidence interval and not a promise of earnings."
            ),
            P(
                "Confidence labels are model diagnostics, not probabilities. The current "
                "model labels blended estimates below 500,000 subscribers as very low, "
                "blended estimates at or above that size as low, and estimates based on only "
                "one available model component as very low. Missing or questionable input "
                "data can also lower the label. Neither label means the estimate has been "
                "validated against the creator's actual earnings."
            ),
            P(
                "The current model version is ads-2026-11-17.3. Peer estimates can disagree "
                "with these scenarios, and the model may be materially wrong for an individual "
                "channel."
            ),
            A(
                "Browse creator profiles",
                href="/creators",
                cls="text-primary underline underline-offset-2",
            ),
            cls="space-y-3",
        ),
    )
