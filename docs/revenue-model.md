# Monthly Ad Revenue Model

## Scope

`utils/revenue_model.py` estimates monthly YouTube advertising revenue only.
It excludes sponsorships, affiliates, merchandise, memberships, and profit.
Cards and profiles use the same `estimate_creator_ad_revenue` row adapter.
Estimates are computed at render time; no migration or worker resync is needed.

The previous model assumed 6% of lifetime views arrived every month and added
sponsorship/direct income to the headline. It reproduced MrBeast's displayed
approximately $229 million/month without establishing actual monthly views.

## Parameters and Updates

`utils/revenue_model.py` keeps the equations, labelled coefficient tables, and
frozen `RevenueParameters` defaults together. There is no JSON loading layer.
Read `estimate_monthly_views`, `estimate_subscriber_revenue`, `blend_revenue`,
then `estimate_monthly_ad_revenue` in that order. The database adapter is separate
from those modelling steps, but lives in the same file for discoverability.

The supplied calibration dataset was not available for independent validation.
FIT means the proposal reports that value as fitted, not that it was fitted or
validated in this repository. PRIOR and GUARD label unvalidated assumptions.
The estimator is not certified against actual creator earnings. Its smaller
numbers are not, by themselves, evidence of greater accuracy.

For a parameter update:

1. Change the annotated Python defaults/tables and increment `version`.
2. Record the source, measurement dates, channel scope and whether targets are
   actual ad earnings or third-party estimates in the change description.
3. Run `python -m pytest tests/test_revenue_model.py tests/test_creator_surface_provenance.py -o addopts=`.
4. Inspect changed reference estimates. Do not update golden values without
   reviewing why they changed. Evaluate small channels separately from mega channels.
5. Deploy the application. Defaults are ordinary Python values, not hot-reloaded settings.

For a candidate experiment, leave the baseline untouched:

```python
from dataclasses import replace
from utils.revenue_model import DEFAULT_PARAMETERS, RevenueProfile, estimate_monthly_ad_revenue

profile = RevenueProfile(
  subscribers=1_000_000, total_views=120_000_000,
  channel_age_years=10, country="US",
)
baseline = estimate_monthly_ad_revenue(profile)
candidate_parameters = replace(DEFAULT_PARAMETERS, version="candidate", base_rpm=2.0)
candidate = estimate_monthly_ad_revenue(profile, candidate_parameters)
print(baseline.views_estimate_usd, baseline.subscriber_estimate_usd, baseline.views_weight)
print(baseline.monthly_usd, candidate.monthly_usd)
```

Each result retains unrounded monthly-view proxy, effective RPM, structural and
subscriber dollar estimates, blend weight, tier, niche, known Shorts share,
diagnostic flags, and model version. Missing Shorts information stays `None` in
the evidence even though the arithmetic needs a zero-share working assumption.

Do not refit and evaluate on the same creator rows. Peer-tool agreement is not
equivalent to validation against creator-disclosed advertising income.

## Why Log Modelling Is Retained

The two logarithmic operations serve different purposes and both remain:

1. The subscriber component is a fitted power law:
   `log10(S) = -1.969 + 0.906 * log10(subscribers) + tier_adj - 0.587 * shorts_share`.
   Convert back to dollars with `S = 10 ** log10(S)`.
2. The combination is a weighted geometric mean:
   `log(R) = w * log(V) + (1 - w) * log(S)`.
   Convert back with `R = exp(log(R))`, equivalently `V ** w * S ** (1 - w)`.

This is appropriate when errors are assessed as multiplicative factors rather
than dollar differences. With `w=0.5`, V=$100 and S=$10,000 give R=$1,000,
not $5,050. The blend lies between the positive component estimates. The choice
of logs does not prove that the weights, proxies or fitted coefficients are good.

The proposal reports a 17-mega-channel leave-one-out comparison of view weights:
0 gave 2.37x median error, 0.15 about 2.15x, 0.5 about 3.74x and 1 about 4.21x.
Those unverified reported results explain the retained 0.15 weight; they do not
justify calling it optimal for all channels. The 0.60 weight below 500K subscribers
was explicitly structural speculation, with almost no small-channel labels.
The sharp 500K switch is retained for fidelity, not endorsed as a natural boundary.

## Proposal Review and Decision Record

| Element from the supplied proposal | Decision and reason |
| --- | --- |
| Separate views-times-RPM and subscriber estimates | Retained, with both returned for inspection. |
| Base-10 subscriber regression and log-space blend | Retained, with named functions and equation-level tests. |
| Age-based view anchor, ratio clipping, square-root adjustment | Retained. This is a conservative proxy that can still miss rapid growth or decline. |
| Reject deltas over 25x the lifetime run rate | Retained; corruption can dominate real creator signals otherwise. |
| Reject positive ratios below 0.25 (introductory comment) | Not implemented in the supplied code. We retain its actual clipping behavior and document the discrepancy. |
| Country tiers, niche multipliers, kids override, RPM guard | Retained with explicit proxy/prior labels; not asserted as verified audience or universal revenue limits. |
| Explicit niche hint | Retained as an optional caller-provided override. Unknown hints fall back to category matching; the database adapter does not invent a hint. |
| /3..*3 range and disagreement diagnostic | Retained as scenarios and flags, not a statistically calibrated confidence interval. |
| Zero revenue for zero/missing subscribers | Changed: hidden/missing subscribers can still use the view component. Zero known views/videos still returns zero. |
| Bare `views_30d` input from the database | Changed: normalize using the recorded observation window; unknown windows do not silently become 30 days. |
| Infer 50% Shorts share from a view surge | Deferred: a corrupt/growing delta does not identify format. Explicit known shares work; unknown shares stay flagged. This remains a major source of error for Shorts-heavy channels. |
| Profile fields for upload cadence and last-10 views | Not carried as unused ad-model inputs; do not imply they influence this estimate. |
| Sponsorship potential function | Deferred. It has assumptions separate from ad earnings and must not enter the headline. |
| "medium" confidence | Replaced by low/very-low until matched-period actual earnings validation is available. |

### Retired Deployed Model Context

The removed `estimate_monthly_revenue_v4` helper was not a logarithmic model.
It used the following arithmetic assumptions, preserved here for review history:

- Monthly views = 6% of lifetime views, described as an active-channel assumption.
- Assumed Shorts share = 80% if video_count/(subscribers+1) exceeded 0.01,
  otherwise 40%. Upload count does not establish a share of views by format.
- Country RPM pairs (long/Shorts USD per 1K): US 14.50/0.33, JP 11.20/0.14,
  GB 10.50/0.17, IN 1.10/0.01, BR 1.40/0.04, default 4.50/0.08.
- Niche multipliers: finance 2.2, tech/science-and-technology 1.7, gaming 0.7.
- Sponsorships = long-form views/1000 * $25 * niche multiplier, assuming those
  views all justified sponsored revenue rather than a bounded sponsored inventory.
- Direct monetisation = 25% of ad revenue above 500K subscribers, otherwise 5%.
  This entered the headline but was absent from the displayed component sum.
- Some callers replaced missing channel country with US. Removed in the new model.
- The old UI asserted kids content earns ~90% less and cannot receive sponsorships,
  while its calculation still used standard rates. Those unsupported universal
  claims are removed rather than silently carried into the new model.

The useful separation of view formats, country/niche effects, and revenue sources
is retained conceptually, with their assumptions explicit. The old comments
calling rates "accurate" or automatically API-derived were not evidence of
calibration. Git retains the original implementation; this note preserves why its
particular assumptions were retired instead of keeping a second callable model.

## Input Semantics

- The view-based component uses lifetime views divided by channel age in months.
  This is an activity proxy, not a measured current monthly run rate.
- `views_change_30d` is actually an observed difference. The adapter uses it only
  with `growth_window_days >= 7`, normalizing to 30 days before the ratio checks.
  Missing, negative, or nonfinite measurements are ignored. Implausibly large
  ratios are rejected; other positive ratios are clipped as in the supplied code.
- Hidden subscribers disable the subscriber component, not the view component.
- Country is channel location, not audience geography. Missing or unmapped
  locations use the supplied default tier, never an implicit US assumption.
- Unknown Shorts share remains unknown (zero-share calculation assumption with
  a diagnostic flag); the unvalidated surge-based Shorts inference is not used.
- Topic categories accept JSON arrays, Python lists or legacy comma-separated
  text; labels and Wikipedia URLs are normalized before niche matching.
- Known zero views/videos produce zero. Insufficient data produces Unavailable.
- The supplied 500K blend boundary is retained. Smaller-channel extrapolations
  and single-component fallbacks are marked very low confidence; other estimates
  remain low confidence. No estimate is labeled high confidence.
- The estimate / 3 to estimate * 3 band is a scenario range, not a confidence interval.

## Public Checks (2026-11-17)

The live directory and text-search endpoint were fetched without authentication.
Canonical @handle profile links returned sign-in content in this check. Input
counts below are rounded display values, not raw database snapshots. No missing
measurement windows, Shorts shares or undisclosed audience data were fabricated.

| Creator | Existing live headline/month | New local ad-only estimate/month |
| --- | ---: | ---: |
| MrBeast | $229.3M | ~$899K |
| T-Series | $345.3M | ~$187K |
| Cocomelon | $355.1M | ~$406K |
| Graham Stephan | $2.2M | ~$14.7K |
| Ali Abdaal | $738.4K | ~$13.8K |
| Erika Kullberg | $518.9K | ~$6.7K |

Sources: [directory](https://www.viralvibes.fyi/creators),
[Graham](https://www.viralvibes.fyi/creators?search=Graham%20Stephan),
[Ali](https://www.viralvibes.fyi/creators?search=Ali%20Abdaal),
[Erika](https://www.viralvibes.fyi/creators?search=Erika%20Kullberg).
These are behavior comparisons, not accuracy scores.

### Creator Disclosures and Media Context

- [Entrepreneur, May 24, 2024](https://www.entrepreneur.com/business-news/heres-how-much-popular-influencer-erika-kullberg-makes/474686)
  reports Erika Kullberg's disclosure of more than $353K from YouTube over about
  five years, before taxes and excluding sponsorships. It distinguishes a 4.1M-view
  Short earning $106.85 from a 3.8M-view long video earning about $45K.
- [Erika's own disclosure](https://erika.com/how-much-social-media-pays/), using
  May 2024 figures, lists $353,135.99 received from YouTube and an annualized
  $88,250 heading (~$7,354/month). This is historical, not a verified November
  2026 run rate. The new ~$6.7K result is not proof of predictive accuracy.
- [Ali Abdaal, April 23, 2021](https://aliabdaal.com/youtube/triple-your-youtube-ad-revenue/)
  states non-ad income represented about 90% of his 2020 income (about GBP900K).
  He explains content, watch time and audience location effects. This supports
  separating revenue types but is not a current monthly ad-earnings label.
- [CNBC, September 9, 2021](https://www.cnbc.com/2021/09/09/how-graham-stephan-makes-6-million-a-year-from-youtube.html)
  has an indexed excerpt reporting $1.42M ad revenue across Graham Stephan's
  various channels by early July 2021. The article body was unavailable during
  this check. This historical multi-channel figure cannot validate a current
  single-channel estimate; it highlights potential underestimation by the model.

The supplied demo (524M subscribers, 146.7B views, 14 years, US, Entertainment,
and a genuinely 30-day 1.7B delta) produces $941,901.72/month. It differs from
the table's MrBeast example because the live rounded subscriber count was 526M
and no actual observation window was available from the public card.

## Deployment Checks

Migration 075 already defines `growth_window_days`; ensure it is present in the
target database before deployment, since the explicit listing projection now
includes it. Coefficients ship with `utils/revenue_model.py`; no separate data
file needs bundling.
After deploying, check directory cards and a signed-in profile for ad-only labels,
matching estimates, model version, missing-data behavior and scenario range.