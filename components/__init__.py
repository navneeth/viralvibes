# components/__init__.py
# Re-exports for clean imports - maintains backward compatibility
# All UI components are now in ui_components.py module

from components.base import (
    # Base structural helpers
    DivCentered,
    DivFullySpaced,
    DivHStacked,
    styled_div,
)
from components.buttons import (
    # Button components
    FeaturePill,
    ViralVibesButton,
    cta,
    paste_button,
    progress_meter,
    small_badge,
)
from components.cards import (
    # Card components
    AnalysisFormCard,
    BenefitsCard,
    FeaturesCard,
    HeaderCard,
    HomepageAccordion,
    MetricCard,
    NewsletterCard,
    PlaylistPreviewCard,
    PlaylistSteps,
    SamplePlaylistButtons,
    SummaryStatsCard,
    accordion,
    benefit,
    create_info_card,
    create_tabs,
    faq_item,
)

# Section Components
from components.sections import (
    ExploreGridSection,
    FooterLinkGroup,
    SectionDivider,
    faq_section,
    features_section,
    footer,
    # Full-page section builders
    hero_section,
    how_it_works_section,
    section_header,
    section_wrapper,
    testimonial_card,
    testimonials_section,
)
from components.steps import StepProgress

# Table Cell Renderers
from components.tables import (
    # Table cell renderers
    VideoExtremesSection,
    category_emoji_cell,
    number_cell,
    thumbnail_cell,
    title_cell,
)
from ui_components import (
    # Dashboard Components
    AnalyticsDashboardSection,
    AnalyticsHeader,
    CachedResultsBanner,
    # Layout Components
    PlaylistMetricsOverview,
)

# Import modal components
from .modals import ExportModal, Modal, ShareModal
from .navigation import NavComponent

__all__ = [
    # UI Helpers
    "cta",
    "small_badge",
    "progress_meter",
    "maxrem",
    # Content Components
    "benefit",
    "FeaturePill",
    "accordion",
    "faq_item",
    # Card Components
    "HeaderCard",
    "FeaturesCard",
    "BenefitsCard",
    "NewsletterCard",
    "SummaryStatsCard",
    # Form Components
    "AnalysisFormCard",
    "paste_button",
    "SamplePlaylistButtons",
    # Complex Components
    "create_tabs",
    "create_info_card",
    # Section Components
    "section_wrapper",
    "section_header",
    "arrow",
    "carousel",
    "ExploreGridSection",
    "SectionDivider",
    # Dashboard Components
    "AnalyticsDashboardSection",
    "AnalyticsHeader",
    "PlaylistMetricsOverview",
    "MetricCard",
    "VideoExtremesSection",
    # Table Cell Renderers
    "thumbnail_cell",
    "title_cell",
    "category_emoji_cell",
    "number_cell",
    # Steps Component
    "PlaylistSteps",
    # Layout Components
    "HomepageAccordion",
    "CachedResultsBanner",
    "PlaylistPreviewCard",
    "ViralVibesButton",
    # Base structural helpers
    "DivCentered",
    "DivHStacked",
    "DivFullySpaced",
    "styled_div",
    # Full-page section builders
    "hero_section",
    "how_it_works_section",
    "features_section",
    "testimonials_section",
    "testimonial_card",
    "faq_section",
    "footer",
    "FooterLinkGroup",
    "NavComponent",
    # Modal exports
    "Modal",
    "ShareModal",
    "ExportModal",
]
