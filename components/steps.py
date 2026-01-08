"""
Step progress components for multi-step workflows.

Provides both simple step progress indicators and complete step wizards
with consistent styling and behavior.
"""

from fasthtml.common import *
from monsterui.all import *
from typing import List, Tuple, Optional
from constants import STEPS_CLS, PLAYLIST_STEPS_CONFIG


def StepProgress(
    completed_steps: int = 0,
    steps_config: List[Tuple[str, str, str]] = PLAYLIST_STEPS_CONFIG,
    steps_cls: str = STEPS_CLS,
) -> Steps:
    """Create a step progress component.

    Args:
        completed_steps: Number of completed steps
        steps_config: List of (title, icon, description) tuples
        steps_cls: CSS classes for the Steps container
    Returns:
        Steps component with styled progress indicators
    """
    steps = []
    for i, (title, icon, description) in enumerate(steps_config):
        # determine step state
        if i < completed_steps:
            step_cls = StepT.success
        elif i == completed_steps:
            step_cls = StepT.primary
        else:
            step_cls = StepT.neutral

        steps.append(
            LiStep(title, cls=step_cls, data_content=icon, description=description)
        )

    return Steps(*steps, cls=steps_cls)


def StepConfig(title: str, icon: str, description: str) -> Tuple[str, str, str]:
    """Create a step configuration tuple.

    Args:
        title: Step title
        icon: Step icon
        description: Step description
    """
    return (title, icon, description)


# ============================================================
# HIGH-LEVEL: Complete step wizard (if needed)
# ============================================================


def step_wizard(
    current_step: int,
    steps: List[Tuple[str, str, str]],
    show_navigation: bool = True,
) -> Div:
    """Create a full step wizard with progress and navigation.

    Args:
        current_step: Current active step (0-indexed)
        steps: List of step configurations
        show_navigation: Whether to show prev/next buttons

    Returns:
        Complete wizard UI

    Example:
        >>> step_wizard(
        ...     current_step=1,
        ...     steps=PLAYLIST_STEPS_CONFIG,
        ...     show_navigation=True
        ... )
    """
    return Div(
        # Progress indicator
        step_progress(completed_steps=current_step, steps=steps),
        # Current step content area
        Div(
            H3(steps[current_step][0], cls="text-xl font-semibold mt-8"),
            P(steps[current_step][2], cls="text-gray-600 mt-2"),
            cls="step-content",
        ),
        # Navigation (if enabled)
        (
            Div(
                Button(
                    "← Previous",
                    disabled=current_step == 0,
                    cls="btn btn-secondary",
                ),
                Button(
                    "Next →",
                    disabled=current_step == len(steps) - 1,
                    cls="btn btn-primary",
                ),
                cls="flex justify-between mt-8",
            )
            if show_navigation
            else None
        ),
        cls="step-wizard",
    )


# ============================================================
# PascalCase ALIASES: For backwards compatibility
# ============================================================

step_progress = StepProgress
step_config = StepConfig
