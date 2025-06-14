"""
Reusable step components for consistent styling and behavior.
"""

from fasthtml.common import *
from monsterui.all import *
from typing import List, Tuple, Optional
from constants import STEPS_CLS, PLAYLIST_STEPS_CONFIG


def StepProgress(completed_steps: int = 0,
                 steps_config: List[Tuple[str, str,
                                          str]] = PLAYLIST_STEPS_CONFIG,
                 steps_cls: str = STEPS_CLS) -> Steps:
    """Create a step progress component.
    
    Args:
        completed_steps: Number of completed steps
        steps_config: List of (title, icon, description) tuples
        steps_cls: Steps container classes
    """
    steps = []
    for i, (title, icon, description) in enumerate(steps_config):
        if i < completed_steps:
            step_cls = StepT.success
        elif i == completed_steps:
            step_cls = StepT.primary
        else:
            step_cls = StepT.neutral

        steps.append(
            LiStep(title,
                   cls=step_cls,
                   data_content=icon,
                   description=description))

    return Steps(*steps, cls=steps_cls)


def StepConfig(title: str, icon: str,
               description: str) -> Tuple[str, str, str]:
    """Create a step configuration tuple.
    
    Args:
        title: Step title
        icon: Step icon
        description: Step description
    """
    return (title, icon, description)
