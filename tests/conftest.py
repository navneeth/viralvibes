# tests/conftest.py
"""
Pytest configuration for ViralVibes tests.
This file automatically runs before any tests and sets up the Python path.
"""

import os
import sys

# Add the project root directory to Python path so tests can import modules
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
