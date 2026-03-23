#!/usr/bin/env python3
"""
Verify Python version consistency across all configuration files.

Source of truth: .python-version at repo root (used by pyenv & GitHub Actions).
Script location:  scripts/verify_python_version.py

Checks:
  - pyproject.toml    requires-python
  - Dockerfile        FROM python:X.Y.Z
  - fly.toml          [build] or [env] python version
  - render.yaml       pythonVersion field
  - runtime.txt       Vercel (optional — not currently in repo)
  - .github/workflows python-version-file usage (optional — skipped if absent)
"""

import re
import sys
from pathlib import Path
from typing import List, Tuple

# scripts/ -> repo root
REPO_ROOT = Path(__file__).parent.parent


# ── Source of truth ───────────────────────────────────────────────────────────


def get_expected_version() -> str:
    """Read from .python-version — the single source of truth."""
    version_file = REPO_ROOT / ".python-version"
    if not version_file.exists():
        print("❌ .python-version not found — cannot determine expected version")
        sys.exit(1)
    return version_file.read_text(encoding="utf-8").strip()


# ── Individual checks ─────────────────────────────────────────────────────────


def check_pyproject(expected_major_minor: str) -> Tuple[bool, str]:
    """pyproject.toml: requires-python = ">=X.Y" """
    path = REPO_ROOT / "pyproject.toml"
    if not path.exists():
        return False, "❌ pyproject.toml: File not found"
    content = path.read_text(encoding="utf-8")
    expected = f'requires-python = ">={expected_major_minor}"'
    if expected in content:
        return True, f"✅ pyproject.toml: requires-python = '>={expected_major_minor}'"
    if "requires-python" in content:
        match = re.search(r'requires-python\s*=\s*"([^"]+)"', content)
        found = match.group(1) if match else "unknown"
        return (
            False,
            f"❌ pyproject.toml: requires-python = '{found}' (expected '>={expected_major_minor}')",
        )
    return False, "❌ pyproject.toml: Missing requires-python"


def check_dockerfile(expected_full: str) -> Tuple[bool, str]:
    """Dockerfile: FROM python:X.Y.Z[-...]"""
    path = REPO_ROOT / "Dockerfile"
    if not path.exists():
        return True, "⚪ Dockerfile: Not present"
    content = path.read_text(encoding="utf-8")
    matches = re.findall(r"FROM python:([\d.]+)", content)
    if not matches:
        return True, "⚪ Dockerfile: No FROM python: line found"
    for match in matches:
        if not expected_full.startswith(match) and match != expected_full:
            return False, f"❌ Dockerfile: FROM python:{match} (expected {expected_full})"
    return True, f"✅ Dockerfile: FROM python:{matches[0]}"


def check_fly_toml(expected_full: str) -> Tuple[bool, str]:
    """fly.toml: checks for hardcoded Python version strings."""
    path = REPO_ROOT / "fly.toml"
    if not path.exists():
        return True, "⚪ fly.toml: Not present"
    content = path.read_text(encoding="utf-8")
    matches = re.findall(r'python[_-]?version\s*=\s*["\']?([\d.]+)', content, re.IGNORECASE)
    if not matches:
        return True, "⚪ fly.toml: No Python version pinned"
    for match in matches:
        if match != expected_full:
            return False, f"❌ fly.toml: Python version '{match}' (expected {expected_full})"
    return True, f"✅ fly.toml: Python version {matches[0]}"


def check_render_yaml(expected_major_minor: str) -> Tuple[bool, str]:
    """render.yaml: pythonVersion field."""
    path = REPO_ROOT / "render.yaml"
    if not path.exists():
        return True, "⚪ render.yaml: Not present"
    content = path.read_text(encoding="utf-8")
    matches = re.findall(r"pythonVersion:\s*['\"]?([\d.]+)", content)
    if not matches:
        return True, "⚪ render.yaml: No pythonVersion field"
    for match in matches:
        if not match.startswith(expected_major_minor):
            return (
                False,
                f"❌ render.yaml: pythonVersion '{match}' (expected {expected_major_minor}.x)",
            )
    return True, f"✅ render.yaml: pythonVersion {matches[0]}"


def check_runtime_txt(expected_full: str) -> Tuple[bool, str]:
    """runtime.txt: Vercel convention (optional)."""
    path = REPO_ROOT / "runtime.txt"
    if not path.exists():
        return True, "⚪ runtime.txt: Not present (optional)"
    content = path.read_text(encoding="utf-8").strip()
    expected = f"python-{expected_full}"
    if content == expected:
        return True, f"✅ runtime.txt: {content}"
    return False, f"❌ runtime.txt: '{content}' (expected '{expected}')"


def check_workflows() -> List[Tuple[bool, str]]:
    """GitHub Actions: workflows should use python-version-file, not hardcoded versions."""
    workflows_dir = REPO_ROOT / ".github" / "workflows"
    if not workflows_dir.exists():
        return [(True, "⚪ .github/workflows: Directory not present")]

    skip = {"deployment-size-check.yml", "snyk.yml"}
    results = []
    for workflow in sorted(workflows_dir.glob("*.yml")):
        if workflow.name in skip:
            continue
        content = workflow.read_text(encoding="utf-8")
        if "python-version-file:" in content and ".python-version" in content:
            results.append((True, f"✅ {workflow.name}: Uses python-version-file"))
        elif re.search(r"python-version:\s*['\"]?\d+\.\d+", content):
            results.append(
                (False, f"❌ {workflow.name}: Hardcoded python-version (use python-version-file)")
            )
        else:
            results.append((True, f"⚪ {workflow.name}: Doesn't set up Python"))
    return results


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    expected_full = get_expected_version()
    major_minor = ".".join(expected_full.split(".")[:2])  # "3.12.0" -> "3.12"

    checks: List[Tuple[bool, str]] = [
        (True, f"✅ .python-version: {expected_full} (source of truth)"),
        check_pyproject(major_minor),
        check_dockerfile(expected_full),
        check_fly_toml(expected_full),
        check_render_yaml(major_minor),
        check_runtime_txt(expected_full),
        *check_workflows(),
    ]

    print("=" * 70)
    print("PYTHON VERSION CONSISTENCY CHECK")
    print("=" * 70)
    print(f"Expected : {expected_full}  (from .python-version)")
    print(f"Repo root: {REPO_ROOT}")
    print("=" * 70)

    all_passed = all(passed for passed, _ in checks)
    for passed, message in checks:
        print(message)

    print("=" * 70)
    if all_passed:
        print("✅ ALL CHECKS PASSED")
    else:
        print("❌ SOME CHECKS FAILED — update the files above to match .python-version")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
