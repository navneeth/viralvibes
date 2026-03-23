#!/usr/bin/env python3
"""
Verify Python version consistency across all configuration files.
OSS-standard approach: workflows use python-version-file parameter.
"""

import re
import sys
from pathlib import Path
from typing import List, Tuple

# Expected Python version (read from .python-version)
EXPECTED_FULL_VERSION = "3.11.9"


def check_file(filepath: Path, pattern: str, expected: str) -> Tuple[bool, str]:
    """Check if file contains expected Python version."""
    if not filepath.exists():
        return False, f"❌ {filepath} does not exist"

    content = filepath.read_text(encoding="utf-8")
    matches = re.findall(pattern, content)

    if not matches:
        return False, f"❌ {filepath}: Pattern not found"

    for match in matches:
        if expected not in match:
            return False, f"❌ {filepath}: Found '{match}', expected '{expected}'"

    return True, f"✅ {filepath}: {matches[0]}"


def check_workflow_uses_version_file(filepath: Path) -> Tuple[bool, str]:
    """Check that workflow uses python-version-file parameter (OSS standard)."""
    if not filepath.exists():
        return False, f"❌ {filepath} does not exist"

    content = filepath.read_text(encoding="utf-8")

    # Check for python-version-file parameter (OSS standard)
    if "python-version-file:" in content:
        if ".python-version" in content:
            return True, f"✅ {filepath}: Uses python-version-file"
        else:
            return (
                False,
                f"❌ {filepath}: Has python-version-file but not '.python-version'",
            )

    # Check for hardcoded python-version (anti-pattern)
    if re.search(r"python-version:\s*['\"]?\d+\.\d+", content):
        return (
            False,
            f"❌ {filepath}: Uses hardcoded python-version (should use python-version-file)",
        )

    # Workflow doesn't set up Python (might be okay)
    return True, f"⚪ {filepath}: Doesn't set up Python"


def main():
    """Run all consistency checks."""
    repo_root = Path(__file__).parent.parent
    checks: List[Tuple[bool, str]] = []

    # 1. Check .python-version exists and has correct format
    python_version_file = repo_root / ".python-version"
    if python_version_file.exists():
        content = python_version_file.read_text(encoding="utf-8").strip()
        if content == EXPECTED_FULL_VERSION:
            checks.append((True, f"✅ .python-version: {content} (matches expected)"))
        else:
            checks.append(
                (
                    False,
                    f"❌ .python-version: {content} (expected {EXPECTED_FULL_VERSION})",
                )
            )
    else:
        checks.append((False, "❌ .python-version: File does not exist"))

    # 2. Check runtime.txt (Vercel)
    checks.append(
        check_file(
            repo_root / "runtime.txt",
            r"python-3\.\d+\.\d+",
            f"python-{EXPECTED_FULL_VERSION}",
        )
    )

    # 3. Check pyproject.toml has requires-python
    pyproject = repo_root / "pyproject.toml"
    if pyproject.exists():
        content = pyproject.read_text(encoding="utf-8")
        if 'requires-python = ">=3.11"' in content:
            checks.append((True, "✅ pyproject.toml: requires-python = '>=3.11'"))
        elif "requires-python" in content:
            checks.append(
                (False, "❌ pyproject.toml: requires-python exists but wrong version")
            )
        else:
            checks.append((False, "❌ pyproject.toml: Missing requires-python"))
    else:
        checks.append((False, "❌ pyproject.toml: File does not exist"))

    # 4. Check all workflow files use python-version-file (OSS standard)
    workflows_dir = repo_root / ".github" / "workflows"
    for workflow in workflows_dir.glob("*.yml"):
        if workflow.name in ["deployment-size-check.yml", "snyk.yml"]:
            continue  # Skip non-critical workflows

        checks.append(check_workflow_uses_version_file(workflow))

    # Print results
    print("=" * 70)
    print("PYTHON VERSION CONSISTENCY CHECK (OSS Standard)")
    print("=" * 70)
    print(f"Expected Version: {EXPECTED_FULL_VERSION}")
    print(f"Source of Truth: .python-version + pyproject.toml")
    print("=" * 70)

    all_passed = True
    for passed, message in checks:
        print(message)
        if not passed:
            all_passed = False

    print("=" * 70)

    if all_passed:
        print("✅ ALL CHECKS PASSED - Using OSS-standard approach!")
        print("\nConfiguration:")
        print("  • .python-version → Used by pyenv & GitHub Actions")
        print("  • pyproject.toml → Python packaging standard (PEP 621)")
        print("  • runtime.txt → Vercel deployment only")
        print("  • Workflows → Use python-version-file parameter")
        return 0
    else:
        print("❌ SOME CHECKS FAILED - Fix inconsistencies above")
        print("\nTo fix:")
        print("  1. Update .python-version to match expected version")
        print("  2. Update runtime.txt (Vercel)")
        print("  3. Ensure pyproject.toml has requires-python")
        print("  4. Update workflows to use python-version-file: '.python-version'")
        return 1


if __name__ == "__main__":
    sys.exit(main())
