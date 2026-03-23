#!/usr/bin/env python3
"""
Verify Python version consistency across all configuration files.
Ensures venv, CI/CD, tests, and Vercel all use the same Python version.
"""

import re
import sys
from pathlib import Path
from typing import List, Tuple

# Expected Python version
EXPECTED_VERSION = "3.12"
EXPECTED_FULL_VERSION = "3.12.8"


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


def main():
    """Run all consistency checks."""
    repo_root = Path(__file__).parent.parent
    checks: List[Tuple[bool, str]] = []

    # 1. Check .python-version
    checks.append(
        check_file(repo_root / ".python-version", r"3\.\d+\.\d+", EXPECTED_FULL_VERSION)
    )

    # 2. Check runtime.txt (Vercel)
    checks.append(
        check_file(
            repo_root / "runtime.txt",
            r"python-3\.\d+\.\d+",
            f"python-{EXPECTED_FULL_VERSION}",
        )
    )

    # 3. Check requirements.txt comment
    checks.append(
        check_file(
            repo_root / "requirements.txt",
            r"Python Version: 3\.\d+\.\d+",
            EXPECTED_FULL_VERSION,
        )
    )

    # 4. Check all workflow files
    workflows_dir = repo_root / ".github" / "workflows"
    for workflow in workflows_dir.glob("*.yml"):
        if workflow.name in ["deployment-size-check.yml", "snyk.yml"]:
            continue  # Skip non-critical workflows

        checks.append(
            check_file(workflow, r"python-version:\s*['\"]?(3\.\d+)", EXPECTED_VERSION)
        )

    # Print results
    print("=" * 70)
    print("PYTHON VERSION CONSISTENCY CHECK")
    print("=" * 70)
    print(f"Expected Version: {EXPECTED_VERSION} ({EXPECTED_FULL_VERSION})")
    print("=" * 70)

    all_passed = True
    for passed, message in checks:
        print(message)
        if not passed:
            all_passed = False

    print("=" * 70)

    if all_passed:
        print("✅ ALL CHECKS PASSED - Python versions are consistent!")
        return 0
    else:
        print("❌ SOME CHECKS FAILED - Fix inconsistencies above")
        print("\nTo fix:")
        print("  1. Update PYTHON_VERSION.py")
        print("  2. Update .python-version")
        print("  3. Update runtime.txt")
        print("  4. Update requirements.txt header")
        print("  5. Update all .github/workflows/*.yml files")
        return 1


if __name__ == "__main__":
    sys.exit(main())
