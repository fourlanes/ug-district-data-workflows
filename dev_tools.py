#!/usr/bin/env python3
"""
Development tools script for running code formatting and linting.

Usage:
    python dev_tools.py format    # Run isort and black
    python dev_tools.py lint      # Run flake8
    python dev_tools.py all       # Run both formatting and linting
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a shell command and return the result."""
    print(f"Running {description}...")
    try:
        # Use bash explicitly to support source command
        result = subprocess.run(
            f"bash -c '{cmd}'", shell=True, capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"✓ {description} completed successfully")
            if result.stdout.strip():
                print(result.stdout)
        else:
            print(f"✗ {description} failed:")
            if result.stderr.strip():
                print(result.stderr)
            if result.stdout.strip():
                print(result.stdout)
        return result.returncode == 0
    except Exception as e:
        print(f"✗ Error running {description}: {e}")
        return False


def format_code():
    """Run code formatting tools."""
    print("=" * 50)
    print("FORMATTING CODE")
    print("=" * 50)

    # Activate venv and run isort
    success1 = run_command(
        "source venv/bin/activate && isort tasks/ flows/ --check-only --diff",
        "isort import sorting check",
    )

    if not success1:
        run_command(
            "source venv/bin/activate && isort tasks/ flows/", "isort import sorting"
        )

    # Run black
    success2 = run_command(
        "source venv/bin/activate && black tasks/ flows/ --check --diff",
        "black code formatting check",
    )

    if not success2:
        run_command(
            "source venv/bin/activate && black tasks/ flows/", "black code formatting"
        )

    return True


def lint_code():
    """Run linting tools."""
    print("=" * 50)
    print("LINTING CODE")
    print("=" * 50)

    # Run flake8
    success = run_command(
        "source venv/bin/activate && flake8 tasks/ flows/ --exclude=test_*.py",
        "flake8 linting",
    )

    return success


def main():
    """Main function."""
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)

    action = sys.argv[1].lower()

    if action == "format":
        format_code()
    elif action == "lint":
        lint_code()
    elif action == "all":
        format_code()
        print()
        lint_code()
    else:
        print(f"Unknown action: {action}")
        print(__doc__)
        sys.exit(1)

    print("\n✓ Development tools completed!")


if __name__ == "__main__":
    main()
