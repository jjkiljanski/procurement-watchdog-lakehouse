"""Compatibility wrapper for the current single-day Silver entrypoint.

Use build_silver_day.py for the maintained day runner. This file is kept so
older ops scripts and docs continue to work while delegating to the current
implementation.
"""

from __future__ import annotations

from build_silver_day import main

if __name__ == "__main__":
    main()
