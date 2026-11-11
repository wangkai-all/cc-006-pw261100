#!/usr/bin/env python3
"""Root entry point for CLI subcommands (advance-watermark / show ...).

Example::

    python3 -I run_cli.py --db runtime/metrics.db show windows
    python3 -I run_cli.py --db runtime/metrics.db advance-watermark \\
        --to 2026-11-11T10:03:00Z
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from device_metrics.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
