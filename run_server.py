#!/usr/bin/env python3
"""Root entry point: start the tailer + HTTP dashboard.

Intended to run in isolated mode (untrusted input data)::

    python3 -I run_server.py --db runtime/metrics.db --input runtime/events.jsonl
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from device_metrics.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(["serve", *sys.argv[1:]]))
